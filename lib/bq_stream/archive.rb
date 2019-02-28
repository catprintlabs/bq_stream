module BqStream
  module Archive
    # Gathers record between earliest BigQuery record and given back date 
    def old_records_full_archive(back_date, override_dataset = nil, verify_oldest = false)
      log(:info, "#{Time.now}: ***** Start Streamline Process *****")
      initialize_bq(override_dataset)
      
      log(:info, "#{Time.now}: ***** Start Update Oldest Record Rows *****")
      
      update_oldest_records
      verify_oldest_records if verify_oldest

      # Reset all rows archived status to false to run through all tables
      OldestRecord.where.not(table_name: '! revision !').update_all(archived: false)

      log(:info, "#{Time.now}: ***** End Update Oldest Record Rows *****")

      process_archiving_tables(back_date)

      log(:info, "#{Time.now}: ***** End Streamline Process *****")
    end

    # Sets up BigQuery and means to write records to it  
    def initialize_bq(override_dataset)
      # Create BigQuery client connection
      create_bq_writer(override_dataset)

      # Create dataset if not present in BigQuery
      @bq_writer.create_dataset(@bq_writer.dataset) unless @bq_writer.datasets_formatted.include?(@bq_writer.dataset) || true # TODO

      # Create table in dataset if not present in BigQuery
      unless @bq_writer.tables_formatted.include?(bq_table_name)
        bq_table_schema = { table_name:   { type: 'STRING', mode: 'REQUIRED' },
                            record_id:    { type: 'INTEGER', mode: 'REQUIRED' },
                            attr:         { type: 'STRING', mode: 'NULLABLE' },
                            new_value:    { type: 'STRING', mode: 'NULLABLE' },
                            updated_at:   { type: 'TIMESTAMP', mode: 'REQUIRED' } }
        @bq_writer.create_table(bq_table_name, bq_table_schema)
      end
    end

    # Check to make sure all attributes that are in BigQuery and bq_attributes are represented in OldestRecord table
    def update_oldest_records
      old_records = @bq_writer.query('SELECT table_name, attr FROM '\
                                      "[#{project_id}:#{ @bq_writer.dataset}.#{bq_table_name}] "\
                                      'GROUP BY table_name, attr')

      if old_records['rows']
        old_records['rows'].each do |r|
          table = r['f'][0]['v']
          trait = r['f'][1]['v']
          if !trait.nil? && bq_attributes[table] && bq_attributes[table].include?(trait.to_sym) 
            OldestRecord.find_or_create_by(table_name: table, attr: trait)
          else
            if !trait.nil? && bq_attributes[table]
              log(:info, "#{Time.now}: No longer tracking: #{table}: #{trait}")
            end
          end
        end
      end
    end

    # Add and Remove bq_attributes based on current bq_attributes
    def verify_oldest_records
      log(:info, "#{Time.now}: ***** Start Verifying Oldest Records *****")
      current_deploy =
        if (`cat #{File.expand_path ''}/REVISION`).blank?
          'None'
        else
          `cat #{File.expand_path ''}/REVISION`
        end
      bq_attributes.each do |k, v|
        # add any records to oldest_records that are new (Or more simply make sure that that there is a record using find_by_or_create)
        v.each do |bqa|
          OldestRecord.find_or_create_by(table_name: k, attr: bqa)
        end
        # delete any records that are not in bq_attributes
        OldestRecord.where(table_name: k).each do |rec|
          rec.destroy unless v.include?(rec.attr.to_sym)
        end
      end
      update_revision = OldestRecord.find_or_create_by(table_name: '! revision !')
      update_revision.update(attr: current_deploy, archived: true)
      log(:info, "#{Time.now}: ***** End Verifying Oldest Records *****")
    end

    # Run through all tables one at a time
    def process_archiving_tables(back_date)
      # Initialize an empty buffer for records that will be sent to BigQuery
      @buffer = []

      # Stop processing when all tables are archived
      until OldestRecord.where(archived: false).empty? && @buffer.empty?
        OldestRecord.table_names.each do |table|
          log(:info, "#{Time.now}: ***** Cycle Table: #{table} *****")
          @oldest_attr_recs = OldestRecord.where(table_name: table)

          assign_earliest_record_id(table)
          assign_back_date_id(table.constantize, back_date)
          
          # Continue to work on one table until all records to back date are sent to BigQuery
          until OldestRecord.where(table_name: table, archived: false).empty?
            gather_records_for_buffer(table)

            # Only write to BigQuery if we have rows set in the buffer
            write_buffer_to_bq(table) unless @buffer.empty?
          end
        end
      end
    end

    # Set id of the earliest record in BigQuery or first record (desc) from the database
    def assign_earliest_record_id(table)
      # See if there are any records for given table in BigQuery
      record_id_query = @bq_writer.query('SELECT table_name, attr, min(record_id) as earliest_record_id '\
                                          "FROM [#{project_id}:#{@bq_writer.dataset}.#{bq_table_name}] "\
                                          "WHERE table_name = '#{table}' AND attr = 'id' "\
                                          'GROUP BY table_name, attr')
    
      # Set earliest record id based on rows in BigQuery or the latest record in the database
      @earliest_record_id = 
        if record_id_query['rows']
          record_id_query['rows'].first['f'].last['v'].to_i
        else
          table.constantize.unscoped.order(id: :desc).limit(1).first.try(:id)
        end
      
      log(:info, "#{Time.now}: ***** earliest_record_id: #{@earliest_record_id} *****")
    end

    # Set id of the first record from back date from the database
    def assign_back_date_id(table_class, back_date)
      @back_date_id =
        # See if the given table has a created_at column
        if table_class.column_names.include?('created_at')
          table_class.unscoped.order(id: :asc).where('created_at >= ?', back_date).limit(1).first.try(:id)
        else
          # Grab very first id if there is no created_at column
          table_class.unscoped.order(id: :asc).limit(1).first.try(:id)
        end
      log(:info, "#{Time.now}: ***** back_date_record_id: #{@back_date_id} *****")
    end

    def gather_records_for_buffer(table)
      # Grab records between earliest written id and back date idea
      # limited to the number of records we can grab, keeping under 10_000 rows
      @next_batch = table.constantize.unscoped.order(id: :desc).where('id > ? AND id <= ?', @back_date_id, @earliest_record_id).limit(10_000 / (@oldest_attr_recs.count.zero? ? 1 : @oldest_attr_recs.count)) rescue []
      log(:info, "#{Time.now}: ***** Next Batch Count for #{table}: #{@next_batch.count} *****")

      if @next_batch.empty?
        # If there are no records in range mark the table's attributes as archived
        OldestRecord.where(table_name: table).each { |row| row.update(archived: true) }
      else
        # Write rows from records for BigQuery and place them into the buffer 
        @oldest_attr_recs.uniq.each do |oldest_attr_rec|
          @next_batch.each do |record|
            new_val = record[oldest_attr_rec.attr] && table.constantize.type_for_attribute(oldest_attr_rec.attr).type == :datetime ? record[oldest_attr_rec.attr].in_time_zone(timezone) : record[oldest_attr_rec.attr].to_s
            @buffer << { table_name: table,
                        record_id: record.id,
                        attr: oldest_attr_rec.attr,
                        new_value: new_val,
                        updated_at: record.try(:created_at) || Time.now } # Using Time.now if no created_at (shows when put into BigQuery)
          end
        end
      end
    end

    def write_buffer_to_bq(table)
      log(:info, "#{Time.now}: ***** Start data pack and insert *****")
      # Create data object for BigQuery insert
      data = @buffer.collect do |i|
        new_val = encode_value(i[:new_value]) rescue nil
        { table_name: i[:table_name], record_id: i[:record_id], attr: i[:attr],
          new_value: new_val ? new_val : i[:new_value], updated_at: i[:updated_at] }
      end
      log(:info, "#{Time.now}: ***** Insert #{@buffer.count} Records for #{table} to BigQuery *****")
      # Insert rows into BigQuery in one call
      @bq_writer.insert(bq_table_name, data) unless data.empty?
      # Lower earliest written id by one to get the next record to be written
      @earliest_record_id = @next_batch.map(&:id).min - 1
      log(:info, "#{Time.now}: ***** updated earliest_record_id: #{@earliest_record_id} *****")
      # Clear the buffer for next cycle through the table
      @buffer = []
      log(:info, "#{Time.now}: ***** End data pack and insert *****")
    end
  end
end
