module BqStream
  module Archive
    def id_streamline_archive(back_date, dataset = BqStream.dataset)
      BqStream.log(:info, "#{Time.now}: ***** Start Streamline Process *****")

      # Initialize an empty buffer for records that will be sent to BigQuery
      buffer = []

      # Create BigQuery client connection
      require 'big_query'
      opts = {}
      opts['client_id']     = BqStream.client_id
      opts['service_email'] = BqStream.service_email
      opts['key']           = BqStream.key
      opts['project_id']    = BqStream.project_id
      opts['dataset']       = dataset
      @bq_archiver = BigQuery::Client.new(opts)

      # Create dataset if not present in BigQuery
      @bq_archiver.create_dataset(dataset) unless @bq_archiver.datasets_formatted.include?(dataset)

      # Create table in dataset if not present in BigQuery
      unless @bq_archiver.tables_formatted.include?(BqStream.bq_table_name)
        bq_table_schema = { table_name:   { type: 'STRING', mode: 'REQUIRED' },
                            record_id:    { type: 'INTEGER', mode: 'REQUIRED' },
                            attr:         { type: 'STRING', mode: 'NULLABLE' },
                            new_value:    { type: 'STRING', mode: 'NULLABLE' },
                            updated_at:   { type: 'TIMESTAMP', mode: 'REQUIRED' } }
        @bq_archiver.create_table(BqStream.bq_table_name, bq_table_schema)
      end

      BqStream.log(:info, "#{Time.now}: ***** Start Update Oldest Record Rows *****")

      # Check to make sure all attributes in BigQuery are represented in OldestRecord table
      old_records = @bq_archiver.query('SELECT table_name, attr FROM '\
                                      "[#{BqStream.project_id}:#{dataset}.#{BqStream.bq_table_name}] "\
                                      'GROUP BY table_name, attr')
      if old_records['rows']
        old_records['rows'].each do |r|
          table = r['f'][0]['v']
          trait = r['f'][1]['v']
          unless trait.nil?
            BqStream::OldestRecord.find_or_create_by(table_name: table, attr: trait)
          end
        end
      end

      # Reset all rows archived status to false to run through all tables
      BqStream::OldestRecord.where.not(table_name: '! revision !').update_all(archived: false)
      BqStream.log(:info, "#{Time.now}: ***** End Update Oldest Record Rows *****")

      # Stop processing when all tables are archived
      until BqStream::OldestRecord.where(archived: false).empty? && buffer.empty?
        BqStream::OldestRecord.table_names.each do |table|
          table_class = table.constantize

          BqStream.log(:info, "#{Time.now}: ***** Cycle Table: #{table} *****")
          oldest_attr_recs = BqStream::OldestRecord.where(table_name: table)

          # See if there are any records for given table in BigQuery
          record_id_query = @bq_archiver.query('SELECT table_name, attr, min(record_id) as earliest_record_id '\
                                                "FROM [#{BqStream.project_id}:#{dataset}.#{BqStream.bq_table_name}] "\
                                                "WHERE table_name = '#{table}' AND attr = 'id' "\
                                                'GROUP BY table_name, attr')

          # Set earliest record id based on rows in BigQuery or the latest record in the database
          if record_id_query['rows']
            earliest_record_id = record_id_query['rows'].first['f'].last['v'].to_i
          else
            earliest_record_id = table_class.unscoped.order(id: :desc).limit(1).first.try(:id)
          end
          BqStream.log(:info, "#{Time.now}: ***** earliest_record_id: #{earliest_record_id} *****")

          # Set id of the first record from back date from the database
          back_date_id = table_class.unscoped.order(id: :asc).where('created_at >= ?', back_date).limit(1).first.try(:id)
          BqStream.log(:info, "#{Time.now}: ***** back_date_record_id: #{back_date_id} *****")

          # Continue to work on one table until all records to back date are sent to BigQuery
          until BqStream::OldestRecord.where(table_name: table, archived: false).empty?
            # Grab records between ealiest written id and back date idea
            # limited to the number of records we can grab, keeping under 10_000 rows
            # MUST use find_by_sql to avoid any default scopes (looking at you JobLogRecord)
            next_batch = table_class.unscoped.order(id: :asc).where('id >= ? AND id <= ?', back_date_id, earliest_record_id).limit(10_000 / (oldest_attr_recs.count.zero? ? 1 : oldest_attr_recs.count)) rescue []

            BqStream.log(:info, "#{Time.now}: ***** Next Batch Count for #{table}: #{next_batch.count} *****")

            if next_batch.empty?
              # If there are no records in range mark the table's attributes as archived
              BqStream::OldestRecord.where(table_name: table).each { |row| row.update(archived: true) }
            else
              # Write rows from records for BigQuery and place them into the buffer 
              oldest_attr_recs.uniq.each do |oldest_attr_rec|
                next_batch.each do |record|
                  new_val = record[oldest_attr_rec.attr] && table.constantize.type_for_attribute(oldest_attr_rec.attr).type == :datetime ? record[oldest_attr_rec.attr].in_time_zone(BqStream.timezone) : record[oldest_attr_rec.attr].to_s
                  buffer << { table_name: table,
                              record_id: record.id,
                              attr: oldest_attr_rec.attr,
                              new_value: new_val,
                              updated_at: record.created_at }
                end
              end
            end

            # Only write to BigQuery if we have rows set in the buffer
            unless buffer.empty?
              BqStream.log(:info, "#{Time.now}: ***** Start data pack and insert *****")
              # Create data object for BigQuery insert
              data = buffer.collect do |i|
                new_val = encode_value(i[:new_value]) rescue nil
                { table_name: i[:table_name], record_id: i[:record_id], attr: i[:attr],
                  new_value: new_val ? new_val : i[:new_value], updated_at: i[:updated_at] }
              end
              BqStream.log(:info, "#{Time.now}: ***** Insert #{buffer.count} Records for #{table} to BigQuery *****")
              # Insert rows into BigQuery in one call
              @bq_archiver.insert(BqStream.bq_table_name, data) unless data.empty?
              # Lower earliest written id by one to get the next record to be written
              earliest_record_id = next_batch.map(&:id).min - 1
              BqStream.log(:info, "#{Time.now}: ***** updated earliest_record_id: #{earliest_record_id} *****")
              # Clear the buffer for next cycle through the table
              buffer = []
              BqStream.log(:info, "#{Time.now}: ***** End data pack and insert *****")
            end
          end
        end
      end
      BqStream.log(:info, "#{Time.now}: ***** End Streamline Process *****")
    end
  end
end
