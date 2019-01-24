module BqStream
  module Archive
    def streamline_archive(back_date, dataset = BqStream.dataset)
      BqStream.log(:info, "#{Time.now}: ***** Start Streamline Process *****")

      buffer = []

      require 'big_query'
      opts = {}
      opts['client_id']     = BqStream.client_id
      opts['service_email'] = BqStream.service_email
      opts['key']           = BqStream.key
      opts['project_id']    = BqStream.project_id
      opts['dataset']       = dataset
      @bq_archiver = BigQuery::Client.new(opts)

      unless @bq_archiver.datasets_formatted.include?(dataset)
        @bq_archiver.create_dataset(dataset)
      end

      unless @bq_archiver.tables_formatted.include?(BqStream.bq_table_name)
        bq_table_schema = { table_name:   { type: 'STRING', mode: 'REQUIRED' },
                            record_id:    { type: 'INTEGER', mode: 'REQUIRED' },
                            attr:         { type: 'STRING', mode: 'NULLABLE' },
                            new_value:    { type: 'STRING', mode: 'NULLABLE' },
                            updated_at:   { type: 'TIMESTAMP', mode: 'REQUIRED' } }
        @bq_archiver.create_table(BqStream.bq_table_name, bq_table_schema)
      end

      BqStream.log(:info, "#{Time.now}: ***** Start Update Oldest Record Rows *****")
      # If there is a crash and records didn't get pushed to BigQuery because we lost those in buffer
      old_records = @bq_archiver.query('SELECT table_name, attr, min(updated_at) as bq_earliest_update FROM '\
                                      "[#{BqStream.project_id}:#{dataset}.#{BqStream.bq_table_name}] "\
                                      'GROUP BY table_name, attr')
      if old_records['rows']
        old_records['rows'].each do |r|
          table = r['f'][0]['v']
          trait = r['f'][1]['v']
          unless trait.nil?
            rec = BqStream::OldestRecord.find_or_create_by(table_name: table, attr: trait)
            rec.update(bq_earliest_update: Time.at(r['f'][2]['v'].to_f))
          end
        end
      else
        BqStream::OldestRecord.update_all(bq_earliest_update: Time.now)
      end

      BqStream::OldestRecord.where.not(table_name: '! revision !').update_all(archived: false)
      BqStream.log(:info, "#{Time.now}: ***** End Update Oldest Record Rows *****")

      until BqStream::OldestRecord.where(archived: false).empty? && buffer.empty?
        BqStream::OldestRecord.table_names.each do |table|
          unless BqStream::OldestRecord.where(table_name: table, archived: false).empty?
            BqStream.log(:info, "#{Time.now}: ***** Cycle Table: #{table} *****")
            oldest_attr_recs = BqStream::OldestRecord.where('table_name = ? AND bq_earliest_update >= ?', table, back_date)
            if table == 'Job' && !oldest_attr_recs.blank?
              puts oldest_attr_recs.first.bq_earliest_update 
            else
              puts 'No oldest_attr_recs'
            end
            earliest_update = oldest_attr_recs.map(&:bq_earliest_update).compact.min

            BqStream.log(:info, "#{Time.now}: ***** Table: #{table} | Back Date: #{back_date} | 'Start' Date: #{earliest_update || Time.now} *****")

            next_batch = table.constantize.where(
              'created_at > ? AND created_at < ?',
              back_date, earliest_update || Time.now
            ).order('created_at DESC').limit(10_000 / (oldest_attr_recs.count.zero? ? 1 : oldest_attr_recs.count))

            if next_batch.empty?
              BqStream::OldestRecord.where(table_name: table).each { |row| row.update(archived: true) }
              next
            else
              earliest_in_batch = next_batch.map(&:created_at).compact.min
              BqStream.log(:info, "#{Time.now}: ***** Earliest Date in Batch: #{earliest_in_batch} *****")
              if (next_batch.count * oldest_attr_recs.count) > 6_000
                next_batch.each { |i| next_batch -= [i] if i.created_at == earliest_in_batch }
                earliest_in_batch = next_batch.map(&:created_at).compact.min
                BqStream.log(:info, "#{Time.now}: ***** Earliest Date in Batch Updated to: #{earliest_in_batch} *****")
              end
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
              if oldest_attr_recs.empty?
                BqStream::OldestRecord.where(table_name: table).update_all(bq_earliest_update: earliest_in_batch)
              else
                oldest_attr_recs.update_all(bq_earliest_update: earliest_in_batch)
              end
            end
            unless buffer.empty?
              BqStream.log(:info, "#{Time.now}: ***** Start data pack and insert *****")
              data = buffer.collect do |i|
                new_val = encode_value(i[:new_value]) rescue nil
                { table_name: i[:table_name], record_id: i[:record_id], attr: i[:attr],
                  new_value: new_val ? new_val : i[:new_value], updated_at: i[:updated_at] }
              end
              BqStream.log(:info, "#{Time.now}: ***** Insert #{buffer.count} Records to BigQuery *****")
              @bq_archiver.insert(BqStream.bq_table_name, data) unless data.empty?
              buffer = []
              BqStream.log(:info, "#{Time.now}: ***** End data pack and insert *****")
            end
          end
        end
      end
      BqStream.log(:info, "#{Time.now}: ***** End Streamline Process *****")
    end

    def id_streamline_archive(back_date, dataset = BqStream.dataset)
      BqStream.log(:info, "#{Time.now}: ***** Start ID Streamline Process *****")

      buffer = []

      require 'big_query'
      opts = {}
      opts['client_id']     = BqStream.client_id
      opts['service_email'] = BqStream.service_email
      opts['key']           = BqStream.key
      opts['project_id']    = BqStream.project_id
      opts['dataset']       = dataset
      @bq_archiver = BigQuery::Client.new(opts)

      unless @bq_archiver.datasets_formatted.include?(dataset)
        @bq_archiver.create_dataset(dataset)
      end

      unless @bq_archiver.tables_formatted.include?(BqStream.bq_table_name)
        bq_table_schema = { table_name:   { type: 'STRING', mode: 'REQUIRED' },
                            record_id:    { type: 'INTEGER', mode: 'REQUIRED' },
                            attr:         { type: 'STRING', mode: 'NULLABLE' },
                            new_value:    { type: 'STRING', mode: 'NULLABLE' },
                            updated_at:   { type: 'TIMESTAMP', mode: 'REQUIRED' } }
        @bq_archiver.create_table(BqStream.bq_table_name, bq_table_schema)
      end

      BqStream.log(:info, "#{Time.now}: ***** Start Update Oldest Record Rows *****")
      # If there is a crash and records didn't get pushed to BigQuery because we lost those in buffer
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

      BqStream::OldestRecord.where.not(table_name: '! revision !').update_all(archived: false)
      BqStream.log(:info, "#{Time.now}: ***** End Update Oldest Record Rows *****")

      until BqStream::OldestRecord.where(archived: false).empty? && buffer.empty?
        BqStream::OldestRecord.table_names.each do |table|
          BqStream.log(:info, "#{Time.now}: ***** Cycle Table: #{table} *****")
          oldest_attr_recs = BqStream::OldestRecord.where(table_name: table)

          record_id_query = @bq_archiver.query('SELECT table_name, attr, min(record_id) as earliest_record_id '\
                                                "FROM [#{BqStream.project_id}:#{dataset}.#{BqStream.bq_table_name}] "\
                                                "WHERE table_name = '#{table}' AND attr = 'id' "\
                                                'GROUP BY table_name, attr')

          earliest_record_id = record_id_query['rows'] ? record_id_query['rows'].first['f'].last['v'].to_i : table.constantize.last.id
          BqStream.log(:info, "#{Time.now}: ***** earliest_record_id: #{earliest_record_id} *****")
          back_date_id = table.constantize.where('created_at >= ?', back_date).first.try(:id)
          BqStream.log(:info, "#{Time.now}: ***** back_date_record_id: #{back_date_id} *****")

          until BqStream::OldestRecord.where(table_name: table, archived: false).empty?
            next_batch = table.constantize.order(id: :desc).where('id <= ? AND id >= ?', earliest_record_id, back_date_id)
                                          .limit(10_000 / (oldest_attr_recs.count.zero? ? 1 : oldest_attr_recs.count))

            BqStream.log(:info, "#{Time.now}: ***** Next Batch Count for #{table}: #{next_batch.count} *****")

            if next_batch.empty?
              BqStream::OldestRecord.where(table_name: table).each { |row| row.update(archived: true) }
            else
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
              BqStream.log(:info, "#{Time.now}: ***** updated earliest_record_id: #{earliest_record_id} *****")
              BqStream.log(:info, "#{Time.now}: ***** back_date_record_id: #{back_date_id} *****")
            end

            unless buffer.empty?
              BqStream.log(:info, "#{Time.now}: ***** Start data pack and insert *****")
              data = buffer.collect do |i|
                new_val = encode_value(i[:new_value]) rescue nil
                { table_name: i[:table_name], record_id: i[:record_id], attr: i[:attr],
                  new_value: new_val ? new_val : i[:new_value], updated_at: i[:updated_at] }
              end
              BqStream.log(:info, "#{Time.now}: ***** Insert #{buffer.count} Records for #{table} to BigQuery *****")
              @bq_archiver.insert(BqStream.bq_table_name, data) unless data.empty?
              earliest_record_id = next_batch.map(&:id).min - 1
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
