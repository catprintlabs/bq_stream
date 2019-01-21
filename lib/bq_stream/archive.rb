def create_bq_archiver
  require 'big_query'
  opts = {}
  opts['client_id']     = BqStream.client_id
  opts['service_email'] = BqStream.service_email
  opts['key']           = BqStream.key
  opts['project_id']    = BqStream.project_id
  opts['dataset']       = BqStream.dataset
  @bq_archiver = BigQuery::Client.new(opts)
end

def oldest_record_archive(back_date)
  BqStream.log(:info, "#{Time.now}: ***** Start Process *****")

  buffer = []
  create_bq_archiver

  BqStream.log(:info, "#{Time.now}: ***** Start Update Oldest Record Rows *****")
  # If there is a crash and records didn't get pushed to BigQuery because we lost those in buffer
  old_records = @bq_archiver.query('SELECT table_name, attr, min(updated_at) '\
                                   'as bq_earliest_update FROM '\
                                   "[#{BqStream.project_id}:#{BqStream.dataset}.#{BqStream.bq_table_name}] "\
                                   'GROUP BY table_name, attr')
  old_records['rows'].each do |r|
    table = r['f'][0]['v']
    trait = r['f'][1]['v']
    unless trait.nil?
      rec = BqStream::OldestRecord.find_or_create_by(table_name: table, attr: trait)
      rec.update(bq_earliest_update: Time.at(r['f'][2]['v'].to_f))
    end
  end if old_records['rows']

  BqStream::OldestRecord.where.not(table_name: '! revision !').update_all(archived: false)
  BqStream.log(:info, "#{Time.now}: ***** End Update Oldest Record Rows *****")
  iteration = 0
  until BqStream::OldestRecord.where(archived: false).empty? && buffer.empty?
    BqStream.log(:info, "#{Time.now}: ***** New Cycle: Buffer Count: #{buffer.count} #{iteration}*****")
    if buffer.count.positive?
      if BqStream::OldestRecord.where(archived: false).empty?
        records = buffer
      elsif buffer.count >= 10_000
        BqStream.log(:info, "#{Time.now}: ***** Grab 10,000 Records from the #{buffer.count} in bufffer *****")
        records = buffer.first(10_000)
        BqStream.log(:info, "#{Time.now}: ***** Grab Complete *****")
      else
        records = nil
      end
      if records
        BqStream.log(:info, "#{Time.now}: ***** Start data pack and insert *****")
        data = records.collect do |i|
          new_val = encode_value(i[:new_value]) rescue nil
          { table_name: i[:table_name], record_id: i[:record_id], attr: i[:attr],
            new_value: new_val ? new_val : i[:new_value], updated_at: i[:updated_at] }
        end
        BqStream.log(:info, "#{Time.now}: ***** Insert #{records.count} Records to BigQuery *****")
        @bq_archiver.insert(BqStream.bq_table_name, data) unless data.empty?
        buffer = []
        BqStream.log(:info, "#{Time.now}: ***** End data pack and insert *****")
      end
    end
    BqStream::OldestRecord.table_names.each do |table|
      unless BqStream::OldestRecord.where(table_name: table, archived: false).empty?
        BqStream.log(:info, "#{Time.now}: ***** Cycle Table: #{table} *****")
        oldest_attr_recs = BqStream::OldestRecord.where('table_name = ? AND bq_earliest_update >= ?', table, back_date)
        earliest_update = oldest_attr_recs.map(&:bq_earliest_update).compact.min

        next_created_at = table.constantize.where(
          'created_at > ? AND created_at < ?', 
          back_date, earliest_update || Time.now
        ).order('created_at DESC').first.try(:created_at)

        next_records = table.constantize.where('created_at = ?', next_created_at) if next_created_at

        if next_records.nil?
          BqStream.log(:info, "#{Time.now}: ***** Cycle Count for #{table}: 0 *****")
          BqStream::OldestRecord.where(table_name: table).each { |row| row.update(archived: true) }
          next
        else
          BqStream.log(:info, "#{Time.now}: ***** Cycle Count for #{table}: #{next_records.count} *****")
          oldest_attr_recs.uniq.each do |oldest_attr_rec|
            next_records.each do |next_record|
              new_val = next_record[oldest_attr_rec.attr] && table.constantize.type_for_attribute(oldest_attr_rec.attr).type == :datetime ? next_record[oldest_attr_rec.attr].in_time_zone(BqStream.timezone) : next_record[oldest_attr_rec.attr].to_s
              buffer << { table_name: table,
                          record_id: next_record.id,
                          attr: oldest_attr_rec.attr,
                          new_value: new_val,
                          updated_at: next_record.created_at }
            end
          end
          if oldest_attr_recs.empty?
            BqStream::OldestRecord.where(table_name: table).update_all(bq_earliest_update: next_records.first.created_at)
          else
            oldest_attr_recs.update_all(bq_earliest_update: next_records.first.created_at)
          end
        end
      end
    end
    iteration += 1
  end
  BqStream.log(:info, "#{Time.now}: ***** End Process *****")
end

def streamline_archive(back_date)
  BqStream.log(:info, "#{Time.now}: ***** Start Streamline Process *****")

  buffer = []
  create_bq_archiver

  BqStream.log(:info, "#{Time.now}: ***** Start Update Oldest Record Rows *****")
  # If there is a crash and records didn't get pushed to BigQuery because we lost those in buffer
  old_records = @bq_archiver.query('SELECT table_name, attr, min(updated_at) '\
                                   'as bq_earliest_update FROM '\
                                   "[#{BqStream.project_id}:#{BqStream.dataset}.#{BqStream.bq_table_name}] "\
                                   'GROUP BY table_name, attr')
  old_records['rows'].each do |r|
    table = r['f'][0]['v']
    trait = r['f'][1]['v']
    unless trait.nil?
      rec = BqStream::OldestRecord.find_or_create_by(table_name: table, attr: trait)
      rec.update(bq_earliest_update: Time.at(r['f'][2]['v'].to_f))
    end
  end if old_records['rows']

  BqStream::OldestRecord.where.not(table_name: '! revision !').update_all(archived: false)
  BqStream.log(:info, "#{Time.now}: ***** End Update Oldest Record Rows *****")
  iteration = 0
  until BqStream::OldestRecord.where(archived: false).empty? && buffer.empty?
    BqStream.log(:info, "#{Time.now}: ***** New Cycle: Buffer Count: #{buffer.count} #{iteration}*****")
    if buffer.count.positive?
      if BqStream::OldestRecord.where(archived: false).empty?
        records = buffer
      elsif buffer.count >= 10_000
        BqStream.log(:info, "#{Time.now}: ***** Grab 10,000 Records from the #{buffer.count} in bufffer *****")
        records = buffer.first(10_000)
        BqStream.log(:info, "#{Time.now}: ***** Grab Complete *****")
      else
        records = nil
      end
      if records
        BqStream.log(:info, "#{Time.now}: ***** Start data pack and insert *****")
        data = records.collect do |i|
          new_val = encode_value(i[:new_value]) rescue nil
          { table_name: i[:table_name], record_id: i[:record_id], attr: i[:attr],
            new_value: new_val ? new_val : i[:new_value], updated_at: i[:updated_at] }
        end
        BqStream.log(:info, "#{Time.now}: ***** Insert #{records.count} Records to BigQuery *****")
        @bq_archiver.insert(BqStream.bq_table_name, data) unless data.empty?
        buffer = []
        BqStream.log(:info, "#{Time.now}: ***** End data pack and insert *****")
      end
    end
    BqStream::OldestRecord.table_names.each do |table|
      unless BqStream::OldestRecord.where(table_name: table, archived: false).empty?
        BqStream.log(:info, "#{Time.now}: ***** Cycle Table: #{table} *****")
        oldest_attr_recs = BqStream::OldestRecord.where('table_name = ? AND bq_earliest_update >= ?', table, back_date)
        earliest_update = oldest_attr_recs.map(&:bq_earliest_update).compact.min

        next_created_at = table.constantize.where(
          'created_at > ? AND created_at < ?', 
          back_date, earliest_update || Time.now
        ).order('created_at DESC').first.try(:created_at)

        next_records = table.constantize.where('created_at = ?', next_created_at) if next_created_at

        if next_records.nil?
          BqStream.log(:info, "#{Time.now}: ***** Cycle Count for #{table}: 0 *****")
          BqStream::OldestRecord.where(table_name: table).each { |row| row.update(archived: true) }
          next
        else
          BqStream.log(:info, "#{Time.now}: ***** Cycle Count for #{table}: #{next_records.count} *****")
          oldest_attr_recs.uniq.each do |oldest_attr_rec|
            next_records.each do |next_record|
              new_val = next_record[oldest_attr_rec.attr] && table.constantize.type_for_attribute(oldest_attr_rec.attr).type == :datetime ? next_record[oldest_attr_rec.attr].in_time_zone(BqStream.timezone) : next_record[oldest_attr_rec.attr].to_s
              buffer << { table_name: table,
                          record_id: next_record.id,
                          attr: oldest_attr_rec.attr,
                          new_value: new_val,
                          updated_at: next_record.created_at }
            end
          end
          if oldest_attr_recs.empty?
            BqStream::OldestRecord.where(table_name: table).update_all(bq_earliest_update: next_records.first.created_at)
          else
            oldest_attr_recs.update_all(bq_earliest_update: next_records.first.created_at)
          end
        end
      end
    end
    iteration += 1
  end
  BqStream.log(:info, "#{Time.now}: ***** End Streamline Process *****")
end
