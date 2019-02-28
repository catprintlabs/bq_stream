require 'big_query'

module BqStream
  extend Configuration
  extend Comparison
  extend Archive

  define_setting :client_id
  define_setting :service_email
  define_setting :key
  define_setting :project_id
  define_setting :dataset
  define_setting :bq_table_name, 'bq_datastream'
  define_setting :back_date, nil
  define_setting :batch_size, 1000
  define_setting :timezone, 'UTC'

  class << self
    attr_accessor :logger
    attr_accessor :error_logger
    attr_accessor :bq_attributes

    def log(type, message)
      return unless logger
      type = :info unless %i[unknown fatal error warn info debug].include?(type)
      logger.send(type, message)
    end

    def error_log(type, message)
      return unless logger
      type = :error unless %i[unknown fatal error warn info debug].include?(type)
      error_logger.send(type, message)
    end

    def create_bq_writer(dataset_override = nil)
      opts = {}
      opts['client_id']     = client_id
      opts['service_email'] = service_email
      opts['key']           = key
      opts['project_id']    = project_id
      opts['dataset']       = dataset_override ? dataset_override : dataset
      @bq_writer = BigQuery::Client.new(opts)
    end

    def config_initialized
      create_bq_writer
      create_bq_dataset unless @bq_writer.datasets_formatted.include?(dataset)
      create_bq_table unless @bq_writer.tables_formatted.include?(bq_table_name)
    end

    def register_bq_attributes(table, bq_attribuites)
      @bq_attributes ||= {}
      @bq_attributes[table] = bq_attribuites
    end

    # Destroy or create rows based on the current bq attributes for given table
    def verify_oldest_records
      log(:info, "#{Time.now}: ***** Verifying Oldest Records *****")
      current_deploy =
        if File.exist?(`cat #{File.expand_path ''}/REVISION`)
          `cat #{File.expand_path ''}/REVISION`
        else
          'None'
        end
      revision = OldestRecord.find_by_table_name('! revision !')
      log(:info, "#{Time.now}: ***** Oldest Record Revision: #{revision.attr} *****") if revision
      log(:info, "#{Time.now}: ***** Current Deploy: #{current_deploy} *****")
      return if revision && revision.attr == current_deploy
      @bq_attributes.each do |k, v|
        # add any records to oldest_records that are new (Or more simply make sure that that there is a record using find_by_or_create)
        v.each do |bqa|
          OldestRecord.find_or_create_by(table_name: k, attr: bqa)
        end
        # delete any records that are not in bq_attributes
        OldestRecord.where(table_name: k).each do |rec|
          rec.destroy unless v.include?(rec.attr.to_sym)
        end
      end
      log(:info, "#{Time.now}: ***** Updating Oldest Record Revision to #{current_deploy} *****")
      OldestRecord.update_all(archived: false)
      update_revision = OldestRecord.find_or_create_by(table_name: '! revision !')
      update_revision.update(attr: current_deploy, archived: true)
    end

    def encode_value(value)
      value.encode('utf-8', invalid: :replace,
                            undef: :replace, replace: '_') rescue nil
    end

    def dequeue_items
      log_code = rand(2**256).to_s(36)[0..7]
      log(:info, "#{Time.now}: ***** Dequeue Items Started ***** #{log_code}")
      if back_date && (OldestRecord.all.empty? || !OldestRecord.where('bq_earliest_update >= ?', BqStream.back_date).empty?)
        verify_oldest_records
        OldestRecord.update_bq_earliest
      end
      create_bq_writer
      # Batch sending to BigQuery is limited to 10_000 rows
      records = QueuedItem.where(sent_to_bq: nil).limit([batch_size, 10_000].min)
      data = records.collect do |i|
        new_val = encode_value(i.new_value) rescue nil
        { table_name: i.table_name, record_id: i.record_id, attr: i.attr,
          new_value: new_val ? new_val : i.new_value, updated_at: i.updated_at }
      end
      insertion = data.empty? ? nil : @bq_writer.insert('bq_table_name', data) rescue nil
      records.update_all(sent_to_bq: true) if insertion
      QueuedItem.where(sent_to_bq: true).delete_all
      log(:info, "#{Time.now}: ***** Dequeue Items Ended ***** #{log_code}")
    end

    def insert_missing_records(records, bq_attributes)
      bq_attributes.each do |bqa|
        records.each do |record|
          new_val = (!record.send(bqa).nil? && record.class.columns_hash[bqa.to_s].type == :datetime ? record.send(bqa).in_time_zone(BqStream.timezone) : record.send(bqa)).to_s
          QueuedItem.create(table_name: record.class.to_s, record_id: record.id, attr: bqa.to_s, new_value: new_val, updated_at: record.updated_at)
        end
      end
    end

    def create_bq_dataset
      @bq_writer.create_dataset(dataset)
    end

    def create_bq_table
      bq_table_schema = { table_name:   { type: 'STRING', mode: 'REQUIRED' },
                          record_id:    { type: 'INTEGER', mode: 'REQUIRED' },
                          attr:         { type: 'STRING', mode: 'NULLABLE' },
                          new_value:    { type: 'STRING', mode: 'NULLABLE' },
                          updated_at:   { type: 'TIMESTAMP', mode: 'REQUIRED' } }
      @bq_writer.create_table(bq_table_name, bq_table_schema)
    end
  end
end
