# require 'bq_stream/config'

module BqStream
  extend Configuration
  extend Comparison

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

    def create_bq_writer
      require 'big_query'
      opts = {}
      opts['client_id']     = client_id
      opts['service_email'] = service_email
      opts['key']           = key
      opts['project_id']    = project_id
      opts['dataset']       = dataset
      @bq_writer = BigQuery::Client.new(opts)
    end

    def config_initialized
      create_bq_writer
      create_bq_dataset unless @bq_writer.datasets_formatted.include?(dataset)
      create_bq_table unless @bq_writer.tables_formatted.include?(bq_table_name)
    end

    # Destroy or create rows based on the current bq attributes for given table
    def update_bq_attribute_records(table, current_bq_attributes)
      if ActiveRecord::Base.connection.table_exists? 'bq_stream_bq_attributes'
        attributes_on_record = BqAttribute.where(table_name: table).pluck(:attr)
        attributes_to_remove = attributes_on_record.reject { |i| current_bq_attributes.include? i }
        attributes_to_add = current_bq_attributes.reject { |i| attributes_on_record.include? i }
        attributes_to_remove.each { |i| OldestRecord.where(table_name: table, attr: i).delete_all }
        attributes_to_add.each { |i| OldestRecord.create(table_name: table, attr: i) }
      end
      if ActiveRecord::Base.connection.table_exists? 'bq_stream_bq_oldest_records' and back_date
        oldest_on_record = OldestRecord.where(table_name: table).pluck(:attr)
        oldest_to_remove = oldest_on_record.reject { |i| current_bq_attributes.include? i }
        oldest_to_add = current_bq_attributes.reject { |i| oldest_on_record.include? i }
        oldest_to_remove.each { |i| OldestRecord.where(table_name: table, attr: i).delete_all }
        oldest_to_add.each { |i| OldestRecord.create(table_name: table, attr: i)}
      end
    end

    def encode_value(value)
      value.encode('utf-8', invalid: :replace,
                            undef: :replace, replace: '_') rescue nil
    end

    def dequeue_items
      log(:info, "#{Time.now}: !!!!! dequeue_items called from HYPERLOOP !!!!!") if back_date
      log(:info, "#{Time.now}: !!!!! dequeue_items called from MASTER !!!!!") unless back_date
      log_code = rand(2**256).to_s(36)[0..7]
      log(:info, "#{Time.now}: ***** Dequeue Items Started ***** #{log_code}")
      log(:info, "#{Time.now}: In dequeue_items Oldest Record count: #{OldestRecord.count}")
      OldestRecord.update_bq_earliest if back_date
      create_bq_writer
      records = QueuedItem.where(sent_to_bq: nil).limit(batch_size)
      data = records.collect do |i|
        new_val = encode_value(i.new_value) rescue nil
        { table_name: i.table_name, record_id: i.record_id, attr: i.attr,
          new_value: new_val ? new_val : i.new_value, updated_at: i.updated_at }
      end
      @bq_writer.insert(bq_table_name, data) unless data.empty?
      records.update_all(sent_to_bq: true)
      # QueuedItem.where(sent_to_bq: true).delete_all # removing delete for testing TODO: reinstate after test
      log(:info, "#{Time.now}: ***** Dequeue Items Ended ***** #{log_code}")
    end

    def insert_missing_records(records, bq_attributes)
      bq_attributes.each do |bqa|
        records.each do |record|
          new_val = (record.class.columns_hash[bqa.to_s].type == :datetime ? record.send(bqa).in_time_zone(BqStream.timezone) : record.send(bqa)).to_s
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
