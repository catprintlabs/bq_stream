# require 'bq_stream/config'

module BqStream
  extend Configuration
  extend Comparison

  define_setting :client_id
  define_setting :service_email
  define_setting :key
  define_setting :project_id
  define_setting :dataset
  define_setting :queued_items_table_name, 'queued_items'
  define_setting :oldest_record_table_name, 'oldest_records'
  define_setting :bq_table_name, 'bq_datastream'
  define_setting :back_date, nil
  define_setting :batch_size, 1000

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
      QueuedItem.build_table
      OldestRecord.build_table
      create_bq_writer
      create_bq_dataset unless @bq_writer.datasets_formatted.include?(dataset)
      create_bq_table unless @bq_writer.tables_formatted.include?(bq_table_name)
      initialize_old_records
    end

    def initialize_old_records
      log(:info, "#{Time.now}: $$$$$$$$$$ Start Init Old Records $$$$$$$$$$")
      old_records = @bq_writer.query('SELECT table_name, attr, min(updated_at) '\
                                     'as bq_earliest_update FROM '\
                                     "[#{project_id}:#{dataset}.#{bq_table_name}] "\
                                     'GROUP BY table_name, attr')
      old_records['rows'].each do |r|
        table = r['f'][0]['v']
        trait = r['f'][1]['v']
        unless trait.nil?
          rec = OldestRecord.find_or_create_by(table_name: table, attr: trait)
          rec.update(bq_earliest_update: Time.at(r['f'][2]['v'].to_f))
        end
      end if old_records['rows']
      log(:info, "#{Time.now}: $$$$$$$$$$ End Init Old Records $$$$$$$$$$")
    end

    def encode_value(value)
      value.encode('utf-8', invalid: :replace,
                            undef: :replace, replace: '_') rescue nil
    end

    def dequeue_items
      start_after_id = QueuedItem.where(sent_to_bq: true).last.id rescue 0 # added for testing TODO: update after test
      log_code = rand(2**256).to_s(36)[0..7]
      log(:info, "#{Time.now}: ***** Dequeue Items Started ***** #{log_code}")
      OldestRecord.update_bq_earliest
      create_bq_writer
      # records = QueuedItem.all.limit(batch_size) # removing for testing TODO: update after test
      records = QueuedItem.where('id > ?', start_after_id).limit(batch_size)
      log(:info, "#{Time.now}: Records Count: #{records.count} #{log_code}")
      data = records.collect do |i|
        new_val = encode_value(i.new_value) rescue nil
        { table_name: i.table_name, record_id: i.record_id, attr: i.attr,
          new_value: new_val ? new_val : i.new_value, updated_at: i.updated_at }
      end
      @bq_writer.insert(bq_table_name, data) unless data.empty?
      records.each { |r| r.update(sent_to_bq: true) } # added for testing TODO: update after test
      # QueuedItem.delete_all_with_limit # removing delete for testing TODO: reinstate after test
      log(:info, "#{Time.now}: ***** Dequeue Items Ended ***** #{log_code}")
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
