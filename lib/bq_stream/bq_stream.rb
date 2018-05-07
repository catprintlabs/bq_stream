# require 'bq_stream/config'

module BqStream
  extend Configuration

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

    def log(type, message)
      return unless logger
      type = :info unless %i[unknown fatal error warn info debug].include?(type)
      logger.send(type, message)
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
      start_after_id = QueuedItem.where(sent_to_bq: true).last.id # added for testing TODO: update after test
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

    def big_query_check(klass, dataset, table, qty)
      create_bq_writer
      bq_gather_and_compare(klass, dataset, table, qty)
    end

    def database_check(klass, date_attr, dataset, table, qty = nil, start_date = Time.current.beginning_of_week - 1.week, end_date =  Time.current.end_of_week - 1.week)
      create_bq_writer
      query_db_records(klass.classify.constantize, date_attr, qty, start_date, end_date)
      collect_bq_records(klass, dataset, table, @db_ids)
      bq_gather_and_compare(klass, dataset, table, qty)
    end

    protected

    def bq_gather_and_compare(klass, dataset, table, qty)
      query_bq_records(qty, dataset, table)
      compare_(@records, klass.classify.constantize)
      @results.each { |result| puts result }
    end

    def query_bq_records(qty, dataset, table)
      @schema = []
      total_rows = @bq_writer.query("SELECT count(*) FROM [#{dataset}.#{table}]")['rows'].first['f'].first['v']
      @bq_query ||= @bq_writer.query("SELECT * FROM [#{dataset}.#{table}] WHERE RAND() < #{qty * 2}/#{total_rows} LIMIT #{qty}")

      @bq_query['schema']['fields'].each do |f|
        @schema << f['name']
      end
      @schema.uniq!
      build_records(@bq_query['rows'])
    end

    def query_db_records(klass, date_attr, qty, start_date, end_date)
      @db_ids = []
      db_records =
        if qty
          klass.where("#{date_attr} > ? AND #{date_attr} < ?", start_date, end_date).shuffle.first(qty)
        else
          klass.where("#{date_attr} > ? AND #{date_attr} < ?", start_date, end_date)
        end
      db_records.each { |r| @db_ids << r.id }
    end

    def collect_bq_records(klass, dataset, table, ids)
      @bq_query = @bq_writer.query("SELECT * FROM [#{dataset}.#{table}] WHERE #{klass}_record_id IN (#{ids.to_s.gsub(/\[|\]/, '')})")
    end

    def build_records(rows)
      @records = []
      rows.each do |row|
        values = []
        row['f'].each_with_index do |value, index|
          values << [@schema[index], value['v']]
        end
        @records << values.to_h
      end
    end

    def compare_(records, class_name)
      @results = []
      records.each do |record|
        @fails = []
        if record['friendly_id'].nil?
          @results << "Order Record #{record['order_record_id']} Failed, friendly_id is nil!"
        else
          record.each do |k, v|
            db_object = class_name.find_by_friendly_id(record['friendly_id'])
            check_for_failures(class_name, k, v, db_object) if db_object.respond_to?(k)
          end
        end
        process_(record, @fails)
      end
    end

    def check_for_failures(klass, attr, val, db)
      if %i[string boolean].include?(klass.columns_hash[attr].type) &&
         db.send(attr) != val
        @fails << " #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val}"
      elsif klass.columns_hash[attr].type == :integer &&
            db.send(attr) != (val.nil? ? nil : val.to_i)
        @fails << " #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val.to_i}"
      elsif klass.columns_hash[attr].type == :decimal &&
            db.send(attr) != (val.nil? ? nil : val.to_d)
        @fails << " #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val.to_d}"
      elsif klass.columns_hash[attr].type == :datetime &&
            db.send(attr).try(:utc) != (val.nil? ? nil : (Time.at(val.to_d).utc + 4.hours))
        @fails << " #{attr}: #{db.send(attr).try(:utc)} != #{val.nil? ? 'nil' : (Time.at(val.to_d).utc + 4.hours)}"
      end
    end

    def process_(record, fails)
      unless record['friendly_id'].nil?
        unless fails.empty?
          @results << "Order Record #{record['order_record_id']} "\
                      'Failed with these errors (db != bq)'
          fails.each { |fail| @results << fail }
        end
      end
    end
  end
end
