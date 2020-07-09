# frozen_string_literal: true

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
  define_setting :report_to_rollbar, Object.const_defined?('Rollbar') && ENV['BQ_ROLLBAR']

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

      revision_file_path = File.join(Dir.pwd, 'REVISION')
      current_deploy     = File.exist?(revision_file_path) ? `cat #{revision_file_path}` : 'None'
      revision           = OldestRecord.find_by_table_name('! revision !')

      log(:info, "#{Time.now}: ***** Oldest Record Revision: #{revision.attr} *****") if revision
      log(:info, "#{Time.now}: ***** Current Deploy: #{current_deploy} *****")

      return if revision && revision.attr == current_deploy

      @bq_attributes&.each do |k, v|
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

    def dequeue_items
      dequeue_time = Time.current

      log(:info, "***** Dequeue Time #{dequeue_time} *****")

      # log_code = rand(2**256).to_s(36)[0..7]
      # log(:info, "#{Time.now}: ***** Dequeue Items Started ***** #{log_code}")
      if back_date && (OldestRecord.all.empty? || !OldestRecord.where('bq_earliest_update >= ?', BqStream.back_date).empty?)
        verify_oldest_records
        OldestRecord.update_bq_earliest
      end

      create_bq_writer

      # Batch sending to BigQuery is limited to 10_000 rows
      # Initiate the records to be compared to the data that will be sent to BigQuery
      records = QueuedItem.where(sent_to_bq: nil).where('updated_at < ?', dequeue_time)
                          .limit([batch_size, 10_000].min)

      return if records.empty?

      # Create data hash that will be sent to BigQuery
      stream_data = records.map { |record| record_as_hash(record) }

      # Compare records to data and create a new array of the records actually being sent
      records_to_send = records.map do |record|
        record_as_json = record.as_json.except('id', 'sent_to_bq', 'time_sent')

        if stream_data.any? { |h| h.stringify_keys == record_as_json }
          record
        else
          if report_to_rollbar
            Rollbar.warning('BigQuery: Record missing from data!', record: record)
          end

          nil
        end
      end.compact

      log(:info, "!!!!! Records !!!!!\n#{records}")
      log(:info, "!!!!! Stream Data !!!!!\n#{stream_data}")
      log(:info, "!!!!! Records Sent !!!!!\n#{records_to_send}")

      return if stream_data.empty?

      # Set up insertion of records that are in the data hash and then insert to BigQuery
      begin
        insertion = @bq_writer.insert(bq_table_name, stream_data)
      rescue StandardError => e
        log(:info, "***** Insertion to #{project_id}:#{dataset}.#{bq_table_name} Failed *****")

        if report_to_rollbar
          Rollbar.error(
            e,
            'Failed to write data to BigQuery',
            project_id:    project_id,
            dataset:       dataset,
            bq_table_name: bq_table_name
          )
        end

        return false
      end

      records_sent = QueuedItem.where('id IN (?)', records_to_send.map(&:id))

      log(:info, "***** #{insertion} *****")

      records_sent.update_all(sent_to_bq: true, time_sent: Time.current)
      QueuedItem.where(sent_to_bq: true).delete_all

      # log(:info, "#{Time.now}: ***** Dequeue Items Ended ***** #{log_code}")
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

    private

    # Force data into UTF-8 format for BigData
    def encode_new_value(record)
      record.new_value.force_encoding('utf-8')

      record.new_value =
        record.new_value.encode('utf-8', invalid: :replace, undef: :replace, replace: '_')
    rescue StandardError => e
      Rollbar.error(e, 'Failed to encode value to UTF-8', record: record) if report_to_rollbar

      nil
    end

    # Build a hash representation of a record expected by BigData
    def record_as_hash(record)
      encode_new_value(record)

      {
        table_name: record.table_name,
        record_id:  record.record_id,
        attr:       record.attr,
        new_value:  record.new_value,
        updated_at: record.updated_at
      }
    end
  end
end
