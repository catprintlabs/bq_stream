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

  def self.log
    @bq_log ||= Logger.new(Rails.root.join('log/bq_stream_errors.log').to_s,
                           File::WRONLY | File::APPEND)
  end

  def self.logger
    @bq_logger ||= Logger.new(Rails.root.join('log/bigquery.log').to_s,
                              File::WRONLY | File::APPEND)
  end

  def self.oldest_record_log
    @bq_oldest_record_log ||= Logger.new(Rails.root.join('log/oldest_record.log').to_s,
                              File::WRONLY | File::APPEND)
  end

  def self.create_bq_writer
    require 'big_query'
    opts = {}
    opts['client_id']     = client_id
    opts['service_email'] = service_email
    opts['key']           = key
    opts['project_id']    = project_id
    opts['dataset']       = dataset
    @bq_writer = BigQuery::Client.new(opts)
  end

  def self.config_initialized
    QueuedItem.build_table
    OldestRecord.build_table
    create_bq_writer
    create_bq_dataset unless @bq_writer.datasets_formatted.include?(dataset)
    create_bq_table unless @bq_writer.tables_formatted.include?(bq_table_name)
    initialize_old_records
  end

  def self.initialize_old_records
    old_records = @bq_writer.query('SELECT table_name, attr, min(updated_at) '\
                                   'as bq_earliest_update FROM '\
                                   "[#{project_id}:#{dataset}.#{bq_table_name}] "\
                                   'GROUP BY table_name, attr')
    logger.info "#{Time.now}: BqStream: #{old_records['rows'].count rescue 0} Old Records in BigQuery"
    old_records['rows'].each do |r|
      rec = OldestRecord.find_by(table_name: r['f'][0]['v'],
                               attr: r['f'][1]['v'])
      rec && rec.update(bq_earliest_update: Time.at(r['f'][2]['v'].to_f))
    end if old_records['rows']
    # TODO: There are 0 records in OldestRecord at this point
  end

  def self.available_rows
    [batch_size - QueuedItem.all.count, 0].max
  end

  def self.encode_value(value)
    value.encode('utf-8', invalid: :replace,
                          undef: :replace, replace: '_') rescue nil
  end

  def self.dequeue_items
    Rollbar.log('info', 'BqStream', message: "dequeue_items start #{OldestRecord.count rescue 0} in OldestRecord")
    OldestRecord.update_bq_earliest do |oldest_record, r|
      QueuedItem.create(table_name: oldest_record.table_name,
                        record_id: r.id,
                        attr: oldest_record.attr,
                        new_value: r[oldest_record.attr],
                        updated_at: r.updated_at)
    end if available_rows > 0
    Rollbar.log('info', 'BqStream', message: "dequeue_items #{OldestRecord.count rescue 0} after update_bq_earliest in OldestRecord")
    # TODO: From here to the next dequeue_items there is an increase in records to OldestRecord
    create_bq_writer
    Rollbar.log('info', 'BqStream', message: "dequeue_items #{OldestRecord.count rescue 0} after create_bq_wwriter in OldestRecord")
    records = BqStream::QueuedItem.all.limit(BqStream.batch_size)
    data = records.collect do |i|
      new_val = encode_value(i.new_value) rescue nil
      { table_name: i.table_name, record_id: i.record_id, attr: i.attr,
        new_value: new_val ? new_val : i.new_value, updated_at: i.updated_at }
    end
    Rollbar.log('info', 'BqStream', message: "dequeue_items #{OldestRecord.count rescue 0} before bq insert in OldestRecord")
    @bq_writer.insert(bq_table_name, data)
    Rollbar.log('info', 'BqStream', message: "dequeue_items #{OldestRecord.count rescue 0} after bq insert in OldestRecord")
    records.each(&:destroy)
    Rollbar.log('info', 'BqStream', message: "dequeue_items end #{OldestRecord.count rescue 0} in OldestRecord")
  end

  def self.create_bq_dataset
    @bq_writer.create_dataset(dataset)
  end

  def self.create_bq_table
    bq_table_schema = { table_name:   { type: 'STRING', mode: 'REQUIRED' },
                        record_id:    { type: 'INTEGER', mode: 'REQUIRED' },
                        attr:         { type: 'STRING', mode: 'NULLABLE' },
                        new_value:    { type: 'STRING', mode: 'NULLABLE' },
                        updated_at:   { type: 'TIMESTAMP', mode: 'REQUIRED' } }
    @bq_writer.create_table(bq_table_name, bq_table_schema)
  end
end
