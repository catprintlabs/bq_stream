# require 'bq_stream/config'

module BqStream
  extend Configuration

  define_setting :client_id
  define_setting :service_email
  define_setting :key
  define_setting :project_id
  define_setting :dataset
  define_setting :queued_items_table_name, 'queued_items'
  define_setting :bq_table_name, 'bq_datastream'

  def self.create_bq_writer
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
    create_bq_writer
    create_bq_dataset unless @bq_writer.datasets_formatted.include?(dataset)
    create_bq_table unless @bq_writer.tables_formatted.include?(bq_table_name)
  end

  def self.dequeue_items
    create_bq_writer
    BqStream::QueuedItem.all.each do |i|
      @bq_writer.insert(bq_table_name, table_name: i.table_name,
                                       record_id: i.record_id, attr: i.attr,
                                       new_value: i.new_value,
                                       updated_at: Time.now)
      BqStream::QueuedItem.destroy(i.id)
    end
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
