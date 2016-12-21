# require 'bq_stream/config'

module BqStream
  extend Configuration

  define_setting :client_id
  define_setting :service_email
  define_setting :key
  define_setting :project_id
  define_setting :dataset
  define_setting :bq_table_name, 'queued_items'

  def self.config_initialized
    QueuedItem.build_table
    opts = {}
    opts['client_id']     = client_id
    opts['service_email'] = service_email
    opts['key']           = key
    opts['project_id']    = project_id
    opts['dataset']       = dataset
    @bq_writer = BigQuery::Client.new(opts)
  end

  def self.dequeue_items
    BqStream::QueuedItem.all.each do |i|
      @bq_writer.insert('table_name', table_name: i.table_name,
                                      record_id: i.record_id, attr: i.attr,
                                      new_value: i.new_value,
                                      updated_at: Time.now)
      BqStream::QueuedItem.destroy(i.id)
    end
  end
end
