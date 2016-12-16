# require 'bq_stream/config'

module BqStream
  extend Configuration

  def self.config_initialized
    QueuedItem.build_table
  end

  define_setting :client_id
  define_setting :service_email
  define_setting :key
  define_setting :project_id
  define_setting :data_set
  define_setting :bq_table_name, 'queued_items'
end
