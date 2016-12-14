# require 'bq_stream/configuration'

module BqStream

  extend Configuration

  def self.config_reset
    # require File.join(File.dirname(__FILE__), '..', 'reactive_record', 'permission_patches')
    Object.send(:remove_const, :Application) if @fake_application_defined
    policy = begin
      Object.const_get 'ApplicationPolicy'
    rescue Exception => e
      raise e unless e.is_a?(NameError) && e.message == "uninitialized constant ApplicationPolicy"
    end
    application = begin
      Object.const_get('Application')
    rescue Exception => e
      raise e unless e.is_a?(NameError) && e.message == "uninitialized constant Application"
    end if policy
    if policy && !application
      Object.const_set 'Application', Class.new
      @fake_application_defined = true
    end
    @pusher = nil
    Connection.build_tables
  end

  define_setting(:transport, :none) do |transport|
    if transport == :action_cable
      require 'synchromesh/action_cable'
      opts[:refresh_channels_every] = :never
    else
      require 'pusher' if transport == :pusher
      opts[:refresh_channels_every] = nil if opts[:refresh_channels_every] == :never
    end
  end

  define_setting :client_id
  define_setting :service_email
  define_setting :key
  define_setting :project_id
  define_setting :bq_table_name, 'bq_table'
end
