require 'active_record_base'

unless RUBY_ENGINE == 'opal'
  require 'active_record'
  require 'dotenv'
  Dotenv.load
  require 'big_query'
  require 'bq_stream/version'
  require 'bq_stream/configuration'
  require 'bq_stream/queued_item'
  require 'bq_stream/oldest_record'
  require 'bq_stream/bq_stream'
  Opal.append_path File.expand_path('../', __FILE__).untaint
end

# Use the requires below for testing

# require 'active_record'
# require 'active_record_base'
# require 'dotenv'
# Dotenv.load
# require 'big_query' # development dependency
# require 'bq_stream/version'
# require 'bq_stream/configuration'
# require 'bq_stream/queued_item'
# require 'bq_stream/oldest_record'
# require 'bq_stream/bq_stream'
