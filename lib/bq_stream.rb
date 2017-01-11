require 'active_record'
require 'active_record_base'

unless RUBY_ENGINE == 'opal'
  require 'dotenv'
  Dotenv.load
  require 'big_query'
  require 'bq_stream/version'
  require 'bq_stream/configuration'
  require 'bq_stream/queued_item'
  require 'bq_stream/oldest_record'
  require 'bq_stream/bq_stream'
end
