unless RUBY_ENGINE == 'opal'
  require 'active_record'
  require 'dotenv'
  Dotenv.load
  require 'bq_stream/version'
  require 'bq_stream/configuration'
  require 'bq_stream/queued_item'
  require 'bq_stream/oldest_record'
  require 'bq_stream/bq_stream'
  Opal.append_path File.expand_path('../', __FILE__).untaint if defined? Opal
  require 'big_query'
  require 'bulk_insert'
end

require 'activerecordbase'
