require 'dotenv'
Dotenv.load

if ENV['PRODUCTION'] == 'false'
  require 'active_record'
  require 'activerecordbase'
  require 'big_query' # development dependency
  require 'bq_stream/version'
  require 'bq_stream/configuration'
  require 'bq_stream/queued_item'
  require 'bq_stream/oldest_record'
  require 'bq_stream/bq_stream'
  require 'bulk_insert'
else
  require 'activerecordbase'

  unless RUBY_ENGINE == 'opal'
    require 'active_record'
    require 'bq_stream/version'
    require 'bq_stream/configuration'
    require 'bq_stream/queued_item'
    require 'bq_stream/oldest_record'
    require 'bq_stream/bq_stream'
    Opal.append_path File.expand_path('../', __FILE__).untaint
  end
end
