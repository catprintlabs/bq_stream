$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'bq_stream'
require 'pry'
require 'timecop'

ActiveRecord::Base.establish_connection adapter: "sqlite3", database: ":memory:"
