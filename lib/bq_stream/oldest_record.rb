module BqStream
  class OldestRecord < ActiveRecord::Base
    def self.build_table
      self.table_name = BqStream.oldest_record_table_name

      connection.create_table(table_name, force: true) do |t|
        t.string   :table_name
        t.string   :attr
        t.datetime :bq_earliest_update
        t.datetime :ar_earliest_update
      end unless connection.tables.include? table_name
    end
  end
end
