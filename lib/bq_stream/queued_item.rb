module BqStream
  class QueuedItem < ActiveRecord::Base

    def self.build_table
      self.table_name = BqStream.bq_table_name

      connection.create_table(table_name, force: true) do |t|
        t.string   :table_name
        t.integer  :record_id
        t.string   :attr
        t.binary   :new_value
        t.datetime :updated_at
      end unless connection.tables.include? table_name
    end
  end
end
