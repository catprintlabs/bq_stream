module BqStream
  class QueuedItem < ActiveRecord::Base
    default_scope { order(updated_at: :asc) }

    def self.buffer
      @buffer ||= []
    end

    def self.available_rows
      # [BqStream.batch_size - (all.count + buffer.count), 0].max
      [BqStream.batch_size - (where(sent_to_bq: nil).count + buffer.count), 0].max
    end

    def self.create_from_buffer
      bulk_insert values: buffer.uniq
    end

    def self.delete_all_with_limit
      # Rails doesn't support delete_all with limit scope
      connection.exec_delete("DELETE FROM #{table_name} ORDER BY updated_at ASC LIMIT #{BqStream.batch_size}", 'DELETE', [])
      # make sure to use the default sort order for ORDER BY
    end

    def self.queued_items_columns
      { table_name: :string, record_id: :integer, attr: :string, new_value: :binary, updated_at: :datetime, sent_to_bq: :boolean }
    end

    def self.indexed_columns
      %i()
    end

    def self.schema_match?
      BqStream::const_get(BqStream.queued_items_table_name.classify).column_names ==
        queued_items_columns.collect { |k, _v| k.to_s }.unshift('id')
    end

    def self.index_match?
      indexes = []
      connection.indexes(:queued_items).each do |i|
        indexes << i.name.split('_on_').last.to_sym
      end
      indexes == indexed_columns
    end

    def self.build_table
      self.table_name = BqStream.queued_items_table_name
      connection.create_table(table_name, force: true) do |t|
        queued_items_columns.each do |k, v|
          t.send(v, k)
          t.index k if indexed_columns.include?(k)
        end
      end unless (connection.tables.include? table_name) && schema_match? && index_match?
    end
  end
end
