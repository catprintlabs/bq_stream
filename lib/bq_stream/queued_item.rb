module BqStream
  class QueuedItem < ActiveRecord::Base
    default_scope { order(updated_at: :asc) }

    # Temporary array of records to be added into QueuedItem table
    def self.buffer
      @buffer ||= []
    end

    # How records that can be  placed into the buffer
    # [BqStream.batch_size - (all.count + buffer.count), 0].max
    def self.available_rows
      [BqStream.batch_size - (where(sent_to_bq: nil).count + buffer.count), 0].max
    end

    # Insert records from buffer into QueuedItem table in one call
    def self.create_from_buffer
      bulk_insert values: buffer.uniq
    end

    # Rails doesn't support delete_all with limit scope
    # make sure to use the default sort order for ORDER BY
    def self.delete_all_with_limit
      connection.exec_delete("DELETE FROM #{table_name} ORDER BY updated_at ASC LIMIT #{BqStream.batch_size}", 'DELETE', [])
    end

    # List of columns that should be in the QueuedItem table
    def self.queued_items_columns
      { table_name: :string, record_id: :integer, attr: :string, new_value: :binary, updated_at: :datetime, sent_to_bq: :boolean }
    end

    # List of columns that should be index from the QueuedItem table
    def self.indexed_columns
      %i(sent_to_bq)
    end

    # Checks to see if the current QueuedItem table matches the desired columns (schema)
    # Desired columns should be listed in self.queued_items_columns
    def self.schema_match?
      BqStream::const_get(BqStream.queued_items_table_name.classify).column_names ==
        queued_items_columns.collect { |k, _v| k.to_s }.unshift('id')
    end

    # Checks to see if the indexed columns of the current QueuedItem table matches the desired indexing
    # Desired columns to be indexed should be listed in self.indexed_columns
    def self.index_match?
      indexes = []
      connection.indexes(:queued_items).each do |i|
        indexes << i.name.split('_on_').last.to_sym
      end
      indexes == indexed_columns
    end

    # Builds QueuedItem table
    # Drops and rebuilds table if it doesn't exist, mismatch of schema or mismach of indexing 
    def self.build_table
      self.table_name = BqStream.queued_items_table_name
      connection.create_table(table_name, force: true) do |t|
        queued_items_columns.each do |k, v|
          t.send(v, k)
          t.index k if indexed_columns.include?(k)
        end
      end unless (connection.tables.include? table_name) && schema_match? && index_match?
    end
    # TODO:
    # Only build table if it doesn't exist
    # If schema or indexing doesn't match it should:
    # remove anything from current table that is not in the given columns 
    # add anything that is not in the current table but in the given columns
  end
end
