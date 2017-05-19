module BqStream
  class QueuedItem < ActiveRecord::Base
    default_scope { order(updated_at: :asc) }

    def self.buffer
      @buffer ||= []
    end

    def self.available_rows
      [BqStream.batch_size - (all.count + buffer.count), 0].max
    end

    def self.create_from_buffer
      BqStream.log.info "#{Time.now}: Buffer Count: #{buffer.count} (before bulk insert)"
      bulk_insert values: buffer.uniq
    end

    def self.delete_all_with_limit
      # Rails doesn't support delete_all with limit scope
      connection.exec_delete("DELETE FROM #{table_name} ORDER BY updated_at ASC LIMIT #{BqStream.batch_size}", 'DELETE', [])
      # make sure to use the default sort order for ORDER BY
    end

    def self.build_table
      self.table_name = BqStream.queued_items_table_name

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
