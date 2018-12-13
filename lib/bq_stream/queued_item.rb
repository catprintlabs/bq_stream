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
  end
end
