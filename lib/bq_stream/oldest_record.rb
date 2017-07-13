module BqStream
  class OldestRecord < ActiveRecord::Base
    def self.update_bq_earliest
      logging_code = rand(2**256).to_s(36)[0..3]
      BqStream::QueuedItem.buffer.clear
      # BqStream.logger.info "#{Time.now}: Queued Item Count: #{BqStream::QueuedItem.count} #{logging_code}" # seen each time
      # BqStream.logger.info "#{Time.now}: Available Rows zero?: #{BqStream::QueuedItem.available_rows.zero?} #{logging_code}" # seen each time
      BqStream.logger.info "#{Time.now}: Table Names Empty: #{table_names.empty?} #{logging_code}" # seen each time
      BqStream.logger.info "#{Time.now}: Table Names Count: #{table_names.count} #{logging_code}" # seen each time
      until BqStream::QueuedItem.available_rows.zero? || table_names.empty?
        table_names.each { |table| update_oldest_records_for(table) }
      end
      BqStream::QueuedItem.create_from_buffer
      BqStream::QueuedItem.buffer.clear
    end

    def self.table_names
      where("table_name <> '! revision !'").pluck(:table_name).uniq
    end

    def buffer_attribute(r)
      BqStream::QueuedItem.buffer << { table_name: table_name,
                                       record_id: r.id,
                                       attr: attr,
                                       new_value: r[attr],
                                       updated_at: r.created_at }
    end

    def self.update_oldest_records_for(table)
      # BqStream.logger.info "#{Time.now}: Update Oldest Record"
      # BqStream.logger.info "#{Time.now}: Table #{table}"
      oldest_attr_recs = where('table_name = ?', table)
      BqStream.logger.info "#{Time.now}: oldest_attr_recs count #{oldest_attr_recs.count}"
      next_record = next_record_to_write(table.constantize, oldest_attr_recs.map(&:bq_earliest_update).uniq.min)
      BqStream.logger.info "#{Time.now}: oldest_attr_recs #{next_record rescue nil}"
      oldest_attr_recs.delete_all && return unless next_record
      oldest_attr_recs.each do |oldest_attr_rec|
        oldest_attr_rec.buffer_attribute(next_record)
      end
      BqStream.logger.info "#{Time.now}: Updating All"
      oldest_attr_recs.update_all(bq_earliest_update: next_record.created_at)
    end

    def self.next_record_to_write(table, earliest_update)
      BqStream.logger.info "#{Time.now}: Next Record to Write"
      table.where(
        'created_at >= ? AND created_at < ?',
        BqStream.back_date, earliest_update || Time.now
      ).order('created_at DESC').first
    end

    def self.build_table
      return if connection.tables.include?(BqStream.oldest_record_table_name) && find_by(table_name: '! revision !', attr: `cat #{Rails.root}/REVISION`)
      self.table_name = BqStream.oldest_record_table_name
      connection.create_table(table_name, force: true) do |t|
        t.string   :table_name
        t.string   :attr
        t.datetime :bq_earliest_update
      end

      create(table_name: '! revision !', attr: `cat #{Rails.root}/REVISION`)
    end

    do_not_synchronize rescue nil # if Hyperloop is running
  end
end
