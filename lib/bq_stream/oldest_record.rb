module BqStream
  class OldestRecord < ActiveRecord::Base
    def self.update_bq_earliest
      BqStream::QueuedItem.buffer.clear
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
      BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                   "For #{table} Starting <<<<<")
      oldest_attr_recs = where('table_name = ?', table)
      earliest_update =
        oldest_attr_recs.map(&:bq_earliest_update).reject(&:nil?).uniq.min
      BqStream.log(:info, "#{Time.now}: Table #{table} "\
                   "count #{oldest_attr_recs.count}")
      next_record = next_record_to_write(table.constantize, earliest_update)
      BqStream.log(:info, "#{Time.now}: $$$$$ Earliest Time #{earliest_update}"\
                   " Blank? #{earliest_update.blank?} $$$$$")
      if next_record
        BqStream.log(:info, "#{Time.now}: oldest_attr_recs id "\
                     "#{next_record.id rescue nil}")
      else
        BqStream.log(:info, "#{Time.now}: >>>>> Deleting & Returning <<<<<")
        BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                     "For #{table} Ending <<<<<")
      end
      BqStream.log(:info, "#{Time.now}: $$$$$ Delete & Return unless this "\
        "is true => #{ next_record && !earliest_update.blank? } $$$$$")
      oldest_attr_recs.delete_all && return unless next_record &&
                                                   !earliest_update.blank?
      oldest_attr_recs.each do |oldest_attr_rec|
        oldest_attr_rec.buffer_attribute(next_record)
      end
      oldest_attr_recs.update_all(bq_earliest_update: next_record.created_at)
      BqStream.log(:info, "#{Time.now}: #{BqStream::QueuedItem.buffer.count}")
      BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                          "For #{table} Ending <<<<<")
    end

    def self.next_record_to_write(table, earliest_update)
      table.where(
        'created_at >= ? AND created_at < ?',
        BqStream.back_date, earliest_update || Time.now
      ).order('created_at DESC').first
    end

    def self.build_table
      return if connection.tables.include?(BqStream.oldest_record_table_name) && find_by(table_name: '! revision !', attr: `cat #{File.expand_path ''}/REVISION`)
      self.table_name = BqStream.oldest_record_table_name
      connection.create_table(table_name, force: true) do |t|
        t.string   :table_name
        t.string   :attr
        t.datetime :bq_earliest_update
      end

        create(table_name: '! revision !', attr: `cat #{File.expand_path ''}/REVISION`)
    end

    do_not_synchronize rescue nil # if Hyperloop is running
  end
end
