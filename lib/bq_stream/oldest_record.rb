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
      next_records = records_to_write(table.constantize, earliest_update)
      unless next_records.empty?
        next_records =
          next_records.where(created_at: next_records.first.created_at)
      end
      BqStream.log(:info, "#{Time.now}: $$$$$ Earliest Time #{earliest_update}"\
                   " Blank? #{earliest_update.blank?} $$$$$")
      if !next_records.empty?
        next_records.each do |next_record|
          BqStream.log(:info, "#{Time.now}: oldest_attr_recs id "\
                       "#{next_record.id}")
        end
      else
        BqStream.log(:info, "#{Time.now}: >>>>> Deleting & Returning <<<<<")
        BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                     "For #{table} Ending <<<<<")
      end
      BqStream.log(:info, "#{Time.now}: $$$$$ Delete & Return if this "\
        "is true => #{next_records.empty? && earliest_update.blank?} $$$$$")
      oldest_attr_recs.delete_all && return if next_records.empty? &&
                                               earliest_update.blank?
      oldest_attr_recs.each do |oldest_attr_rec|
        next_records.each do |next_record|
          oldest_attr_rec.buffer_attribute(next_record)
        end
      end
      oldest_attr_recs.update_all(bq_earliest_update: next_records.first.created_at)
      BqStream.log(:info, "#{Time.now}: #{BqStream::QueuedItem.buffer.count}")
      BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                          "For #{table} Ending <<<<<")
    end

    def self.records_to_write(table, earliest_update)
      table.where(
        'created_at >= ? AND created_at < ?',
        BqStream.back_date, earliest_update || Time.now
      ).order('created_at DESC')
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
