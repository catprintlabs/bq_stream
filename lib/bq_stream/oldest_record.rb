module BqStream
  class OldestRecord < ActiveRecord::Base
    # Runs through latest records for each table name in Oldestrecord
    # filling buffer until reaching batch size; then writes to QueuedItem
    def self.update_bq_earliest
      # Clear the buffer, just in case it is not empty
      BqStream::QueuedItem.buffer.clear
      # Check to see if room availble in batch and if rows exist
      until BqStream::QueuedItem.available_rows.zero? || table_names.empty?
        # Cycle through table names and grab latest records for each one
        table_names.each { |table| update_oldest_records_for(table) }
      end
      # Create Queued Items from the data in the buffer
      BqStream::QueuedItem.create_from_buffer
      # Clear the buffer after all is said and done
      BqStream::QueuedItem.buffer.clear

      # TODO:
      # Grab the table with the latest date updated (first if equal)
      # Only run one table per qequeue process
    end

    # Return a unique list of table names excluding revision row
    def self.table_names
      where("table_name <> '! revision !'").pluck(:table_name).uniq
    end

    # Adds record to buffer
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
      # Grab all rows with the same table name
      oldest_attr_recs = where('table_name = ?', table)
      # Grab the earliest bq_earliest_update (datetime)
      # for given rows in given table name
      earliest_update =
        oldest_attr_recs.map(&:bq_earliest_update).reject(&:nil?).uniq.min
      BqStream.log(:info, "#{Time.now}: Table #{table} "\
                   "count #{oldest_attr_recs.count}")
      # Grab the next records back in time. This will most likely be one record,
      # unless there are records create at the exact same time
      next_records = records_to_write(table.constantize, earliest_update)
      BqStream.log(:info, "#{Time.now}: $$$$$ Earliest Time #{earliest_update}"\
                   " Blank? #{earliest_update.blank?} $$$$$")
      # Check if we have any records to be queued for BigQuery
      if next_records.nil?
        BqStream.log(:info, "#{Time.now}: >>>>> Deleting & Returning <<<<<")
        BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                     "For #{table} Ending <<<<<")
        # If there are no next_records, destroy all lines in
        # OldestRecord table with the given table name
        oldest_attr_recs.delete_all && return
      else
        # Cycle through gathered OldestRecord rows...
        oldest_attr_recs.each do |oldest_attr_rec|
          # ...with each of the next records to be written...
          next_records.each do |next_record|
            BqStream.log(:info, "#{Time.now}: oldest_attr_recs id "\
                         "#{next_record.id}")
            # ...place data into buffer based on attr of OldestRecord row
            oldest_attr_rec.buffer_attribute(next_record)
          end
        end
        # Make all gathered OldestRecord rows to lastest created_at
        oldest_attr_recs.update_all(bq_earliest_update: next_records.first.created_at)
        BqStream.log(:info, "#{Time.now}: #{BqStream::QueuedItem.buffer.count}")
        BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                            "For #{table} Ending <<<<<")
      end
    end

    # Grabs the next record of given table name and any other records
    # of the same table that have the same created_at
    def self.records_to_write(table, earliest_update)
      # Grab the created_at of the record before the record with
      # the earliest update. This is done instead of grabbing the record itself,
      # in case more than one was created at the same time
      next_created_at = table.where(
        'created_at >= ? AND created_at < ?',
        BqStream.back_date, earliest_update || Time.now
      ).order('created_at DESC').pluck(:created_at).first

      # Get any records that have the next_created_at date
      # These are to be the next_records to be processed
      table.where('created_at = ?', next_created_at) if next_created_at
      
      # TODO:
      # Add notes to speed this up
    end

    # Builds OldestRecord table
    # Drops and rebuilds table if it doesn't exist or after eaach new deployment  
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
