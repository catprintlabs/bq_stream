module BqStream
  class OldestRecord < ActiveRecord::Base
    self.table_name = 'bq_stream_oldest_records'
    # Runs through latest records for each table name in Oldestrecord
    # filling buffer until reaching batch size; then writes to QueuedItem
    def self.update_bq_earliest
      BqStream.log(:info, "#{Time.now}: Start of update_bq_earliest Oldest Record count: #{count}")
      # Clear the buffer, just in case it is not empty
      BqStream::QueuedItem.buffer.clear
      # Check to see if room availble in batch and if rows exist
      until BqStream::QueuedItem.available_rows.zero? || where(archived: false).empty?
        BqStream.log(:info, "#{Time.now}: Start, while there are available rows, Oldest Record count: #{count}")
        BqStream.log(:info, "#{Time.now}: ***** Start current rows in OldestRecord *****")
        all.order(:table_name, :attr).each do |record|
          BqStream.log(:info, "#{Time.now}: #{record.id} #{record.table_name} #{record.attr} #{record.bq_earliest_update}")
        end
        BqStream.log(:info, "#{Time.now}: ***** End current rows in OldestRecord *****")
        # Cycle through table names and grab latest records for each one
        table_names.each do |table|
          BqStream.log(:info, "#{Time.now}: Oldest Record count before update_oldest_records_for #{table}: #{where('table_name = ?', table).count}")
          update_oldest_records_for(table) unless where(table_name: table, archived: false).empty?
        end
        BqStream.log(:info, "#{Time.now}: End, while there are available rows, Oldest Record count: #{count}")
      end
      # Create Queued Items from the data in the buffer
      BqStream::QueuedItem.create_from_buffer
      # Clear the buffer after all is said and done
      BqStream::QueuedItem.buffer.clear
    end

    # Return a unique list of table names excluding revision row
    def self.table_names
      where("table_name <> '! revision !'").pluck(:table_name).uniq
    end

    # Adds record to buffer
    def buffer_attribute(r)
      new_val = r[attr] && table_name.constantize.type_for_attribute(attr).type == :datetime ? r[attr].in_time_zone(BqStream.timezone) : r[attr].to_s
      BqStream.log(:info, "#{Time.now}: Buffer #{table_name}, #{r.id}, #{attr}, #{new_val}, #{r.created_at}")
      BqStream::QueuedItem.buffer << { table_name: table_name,
                                       record_id: r.id,
                                       attr: attr,
                                       new_value: new_val,
                                       updated_at: r.created_at }
    end

    # Grabs next available record to be written, writes to buffer
    # and properly update OldestRecord rows
    def self.update_oldest_records_for(table)
      BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                   "For #{table} Starting <<<<<")
      BqStream.log(:info, "#{Time.now}: Buffer: #{BqStream::QueuedItem.buffer.count} / #{BqStream::QueuedItem.available_rows}")
      # Grab all rows with the same table name
      BqStream.log(:info, "#{Time.now}: Oldest Record count: #{count}")
      BqStream.log(:info, "#{Time.now}: Oldest Record count for #{table}: #{where('table_name = ?', table).count}")
      oldest_attr_recs = where('table_name = ? AND bq_earliest_update >= ?', table, BqStream.back_date)
      BqStream.log(:info, "#{Time.now}: Initial oldest_attr_recs count: #{oldest_attr_recs.count}")
      # Grab the earliest bq_earliest_update (datetime)
      # for given rows in given table name
      earliest_update =
        oldest_attr_recs.map(&:bq_earliest_update).compact.min
      # Grab the next records back in time. This will most likely be one record,
      # unless there are records create at the exact same time
      next_records = records_to_write(table.constantize, earliest_update)
      # Check if we have any records to be queued for BigQuery
      if next_records.nil?
        BqStream.log(:info, "#{Time.now}: >>>>> Returning <<<<<")
        BqStream.log(:info, "#{Time.now}: >>>>> Update Oldest Records "\
                     "For #{table} Ending <<<<<")
        where(table_name: table).each { |row| row.update(archived: true) }
        # Return if there are no next_records
        return
        # oldest_attr_recs.delete_all && return
      else
        # Cycle through gathered OldestRecord rows...
        BqStream.log(:info, "#{Time.now}: oldest_attr_recs count: #{oldest_attr_recs.count}")
        BqStream.log(:info, "#{Time.now}: next_records count: #{next_records.count}")
        oldest_attr_recs.uniq.each do |oldest_attr_rec|
          # ...with each of the next records to be written...
          next_records.each do |next_record|
            # ...place data into buffer based on attr of OldestRecord row
            oldest_attr_rec.buffer_attribute(next_record)
          end
        end
        # Make all OldestRecord rows for table to lastest created_at
        # it is possible to have no oldest_attr_recs but have next_records, because bq_earliest_update are all nil
        if oldest_attr_recs.empty?
          where(table_name: table).update_all(bq_earliest_update: next_records.first.created_at)
        else
          oldest_attr_recs.update_all(bq_earliest_update: next_records.first.created_at)
        end
        BqStream.log(:info, "#{Time.now}: after update oldest_attr_recs count: #{oldest_attr_recs.count}")
        BqStream.log(:info, "#{Time.now}: Buffer: #{BqStream::QueuedItem.buffer.count} / #{BqStream::QueuedItem.available_rows}")
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
        # created_at >= can cause an infinite loop if latest record is equal to back_date
        'created_at > ? AND created_at < ?',
        BqStream.back_date, earliest_update || Time.now
      ).order('created_at DESC').first.try(:created_at)

      # Get any records that have the next_created_at date
      # These are to be the next_records to be processed
      table.where('created_at = ?', next_created_at) if next_created_at
    end

    do_not_synchronize rescue nil # if Hyperloop is running
  end
end
