module BqStream
  class OldestRecord < ActiveRecord::Base
    def self.update_bq_earliest(&block)
      Rollbar.log('info', 'BqStream', message: "#{OldestRecord.count rescue 0} in update_bq_earliest in OldestRecord")
      OldestRecord.all.each { |old_rec| old_rec.update_oldest_records(&block) }
    end

    def update_oldest_records
      # Rollbar.log('info', 'BqStream', message: "#{OldestRecord.count rescue 0} before destroy in OldestRecord")
      destroy && return if older_records.empty?
      # Rollbar.log('info', 'BqStream', message: "#{OldestRecord.count rescue 0} after destroy in OldestRecord")
      return if BqStream.available_rows.zero?
      Rollbar.log('info', 'BqStream', message: "#{older_records.count rescue 0} Older Records for #{table_class}.#{attr} with #{BqStream.available_rows} available rows")
      records = older_records.limit(BqStream.available_rows)
      records.each { |r| yield self, r }
      Rollbar.log('info', 'BqStream', message: "attr #{attr} updated to #{records.last.updated_at}")
      update(bq_earliest_update: records.last.updated_at)
    end

    def table_class
      @table_class ||= table_name.constantize
    end

    def older_records
      table_class.where(
        'updated_at >= ? AND updated_at < ?',
        BqStream.back_date, bq_earliest_update || Time.now
      ).order('updated_at DESC')
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
      BqStream.oldest_record_log.info "#{Time.now}: Table Name: #{name}, Attr: #{attribute}, OldestRecord Count: #{BqStream::OldestRecord.count} and Record already exists #{!!record}"
    end

    do_not_synchronize rescue nil # if Hyperloop is running
  end
end
