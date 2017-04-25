module BqStream
  class OldestRecord < ActiveRecord::Base
    def self.update_bq_earliest(&block)
      Rollbar.log('info', 'BqStream', message: "#{OldestRecord.count rescue 0} in OldestRecord")
      OldestRecord.all.each { |old_rec| old_rec.update_oldest_records(&block) }
    end

    def update_oldest_records
      Rollbar.log('info', 'BqStream', message: "#{older_records.count rescue 0} Older Records for #{attr} with #{BqStream.available_rows} available rows")
      destroy && return if older_records.empty?
      return if BqStream.available_rows.zero?
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
      self.table_name = BqStream.oldest_record_table_name

      connection.create_table(table_name, force: true) do |t|
        t.string   :table_name
        t.string   :attr
        t.datetime :bq_earliest_update
      end
    end
  end
end
