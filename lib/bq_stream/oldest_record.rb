module BqStream
  class OldestRecord < ActiveRecord::Base
    def self.update_bq_earliest(&block)
      BqStream.logger.info "#{Time.now}: #{OldestRecord.count} in OldestRecord"
      OldestRecord.all.each { |old_rec| old_rec.update_oldest_records(&block) }
    end

    def update_oldest_records
      Rollbar.info("BqStream Log: #{older_records.count} Older Records for "\
                   "#{attr} with #{available_rows} available rows")
      BqStream.logger.info "#{Time.now}: #{older_records.count} Older Records "\
                           "for #{attr} with #{available_rows} available rows"
      destroy && return if older_records.empty?
      return if BqStream.available_rows.zero?
      records = older_records.limit(BqStream.available_rows)
      records.each { |r| yield self, r }
      BqStream.logger.info "#{Time.now}: attr #{attr} updated to "\
                           "#{records.last.updated_at}"
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
