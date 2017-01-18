module BqStream
  class OldestRecord < ActiveRecord::Base
    def self.update_bq_earliest(&block)
      OldestRecord.all.each { |old_rec| old_rec.update_oldest_records(&block) }
    end

    def update_oldest_records
      destroy && return if older_records.empty?
      older_records.each { |r| yield self, r }
      update(bq_earliest_update: older_records.first.updated_at)
    end

    def table_class
      @table_class ||= table_name.constantize
    end

    def older_records
      table_class.where(
        'updated_at < ? AND updated_at >= ?',
        bq_earliest_update || Time.now, BqStream.back_date
      ).order('updated_at DESC').limit(BqStream.available_rows)
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
