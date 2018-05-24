module BqStream
  module Comparison
    def big_query_check(klass, dataset, table, qty)
      create_bq_writer
      bq_gather_and_compare(klass, dataset, table, qty)
    end

    def database_check(klass, date_attr, dataset, table, qty = nil, start_date = Time.current.beginning_of_week - 1.week, end_date =  Time.current.end_of_week - 1.week)
      create_bq_writer
      query_db_records(klass.classify.constantize, date_attr, qty, start_date, end_date)
      collect_bq_records(klass, dataset, table, @db_ids)
      bq_gather_and_compare(klass, dataset, table, qty)
    end

    protected

    def bq_gather_and_compare(klass, dataset, table, qty)
      query_bq_records(qty, dataset, table)
      compare_(@records, klass.classify.constantize)
      @results.each { |result| puts result }
    end

    def query_bq_records(qty, dataset, table)
      @schema = []
      total_rows = @bq_writer.query("SELECT count(*) FROM [#{dataset}.#{table}]")['rows'].first['f'].first['v']
      @bq_query ||= @bq_writer.query("SELECT * FROM [#{dataset}.#{table}] WHERE RAND() < #{qty * 2}/#{total_rows} LIMIT #{qty}")

      @bq_query['schema']['fields'].each do |f|
        @schema << f['name']
      end
      @schema.uniq!
      build_records(@bq_query['rows'])
    end

    def query_db_records(klass, date_attr, qty, start_date, end_date)
      @db_ids = []
      db_records =
        if qty
          klass.where("#{date_attr} > ? AND #{date_attr} < ?", start_date, end_date).shuffle.first(qty)
        else
          klass.where("#{date_attr} > ? AND #{date_attr} < ?", start_date, end_date)
        end
      db_records.each { |r| @db_ids << r.id }
    end

    def collect_bq_records(klass, dataset, table, ids)
      @bq_query = @bq_writer.query("SELECT * FROM [#{dataset}.#{table}] WHERE #{klass}_record_id IN (#{ids.to_s.gsub(/\[|\]/, '')})")
    end

    def build_records(rows)
      @records = []
      rows.each do |row|
        values = []
        row['f'].each_with_index do |value, index|
          values << [@schema[index], value['v']]
        end
        @records << values.to_h
      end
    end

    def compare_(records, class_name)
      @results = []
      records.each do |record|
        @fails = []
        if record['friendly_id'].nil?
          @results << "Order Record #{record['order_record_id']} Failed, friendly_id is nil!"
        else
          record.each do |k, v|
            db_object = class_name.find_by_friendly_id(record['friendly_id'])
            check_for_failures(class_name, k, v, db_object) if db_object.respond_to?(k)
          end
        end
        process_(record, @fails)
      end
    end

    def check_for_failures(klass, attr, val, db)
      if %i[string boolean].include?(klass.columns_hash[attr].type) &&
         db.send(attr) != val
        @fails << " #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val}"
      elsif klass.columns_hash[attr].type == :integer &&
            db.send(attr) != (val.nil? ? nil : val.to_i)
        @fails << " #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val.to_i}"
      elsif klass.columns_hash[attr].type == :decimal &&
            db.send(attr) != (val.nil? ? nil : val.to_d)
        @fails << " #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val.to_d}"
      elsif klass.columns_hash[attr].type == :datetime &&
            db.send(attr).try(:utc) != (val.nil? ? nil : (Time.at(val.to_d).utc + 4.hours))
        @fails << " #{attr}: #{db.send(attr).try(:utc)} != #{val.nil? ? 'nil' : (Time.at(val.to_d).utc + 4.hours)}"
      end
    end

    def process_(record, fails)
      unless record['friendly_id'].nil?
        unless fails.empty?
          @results << "Order Record #{record['order_record_id']} "\
                      'Failed with these errors (db != bq)'
          fails.each { |fail| @results << fail }
        end
      end
    end
  end
end
