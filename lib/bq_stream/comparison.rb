module BqStream
  module Comparison
    def big_query_check(klass, dataset, table, qty)
      create_bq_writer
      bq_gather_and_compare(klass, dataset, table, qty)
    end

    def database_check(klass, date_attr, dataset, table, qty = nil, start_date = Time.current.beginning_of_week - 1.week, end_date =  Time.current.end_of_week - 1.week)
      create_bq_writer
      query_db_records(klass.classify.constantize, date_attr, qty, start_date, end_date)
      collect_bq_records(dataset, table, @db_ids)
      bq_gather_and_compare(klass, dataset, table, qty)
    end

    protected

    def bq_gather_and_compare(klass, dataset, table, qty)
      query_bq_records(klass, dataset, table, qty)
      compare_(@records, klass)
      @results.each { |result| puts result }
    end

    def query_bq_records(klass, dataset, table, qty)
      @schema = []
      total_rows = @bq_writer.query("SELECT count(*) FROM [#{dataset}.#{table}] WHERE table_name = '#{klass}'")['rows'].first['f'].first['v']
      @bq_query = @bq_writer.query("SELECT * FROM (SELECT *, RANK() OVER(PARTITION BY record_id, attr ORDER BY updated_at DESC) rank FROM [#{dataset}.#{table}]) WHERE table_name = '#{klass}' AND RAND() < #{qty * 4}/#{total_rows} AND rank=1 LIMIT #{qty}")
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

    def collect_bq_records(dataset, table, ids)
      @bq_query = @bq_writer.query("SELECT * FROM [#{dataset}.#{table}] WHERE record_id IN (#{ids.to_s.gsub(/\[|\]/, '')})")
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

    def compare_(records, klass)
      @results = []
      records.each do |record|
        @fails = []
        if record['record_id'].nil?
          @results << "#{klass} Record #{record['record_id']} Failed, id is nil!"
        else
          db_object = klass.classify.constantize
                           .find(record['record_id'])
          if db_object.respond_to?(record['attr'])
            check_for_failures(klass.classify.constantize, record['attr'], record['new_value'], db_object)
          end
        end
        process_(klass, record, @fails)
      end
    end

    def check_for_failures(klass, attr, val, db)
      if klass.columns_hash[attr].type == :string && db.send(attr) != val
        @fails << "  #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val}"
      elsif klass.columns_hash[attr].type == :boolean
        val = true if val == 'true'
        val = false if val == 'false'
        if db.send(attr) != val
          @fails << "  #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val}"
        end
      elsif klass.columns_hash[attr].type == :integer &&
            db.send(attr) != (val.nil? ? nil : val.to_i)
        @fails << "  #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val.to_i}"
      elsif klass.columns_hash[attr].type == :decimal &&
            db.send(attr) != (val.nil? ? nil : val.to_d.round(5))
        @fails << "  #{attr}: #{db.send(attr)} != #{val.nil? ? 'nil' : val.to_d}"
      elsif klass.columns_hash[attr].type == :datetime &&
            db.send(attr).try(:utc) != (val.nil? ? nil : Time.at(val.to_datetime).utc)
        @fails << "  #{attr}: #{db.send(attr).try(:utc)} != #{val.nil? ? 'nil' : (Time.at(val.to_datetime).utc)}"
      end
    end

    def process_(klass, record, fails)
      unless fails.empty?
        @results << "#{klass} Record #{record['record_id']} "\
                    'Failed with these errors (db != bq)'
        fails.each { |fail| @results << fail }
      end
    end
  end
end
