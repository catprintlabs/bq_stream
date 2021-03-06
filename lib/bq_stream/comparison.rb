module BqStream
  module Comparison
    def big_query_check(klass, dataset, table, qty)
      create_bq_writer
      bq_gather_and_compare(klass, dataset, table, qty)
      display_results
    end

    def database_check(klass, date_attr, dataset, table, qty = nil, start_date = Time.current.beginning_of_week - 1.week, end_date =  Time.current.end_of_week - 1.week)
      create_bq_writer
      query_db_records(klass.classify.constantize, date_attr,
                       qty, start_date, end_date)
      collect_bq_records(klass, dataset, table, @db_ids)
      compare_records(@db_records, @bq_records, klass)
      display_results
    end

    protected

    def bq_gather_and_compare(klass, dataset, table, qty)
      query_bq_records(klass, dataset, table, qty)
      compare_(@bq_records, klass)
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
      @db_records =
        if qty
          klass.where("#{date_attr} > ? AND #{date_attr} < ?", start_date, end_date).shuffle.first(qty)
        else
          klass.where("#{date_attr} > ? AND #{date_attr} < ?", start_date, end_date)
        end
      @db_records.each { |r| @db_ids << r.id }
    end

    def collect_bq_records(klass, dataset, table, ids)
      @schema = []
      if !ids.blank?
        @bq_query = @bq_writer.query("SELECT * FROM (SELECT *, RANK() OVER(PARTITION BY record_id, attr ORDER BY updated_at DESC) rank FROM [#{dataset}.#{table}]) WHERE table_name = '#{klass}' AND record_id IN (#{ids.to_s.gsub(/\[|\]/, '')}) AND rank=1")
        @bq_query['schema']['fields'].each do |f|
          @schema << f['name']
        end
        @schema.uniq!
        build_records(@bq_query['rows'])
      else
        @bq_records = []
      end
    end

    def build_records(rows)
      @bq_records = []
      if rows
        rows.each do |row|
          values = []
          row['f'].each_with_index do |value, index|
            values << [@schema[index], value['v']]
          end
          @bq_records << values.to_h
        end
      end
    end

    def compare_(records, klass)
      @results = []
      if !records.count.zero?
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
      else
        @results << '!!! No Records Found in BigQuery !!!'
      end
    end

    def compare_records(db_records, bq_records, klass)
      @results = []
      if bq_records.blank?
        @results << '!!! No records found in BigQuery !!!'
      else
        bq_records.each do |bq_record|
          @fails = []
          if bq_record['record_id'].nil?
            @results << "#{klass} Record #{bq_record['record_id']} Failed, id is nil!"
          else
            # db_object = db_records.find(bq_record['record_id'])
            db_object = db_records.select { |record| record.id == bq_record['record_id'].to_i }.first
            if db_object && db_object.respond_to?(bq_record['attr'])
              check_for_failures(klass.classify.constantize, bq_record['attr'], bq_record['new_value'], db_object)
            else
              @results << "#{klass} Record #{bq_record['record_id']} Failed, not found in the database!"
            end
          end
          process_(klass, bq_record, @fails)
        end
      end
    end

    def check_for_failures(klass, attr, val, db)
      if klass.columns_hash[attr].type == :string && db.send(attr).to_s != val
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
            db.send(attr).try(:in_time_zone, BqStream.timezone) != (val.nil? ? nil : Time.at(val.to_datetime).in_time_zone(BqStream.timezone))
        @fails << "  #{attr}: #{db.send(attr).try(:in_time_zone, BqStream.timezone)} != #{val.nil? ? 'nil' : (Time.at(val.to_datetime).in_time_zone(BqStream.timezone))}"
      end
    end

    def process_(klass, record, fails)
      unless fails.empty?
        @results << "#{klass} Record #{record['record_id']} "\
                    'Failed with these errors (db != bq)'
        fails.each { |fail| @results << fail }
      end
    end

    def display_results
      if @results.empty?
        puts '!!! Comparison Matches Successfully !!!'
      else
        @results.each { |result| puts result }
      end
    end
  end
end
