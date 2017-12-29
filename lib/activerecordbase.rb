class ActiveRecord::Base
  def self.bq_attributes(opts = {})
    unless RUBY_ENGINE == 'opal'
      if opts == :all
        bq_atr_of_interest = column_names.map(&:to_sym)
      elsif opts[:only]
        raise 'opts must be an array' unless opts[:only].is_a? Array
        bq_atr_of_interest = opts[:only].map(&:to_sym)
      elsif opts[:except]
        raise 'opts must be an array.' unless opts[:except].is_a? Array
        bq_atr_of_interest = column_names.map(&:to_sym).select do |column|
          !opts[:except].include?(column)
        end
      else
        raise 'You must declare an opts hash with a key of :all, :only '\
          'or :except) and a value as an array, if using :only or :except.'
      end
      bq_atr_of_interest.each do |attribute|
        record = BqStream::OldestRecord.find_by(table_name: name, attr: attribute)
        BqStream::OldestRecord.create(table_name: name, attr: attribute) unless record
      end if BqStream.back_date
      after_create { queue_default(bq_atr_of_interest) }
      after_save { queue_item(bq_atr_of_interest) }
      after_destroy do
        BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id)
      end
    end
  end

  def queue_default(attributes_of_interest)
    items = self.class.columns_hash.collect do |k, v|
      if attributes_of_interest.include?(k.to_sym) && !v.default.nil? && changes.exclude?(k)
        { k => v.default }
      end
    end.compact
    BqStream.logger.info "#{Time.now}: |Queue Default|" if items.present?
    items.each do |i|
      BqStream.logger.info "#{Time.now}: Record: #{id} | Table: #{self.class} | Attr: #{i.keys.first} | Value: #{i[i.keys.first]}"
      BqStream::QueuedItem.create(table_name: self.class.to_s,
                                  record_id: id, attr: i.keys.first.to_sym,
                                  new_value: i[i.keys.first].to_s)
    end
  end

  def queue_item(attributes_of_interest)
    BqStream.logger.info "#{Time.now}: |Queue Item|" if changes.present?
    changes.each do |k, v|
      if attributes_of_interest.include?(k.to_sym)
        BqStream.logger.info "#{Time.now}: Record: #{id} | Table: #{self.class} | Attr: #{k} | Value: #{v[1]}"
        BqStream::QueuedItem.create(table_name: self.class.to_s,
                                    record_id: id, attr: k,
                                    new_value: v[1].to_s)
      end
    end
  rescue Exception => e
    BqStream.log.info "#{Time.now}: EXCEPTION: #{e}"
  end unless RUBY_ENGINE == 'opal'
end
