class ActiveRecord::Base
  def self.bq_attributes(opts = {})
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
    after_save { queue_item(bq_atr_of_interest) }
    after_destroy do
      BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id)
    end
  end

  def queue_item(attributes_of_interest)
    changes.each do |k, v|
      if attributes_of_interest.include?(k.to_sym)
        BqStream::QueuedItem.create(table_name: self.class.to_s,
                                    record_id: id, attr: k,
                                    new_value: v[1].to_s)
      end
    end
  end

  def dequeue_item
  end
end