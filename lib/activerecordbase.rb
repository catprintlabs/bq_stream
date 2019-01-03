class ActiveRecord::Base
  def transaction_changed_attributes
    @transaction_changed_attributes ||= HashWithIndifferentAccess.new
  end

  def self.bq_attributes(opts = {})
    unless RUBY_ENGINE == 'opal' || !BqStream.dataset
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

      # Add or remove bq_attributes to/from BqAttribute and OldestRecord for table (name)
      BqStream.update_bq_attribute_records(name, bq_atr_of_interest)

      after_save do
        changes.each do |k, v|
          transaction_changed_attributes[k] = v[1]
        end
      end

      after_commit on: [:create] do
        queue_create(bq_atr_of_interest)
        @transaction_changed_attributes = nil
      end

      after_commit on: [:update] do
        queue_update(bq_atr_of_interest)
        @transaction_changed_attributes = nil
      end

      after_commit on: [:destroy] do
        queue_destroy
        @transaction_changed_attributes = nil
      end

      after_rollback do
        @transaction_changed_attributes = nil
      end
    end
  end

  def create_queued_item(key, value)
    new_val = (value && self.class.type_for_attribute(key).type == :datetime ? value.in_time_zone(BqStream.timezone) : value).to_s
    BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id, attr: key, new_value: new_val)
  end

  def queue_create(attributes_of_interest)
    attributes.each do |k, v|
      next unless attributes_of_interest.include?(k.to_sym)
      create_queued_item(k, v)
    end
  rescue Exception => e
    BqStream.log(:error, "#{Time.now}: EXCEPTION: #{e}")
  end unless RUBY_ENGINE == 'opal'

  def queue_update(attributes_of_interest)
    transaction_changed_attributes.each do |k, v|
      next unless attributes_of_interest.include?(k.to_sym)
      create_queued_item(k, v)
    end
  rescue Exception => e
    BqStream.log(:error, "#{Time.now}: EXCEPTION: #{e}")
  end unless RUBY_ENGINE == 'opal'

  def queue_destroy
    BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id,
                                attr: 'Destroyed', new_value: 'True')
  end
end
