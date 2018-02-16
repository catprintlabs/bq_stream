class ActiveRecord::Base
  def transaction_changed_attributes
    @transaction_changed_attributes ||= HashWithIndifferentAccess.new
  end

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
      if BqStream.back_date
        bq_atr_of_interest.each do |attribute|
          record = BqStream::OldestRecord.find_by(table_name: name, attr: attribute)
          BqStream::OldestRecord.create(table_name: name, attr: attribute) unless record
        end
      end

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

  def queue_create(attributes_of_interest)
    attributes.each do |k, v|
      next unless attributes_of_interest.include?(k.to_sym) && !v.nil?
      BqStream::QueuedItem.create(table_name: self.class.to_s,
                                  record_id: id, attr: k,
                                  new_value: v.to_s)
    end
  end

  def queue_update(attributes_of_interest)
    transaction_changed_attributes.each do |k, v|
      next unless attributes_of_interest.include?(k.to_sym)
      BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id,
                                  attr: k, new_value: v.to_s)
    end
  rescue Exception => e
    BqStream.error_logger.error("#{Time.now}: EXCEPTION: #{e}")
  end unless RUBY_ENGINE == 'opal'

  def queue_destroy
    BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id,
                                attr: 'Destroyed', new_value: 'True')
  end
end
