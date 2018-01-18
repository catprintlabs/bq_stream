class ActiveRecord::Base
  # alias _previous_run_commit_callbacks _run_commit_callbacks
  # alias _previous_run_rollback_callbacks _run_rollback_callbacks
  #
  # def _run_commit_callbacks
  #   _previous_run_commit_callbacks
  # ensure
  #   @transaction_changed_attributes = nil
  # end
  #
  # def _run_rollback_callbacks
  #   _previous_run_rollback_callbacks
  # ensure
  #   @transaction_changed_attributes = nil
  # end
  #
  # def transaction_changed_attributes
  #   @transaction_changed_attributes ||= HashWithIndifferentAccess.new
  # end
  #
  # method_name = if ActiveRecord.gem_version >= Gem::Version.new("5.2.0.beta1")
  #   "_write_attribute"
  # else
  #   "write_attribute"
  # end
  # alias_method :_previous_write_attribute, method_name.to_sym
  #
  # define_method(method_name) do |attr_name, value|
  #   attr_name = attr_name.to_s
  #   old_value = read_attribute(attr_name)
  #   ret = _previous_write_attribute(attr_name, value)
  #   unless transaction_changed_attributes.key?(attr_name) || value == old_value
  #     transaction_changed_attributes[attr_name] = old_value
  #   end
  #   ret
  # end

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

      after_commit on: [:create] do
        queue_create(bq_atr_of_interest)
      end
      after_commit on: [:update] do
        queue_update(bq_atr_of_interest)
      end
      after_commit :queue_destroy, on: [:destroy]
    end
  end

  def queue_create(attributes_of_interest)
    prev_changes = []
    previous_changes.each_key do |key|
      prev_changes << key
    end

    attributes.each do |k, v|
      unless prev_changes.include?(k) || v.nil?
        BqStream.log.info "#{Time.now}: #{self.class} attribute #{k} is "\
          'not nil in attributes and not included in previous changes'
      end
      next unless attributes_of_interest.include?(k.to_sym) && !v.nil?
      BqStream::QueuedItem.create(table_name: self.class.to_s,
                                  record_id: id, attr: k,
                                  new_value: v.to_s)
    end
  end

  def queue_update(attributes_of_interest)
    previous_changes.each do |k, v|
      next unless attributes_of_interest.include?(k.to_sym)
      BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id,
                                  attr: k, new_value: v[1].to_s)
    end
  rescue Exception => e
    BqStream.log.info "#{Time.now}: EXCEPTION: #{e}"
  end unless RUBY_ENGINE == 'opal'

  def queue_destroy
    BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id,
                                attr: 'Destroyed', new_value: 'True')
  end
end
