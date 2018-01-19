class ActiveRecord::Base
  # class << self
  #   alias before_bq_stream_after_commit after_commit
  #   def after_commit(*args, &block)
  #     before_bq_stream_after_commit(*args, &block)
  #     unless method_defined? :before_bq_stream_run_commit_callbacks
  #       binding.pry
  #       alias before_bq_stream_run_commit_callbacks _run_commit_callbacks
  #       alias before_bq_stream_run_rollback_callbacks _run_rollback_callbacks
  #       define_method :_run_commit_callbacks do
  #         begin
  #           before_bq_stream_run_commit_callbacks
  #         ensure
  #           @transaction_changed_attributes = nil
  #         end
  #       end
  #       define_method :_run_rollback_callbacks do
  #         begin
  #           before_bq_stream_run_rollback_callbacks
  #         ensure
  #           @transaction_changed_attributes = nil
  #         end
  #       end
  #     end
  #   end
  # end

  #   def _run_commit_callbacks
  #     super
  #   ensure
  #     @transaction_changed_attributes = nil
  #   end
  #
  #   def _run_rollback_callbacks
  #     super
  #   ensure
  #     @transaction_changed_attributes = nil
  #   end

  # class << self
  #   alias before_bq_stream_inherited inherited
  #   def inherited(child)
  #
  #     before_bq_stream_inherited(child)
  #     child.class_eval do
  #       puts "++++++++++++++++++++++++++++++ defining #{child}#write_attribute ++++++++++++++++++++++++++++++++++++++++++++++++++"
  #       define_method(:write_attribute) do |attr_name, value|
  #         attr_name = attr_name.to_s
  #         old_value = read_attribute(attr_name)
  #         ret = super(attr_name, value)
  #         puts "@@@@@@@@@@@@@@@@@@@@@@@@@@@@ #{self.object_id} | #{transaction_changed_attributes.length}| #{attr_name} | #{value} @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@" if self.class == Order
  #         unless transaction_changed_attributes.key?(attr_name) || value == old_value
  #           transaction_changed_attributes[attr_name] = old_value
  #         end
  #         ret
  #       end
  #     end
  #   end
  # end

  def transaction_changed_attributes
    @transaction_changed_attributes ||= HashWithIndifferentAccess.new
  end

###############
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

      # after_commit on: [:create] do
      #   queue_create(bq_atr_of_interest)
      #   @transaction_changed_attributes = nil
      # end

      after_save do
        binding.pry if self.class == Order
        changes.each do |k, v|
          transaction_changed_attributes[k] = v[1]
          puts "===================== #{self.class} | #{k} | #{v} | #{v[1]}"
        end
      end

      after_commit on: [:create, :update] do
        if self.class == Order
          puts "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& #{self.object_id} | #{transaction_changed_attributes.length} &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
        end
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
    BqStream.log.info "#{Time.now}: EXCEPTION: #{e}"
  end unless RUBY_ENGINE == 'opal'

  def queue_destroy
    BqStream::QueuedItem.create(table_name: self.class.to_s, record_id: id,
                                attr: 'Destroyed', new_value: 'True')
  end
end
