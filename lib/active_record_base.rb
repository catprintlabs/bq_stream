class ActiveRecord::Base
  def self.bq_attributes(opts = {})
    if opts == :all
      column_names.map(&:to_sym)
    elsif opts[:only]
      raise 'opts must be an array' unless opts[:only].is_a? Array
      opts[:only].map(&:to_sym)
    elsif opts[:except]
      raise 'opts must be an array.' unless opts[:except].is_a? Array
      all_atr = column_names.map(&:to_sym)
      opts[:except].map(&:to_sym).each do |del|
        all_atr.delete_at(all_atr.index(del))
      end
      all_atr
    else
      raise 'You must declare an opts hash with a key of :all, :only or :except) '\
        'and a value as an array, if using :only or :except.'
    end
  end
end
