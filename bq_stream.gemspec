# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'bq_stream/version'

Gem::Specification.new do |spec|
  spec.name          = 'bq_stream'
  spec.version       = BqStream::VERSION
  spec.authors       = ['Jon Weaver']
  spec.email         = ['jon@catprint.com']

  spec.summary       = 'This is a gem to write records '\
                       'from your database to BigQuery'
  # spec.description   = %q{Write a longer description or delete this line.}
  # spec.homepage      = "Put your gem's website or public repo URL here."
  spec.license       = 'MIT'

  # Prevent pushing this gem to RubyGems.org.
  # To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host
  # or delete this section to allow pushing to any host.
  # if spec.respond_to?(:metadata)
  #   spec.metadata['allowed_push_host'] = 'http://rubygems.org'
  # else
  #   raise 'RubyGems 2.0 or newer is required '\
  #         'to protect against public gem pushes.'
  # end

  spec.files = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_dependency 'activerecord', '>= 5.2'
  spec.add_dependency 'dotenv'

  spec.add_development_dependency 'bigquery', '~>0.9.0'
  spec.add_development_dependency 'bulk_insert'
  spec.add_development_dependency 'bundler', '< 2'
  spec.add_development_dependency 'database_cleaner'
  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'pry-rescue'
  spec.add_development_dependency 'pry-byebug'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '~> 3.2'
  spec.add_development_dependency 'rubocop'
  spec.add_development_dependency 'rubocop-performance'
  spec.add_development_dependency 'sqlite3', '~> 1.3.6'
  spec.add_development_dependency 'timecop'
end
