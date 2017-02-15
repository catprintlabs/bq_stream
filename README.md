# BqStream

BqStream can be used Google's BigQuery to send selected model attribute values when records are created, updated and destroyed.  It was created to help better analyze ever-changing records within a project's database.

## Installation

Add the following lines to your application's Gemfile:

```ruby
gem 'bq_stream'
gem 'bigquery', '~>0.9.0'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install bq_stream

## Usage

TODO: Write usage instructions here

## Development

After checking out the repo, run `bin/setup` to install dependencies. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`.

## Testing

An RSpec test suite is available to ensure proper functionality. Run `rake spec` to run the tests. There is also a test with a live connection to BigQuery that be called by using:

    $ rspec spec/bq_example

Before live testing you should be sure to have created an `.env` file in the root directory with the followng Google BigQuery credentials:

  + Client Id
  + Service Email
  + KEY
  + Project Id
  + Dataset

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/catprintlabs/bq_stream. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.


## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
