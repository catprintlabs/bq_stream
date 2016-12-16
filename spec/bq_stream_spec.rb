require 'spec_helper'

describe BqStream do
  it 'has a version number' do
    expect(BqStream::VERSION).not_to be nil
  end

  it 'can be configured' do
    BqStream.configuration do |config|
      config.client_id = 'client_id'
      config.service_email = 'service_email'
      config.key = 'key'
      config.project_id = 'project_id'
    end

    expect(BqStream.client_id).to eq('client_id')
    expect(BqStream.service_email).to eq('service_email')
    expect(BqStream.key).to eq('key')
    expect(BqStream.project_id).to eq('project_id')
    expect(BqStream.data_set).to_not eq('production')
    expect(BqStream.bq_table_name).to eq('queued_items')
    expect(BqStream::QueuedItem.all).to be_empty
  end

  context 'should be able to queue and dequeue items from table' do
    before(:each) do
      class TableFirst < ActiveRecord::Base
        # unless bq_attributes is used nothing gets written
        # bq_attributes only: [:sym, :sym, :sym]
        # bq_attributes :all
        # bq_attributes except: [...]

        # def self.bq_attributes(opts = {})
        #   if opts == :all
        #     columns.each { |column_name| method_foo(column_name) }
        #   elsif opts[:only]
        #     raise "opts  .... " unless opts[:only].is_a? Array
        #     opts[:only].each { |column_name| method_foo(column_name) }
        #   elsif ...
        #   else
        #     raise
        #   end
        # end

        def self.build_table
          connection.create_table :table_firsts, force: true do |t|
            t.string :name
            t.text :description
            t.boolean :required
            t.timestamps
          end
        end
      end

      class TableSecond < ActiveRecord::Base
        def self.build_table
          connection.create_table :table_seconds, force: true do |t|
            t.string :name
            t.string :status
            t.integer :rank
            t.timestamps
          end
        end
      end

      TableFirst.build_table
      TableSecond.build_table

      TableFirst.create(name: 'primary record',
                        description: 'first into the table',
                        required: true)
      TableSecond.create(name: 'secondary record',
                         status: 'active',
                         rank: 1)
      TableFirst.update(1, required: false)
    end

    xit 'should write queued item records to table' do
      expect(BqStream::QueuedItem.all.as_json).to eq([{
        'table_name' => 'TableFirst',
        'attr' => 'name',
        'new_value' => 'primary record',
        'updated_at' => Time.now
      },
        {} #  x6 more
      ])
    end

    xit 'should send queded items to bigquery then delete them' do
      # stub calls to BigQuery
      BqStream.send_to_bigquery
      # expect methods from BigQuery gem to be called
      expect(BqStream::QueuedItem.all).to be_empty
    end
  end
end
