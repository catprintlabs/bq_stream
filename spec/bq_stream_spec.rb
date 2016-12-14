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

    expect(BqStream.configuration.client_id).to eq('client_id')
    expect(BqStream.configuration.service_email).to eq('service_email')
    expect(BqStream.configuration.key).to eq('key')
    expect(BqStream.configuration.project_id).to eq('project_id')
    expect(BqStream.configuration.data_set).to_not eq('production')
    expect(BqStream.configuration.bq_table_name).to eq('bq_table')
    expect(BqStream::QueuedItem.all).to be_empty
  end

  xit 'should write queued item records to table' do
    class TableFirst < ActiveRecord::Base
      def self.build_tables
        connection.create_table :table_firsts, force: true do |t|
          t.string :name
          t.text :description
          t.boolean :required
          t.timestamps
        end
      end
    end

    class TableSecond < ActiveRecord::Base
      def self.build_tables
        connection.create_table :table_seconds, force: true do |t|
          t.string :name
          t.string :status
          t.integer :rank
          t.timestamps
        end
      end
    end

    TableOne.build_tables # rescue nil
    TableTwo.build_tables # rescue nil

    TableOne.create(name: 'primary record',
                    description: 'first into the table',
                    required: true)
    TableTwo.create(name: 'secondary record',
                    status: 'active',
                    rank: 1)
    TableOne.update(1, required: false)

    expect(BqStream::QueuedItem.all.as_json).to =~ []
  end

  xit 'should send queded items to bigquery then delete them' do
    # stub calls to BigQuery
    BqStream.send_to_bigquery
    # expect methods from BigQuery gem to be called
    expect(BqStream::QueuedItem.all).to be_empty
  end
end
