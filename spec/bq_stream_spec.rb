require 'spec_helper'

describe BqStream do
  before do
    Timecop.freeze(Time.parse('2017-01-01 00:00:00 UTC'))
  end

  after do
    Timecop.return
  end

  before(:each) do
    BqStream.configuration do |config|
      config.client_id = 'client_id'
      config.service_email = 'service_email'
      config.key = 'key'
      config.project_id = 'project_id'
    end
    @time_stamp = Time.parse('2017-01-01 00:00:00 +0000')
  end

  it 'has a version number' do
    expect(BqStream::VERSION).not_to be nil
  end

  it 'can be configured' do
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
        def self.build_table
          connection.create_table :table_firsts, force: true do |t|
            t.string :name
            t.text :description
            t.boolean :required
            t.timestamps
          end
          bq_attributes :all
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
          bq_attributes(only: [:name, :status])
        end
      end

      class TableThird < ActiveRecord::Base
        def self.build_table
          connection.create_table :table_thirds, force: true do |t|
            t.string :name
            t.string :notes
            t.integer :order
            t.timestamps
          end
          bq_attributes(except: [:id, :order, :created_at])
        end
      end

      class TableForth < ActiveRecord::Base
        def self.build_table
          connection.create_table :table_forths, force: true do |t|
            t.string :name
            t.string :listing
            t.integer :level
            t.timestamps
          end
        end
      end

      TableFirst.build_table
      TableSecond.build_table
      TableThird.build_table
      TableForth.build_table

      TableFirst.create(name: 'primary record',
                        description: 'first into the table',
                        required: true)
      TableSecond.create(name: 'secondary record',
                         status: 'active',
                         rank: 1)
      TableThird.create(name: 'third record',
                        notes: 'remember',
                        order: 22)
      TableForth.create(name: 'forth record',
                        listing: 'important',
                        level: 61)
      TableFirst.update(1, required: false)
    end

    it 'should write queued item records to table' do
      expect(BqStream::QueuedItem.all.as_json).to eq([
      {
        'id' => 1,
        'table_name' => 'TableFirst',
        'record_id' => 1,
        'attr' => 'name',
        'new_value' => 'primary record',
        'updated_at' => @time_stamp
      },
      {
        'id' => 2,
        'table_name' => 'TableFirst',
        'record_id' => 1,
        'attr' => 'description',
        'new_value' => 'first into the table',
        'updated_at' => @time_stamp
      },
      {
        'id' => 3,
        'table_name' => 'TableFirst',
        'record_id' => 1,
        'attr' => 'required',
        'new_value' => 'true',
        'updated_at' => @time_stamp
      },
      {
        'id' => 4,
        'table_name' => 'TableFirst',
        'record_id' => 1,
        'attr' => 'created_at',
        'new_value' => '2017-01-01 00:00:00 UTC',
        'updated_at' => @time_stamp
      },
      {
        'id' => 5,
        'table_name' => 'TableFirst',
        'record_id' => 1,
        'attr' => 'updated_at',
        'new_value' => '2017-01-01 00:00:00 UTC',
        'updated_at' => @time_stamp
      },
      {
        'id' => 6,
        'table_name' => 'TableFirst',
        'record_id' => 1,
        'attr' => 'id',
        'new_value' => '1',
        'updated_at' => @time_stamp
      },
      {
        'id' => 7,
        'table_name' => 'TableSecond',
        'record_id' => 1,
        'attr' => 'name',
        'new_value' => 'secondary record',
        'updated_at' => @time_stamp
      },
      {
        'id' => 8,
        'table_name' => 'TableSecond',
        'record_id' => 1,
        'attr' => 'status',
        'new_value' => 'active',
        'updated_at' => @time_stamp
      },
      {
        'id' => 9,
        'table_name' => 'TableThird',
        'record_id' => 1,
        'attr' => 'name',
        'new_value' => 'third record',
        'updated_at' => @time_stamp
      },
      {
        'id' => 10,
        'table_name' => 'TableThird',
        'record_id' => 1,
        'attr' => 'notes',
        'new_value' => 'remember',
        'updated_at' => @time_stamp
      },
      {
        'id' => 11,
        'table_name' => 'TableThird',
        'record_id' => 1,
        'attr' => 'updated_at',
        'new_value' => '2017-01-01 00:00:00 UTC',
        'updated_at' => @time_stamp
      },
      {
        'id' => 12,
        'table_name' => 'TableFirst',
        'record_id' => 1,
        'attr' => 'required',
        'new_value' => 'false',
        'updated_at' => @time_stamp
      }
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
