require 'spec_helper'

describe BqStream do
  before(:each) do
    BqStream.configuration do |config|
      config.client_id = ENV['CLIENT_ID']
      config.service_email = ENV['SERVICE_EMAIL']
      config.key = ENV['KEY']
      config.project_id = ENV['PROJECT_ID']
      config.dataset = ENV['DATASET']
    end
  end

  context 'should be able to queue and dequeue items from table' do
    before(:all) do
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

      class TableFourth < ActiveRecord::Base
        def self.build_table
          connection.create_table :table_fourths, force: true do |t|
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
      TableFourth.build_table
    end

    before(:each) do
      @first_record =
        TableFirst.create(name: 'primary record',
                          description: 'first into the table',
                          required: true)
      @second_record =
        TableSecond.create(name: 'secondary record',
                           status: 'active',
                           rank: 1)
      @third_record =
        TableThird.create(name: 'third record',
                          notes: 'remember',
                          order: 22)
      TableFourth.create(name: 'fourth record',
                        listing: 'important',
                        level: 61)
      @first_record.update(required: false)
      @second_record.destroy
    end

    after(:each) do
      BqStream::QueuedItem.destroy_all
    end

    it 'should send queueded items to bigquery' do
      BqStream.dequeue_items
      expect(BqStream::QueuedItem.all).to be_empty
    end
  end
end
