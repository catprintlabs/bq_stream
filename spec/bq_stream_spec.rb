require 'spec_helper'

describe BqStream do
  before do
    Timecop.freeze(Time.parse('2017-01-01 00:00:00 UTC'))
  end

  after do
    Timecop.return
  end

  before(:each) do
    BigQuery::Client.class_eval do
      attr_accessor :initial_args
      attr_reader :inserted_records, :bq_table_columns
      def initialize(*args)
        @inserted_records = []
        @bq_table_columns = []
        @initial_args = args
      end

      def insert(*args)
        args[1][:updated_at] = Time.parse('2017-01-01 00:00:00 +0000')
        inserted_records << [args]
      end

      def create(*args)
        bq_table_columns << [args]
      end

      def tables
        []
      end
    end
    BqStream.class_eval do
      class << self
        attr_reader :bq_writer
      end
    end
    BqStream.configuration do |config|
      config.client_id = 'client_id'
      config.service_email = 'service_email'
      config.key = 'key'
      config.project_id = 'project_id'
      config.dataset = 'dataset'
    end
    @time_stamp = Time.parse('2017-01-01 00:00:00 +0000')
  end

  it 'has a version number' do
    expect(BqStream::VERSION).not_to be nil
  end

  context 'during configuration' do
    it 'can be configured' do
      expect(BqStream.client_id).to eq('client_id')
      expect(BqStream.service_email).to eq('service_email')
      expect(BqStream.key).to eq('key')
      expect(BqStream.project_id).to eq('project_id')
      expect(BqStream.dataset).to_not eq('production')
      expect(BqStream.queued_items_table_name).to eq('queued_items')
      expect(BqStream.bq_table_name).to eq('bq_datastream')
      expect(BqStream::QueuedItem.all).to be_empty
    end

    it 'should return an existing big query table name' do
      @bq_writer.stub(:tables) { ['bq_datastream'] }
      expect(@bq_writer.tables).to include('bq_datastream')
    end

    it 'should create the big query table' do
      expect(BqStream.bq_writer.bq_table_columns)
        .to eq([[['bq_datastream',
                  { table_name: { type: 'STRING' },
                    record_id: { type: 'INTEGER' },
                    new_value: { type: 'STRING' },
                    updated_at: { type: 'DATETIME' } }]]])
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
      TableForth.create(name: 'forth record',
                        listing: 'important',
                        level: 61)
      @first_record.update(required: false)
      @second_record.destroy
    end

    after(:each) do
      BqStream::QueuedItem.destroy_all
    end

    it 'should write queued item to table when bq_attributes is called' do
      expect(BqStream::QueuedItem.all.as_json)
        .to eq([{
                 'id' => 1,
                 'table_name' => 'TableFirst',
                 'record_id' => @first_record.id,
                 'attr' => 'name',
                 'new_value' => 'primary record',
                 'updated_at' => @time_stamp
               },
                {
                  'id' => 2,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'description',
                  'new_value' => 'first into the table',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 3,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'required',
                  'new_value' => 'true',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 4,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'created_at',
                  'new_value' => '2017-01-01 00:00:00 UTC',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 5,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'updated_at',
                  'new_value' => '2017-01-01 00:00:00 UTC',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 6,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'id',
                  'new_value' => '1',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 7,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'name',
                  'new_value' => 'secondary record',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 8,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'status',
                  'new_value' => 'active',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 9,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'name',
                  'new_value' => 'third record',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 10,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'notes',
                  'new_value' => 'remember',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 11,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'updated_at',
                  'new_value' => '2017-01-01 00:00:00 UTC',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 12,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'required',
                  'new_value' => 'false',
                  'updated_at' => @time_stamp
                },
                {
                  'id' => 13,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => nil,
                  'new_value' => nil,
                  'updated_at' => @time_stamp
                }])
    end

    it 'should send queueded items to bigquery then delete them' do
      BqStream.dequeue_items
      expect(BqStream::QueuedItem.all).to be_empty
      expect(BqStream.bq_writer.initial_args)
        .to eq([
                 {
                   'client_id' => 'client_id',
                   'service_email' => 'service_email',
                   'key' => 'key',
                   'project_id' => 'project_id',
                   'dataset' => 'dataset'
                 }
               ])
      expect(BqStream.bq_writer.inserted_records)
        .to eq([[['bq_datastream',
                  { table_name: 'TableFirst',
                    record_id: @first_record.id,
                    attr: 'name',
                    new_value: 'primary record',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableFirst',
                    record_id: @first_record.id,
                    attr: 'description',
                    new_value: 'first into the table',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableFirst',
                    record_id: @first_record.id,
                    attr: 'required',
                    new_value: 'true',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableFirst',
                    record_id: @first_record.id,
                    attr: 'created_at',
                    new_value: '2017-01-01 00:00:00 UTC',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableFirst',
                    record_id: @first_record.id,
                    attr: 'updated_at',
                    new_value: '2017-01-01 00:00:00 UTC',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableFirst',
                    record_id: @first_record.id,
                    attr: 'id',
                    new_value: @first_record.id.to_s,
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableSecond',
                    record_id: @second_record.id,
                    attr: 'name',
                    new_value: 'secondary record',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableSecond',
                    record_id: @second_record.id,
                    attr: 'status',
                    new_value: 'active',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableThird',
                    record_id: @third_record.id,
                    attr: 'name',
                    new_value: 'third record',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableThird',
                    record_id: @third_record.id,
                    attr: 'notes',
                    new_value: 'remember',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableThird',
                    record_id: @third_record.id,
                    attr: 'updated_at',
                    new_value: '2017-01-01 00:00:00 UTC',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableFirst',
                    record_id: @first_record.id,
                    attr: 'required',
                    new_value: 'false',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableSecond',
                    record_id: @second_record.id,
                    attr: nil,
                    new_value: nil,
                    updated_at: @time_stamp }]]])
    end
  end
end
