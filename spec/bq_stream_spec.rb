require 'spec_helper'
require 'database_cleaner'

describe BqStream do
  before(:all) do
    class QueuedItem < ActiveRecord::Base
      def self.build_table
        connection.create_table :bq_stream_queued_items, force: true do |t|
          t.string    :table_name
          t.integer   :record_id
          t.string    :attr
          t.binary    :new_value
          t.datetime  :updated_at
          t.boolean   :sent_to_bq
          t.index     :sent_to_bq
          t.datetime  :time_sent
        end
      end
    end

    class OldestRecord < ActiveRecord::Base
      def self.build_table
        connection.create_table :bq_stream_oldest_records, force: true do |t|
          t.string   :table_name
          t.string   :attr
          t.datetime :bq_earliest_update
          t.boolean  :archived
        end
      end
    end

    class TableFirst < ActiveRecord::Base
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

    class TableThird < ActiveRecord::Base
      def self.build_table
        connection.create_table :table_thirds, force: true do |t|
          t.string :name
          t.string :notes
          t.integer :order
          t.timestamps
        end
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

    QueuedItem.build_table
    OldestRecord.build_table
    TableFirst.build_table
    TableSecond.build_table
    TableThird.build_table
    TableFourth.build_table

    Timecop.freeze(Time.parse('2016-02-26 00:00:00').in_time_zone(BqStream.timezone))

    @oldest_record =
      TableThird.create(name: 'oldest record',
                        notes: 'the oldest record',
                        order: 1)

    Timecop.freeze(Time.parse('2016-09-21 00:00:00').in_time_zone(BqStream.timezone))

    @old_record =
      TableThird.create(name: 'old record',
                        notes: 'an old record',
                        order: 2)

    Timecop.freeze(Time.parse('2016-12-31 19:00:00').in_time_zone(BqStream.timezone))
  end

  before(:each) do
    DatabaseCleaner.clean
    BigQuery::Client.class_eval do
      attr_accessor :initial_args
      attr_reader :inserted_records, :bq_table_columns, :bq_datasets
      def initialize(*args)
        @inserted_records = []
        @bq_table_columns = []
        @bq_datasets = []
        @initial_args = args
      end

      def insert(*args)
        args[1].each do |i|
          i[:updated_at] = Time.now
        end
        inserted_records << [args]
      end

      def create_table(*args)
        bq_table_columns << [args]
      end

      def create_dataset(*args)
        bq_datasets << [args]
      end

      def datasets_formatted
        []
      end

      def tables_formatted
        []
      end

      def query(_q)
        { 'kind' => 'bigquery#queryResponse',
          'schema' => { 'fields' =>
            [{ 'name' => 'table_name', 'type' => 'STRING',
               'mode' => 'NULLABLE' },
             { 'name' => 'attr', 'type' => 'STRING', 'mode' => 'NULLABLE' },
             { 'name' => 'bq_earliest_update',
               'type' => 'TIMESTAMP', 'mode' => 'NULLABLE' }] },
          'jobReference' =>
              { 'projectId' => 'project_id',
                'jobId' => 'job_id' },
          'totalRows' => '12',
          'rows' =>
          [{ 'f' => [{ 'v' => 'TableThird' }, { 'v' => 'notes' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableFirst' }, { 'v' => 'name' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableFirst' }, { 'v' => 'created_at' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableSecond' }, { 'v' => 'name' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableFirst' }, { 'v' => 'description' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableThird' }, { 'v' => 'updated_at' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableThird' }, { 'v' => 'name' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableFirst' }, { 'v' => 'id' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableFirst' }, { 'v' => 'required' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableSecond' }, { 'v' => 'status' },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableSecond' }, { 'v' => nil },
                     { 'v' => '1483228800.0' }] },
           { 'f' => [{ 'v' => 'TableFirst' }, { 'v' => 'updated_at' },
                     { 'v' => '1483228800.0' }] }],
          'totalBytesProcessed' => '0',
          'jobComplete' => true,
          'cacheHit' => true }
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
      config.back_date = Time.parse('2016-09-20 20:00:00').in_time_zone('UTC')
      config.timezone = 'UTC'
    end
    @time_stamp = Time.now.in_time_zone(BqStream.timezone)
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
      expect(BqStream.bq_table_name).to eq('bq_datastream')
      expect(BqStream.back_date).to eq('2016-09-21 00:00:00.000000000 +0000')
      expect(BqStream.timezone).to eq('UTC')
      expect(BqStream::QueuedItem.all).to be_empty
      expect(BqStream::OldestRecord.all).to be_empty
    end

    it 'should create the bigquery dataset' do
      expect(BqStream.bq_writer.bq_datasets)
        .to eq([[['dataset']]])
    end

    it 'should return an existing bq table and not create a new table' do
      bq_writer = double(:bq_writer)
      allow(bq_writer).to receive(:tables_formatted) { ['bq_datastream'] }
      BqStream.create_bq_table unless bq_writer
                                      .tables_formatted
                                      .include?(BqStream.bq_table_name)
      expect(BqStream.bq_writer.bq_table_columns)
        .to eq([[['bq_datastream',
                  { table_name: { type: 'STRING', mode: 'REQUIRED' },
                    record_id:  { type: 'INTEGER', mode: 'REQUIRED' },
                    attr:       { type: 'STRING', mode: 'NULLABLE' },
                    new_value:  { type: 'STRING', mode: 'NULLABLE' },
                    updated_at: { type: 'TIMESTAMP', mode: 'REQUIRED' } }]]])
    end

    it 'should create the big query table' do
      expect(BqStream.bq_writer.bq_table_columns)
        .to eq([[['bq_datastream',
                  { table_name: { type: 'STRING', mode: 'REQUIRED' },
                    record_id:  { type: 'INTEGER', mode: 'REQUIRED' },
                    attr:       { type: 'STRING', mode: 'NULLABLE' },
                    new_value:  { type: 'STRING', mode: 'NULLABLE' },
                    updated_at: { type: 'TIMESTAMP', mode: 'REQUIRED' } }]]])
    end
  end

  context 'should be able to queue and dequeue items from tables' do
    before(:all) do
      class TableFirst < ActiveRecord::Base
        bq_attributes :all
      end

      class TableSecond < ActiveRecord::Base
        bq_attributes(only: [:name, :status])
      end

      class TableThird < ActiveRecord::Base
        bq_attributes(except: [:id, :order, :created_at])
      end
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
                          notes: '12.50',
                          order: 22)
      TableFourth.create(name: 'fourth record',
                         listing: 'important',
                         level: 61)
      @first_record.update(required: false)
      @second_record.destroy
    end

    after(:each) do
      BqStream::QueuedItem.destroy_all
      BqStream::OldestRecord.destroy_all
      ActiveRecord::Base.connection.execute("DELETE from sqlite_sequence where name = 'bq_stream_queued_items'") 
      ActiveRecord::Base.connection.execute("DELETE from sqlite_sequence where name = 'bq_stream_oldest_records'") 
    end

    it 'should write queued item to table when bq_attributes is called' do
      expect(BqStream::QueuedItem.all.as_json)
        .to eq([{
                  'id' => 1,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'id',
                  'new_value' => @first_record.id.to_s,
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 2,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'name',
                  'new_value' => 'primary record',
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 3,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'description',
                  'new_value' => 'first into the table',
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 4,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'required',
                  'new_value' => 'true',
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 5,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'created_at',
                  'new_value' => @time_stamp.to_s,
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 6,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'updated_at',
                  'new_value' => @time_stamp.to_s,
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 7,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'name',
                  'new_value' => 'secondary record',
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 8,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'status',
                  'new_value' => 'active',
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 9,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'name',
                  'new_value' => 'third record',
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 10,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'notes',
                  'new_value' => '12.50',
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 11,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'updated_at',
                  'new_value' => @time_stamp.to_s,
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                },
                {
                  'id' => 12,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'Destroyed',
                  'new_value' => 'True',
                  'updated_at' => @time_stamp,
                  'sent_to_bq' => nil,
                  'time_sent' => nil
                }])
    end

    it 'should complete full archive to bigquery' do
      BqStream.old_records_full_archive(@time_stamp)
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
                  [{ table_name: 'TableThird', 
                     record_id: @third_record.id,
                     attr: 'notes',
                     new_value: '12.50',
                     updated_at: @time_stamp },
                   { table_name: 'TableThird',
                     record_id: @third_record.id,
                     attr: 'updated_at',
                     new_value: @time_stamp,
                     updated_at: @time_stamp },
                   { table_name: 'TableThird',
                     record_id: @third_record.id,
                     attr: 'name',
                     new_value: 'third record',
                     updated_at: @time_stamp }]]],
                [['bq_datastream',
                  [{ table_name: 'TableFirst',
                     record_id: @second_record.id,
                     attr: 'name',
                     new_value: 'primary record',
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'created_at',
                     new_value: @time_stamp,
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'description',
                     new_value: 'first into the table',
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'id',
                     new_value: @first_record.id.to_s,
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'required',
                     new_value: 'false',
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'updated_at',
                     new_value: @time_stamp.to_s,
                     updated_at: @time_stamp }]]]])
    end

    # Also tests that only valid attributes to be sent to BigQuery
    it 'should complete partial archive with valid attributes to bigquery' do
      BqStream.partial_archive(@time_stamp, 'TableFirst', [:name, :description, :required, :fake_attr])
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
                  [{ table_name: 'TableFirst', 
                     record_id: @first_record.id,
                     attr: 'name',
                     new_value: 'primary record',
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst', 
                     record_id: TableFirst.second.id,
                     attr: 'name',
                     new_value: 'primary record',
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst', 
                     record_id: @first_record.id,
                     attr: 'description',
                     new_value: 'first into the table',
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst', 
                     record_id: TableFirst.second.id,
                     attr: 'description',
                     new_value: 'first into the table',
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst', 
                     record_id: @first_record.id,
                     attr: 'required',
                     new_value: 'false',
                     updated_at: @time_stamp },
                   { table_name: 'TableFirst', 
                     record_id: TableFirst.second.id,
                     attr: 'required',
                     new_value: 'false',
                     updated_at: @time_stamp }]]]])
    end

    it 'should send no queued items to bigquery' do
      BqStream.dequeue_items
      expect(BqStream.bq_writer.inserted_records)
        .to eq([])
    end

    it 'should send queued items to bigquery' do
      time_stamp =
        Timecop.freeze(Time.parse('2016-12-31 19:05:00').in_time_zone(BqStream.timezone))
      BqStream.dequeue_items
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
                  [{ table_name: 'TableThird',
                     record_id: 2,
                     attr: 'name',
                     new_value: 'old record',
                     updated_at: time_stamp },
                   { table_name: 'TableThird',
                     record_id: 2,
                     attr: 'notes',
                     new_value: 'an old record',
                     updated_at: time_stamp },
                   { table_name: 'TableThird',
                     record_id: 2,
                     attr: 'updated_at',
                     new_value: "2016-09-21 04:00:00 UTC",
                     updated_at: time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'id',
                     new_value: @first_record.id.to_s,
                     updated_at: time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'name',
                     new_value: 'primary record',
                     updated_at: time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'description',
                     new_value: 'first into the table',
                     updated_at: time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'required',
                     new_value: 'true',
                     updated_at: time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'created_at',
                     new_value: @time_stamp.to_s,
                     updated_at: time_stamp },
                   { table_name: 'TableFirst',
                     record_id: @first_record.id,
                     attr: 'updated_at',
                     new_value: @time_stamp.to_s,
                     updated_at: time_stamp },
                   { table_name: 'TableSecond',
                     record_id: @second_record.id,
                     attr: 'name',
                     new_value: 'secondary record',
                     updated_at: time_stamp },
                   { table_name: 'TableSecond',
                     record_id: @second_record.id,
                     attr: 'status',
                     new_value: 'active',
                     updated_at: time_stamp },
                   { table_name: 'TableThird',
                     record_id: @third_record.id,
                     attr: 'name',
                     new_value: 'third record',
                     updated_at: time_stamp },
                   { table_name: 'TableThird',
                     record_id: @third_record.id,
                     attr: 'notes',
                     new_value: '12.50',
                     updated_at: time_stamp },
                   { table_name: 'TableThird',
                     record_id: @third_record.id,
                     attr: 'updated_at',
                     new_value: @time_stamp.to_s,
                     updated_at: time_stamp },
                   { table_name: 'TableSecond',
                     record_id: @second_record.id,
                     attr: 'Destroyed',
                     new_value: 'True',
                     updated_at: time_stamp }]]]])
      expect(BqStream::QueuedItem.all.as_json)
        .to eq([{
                  'id' => 1,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'id',
                  'new_value' => @first_record.id.to_s,
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 2,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'name',
                  'new_value' => 'primary record',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 3,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'description',
                  'new_value' => 'first into the table',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 4,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'required',
                  'new_value' => 'true',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 5,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'created_at',
                  'new_value' => @time_stamp.to_s,
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 6,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'updated_at',
                  'new_value' => @time_stamp.to_s,
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 7,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'name',
                  'new_value' => 'secondary record',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 8,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'status',
                  'new_value' => 'active',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 9,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'name',
                  'new_value' => 'third record',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 10,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'notes',
                  'new_value' => '12.50',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 11,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'updated_at',
                  'new_value' => @time_stamp.to_s,
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 12,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'Destroyed',
                  'new_value' => 'True',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 13,
                  'table_name' => 'TableThird',
                  'record_id' => 2,
                  'attr' => 'name',
                  'new_value' => 'old record',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 14,
                  'table_name' => 'TableThird',
                  'record_id' => 2,
                  'attr' => 'notes',
                  'new_value' => 'an old record',
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                },
                {
                  'id' => 15,
                  'table_name' => 'TableThird',
                  'record_id' => 2,
                  'attr' => 'updated_at',
                  'new_value' => Time.parse('2016-09-21 04:00:00 UTC').to_s,
                  'updated_at' => @time_stamp + 5.minutes,
                  'sent_to_bq' => true,
                  'time_sent' => @time_stamp + 5.minutes
                }])
    end

    context 'oldest record table' do
      it 'should be empty until first dequeue' do
        expect(BqStream::OldestRecord.all.as_json).to eq([])
      end

      it 'should update oldest records' do
        time_stamp = @time_stamp - 5.minutes
        BqStream.dequeue_items
        expect(BqStream::OldestRecord.all.as_json)
          .to eq([{ 'id' => 1,
                    'table_name' => 'TableFirst',
                    'attr' => 'id',
                    'bq_earliest_update' => time_stamp,
                    'archived' => true },
                  { 'id' => 2,
                    'table_name' => 'TableFirst',
                    'attr' => 'name',
                    'bq_earliest_update' => time_stamp,
                    'archived' => true },
                  { 'id' => 3,
                    'table_name' => 'TableFirst',
                    'attr' => 'description',
                    'bq_earliest_update' => time_stamp,
                    'archived' => true },
                  { 'id' => 4,
                    'table_name' => 'TableFirst',
                    'attr' => 'required',
                    'bq_earliest_update' => time_stamp,
                    'archived' => true },
                  { 'id' => 5,
                    'table_name' => 'TableFirst',
                    'attr' => 'created_at',
                    'bq_earliest_update' => time_stamp,
                    'archived' => true },
                  { 'id' => 6,
                    'table_name' => 'TableFirst',
                    'attr' => 'updated_at',
                    'bq_earliest_update' => time_stamp,
                    'archived' => true },
                  { 'id' => 7,
                    'table_name' => 'TableSecond',
                    'attr' => 'name',
                    'bq_earliest_update' => nil,
                    'archived' => true },
                  { 'id' => 8,
                    'table_name' => 'TableSecond',
                    'attr' => 'status',
                    'bq_earliest_update' => nil,
                    'archived' => true },
                  { 'id' => 9,
                    'table_name' => 'TableThird',
                    'attr' => 'name',
                    'bq_earliest_update' => Time.parse('2016-09-21 00:00:00'),
                    'archived' => true },
                  { 'id' => 10,
                    'table_name' => 'TableThird',
                    'attr' => 'notes',
                    'bq_earliest_update' => Time.parse('2016-09-21 00:00:00'),
                    'archived' => true },
                  { 'id' => 11,
                    'table_name' => 'TableThird',
                    'attr' => 'updated_at',
                    'bq_earliest_update' => Time.parse('2016-09-21 00:00:00'),
                    'archived' => true },
                  { 'id' => 12,
                    'table_name' => '! revision !',
                    'attr' => 'None',
                    'bq_earliest_update' => nil,
                    'archived' => true }])
      end
    end

    context 'archive oldest records' do
      it 'should set all oldest record rows archived attributes to true' do
        BqStream.old_records_full_archive(@time_stamp, 'dataset')
        expect(BqStream::OldestRecord.where(archived: false)).to be_empty
      end
    end
  end
end
