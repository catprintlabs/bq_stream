require 'spec_helper'

describe BqStream do
  before(:all) do
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

    Timecop.freeze(Time.parse('2016-02-26 00:00:00 UTC'))

    @oldest_record =
      TableThird.create(name: 'oldest record',
                        notes: 'the oldest record',
                        order: 1)

    Timecop.freeze(Time.parse('2016-09-21 00:00:00 UTC'))

    @old_record =
      TableThird.create(name: 'old record',
                        notes: 'an old record',
                        order: 2)

    Timecop.freeze(Time.parse('2017-01-01 00:00:00 UTC'))
  end

  before(:each) do
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
        args[1][:updated_at] = Time.now
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
      config.back_date = Time.parse('2016-09-21 00:00:00 UTC')
    end
    @time_stamp = Time.now
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
      expect(BqStream.back_date).to eq('2016-09-21 00:00:00.000000000 +0000')
      expect(BqStream::QueuedItem.all).to be_empty
    end

    it 'should create the bigquery dataset' do
      expect(BqStream.bq_writer.bq_datasets)
        .to eq([[['dataset']]])
    end

    it 'should return an existing bq table and not create a new table' do
      @bq_writer.stub(:tables_formatted) { ['bq_datastream'] }
      BqStream.create_bq_table unless @bq_writer
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
                    attr:         { type: 'STRING', mode: 'NULLABLE' },
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
                 'updated_at' => @time_stamp.getutc
               },
                {
                  'id' => 2,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'description',
                  'new_value' => 'first into the table',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 3,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'required',
                  'new_value' => 'true',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 4,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'created_at',
                  'new_value' => '2017-01-01 00:00:00 UTC',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 5,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'updated_at',
                  'new_value' => '2017-01-01 00:00:00 UTC',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 6,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'id',
                  'new_value' => '1',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 7,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'name',
                  'new_value' => 'secondary record',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 8,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => 'status',
                  'new_value' => 'active',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 9,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'name',
                  'new_value' => 'third record',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 10,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'notes',
                  'new_value' => 'remember',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 11,
                  'table_name' => 'TableThird',
                  'record_id' => @third_record.id,
                  'attr' => 'updated_at',
                  'new_value' => '2017-01-01 00:00:00 UTC',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 12,
                  'table_name' => 'TableFirst',
                  'record_id' => @first_record.id,
                  'attr' => 'required',
                  'new_value' => 'false',
                  'updated_at' => @time_stamp.getutc
                },
                {
                  'id' => 13,
                  'table_name' => 'TableSecond',
                  'record_id' => @second_record.id,
                  'attr' => nil,
                  'new_value' => nil,
                  'updated_at' => @time_stamp.getutc
                }])
    end

    it 'should send queueded items to bigquery and then delete them' do
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
                  { table_name: 'TableThird',
                    record_id: @old_record.id,
                    attr: 'notes',
                    new_value: 'an old record',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableThird',
                    record_id: @old_record.id,
                    attr: 'updated_at',
                    new_value: '2016-09-21 00:00:00.000000000 +0000',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
                  { table_name: 'TableThird',
                    record_id: @old_record.id,
                    attr: 'name',
                    new_value: 'old record',
                    updated_at: @time_stamp }]],
                [['bq_datastream',
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

    context 'oldest record table' do
      it 'should write bigquery items to oldest record table' do
        expect(BqStream::OldestRecord.all.as_json)
          .to eq([{ 'id' => 1,
                    'table_name' => 'TableThird',
                    'attr' => 'notes',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 2,
                    'table_name' => 'TableFirst',
                    'attr' => 'name',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 3,
                    'table_name' => 'TableFirst',
                    'attr' => 'created_at',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 4,
                    'table_name' => 'TableSecond',
                    'attr' => 'name',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 5,
                    'table_name' => 'TableFirst',
                    'attr' => 'description',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 6,
                    'table_name' => 'TableThird',
                    'attr' => 'updated_at',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 7,
                    'table_name' => 'TableThird',
                    'attr' => 'name',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 8,
                    'table_name' => 'TableFirst',
                    'attr' => 'id',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 9,
                    'table_name' => 'TableFirst',
                    'attr' => 'required',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 10,
                    'table_name' => 'TableSecond',
                    'attr' => 'status',
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 11,
                    'table_name' => 'TableSecond',
                    'attr' => nil,
                    'bq_earliest_update' => @time_stamp },
                  { 'id' => 12,
                    'table_name' => 'TableFirst',
                    'attr' => 'updated_at',
                    'bq_earliest_update' => @time_stamp }])
      end

      it 'should update oldest records' do
        BqStream.dequeue_items
        expect(BqStream::OldestRecord.all.as_json)
          .to eq([{ 'id' => 1,
                    'table_name' => 'TableThird',
                    'attr' => 'notes',
                    'bq_earliest_update' => '2016-09-21 00:00:00 UTC' },
                  { 'id' => 6,
                    'table_name' => 'TableThird',
                    'attr' => 'updated_at',
                    'bq_earliest_update' => '2016-09-21 00:00:00 UTC' },
                  { 'id' => 7,
                    'table_name' => 'TableThird',
                    'attr' => 'name',
                    'bq_earliest_update' => '2016-09-21 00:00:00 UTC' }])
      end
    end
  end
end
