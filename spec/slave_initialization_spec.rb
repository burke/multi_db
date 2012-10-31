require './lib/multi_db/slave_initialization'

describe MultiDb do

  let(:configurations) { YAML.load_file('./spec/config/database.yml') }

  let(:fake_ar_base) {
    Class.new do
      def self.abstract_class=(bool) ; end
      def self.establish_connection(name); @connection = name ; end
      class << self ; attr_reader :connection ; end
    end
  }

  before(:each) do
    MultiDb.stub(environment: "test")
    insert_fake_activerecord_base!
  end

  after(:each) do
    MultiDb.send(:remove_const, :ActiveRecord)
  end

  specify {
    klasses = MultiDb.init_slaves(configurations)
    klasses.map(&:name).should == [
      "MultiDb::SlaveDatabase1",
      "MultiDb::SlaveDatabase2",
      "MultiDb::SlaveDatabase3",
      "MultiDb::SlaveDatabase4"
    ]
    klasses.map(&:connection).should == [
      :test_slave_database_1,
      :test_slave_database_2,
      :test_slave_database_3,
      :test_slave_database_4
    ]
    klasses.map{|k|k::WEIGHT}.should == [1, 10, 5, 10]
    klasses.each do |k|
      k.superclass.should == fake_ar_base
    end
  }

  private

  def insert_fake_activerecord_base!
    ar = Module.new.tap{|x|x.const_set(:Base, fake_ar_base)}
    MultiDb.const_set(:ActiveRecord, ar)
  end
end
