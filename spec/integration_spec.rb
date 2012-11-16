require 'rails/all'

require File.expand_path(File.dirname(__FILE__) + '/spec_helper')
require MULTI_DB_SPEC_DIR + '/../lib/multi_db'

describe MultiDb::ConnectionProxy do

  before(:all) do
    Object.const_set("RAILS_CACHE", ActiveSupport::Cache.lookup_store(:memory_store, :namespace => "multidbspecs"))
    MultiDb::LagMonitor.stub(:slave_lag).and_return(0)
    MultiDb::Railtie.insert!
    ActiveRecord::Base.configurations = MULTI_DB_SPEC_CONFIG
    ActiveRecord::Base.establish_connection :test
    ActiveRecord::Migration.verbose = false
    # ActiveRecord::Migration.create_table(:master_models, :force => true) {}
    class MasterModel < ActiveRecord::Base; end
    # ActiveRecord::Migration.create_table(:foo_models, :force => true) {|t| t.string :bar}
    class FooModel < ActiveRecord::Base; end
    @sql = 'SELECT 1 + 1 FROM DUAL'
  end

  after(:all) do
    MultiDb.slave_classes.each do |klass|
      MultiDb.send(:remove_const,klass.name.sub(/^.*:/,''))
    end
  end

  before(:each) do
    @proxy = ActiveRecord::Base.connection_proxy
    @proxy.reset_blacklist
    Thread.current[:multi_db] = nil
    @master = @proxy.master.retrieve_connection
    @slave1 = MultiDb::SlaveDatabase1.retrieve_connection
    @slave2 = MultiDb::SlaveDatabase2.retrieve_connection
    @slave3 = MultiDb::SlaveDatabase3.retrieve_connection
    @slave4 = MultiDb::SlaveDatabase4.retrieve_connection

    MultiDb::LagMonitor.stub(:slave_lag).and_return(0)
  end

  it 'AR::B should respond to #connection_proxy' do
    ActiveRecord::Base.connection_proxy.should be_kind_of(MultiDb::ConnectionProxy)
  end

  it 'FooModel#connection should return an instance of MultiDb::ConnectionProxy' do
    FooModel.connection.should be_kind_of(MultiDb::ConnectionProxy)
  end

  it 'MasterModel#connection should return an instance of MultiDb::ConnectionProxy' do
    MasterModel.connection.should be_kind_of(MultiDb::ConnectionProxy)
  end

  it "should generate classes for each entry in the database.yml" do
    defined?(MultiDb::SlaveDatabase1).should_not be_nil
    defined?(MultiDb::SlaveDatabase2).should_not be_nil
  end

  it 'should perform transactions on the master' do
    @master.should_receive(:select_all).exactly(1) # makes sure the first one goes to a slave
    @proxy.with_slave do
      @proxy.select_all(@sql)
      ActiveRecord::Base.transaction do
        @proxy.select_all(@sql)
      end
    end
  end

  it 'should send dangerous methods to the master' do
    @proxy.with_slave do
      meths = [:insert, :update, :delete, :execute]
      meths.each do |meth|
        @slave1.stub!(meth).and_raise(RuntimeError)
        @master.should_receive(meth).and_return(true)
        @proxy.send(meth, @sql)
      end
    end
  end

  it 'should dynamically generate safe methods' do
    @proxy.with_slave do
      @proxy.methods.should_not include(:select_rows)
      @proxy.select_rows(@sql)
      @proxy.methods.should include(:select_rows)
    end
  end

  it 'should cache queries using select_all' do
    ActiveRecord::Base.cache do
      @proxy.with_slave do
        @proxy.connection_stack.current.retrieve_connection.should_receive(:select_all).exactly(1)
        @master.should_not_receive(:select_all)
        3.times { @proxy.select_all(@sql) }
      end
    end
  end

  it 'should invalidate the cache on insert, delete and update' do
    ActiveRecord::Base.cache do
      meths = [:insert, :update, :delete, :insert, :update]
      meths.each do |meth|
        @master.should_receive(meth).and_return(true)
      end
      Thread.current[:multi_db] = nil
      @proxy.with_slave do
        @proxy.connection_stack.current.retrieve_connection.should_receive(:select_all).exactly(5).times
        5.times do |i|
          @proxy.select_all(@sql)
          @proxy.send(meths[i])
        end
      end
    end
  end

  it 'should retry the next slave when one fails and finally fall back to the master' do
    @proxy.with_slave do
      @slave1.should_receive(:select_all).once.and_raise(ActiveRecord::ConnectionNotEstablished)
      @slave2.should_receive(:select_all).once.and_raise(ActiveRecord::ConnectionNotEstablished)
      @slave3.should_receive(:select_all).once.and_raise(ActiveRecord::ConnectionNotEstablished)
      @slave4.should_receive(:select_all).once.and_raise(ActiveRecord::ConnectionNotEstablished)
      @master.should_receive(:select_all).and_return(true)
      @proxy.with_slave do
        @proxy.select_all(@sql)
      end
    end
  end

  it 'should try to reconnect the master connection after the master has failed' do
    @master.should_receive(:update).and_raise(ActiveRecord::ConnectionNotEstablished)
    lambda { @proxy.update(@sql) }.should raise_error
    @master.should_receive(:reconnect!).and_return(true)
    @master.should_receive(:insert).and_return(1)
    @proxy.insert(@sql)
  end

  it 'should reload models from the master' do
    foo = FooModel.create!(:bar => 'baz')
    foo.bar = "not_saved"
    @slave1.should_not_receive(:select_all)
    @slave2.should_not_receive(:select_all)
    foo.reload
    # we didn't stub @master#select_all here, check that we actually hit the db
    foo.bar.should == 'baz'
  end

  describe '(accessed from multiple threads)' do

    it '#with_slave should be local to the thread' do
      @proxy.connection_stack.current.should == @proxy.master
      @proxy.with_slave do
        @proxy.connection_stack.current.should_not == @proxy.master
        Thread.new do
          @proxy.connection_stack.current.should == @proxy.master
          @proxy.with_slave do
            @proxy.connection_stack.current.should_not == @proxy.master
          end
          @proxy.connection_stack.current.should== @proxy.master
        end
        @proxy.connection_stack.current.should_not == @proxy.master
      end
      @proxy.connection_stack.current.should == @proxy.master
    end

  end

end

