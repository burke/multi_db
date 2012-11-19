require './lib/multi_db/connection_proxy'

describe MultiDb::ConnectionProxy do

  class FakeConnection < Struct.new(:name)
    def retrieve_connection ; self ; end
  end

  let(:master) { FakeConnection.new("master") }
  let(:slave1) { FakeConnection.new("slave1") }
  let(:slave2) { FakeConnection.new("slave2") }

  let(:proxy) { MultiDb::ConnectionProxy.new(master, [slave1]) }

  before do
    Thread.current[:sticky_expires] = nil
    Thread.current[:sticky_tables] = nil

    logger = stub(warn: nil, error: nil, fatal: nil)
    MultiDb::ConnectionProxy.any_instance.stub(logger: logger)
    MultiDb::ConnectionStack.any_instance.stub(logger: logger)

    proxy.connection_stack.push_slave
  end

  after { proxy.connection_stack.pop }

  describe 'connection_stack initialization' do

    it 'creates a new ConnectionStack for each thread' do
      stacks = [proxy.connection_stack]
      3.times do
        Thread.new { stacks << proxy.connection_stack }.join
      end
      stacks.map(&:object_id).sort.uniq.size.should == 4
    end

    it 'allows manipulation of each thread\'s stack independently' do
      stack1 = proxy.connection_stack
      stack2 = nil
      Thread.new { stack2 = proxy.connection_stack }.join
      stack1.push_master
      stack2.push_slave
      stack1.current.should == master
      stack2.current.should == slave1
    end

  end

  describe 'handling database errors' do

    it 'switches to the next slave when a connection error happens' do
      my_proxy = MultiDb::ConnectionProxy.new(master, [slave1, slave2])
      my_proxy.with_slave do
        slave1.should_receive(:select_all).and_raise(ActiveRecord::ConnectionNotEstablished)
        slave2.should_receive(:select_all).and_raise(ActiveRecord::ConnectionNotEstablished)
        master.should_receive(:select_all)
        my_proxy.select_all("SELECT 1")
      end
    end

    it 'blows up when the master raises an error that would be recoverable for a slave' do
      slave1.should_receive(:select_all).and_raise(ActiveRecord::ConnectionNotEstablished)
      master.should_receive(:select_all).and_raise(ActiveRecord::ConnectionNotEstablished)
      lambda {
        proxy.select_all("SELECT 1")
      }.should raise_error(ActiveRecord::ConnectionNotEstablished)
    end

    it 'blacklists the slave on AR::ConnectionNotEstablished' do
      slave1.should_receive(:select_all).and_raise(ActiveRecord::ConnectionNotEstablished)
      master.should_receive(:select_all)
      proxy.select_all("SELECT 1")
    end

    it 'blacklists the slave on "can\'t connect to MySQL server' do
      slave1.should_receive(:select_all).and_raise(Mysql2::Error.new("Can't connect to MySQL server"))
      master.should_receive(:select_all)
      proxy.select_all("SELECT 1")
    end

    it 'blacklists the slave on "server has gone away"' do
      slave1.should_receive(:select_all).and_raise(ActiveRecord::StatementInvalid.new("server has gone away"))
      master.should_receive(:select_all)
      proxy.select_all("SELECT 1")
    end

    it 'reraises other AR::StatementInvalid errors' do
      slave1.should_receive(:select_all).and_raise(ActiveRecord::StatementInvalid.new("invalid sql"))
      lambda {
        proxy.select_all("SELECT 1")
      }.should raise_error(ActiveRecord::StatementInvalid)
    end

    it 'reraises other Mysql2::Error errors' do
      slave1.should_receive(:select_all).and_raise(Mysql2::Error.new("SOMETHING BROKE"))
      lambda {
        proxy.select_all("SELECT 1")
      }.should raise_error(Mysql2::Error)
    end

  end

  it 'creates proxy methods to speed up access after the first method_missing hit' do
    proxy.methods.should_not include(:quote_table_name)
    FakeConnection.any_instance.should_receive(:quote_table_name).and_return("`employees`")
    proxy.quote_table_name("employees")
    proxy.methods.should include(:quote_table_name)
  end

  it 'doesn\'t create proxy methods for nonexistant methods' do
    proxy.methods.should_not include(:foobarbaz)
    lambda { proxy.foobarbaz }.should raise_error NoMethodError
    proxy.methods.should_not include(:foobarbaz)
  end

  it 'sends "safe" methods to the current slave' do
    expect_query_on(slave1, :select_all, "SOME SQL")
    expect_query_on(slave1, :active?)
    expect_query_on(slave1, :columns, "employees")
  end

  it 'sends unsafe methods to the master' do
    expect_query_on(master, :insert, "INSERT INTO foo")
    expect_query_on(master, :delete, "DELETE FROM foo")
  end

  describe 'transactions' do

    it 'switches to master-only mode when a transaction is started' do
      expect_query_on(slave1, :select_all, "SELECT * FROM employees")
      expect_query_on(master, :begin_db_transaction)
      expect_query_on(master, :insert, "INSERT INTO employees")
      expect_query_on(master, :commit_db_transaction)
    end

    it 'switches back out of master-only mode when a transaction is committed' do
      expect_query_on(master, :begin_db_transaction)
      expect_query_on(master, :insert, "INSERT INTO employees")
      expect_query_on(master, :commit_db_transaction)
      Thread.current[:sticky_expires] = nil
      expect_query_on(slave1, :select_all, "SELECT * FROM employees")
    end

    it 'switches back out of master-only mode when a transaction is rolled back' do
      expect_query_on(master, :begin_db_transaction)
      expect_query_on(master, :insert, "INSERT INTO employees")
      expect_query_on(master, :rollback_db_transaction)
      Thread.current[:sticky_expires] = nil
      expect_query_on(slave1, :select_all, "SELECT * FROM employees")
    end

    it 'sends even safe queries to the master when in master-only mode' do
      expect_query_on(master, :begin_db_transaction)
      expect_query_on(master, :select_all, "SELECT * FROM employees")
      expect_query_on(master, :rollback_db_transaction)
      expect_query_on(slave1, :select_all, "SELECT * FROM employees")
    end

  end

  describe 'stickiness' do

    it 'marks the session as sticky on unsafe queries, causing safe queries to go to master' do
      expect_query_on(master, :insert, "INSERT INTO employees")
      expect_query_on(master, :select_all, "SELECT * FROM employees")
    end

    it 'does not mark sessions as sticky for safe queries that are routed to the master' do
      expect_query_on(master, :begin_db_transaction)
      expect_query_on(master, :select_all, "SELECT * FROM employees")
      expect_query_on(master, :rollback_db_transaction)
      expect_query_on(slave1, :select_all, "SELECT * FROM employees")
    end

  end

  describe 'statistics' do
    specify { pending }
  end

  private

  def expect_query_on(connection, method, *args)
    connection.should_receive(method)
    proxy.send(method, *args)
  end

end
