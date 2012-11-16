require './lib/multi_db/scheduler'
require 'set'

describe MultiDb::Scheduler do

  let(:s1) { mock("s1", name: "Slave1") }
  let(:s2) { mock("s2", name: "Slave2") }
  let(:s3) { mock("s3", name: "Slave3") }
  let(:s4) { mock("s4", name: "Slave4") }

  before(:each) do
    lag = {"Slave1" => 0, "Slave2" => 0, "Slave3" => 0, "Slave4" => 0}
    MultiDb::LagMonitor.stub(replica_lag_for_connections: [0,0,0,0])
    @items = [s1, s2, s3, s4]
    @scheduler = MultiDb::Scheduler.new(@items.clone, 1)
    @scheduler.next until @scheduler.current == @items.first
  end

  it "should return items in a round robin fashion" do
    first = @items.shift
    @scheduler.current.should == first
    @items.each do |item|
      @scheduler.next.should == item
    end
    @scheduler.next.should == first
  end

  it 'should not return blacklisted items' do
    @scheduler.blacklist!(s2)
    @items.size.times do
      @scheduler.next.should_not == s2
    end
  end

  describe 'finding items with acceptable lag' do
    it 'knows whether the current item is within acceptable lag tolerance' do
      @scheduler.stub(replica_lag: [0,0,0,0])
      item = @scheduler.item_with_replica_lag_less_than(5)
      item.should == @scheduler.current
    end

    it 'can find an item with acceptable replica lag' do
      @scheduler.stub(replica_lag: [8,0,0,0])
      item = @scheduler.item_with_replica_lag_less_than(5)
      item.should_not == @scheduler.current
    end

    it 'finds items randomly' do
      @scheduler.stub(replica_lag: [8,0,0,0])
      items = Set.new
      200.times {
        items << @scheduler.item_with_replica_lag_less_than(5)
      }
      items.should == Set.new.tap{|s|s << s2 << s3 << s4}
    end

    it "skips any slaves that are not currently replicating" do
      @scheduler.stub(replica_lag: [MultiDb::LagMonitor::NotReplicating, 0,0,0])
      item = @scheduler.item_with_replica_lag_less_than(5)
      item.should_not == s1
    end

    it "doesn't return blacklisted items when searching for acceptable lag" do
      @scheduler.stub(replica_lag: [8,0,0,0])
      @scheduler.blacklist!(s2)
      @scheduler.blacklist!(s3)
      item = @scheduler.item_with_replica_lag_less_than(5)
      item.should == s4
    end

    it 'raises if there are no acceptable slaves' do
      @scheduler.stub(replica_lag: [9,0,9,9])
      @scheduler.blacklist!(s2)
      lambda {
        @scheduler.item_with_replica_lag_less_than(5)
      }.should raise_error(MultiDb::Scheduler::NoMoreItems)
    end

  end

  describe 'retrieving replica lag' do

    it 'asks LagMonitor' do
      Speedytime.stub(current: Speedytime.current + 1) # cheap cache invalidation
      MultiDb::LagMonitor.stub(:replica_lag_for_connections).and_return([42,0,7,0])
      @scheduler.replica_lag.should == [42,0,7,0]
    end

    it 'caches for one second' do
      MultiDb::LagMonitor.should_receive(:replica_lag_for_connections).twice.and_return([42,0,7,0])

      time = Speedytime.current

      Speedytime.stub(current: time + 1)
      10.times { @scheduler.replica_lag }
      Speedytime.stub(current: time + 2)
      10.times { @scheduler.replica_lag }
    end

    it 'raises if a class is missing from the results' do
      Speedytime.stub(current: Speedytime.current + 1) # cheap cache invalidation
      MultiDb::LagMonitor.stub(:replica_lag_for_connections).and_return([0,7,0])
      lambda {
        @scheduler.replica_lag
      }.should raise_error(RuntimeError)
    end

  end

  it 'should raise NoMoreItems if all are blacklisted' do
    @items.each do |item|
      @scheduler.blacklist!(item)
    end
    lambda {
      @scheduler.next
    }.should raise_error(MultiDb::Scheduler::NoMoreItems)
  end

  it 'should unblacklist items automatically' do
    Speedytime.stub(current: 1000)
    @scheduler.current.should == s1
    @scheduler.blacklist!(s2)
    Speedytime.stub(current: 1002)
    @scheduler.next.should == s2
  end

  describe '(accessed from multiple threads)' do

    it '#current and #next should return the same item for the same thread' do
      @scheduler.next until @scheduler.current == @items.first
      # gross, but...
      @scheduler.instance_variable_get("@_tlattr_current_index").default = 0
      3.times {
        Thread.new do
          @scheduler.current.should == s1
          @scheduler.next.should == s2
        end.join
      }
      @scheduler.next.should == s2
    end

  end

end

