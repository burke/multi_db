require './lib/multi_db/lag_monitor'

describe MultiDb::LagMonitor do

  describe "fetching the replica lag from cache" do

    let(:cache) {
      Object.new.tap do |o|
        class << o
          def fetch(*)
            yield
          end
        end
      end
    }

    before(:each) do
      MultiDb::LagMonitor.stub(cache: cache)
    end

    # stubbing out mysql would be preferable here, but it's a lot of work.
    # The MySQL access could be pulled into another object I guess.
    it 'returns a parallel array to the input array' do
      conns = [stub, stub]
      MultiDb::LagMonitor.should_receive(:slave_lag).with(conns[0]).and_return(MultiDb::LagMonitor::NotReplicating)
      MultiDb::LagMonitor.should_receive(:slave_lag).with(conns[1]).and_return(1)
      expected = [:not_replicating, 1]
      MultiDb::LagMonitor.instance_variable_set(:@lag_cache,nil)
      MultiDb::LagMonitor.replica_lag_for_connections(conns).should == expected
    end

    it 'caches the result for two seconds' do
      conns = [stub]
      MultiDb::LagMonitor.should_receive(:slave_lag).with(conns[0]).twice.and_return(0)

      Speedytime.stub(current: 1000)
      100.times { MultiDb::LagMonitor.replica_lag_for_connections(conns) }
      Speedytime.stub(current: 1001)
      100.times { MultiDb::LagMonitor.replica_lag_for_connections(conns) }
    end

  end

  describe "replication_lag_too_high?" do

    it "is false it the lag is zero" do
      subject.stub(slave_lag: 0)
      subject.replication_lag_too_high?(anything).should be_false
    end

    it "is true if the slave is not replicating" do
      subject.stub(slave_lag: MultiDb::LagMonitor::NotReplicating)
      subject.replication_lag_too_high?(anything).should be_true
    end

    it "is false it the lag is reasonable" do
      subject.stub(slave_lag: 10)
      subject.replication_lag_too_high?(anything).should be_false
    end

    it "is true it the lag is too high" do
      subject.stub(slave_lag: 11)
      subject.replication_lag_too_high?(anything).should be_true
    end

  end

end
