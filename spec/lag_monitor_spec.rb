require './lib/multi_db/lag_monitor'

describe MultiDb::LagMonitor do

  describe "sticky_master_duration" do

    it "returns 3 seconds even when there is no replica lag" do
      subject.stub(slave_lag: 0)
      subject.sticky_master_duration(anything).should == 3
    end

    it "pads a bit" do
      subject.stub(slave_lag: 1)
      subject.sticky_master_duration(anything).should == 5

      subject.stub(slave_lag: 2)
      subject.sticky_master_duration(anything).should == 6

      subject.stub(slave_lag: 3)
      subject.sticky_master_duration(anything).should == 7
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
