require './lib/multi_db/connection_stack'

describe MultiDb::ConnectionStack do
  CS = MultiDb::ConnectionStack

  let(:master) { stub("master") }
  let(:slave1) { stub("slave1") }
  let(:slave2) { stub("slave2") }

  let(:scheduler) {
    stub(current: slave1, next: slave2)
  }

  subject { CS.new(master, scheduler) }

  before do
    subject.stub(logger: stub(warn: nil))
  end

  it 'defaults to the master connection' do
    subject.current.should == master
  end

  it 'can use a slave' do
    subject.push_slave
    subject.current.should == slave1
  end

  it 'can use a master' do
    subject.push_slave
    subject.push_master
    subject.current.should == master
  end

  it 'can use a slave for a block' do
    subject.current.should == master
    subject.with_slave do
      subject.current.should == slave1
    end
    subject.current.should == master
  end

  it 'can use a master for a block' do
    subject.push_slave
    subject.current.should == slave1
    subject.with_master do
      subject.current.should == master
    end
    subject.current.should == slave1
  end

  it 'can use a slave for a block conditional on there being no open transactions' do
    master.stub(retrieve_connection: stub(open_transactions: 0))
    subject.with_slave_unless_in_transaction do
      subject.current.should == slave1
    end
    master.stub(retrieve_connection: stub(open_transactions: 1))
    subject.with_slave_unless_in_transaction do
      subject.current.should == master
    end
  end

  it 'can pop off the stack' do
    subject.push_slave
    subject.pop
    subject.current.should == master
  end

  it 'mutates the current stack entry to get the next reader' do
    subject.push_slave
    subject.current.should == slave1
    subject.next_reader!
    subject.current.should == slave2
    subject.pop
    subject.current.should == master
  end

  it 'mutates the current stack entry to master if the scheduler has no items' do
    subject.push_slave
    subject.current.should == slave1
    scheduler.should_receive(:next).and_raise(MultiDb::Scheduler::NoMoreItems)
    subject.next_reader!
    subject.current.should == master
    subject.pop
    subject.current.should == master
  end

  it 'blacklists items' do
    subject.push_slave
    scheduler.should_receive(:blacklist!).with(slave1)
    subject.blacklist_current!
    subject.current.should == slave2
  end

  it 'blacklists slaves with unacceptable replication statuses' do
    Thread.current[:sticky_expires] = nil
    MultiDb::LagMonitor.should_receive(:replication_lag_too_high?).with(slave1).and_return(true)
    MultiDb::LagMonitor.should_receive(:replication_lag_too_high?).with(slave2).and_return(false)
    scheduler.should_receive(:blacklist!).with(slave1)
    subject.with_slave do
      subject.find_up_to_date_reader!
    end
  end

  describe 'with_slave_having_replica_lag_under' do
    before do
      subject.push_slave
      scheduler.stub(:item_with_replica_lag_less_than).and_return(slave2)
    end

    it 'finds a slave with acceptable replica lag' do
      subject.current.should == slave1
      subject.with_slave_having_replica_lag_under(5) do
        subject.current.should == slave2
      end
      subject.current.should == slave1
    end

    it 'does not switch away from master' do
      subject.push_master
      subject.with_slave_having_replica_lag_under(5) do
        subject.current.should == master
      end
      subject.current.should == master
    end

    it 'assigns the master if no suitable slave was found' do
      scheduler.stub(:item_with_replica_lag_less_than).and_raise(MultiDb::Scheduler::NoMoreItems)
      subject.current.should == slave1
      subject.with_slave_having_replica_lag_under(5) do
        subject.current.should == master
      end
      subject.current.should == slave1
    end

    it 'does not push an entry if the current stack frame is already acceptable' do
      subject.next_reader!
      subject.current.should == slave2
      subject.with_slave_having_replica_lag_under(5) do
        subject.current.should == slave2
      end
      subject.current.should == slave2
    end

  end

end
