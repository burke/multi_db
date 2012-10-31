require File.expand_path '../scheduler', __FILE__
require File.expand_path '../lag_monitor', __FILE__

module MultiDb
  class ConnectionStack

    def initialize(master, scheduler)
      @master = master
      @stack = [master]
      @scheduler = scheduler
    end

    def current
      @stack.first
    end

    def with_slave_unless_in_transaction(&block)
      if @master.retrieve_connection.open_transactions.zero?
        with_slave(&block)
      else
        yield
      end
    end

    def with_master
      push_master
      yield
    ensure
      pop
    end

    def with_slave
      push_slave
      yield
    ensure
      pop
    end

    def push_master
      @stack.unshift @master
    end

    def push_slave
      @stack.unshift slave
    end

    def pop
      @stack.shift
    end

    def slave
      @scheduler.current
    end

    def retrieve_connection
      current.retrieve_connection
    end

    def find_up_to_date_reader!
      # This will, as a worst case, terminate when we give up on
      # slaves and set current to master, since master always has
      # replica lag of 0.
      while LagMonitor.replication_lag_too_high?(current)
        blacklist_current!
      end
    end

    def blacklist_current!
      @scheduler.blacklist!(current)
      next_reader!
    end

    # Switches to the next slave database for read operations.
    # Fails over to the master database if all slaves are unavailable.
    def next_reader!
      return if @stack.first == @master
      @stack[0] = @scheduler.next
    rescue Scheduler::NoMoreItems
      logger.warn "[MULTIDB] All slaves are blacklisted. Reading from master"
      @stack[0] = @master
    end

    def master?
      current == @master
    end

    def logger
      ActiveRecord::Base.logger
    end

  end
end
