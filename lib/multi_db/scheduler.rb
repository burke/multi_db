require 'tlattr_accessors'
require 'speedytime'
require 'active_support/core_ext/module/delegation'
require File.expand_path '../lag_monitor', __FILE__

module MultiDb
  class Scheduler
    NoMoreItems = Class.new(Exception)
    extend ThreadLocalAccessors

    attr :items
    delegate :[], :[]=, to: :items
    tlattr_accessor :current_index, true

    def initialize(items, blacklist_timeout = 30)
      @n = items.length
      @items     = items
      reset_blacklist
      fetch_replica_lag
      @blacklist_timeout = blacklist_timeout
      self.current_index = proc{rand(@n)}
    end

    def blacklist!(item)
      index = @items.index(item)
      @blacklist[index] = Speedytime.current if index
    end

    def reset_blacklist
      @blacklist = Array.new(@n, 0)
    end

    def current
      @items[current_index_i]
    end

    def current_index_i
      index = current_index
      if Proc === index
        self.current_index = index.call
      else
        index
      end
    end

    def next
      previous = current_index_i
      threshold = Speedytime.current - @blacklist_timeout
      until(@blacklist[next_index!] < threshold) do
        raise NoMoreItems, 'All items are blacklisted' if current_index == previous
      end
      current
    end

    def replica_lag
      if Speedytime.current == @replica_lag_cached_at
        @replica_lag
      else
        fetch_replica_lag
      end
    end

    def fetch_replica_lag
      lag_array = LagMonitor.replica_lag_for_connections(@items)

      if lag_array.any?(&:nil?) || lag_array.size != @n
        raise "Unexpected result from replica lag aggregation: Got #{lag_array.inspect}"
      end

      @replica_lag_cached_at = Speedytime.current
      @replica_lag = lag_array
    end

    def item_with_replica_lag_less_than(max_lag)
      lag = replica_lag[current_index_i]
      # if the current item is acceptable, return it
      return current if lag != LagMonitor::NotReplicating && lag < max_lag

      # the current item is not acceptable. Try to find one that is.
      # Choose a random index to start from, and find a slave that isn't
      # blacklisted and has a lag <= the max_lag
      starting_index = index = rand(@n)
      blacklist_threshold = Speedytime.current - @blacklist_timeout
      loop do
        if @blacklist[index] < blacklist_threshold &&
          replica_lag[index] != LagMonitor::NotReplicating &&
          replica_lag[index] < max_lag
        then
          return @items[index]
        end
        index = next_index(index)
        raise NoMoreItems, 'No suitable item' if index == starting_index
      end
    end

    protected

    def next_index!
      self.current_index = next_index(current_index)
    end

    def next_index(index)
      (index + 1) % @n
    end

  end
end
