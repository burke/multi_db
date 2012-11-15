require 'speedytime'
require File.expand_path '../query_analyzer', __FILE__

module MultiDb
  module LagMonitor

    NotReplicating = :not_replicating
    LAG_CACHE_KEY = "multidb:replica_lag"

    # In exceptionally slow replication scenarios, we'd rather just redirect
    # everything to master and fail hard than show especially inconsistent
    # application state.
    def self.replication_lag_too_high?(connection)
      lag = slave_lag(connection)
      lag == NotReplicating || lag > QueryAnalyzer::REPLICA_LAG_THRESHOLD
    end

    def self.replica_lag_for_connections(items)
      lag_cache_fetch do
        # MultiDb.logger.warn "Replica lag cache worker hasn't updated the cache in over 2 seconds!"
        items.map { |item| slave_lag(item) }
      end
    end

    private

    def self.cache
      Rails.cache
    end

    # caches locally for one second
    def self.lag_cache_fetch(&block)
      value, cached_at = @lag_cache
      current_time = Speedytime.current
      if cached_at.nil? || current_time != cached_at
        value = cache.fetch(LAG_CACHE_KEY, :expires_in => 1, &block)
        @lag_cache = [value, current_time]
      end
      value
    end

    def self.report_lag_statistic(connection_name, lag)
      # hook method
    end

    def self.slave_lag(connection_class)
      connection = connection_class.retrieve_connection
      lag = slave_lag_from_mysql(connection)

      # If the database is not currently replicating,
      # SHOW SLAVE STATUS returns no rows.
      return NotReplicating if lag.nil?

      report_lag_statistic(connection_class.name, lag.to_i)

      lag.to_i
    end

    def self.slave_lag_from_mysql(connection)
      result = connection.execute("SHOW SLAVE STATUS")
      index = result.fields.index("Seconds_Behind_Master")
      lag = result.first.try(:[], index)
    end

  end
end

