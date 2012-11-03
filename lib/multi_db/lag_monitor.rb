module MultiDb
  module LagMonitor

    # There's no especially solid reasoning behind these factors.
    # STICKY_DURATION_PADDING ensures that even if a slave is reporting
    # no latency, we still bank on there being a little bit.
    REPLICA_LAG_THRESHOLD      = 10 # seconds
    STICKY_DURATION_MULTIPLIER = 1.2 # coefficient
    STICKY_DURATION_PADDING    = 3 # seconds

    # How long, after doing a write, should all reads be sent to the master?
    def self.sticky_master_duration(connection) # in seconds
      ((slave_lag(connection) * STICKY_DURATION_MULTIPLIER) + STICKY_DURATION_PADDING).ceil
    end

    # In exceptionally slow replication scenarios, we'd rather just redirect
    # everything to master and fail hard than show especially inconsistent
    # application state.
    def self.replication_lag_too_high?(connection)
      slave_lag(connection) > REPLICA_LAG_THRESHOLD
    end

    private

    def self.slave_lag(klass)
      cache_fetch("slave_lag:#{klass.name}") {
        actual_slave_lag(klass)
      }
    end

    def self.cache_fetch(key, expiry = 10, &block)
      @lag_cache ||= {}
      value, expire_time = @lag_cache[key]
      if expire_time.nil? || expire_time < Time.now
        value = Rails.cache.fetch(key, :expires_in => expiry / 2, &block)
        @lag_cache[key] = [value, Time.now + expiry]
      end
      value
    end

    def self.report_lag_statistic(connection_name, lag)
      # hook method
    end

    def self.actual_slave_lag(connection_class)
      connection = connection_class.retrieve_connection
      result = connection.execute("SHOW SLAVE STATUS")
      index = result.fields.index("Seconds_Behind_Master")
      lag = result.first.try(:[], index).to_i

      report_lag_statistic(connection_class.name, lag)

      lag
    end

  end
end

