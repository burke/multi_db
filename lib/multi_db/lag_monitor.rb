module MultiDb
  module LagMonitor

    # There's no especially solid reasoning behind these factors.
    # STICKY_DURATION_PADDING ensures that even if a slave is reporting
    # no latency, we still bank on there being a little bit.
    REPLICA_LAG_THRESHOLD      = 10 # seconds
    STICKY_DURATION_MULTIPLIER = 1.2 # coefficient
    STICKY_DURATION_PADDING    = 0.5 # seconds

    # How long, after doing a write, should all reads be sent to the master?
    def self.sticky_master_duration(connection) # in seconds
      (slave_lag(connection) * STICKY_DURATION_MULTIPLIER) + STICKY_DURATION_PADDING
    end

    # In exceptionally slow replication scenarios, we'd rather just redirect
    # everything to master and fail hard than show especially inconsistent
    # application state.
    def self.replication_lag_too_high?(connection)
      slave_lag(connection) > REPLICA_LAG_THRESHOLD
    end

    private

    def self.slave_lag(klass)
      Rails.cache.fetch("slave_lag:#{klass.name}", :expires_in => 10.seconds) {
        actual_slave_lag(klass.retrieve_connection)
      }
    end

    def self.report_lag_statistic(lag)
      return unless defined?(StatsD)
      key = "MultiDb.slaveLag"
      StatsD.write(key, lag * 1000, :ms)
    end

    def self.actual_slave_lag(connection)
      result = connection.execute("SHOW SLAVE STATUS")
      index = result.fields.index("Seconds_Behind_Master")
      lag = result.first.try(:[], index).to_i

      report_lag_statistic(lag)

      lag
    end

  end
end

