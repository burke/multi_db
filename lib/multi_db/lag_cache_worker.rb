module MultiDb
  module LagCacheWorker
    def self.run!
      %w(INT TERM SIGUSR2).each{|sig| trap(sig) { Rails.logger.info("Caught #{sig}, terminating."); $exit = true } }

      Rails.logger.info "[MULTIDB] initializing cache worker"

      $lag_values = []
      $lag_heartbeats = []

      klasses = MultiDb.slave_classes
      klasses.each.with_index do |klass, index|
        start_slave_watcher_thread(klass, index)
      end

      sleep 2 # let the threads complete their initial iteration so we have heartbeats.

      Rails.logger.info "[MULTIDB] cache worker running"

      while !$exit do
        if $lag_heartbeats.any? { |ts| ts < (Speedytime.current - 5) }
          abort "Slave monitoring thread died! Terminating."
        end

        Rails.cache.write(LagMonitor::LAG_CACHE_KEY, $lag_values, expires_in: 2)
        Rails.logger.info "[MULTIDB] Writing lag values to cache: #{$lag_values.inspect}"
        break if $exit
        sleep 0.5
      end

    end

    def self.start_slave_watcher_thread(klass, index)
      Thread.new do
        loop do
          v = begin
            LagMonitor.slave_lag(klass)
          rescue ConnectionProxy::RECONNECT_EXCEPTIONS => e
            Rails.logger.error "[MULTIDB] Can't reach database: #{e.message}"
            LagMonitor::NotReplicating
          rescue => e
            Rails.logger.error "[MULTIDB] Cache worker failed to connect!: #{e.message}"
            LagMonitor::NotReplicating
          end

          $lag_heartbeats[index] = Speedytime.current
          $lag_values[index] = v
          sleep 0.5
        end
      end
    end

  end
end
