namespace :multidb do

  desc 'Starts a worker to push slave lag values into memcached every second'
  task start_replica_lag_cache_worker: :environment do
    MultiDb::LagCacheWorker.run!
  end

end
