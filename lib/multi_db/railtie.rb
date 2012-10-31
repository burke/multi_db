require 'rails/railtie'

module MultiDb
  class Railtie < ::Rails::Railtie

    def self.insert!
      slaves = MultiDb.init_slaves
      raise "No slaves databases defined for environment: #{Rails.env}" if slaves.empty?

      ActiveRecord::Base.send     :include, MultiDb::ActiveRecordExtensions
      proxy = MultiDb::ConnectionProxy.new(ActiveRecord::Base, slaves)
      ActiveRecord::Base.connection_proxy = proxy

      Rails.application.config.after_initialize do
        ActiveRecord::Observer.send :include, MultiDb::ObserverExtensions
        ActionController::Base.send :include, MultiDb::Session

        ActiveRecord::Base.logger.info("** multi_db with master and #{slaves.length} slave#{"s" if slaves.length > 1} loaded.")
      end

    end

    initializer 'multi_db.insert' do
      return if defined?(Rake) # we don't want to do this in rake tasks.

      MultiDb::Railtie.insert!
    end

  end
end
