require 'rails/railtie'

module MultiDb
  class Railtie < ::Rails::Railtie

    def self.insert!
      slaves = MultiDb.init_slaves
      raise "No slaves databases defined for environment: #{Rails.env}" if slaves.empty?

      ActiveRecord::Base.send     :include, MultiDb::ActiveRecordExtensions
      proxy = MultiDb::ConnectionProxy.new(ActiveRecord::Base, slaves)
      ActiveRecord::Base.connection_proxy = proxy

      after_init = lambda { |*args|
        ActiveRecord::Observer.send :include, MultiDb::ObserverExtensions
        ActionController::Base.send :include, MultiDb::Session

        ActiveRecord::Base.logger.info("** multi_db with master and #{slaves.length} slave#{"s" if slaves.length > 1} loaded.")
      }

      # makes testing easier.
      if Rails.application
        Rails.application.config.after_initialize(&after_init)
      else
        after_init.call
      end

    end

    initializer 'multi_db.insert' do
      if $0 =~ /rake$/ && ARGV.grep(/^db:/).any?
        puts "\x1b[33mNot initializing multidb for db:* rake task\x1b[0m"
      else
        MultiDb::Railtie.insert!
      end
    end

  end
end
