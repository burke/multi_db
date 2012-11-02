require 'rails/railtie'

module MultiDb
  class Railtie < ::Rails::Railtie

    def self.insert!
      ActiveRecord::Base.send :include, MultiDb::ActiveRecordExtensions

      after_init = lambda { |*args|
        ActiveRecord::Observer.send :include, MultiDb::ObserverExtensions
        ActionController::Base.send :include, MultiDb::Session
      }

      # makes testing easier.
      if Rails.application
        Rails.application.config.after_initialize(&after_init)
      else
        after_init.call
      end

    end

    initializer 'multi_db.insert' do
      MultiDb::Railtie.insert!
    end

  end
end
