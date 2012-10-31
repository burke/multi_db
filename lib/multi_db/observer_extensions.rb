module MultiDb
  module ObserverExtensions
    def self.included(base)
      base.alias_method_chain :update, :masterdb
    end

    # Send observed_method(object) if the method exists.
    def update_with_masterdb(observed_method, *objects) #:nodoc:
      klass = objects[0].class == Class ? objects[0] : objects[0].class

      if klass.connection.respond_to?(:with_master)
        klass.connection.with_master do
          update_without_masterdb(observed_method, *objects)
        end
      else
        update_without_masterdb(observed_method, *objects)
      end
    end
  end
end
