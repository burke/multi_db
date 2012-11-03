module MultiDb
  module ActiveRecordExtensions
    def self.included(base)
      base.send :include, InstanceMethods
      base.send :extend, ClassMethods
      base.cattr_accessor :connection_proxy
      # handle subclasses which were defined by the framework or plugins
      base.hijack_connection
      base.send(:descendants).each do |child|
        child.hijack_connection
      end
      class << base
        alias_method_chain :establish_connection, :proxy
      end
    end

    module InstanceMethods
      def reload(options = nil)
        self.connection_proxy.with_master { super }
      end
    end

    module ClassMethods

      def config_implies_we_keep_multidb(config)
        return true if config.nil?
        config = config.to_s if Symbol === config
        if String === config
          return config =~ /slave_database/
        end
        false
      end

      # The goal here is to clobber MultiDb if establish_connection is called with anything other than nil or *slave_database*
      # This is complicated by the fact that establish_connection calls itself recursively with increasingly
      # more specific config specifiers, even when called with nil. The ivar/lvar magic deals with that.
      # This doesn't feel like the *right* solution though. TODO: Figure out the right solution.
      def establish_connection_with_proxy(config = nil)
        if config_implies_we_keep_multidb(config)
          owner_of_ivar = :me
          @dont_kill_multidb = true
        end

        if !@dont_kill_multidb
          unhijack_connection
        end

        establish_connection_without_proxy(config)
      ensure
        remove_instance_variable(:@dont_kill_multidb) if owner_of_ivar
      end

      # Make sure transactions always switch to the master
      def transaction(options = {}, &block)
        if self.connection.kind_of?(ConnectionProxy)
          super
        else
          self.connection_proxy.with_master { super }
        end
      end

      # make caching always use the ConnectionProxy
      def cache(&block)
        if ActiveRecord::Base.configurations.blank?
          yield
        else
          self.connection_proxy.cache(&block)
        end
      end

      def inherited(child)
        super
        child.hijack_connection
      end

      def unhijack_connection(recurse = true)
        return unless methods(false).include?(:_actual_connection_before_multidb)
        class << self
          remove_method :connection
          alias_method :connection, :retrieve_connection
          remove_method :_actual_connection_before_multidb
        end
        return unless recurse
        descendants.each do |klass|
          klass.unhijack_connection(false)
        end
      end

      def hijack_connection
        logger.info "[MULTIDB] hijacking connection for #{self.to_s}" if logger
        class << self
          alias_method :_actual_connection_before_multidb, :connection
          def connection
            self.connection_proxy.establish_initial_connection
          end
        end
      end
    end
  end
end
