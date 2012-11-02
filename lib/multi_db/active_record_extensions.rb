module MultiDb
  module ActiveRecordExtensions
    def self.included(base)
      base.send :include, InstanceMethods
      base.send :extend, ClassMethods
      base.cattr_accessor :connection_proxy
      # handle subclasses which were defined by the framework or plugins
      base.hijack_connection
      class << base
        alias_method_chain :establish_connection, :multidb
      end
      base.send(:descendants).each do |child|
        child.hijack_connection
      end
    end

    module InstanceMethods
      def reload(options = nil)
        self.connection_proxy.with_master { super }
      end
    end

    module ClassMethods
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

      def establish_connection_with_multidb(spec = nil)
        establish_connection_without_multidb(spec)
        if ActiveRecord::Base::ConnectionSpecification === spec && name !~ /^MultiDb::/
          hijack_connection(spec) if respond_to?(:hijack_connection)
        end
      end

      def hijack_connection(spec=nil)
        logger.info "[MULTIDB] hijacking connection for #{self.to_s}" if logger

        spec = connection_pool.spec # the hash loaded from yaml
        self.connection_proxy = ConnectionProxyFactory.build(spec)

        metaclass = class << self ; self ; end
        metaclass.send(:define_method, :connection) {
          connection_proxy.establish_initial_connection
        }
      end
    end
  end
end
