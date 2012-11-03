require 'digest/md5'

module MultiDb
  class ConnectionProxyFactory
    @specs_to_modules = {}
    @connection_proxies = {}

    # connection_spec is the has used to specify the connection in database.yml
    def self.build(connection_spec)
      master_connection = find_or_build_connection("master", connection_spec.config)

      slave_specs = connection_spec.config[:slaves] || {}
      slave_connections = slave_specs.map do |name, slave_spec|
        find_or_build_connection(name, slave_spec)
      end

      fetch_connection_proxy(master_connection, slave_connections)
    end

    def self.all_connection_classes
      @specs_to_modules.values
    end

    private

    def self.find_or_build_connection(name, spec)
      @specs_to_modules[spec] ||= build_connection(name, spec)
    end

    def self.build_connection(name, spec)
      digest = Digest::MD5.hexdigest(spec.inspect)[0..10]
      klassname = "#{name}#{digest}".camelize

      weight = spec['weight']
      weight = weight.blank? ? 1 : weight.to_i.abs
      weight.zero? and raise "weight can't be zero"

      MultiDb.module_eval <<-CODE, __FILE__, __LINE__
        class #{klassname} < ActiveRecord::Base
          self.abstract_class = true
          @_multidb_connection_parameters = #{spec.inspect}
          establish_connection @_multidb_connection_parameters
          WEIGHT = #{weight} unless const_defined?('WEIGHT')
        end
      CODE

      "MultiDb::#{klassname}".constantize
    end

    def self.fetch_connection_proxy(master, slaves)
      @connection_proxies[[master, slaves]] ||= ConnectionProxy.new(master, slaves)
    end

  end
end
