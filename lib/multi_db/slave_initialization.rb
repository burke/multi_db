require 'active_support/core_ext/string'
require 'active_record'

module MultiDb

  # Slave entries in the database.yml must be named like this
  #   development_slave_database:
  # or
  #   development_slave_database1:
  # or
  #   production_slave_database_someserver:
  # These would be available later as MultiDb::SlaveDatabaseSomeserver
  def self.init_slaves(configs = ActiveRecord::Base.configurations)
    configs.map do |name, values|
      maybe_init_slave(name, values)
    end.compact
  end

  private

  def self.environment
    Rails.env
  end



  def self.maybe_init_slave(name, values)
    return unless name.to_s =~ /#{environment}_(slave_database.*)/
    klassname = $1.camelize

    weight = values['weight']
    weight = weight.blank? ? 1 : weight.to_i.abs
    weight.zero? and raise "weight can't be zero"

    MultiDb.module_eval <<-CODE, __FILE__, __LINE__
      class #{klassname} < ActiveRecord::Base
        self.abstract_class = true
        @_multidb_connection_name = :#{name}
        establish_connection @_multidb_connection_name
        WEIGHT = #{weight} unless const_defined?('WEIGHT')
      end
    CODE

    "MultiDb::#{klassname}".constantize
  end
end
