require 'tlattr_accessors'
require 'multi_db/scheduler'
require 'multi_db/active_record_extensions'
require 'multi_db/observer_extensions'
require 'multi_db/query_cache_compat'
require 'multi_db/connection_proxy'
require 'multi_db/lag_monitor'
require 'multi_db/query_analyzer'
require 'multi_db/slave_initialization'
require 'multi_db/session'
require 'multi_db/railtie'
require 'multi_db/connection_stack'

module MultiDb

  def self.disconnect_slaves!
    slave_classes.each do |slave|
      slave.connection.disconnect!
    end
  end

  def self.reconnect_slaves!
    slave_classes.each do |slave|
      slave.establish_connection slave.instance_variable_get("@_multidb_connection_name")
    end
  end

  private

  def self.slave_classes
    MultiDb.constants.
      map{|c|MultiDb.const_get c}.
      select{|c|c.ancestors.include? ActiveRecord::Base}
  end

end
