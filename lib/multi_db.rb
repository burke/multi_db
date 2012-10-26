require 'tlattr_accessors'
require 'multi_db/scheduler'
require 'multi_db/active_record_extensions'
require 'multi_db/observer_extensions'
require 'multi_db/query_cache_compat'
require 'multi_db/connection_proxy'
require 'multi_db/lag_monitor'
require 'multi_db/query_analyzer'

module MultiDb
  def self.config
    @config ||= Config.new
  end

  class Config
    def initialize
      @only_profile = false
    end

    def only_profile?
      @only_profile
    end

    def only_profile!
      @only_profile = true
    end
  end
end
