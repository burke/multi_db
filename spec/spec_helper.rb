require 'rubygems'
%w[tlattr_accessors yaml erb rspec logger].each {|lib| require lib}
require 'rails/all'
require 'active_record/connection_adapters/connection_handler'

module Rails
  def self.env
    ActiveSupport::StringInquirer.new("test")
  end
end

MULTI_DB_SPEC_DIR = File.dirname(__FILE__)
MULTI_DB_SPEC_CONFIG = YAML::load(File.open(MULTI_DB_SPEC_DIR + '/config/database.yml'))

ActiveRecord::Base.logger = Logger.new(MULTI_DB_SPEC_DIR + "/debug.log")
ActiveRecord::Base.configurations = MULTI_DB_SPEC_CONFIG
