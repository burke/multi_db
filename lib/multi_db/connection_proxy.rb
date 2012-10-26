require 'active_record/connection_adapters/abstract/query_cache'

module MultiDb
  class ConnectionProxy
    include ActiveRecord::ConnectionAdapters::QueryCache
    include QueryCacheCompat
    extend ThreadLocalAccessors

    # Safe methods are those that should either go to the slave ONLY or go
    # to the current active connection.
    SAFE_METHODS = %w(
      select_all select_one select_value select_values
      select_rows select verify! raw_connection active? reconnect!
      disconnect! reset_runtime log log_info table_exists?
      sanitize_limit quote_table_name ids_in_list_limit quote
      quote_column_name prefetch_primary_key? case_sensitive_equality_operator
      table_alias_for columns indexes
    ).inject({}) { |acc, val|
      acc[val.to_sym]=true
      acc
    }.freeze

    IGNORABLE_METHODS = %w(
      log log_info sanitize_limit quote_table_name quote quote_column_name
      prefetch_primary_key?  case_sensitive_equality_operator table_alias_for
    ).inject({}) { |acc, val|
      acc[val.to_sym] = true
      acc
    }.freeze

    if ActiveRecord.const_defined?(:SessionStore) # >= Rails 2.3
      DEFAULT_MASTER_MODELS = ['ActiveRecord::SessionStore::Session']
    else # =< Rails 2.3
      DEFAULT_MASTER_MODELS = ['CGI::Session::ActiveRecordStore::Session']
    end

    attr_accessor :master
    tlattr_accessor :master_depth, :current, true

    class << self

      # defaults to Rails.env if multi_db is used with Rails
      # defaults to 'development' when used outside Rails
      attr_accessor :environment

      # a list of models that should always go directly to the master
      #
      # Example:
      #
      #  MultiDb::ConnectionProxy.master_models = ['MySessionStore', 'PaymentTransaction']
      attr_accessor :master_models

      # if master should be the default db
      attr_accessor :defaults_to_master

      # Replaces the connection of ActiveRecord::Base with a proxy and
      # establishes the connections to the slaves.
      def setup!(scheduler = Scheduler)
        self.master_models ||= DEFAULT_MASTER_MODELS
        self.environment   ||= (defined?(Rails) ? Rails.env : 'development')

        master = ActiveRecord::Base
        slaves = init_slaves
        raise "No slaves databases defined for environment: #{self.environment}" if slaves.empty?
        master.send :include, MultiDb::ActiveRecordExtensions
        ActiveRecord::Observer.send :include, MultiDb::ObserverExtensions
        master.connection_proxy = new(master, slaves, scheduler)
        master.logger.info("** multi_db with master and #{slaves.length} slave#{"s" if slaves.length > 1} loaded.")
      end

      protected

      # Slave entries in the database.yml must be named like this
      #   development_slave_database:
      # or
      #   development_slave_database1:
      # or
      #   production_slave_database_someserver:
      # These would be available later as MultiDb::SlaveDatabaseSomeserver
      def init_slaves
        slaves = []

        ActiveRecord::Base.configurations.each do |name, values|
          if name.to_s =~ /#{self.environment}_(slave_database.*)/
            if values['weight'].blank?
              weight = 1
            elsif (v=values['weight'].to_i.abs) > 0
              weight = v
            else
              weight = 1
            end
            MultiDb.module_eval %Q{
              class #{$1.camelize} < ActiveRecord::Base
                self.abstract_class = true
                establish_connection :#{name}
                WEIGHT = #{weight} unless const_defined?('WEIGHT')
              end
            }, __FILE__, __LINE__
            slaves << "MultiDb::#{$1.camelize}".constantize
          end
        end

        slaves
      end

      private :new

    end

    def initialize(master, slaves, scheduler = Scheduler)
      @slaves    = scheduler.new(slaves)
      @master    = master
      @reconnect = false
      @query_cache = {}
      if self.class.defaults_to_master
        self.current = @master
        self.master_depth = 1
      else
        self.current = @slaves.current
        self.master_depth = 0
      end
    end

    def slave
      @slaves.current
    end

    def scheduler
      @slaves
    end


    def with_master
      self.current = @master
      self.master_depth += 1
      yield
    ensure
      self.master_depth -= 1
      self.current = slave if (master_depth <= 0)
    end


    def with_slave
      self.current = slave
      self.master_depth -= 1
      yield
    ensure
      self.master_depth += 1
      self.current = @master if (master_depth > 0)
    end

    def transaction(start_db_transaction = true, &block)
      with_master { @master.retrieve_connection.transaction(start_db_transaction, &block) }
    end

    # Calls the method on master/slave and dynamically creates a new
    # method on success to speed up subsequent calls
    def method_missing(method, *args, &block)
      send(target_method(method), method, *args, &block).tap do
        create_delegation_method!(method)
      end
    end

    # Switches to the next slave database for read operations.
    # Fails over to the master database if all slaves are unavailable.
    def next_reader!
      return if  master_depth > 0  # don't if in with_master block
      self.current = @slaves.next
    rescue Scheduler::NoMoreItems
      logger.warn "[MULTIDB] All slaves are blacklisted. Reading from master"
      self.current = @master
    end

    protected

    def create_delegation_method!(method)
      self.instance_eval %Q{
        def #{method}(*args, &block)
          next_reader! if rand < 0.02
          #{target_method(method)}(:#{method}, *args, &block)
        end
      }, __FILE__, __LINE__
    end

    def target_method(method)
      return :send_to_master if unsafe?(method)

      # This will, as a worst case, terminate when we give up on
      # slaves and set current to master, since master always has
      # replica lag of 0.
      while LagMonitor.replication_lag_too_high?(current)
        @slaves.blacklist!(current)
        next_reader!
      end

      :send_to_current
    end

    NONCOMMUNICATING_MASTER_METHODS = [:open_transactions, :add_transaction_record]

    RECONNECT_EXCEPTIONS = [ActiveRecord::ConnectionNotEstablished]

    def stickify(method, sql)
      return if NONCOMMUNICATING_MASTER_METHODS.include?(method)
      return unless String === sql

      sess = Thread.current[:get_session].try(:call)
      sess ||= Thread.current # if not in a http request, just store in Thread.current

      duration = LagMonitor.sticky_master_duration(slave).seconds
      QueryAnalyzer.mark_sticky_tables_in_session(sess, sql, duration)
    end

    def send_to_master(method, *args, &block)
      stickify(method, args[0]) if unsafe?(method)

      record_statistic(method, "master")
      reconnect_master! if @reconnect
      with_master do
        @master.retrieve_connection.send(method, *args, &block)
      end
    rescue *RECONNECT_EXCEPTIONS => e
      raise_master_error(e)
    rescue ActiveRecord::StatementInvalid => e
      if e.message =~ /server has gone away/
        raise_master_error(e)
      else
        raise
      end
    end

    def record_statistic(method, connection)
      return if IGNORABLE_METHODS[method]
      StatsD.increment("MultiDB.queries.#{connection}", 1, 0.01)
    end

    def needs_sticky_master?(method, sql)
      return false unless String === sql

      sess = Thread.current[:get_session].try(:call)
      sess ||= Thread.current

      QueryAnalyzer.query_requires_sticky?(sess, sql)
    end

    def send_to_current(method, *args, &block)
      if needs_sticky_master?(method, args[0])
        with_master do
          return send_to_master(method, *args, &block)
        end
      end

      record_statistic(method, current.name)

      reconnect_master! if @reconnect && master?
      if Rails.env.test?
        @master.retrieve_connection.send(method, *args, &block)
      else
        current.retrieve_connection.send(method, *args, &block)
      end
    rescue *RECONNECT_EXCEPTIONS, ActiveRecord::StatementInvalid => e
      if e.class == ActiveRecord::StatementInvalid && e.message !~ /server has gone away/
        raise
      end

      raise_master_error(e) if master?
      logger.warn "[MULTIDB] Error reading from slave database"
      logger.error %(#{e.message}\n#{e.backtrace.join("\n")})
      @slaves.blacklist!(current)
      next_reader!
      retry
    end

    def reconnect_master!
      @master.retrieve_connection.reconnect!
      @reconnect = false
    end

    def raise_master_error(error)
      logger.fatal "[MULTIDB] Error accessing master database. Scheduling reconnect"
      @reconnect = true
      raise error
    end

    def unsafe?(method)
      !SAFE_METHODS[method]
    end

    def master?
      current == @master
    end

    def logger
      ActiveRecord::Base.logger
    end

  end
end

