require 'speedytime'

module MultiDb
  module QueryAnalyzer

    # See specs for sample matches
    KEYWORD = /(?:JOIN|FROM|INTO|UPDATE)/i
    TABLE_NAME = /`?(\w+)`?/
    MORE_TABLES = /(?:\s*,\s*`?(?:\w+)`?)/ # for e.g.: SELECT * FROM `a`, `b`
    TABLE_MATCH = /#{KEYWORD}\s+#{TABLE_NAME}(#{MORE_TABLES}*)/

    Unbounded = :unbounded

    TEMP_DISABLE = false

    REPLICA_LAG_THRESHOLD = 10 # Slaves over this threshold won't be considered.
    STICKY_PADDING_FACTOR = 2 # 

    def self.max_lag_for_query(session, sql)
      sess_mdb = session[:multi_db]
      return Unbounded if sess_mdb.nil? or sess_mdb[:last_write].nil?

      TEMP_DISABLE and return max_lag_from_timestamp(sess_mdb[:last_write])

      sess_tables = sess_mdb[:table_writes]
      return Unbounded if sess_tables.nil?

      latest_write = tables(sql).map { |table| sess_tables[table] }.max

      latest_write.nil? ? Unbounded : max_lag_from_timestamp(latest_write)
    end

    def self.record_write_to_session(session, sql)
      sess_mdb = session[:multi_db] ||= {}
      current_time = Speedytime.current

      sess_mdb[:last_write] = current_time

      TEMP_DISABLE and return session

      sess_tables = sess_mdb[:table_writes] ||= {}

      sess_tables.each do |table, last_write|
        if last_write < (current_time - (REPLICA_LAG_THRESHOLD + 5))
          sess_tables.delete(table)
        end
      end

      tables(sql).each do |table|
        sess_tables[table] = current_time
      end

      session
    end

    def self.tables(sql)
      return [] unless String === sql
      tables = Set.new
      sql.scan(TABLE_MATCH).each do |table_name, more_tables|
        tables << table_name.to_sym
        next if more_tables.empty?
        more_tables.split(/\s*,\s*/).drop(1).each do |table|
          tables << table.tr('`', '').to_sym
        end
      end
      tables.to_a
    end

    def self.max_lag_from_timestamp(ts)
      unpadded = Speedytime.current - ts
      padded = unpadded + STICKY_PADDING_FACTOR
      padded < 0 ? 0 : padded
    end

  end
end
