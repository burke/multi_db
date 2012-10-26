require 'set'

module MultiDb
  module QueryAnalyzer

    # See specs for sample matches
    KEYWORD = /(?:JOIN|FROM|INTO|UPDATE)/i
    TABLE_NAME = /`?(\w+)`?/
    MORE_TABLES = /(?:\s*,\s*`?(?:\w+)`?)/ # for e.g.: SELECT * FROM `a`, `b`
    TABLE_MATCH = /#{KEYWORD}\s+#{TABLE_NAME}(#{MORE_TABLES}*)/


    def self.query_requires_sticky?(session, query)
      exp = session[:sticky_expires]
      return false if exp.nil? || exp <= Time.now.to_i

      stickied = session[:sticky_tables] || {}
      tables(query).each do |asked_for|
        if stickied[asked_for] && stickied[asked_for] >= Time.now.to_i
          return true
        end
      end

      return false
    end

    def self.mark_sticky_tables_in_session(session, query, timeout)
      session[:sticky_tables] ||= {}

      expiry = Time.now.to_i + timeout.to_i
      if session[:sticky_expires].nil? || session[:sticky_expires] < expiry
        session[:sticky_expires] = expiry
      end

      tables(query).each do |table|
        session[:sticky_tables][table] = expiry
      end

      session[:sticky_tables].each do |k,v|
        session[:sticky_tables].delete(k) if v < expiry
      end

      session
    end


    def self.tables(sql)
      tables = Set.new
      sql.scan(TABLE_MATCH).each do |table_name, more_tables|
        tables << table_name
        next if more_tables.empty?
        more_tables.split(/\s*,\s*/).drop(1).each do |table|
          tables << table.tr('`', '')
        end
      end
      tables.to_a
    end

  end
end
