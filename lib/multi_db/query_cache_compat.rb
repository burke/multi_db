module MultiDb
  # Implements the methods expected by the QueryCache module
  module QueryCacheCompat
    def select_all(*a, &b)
      send_to_current(:select_all, *a, &b)
    end

    def select_one(sql, name = nil)
      result = select_all(sql, name)
      result.first if result
    end

    def select_value(sql, name = nil)
      if result = select_one(sql, name)
        result.values.first
      end
    end

    def columns(*a, &b)
      send_to_current(:columns, *a, &b)
    end
    def insert(*a, &b)
      send_to_master(:insert, *a, &b)
    end
    def update(*a, &b)
      send_to_master(:update, *a, &b)
    end
    def delete(*a, &b)
      send_to_master(:delete, *a, &b)
    end
  end
end
