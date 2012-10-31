module MultiDb
  module Session

    def self.included(base)
      base.prepend_around_filter :store_session_for_multi_db
    end

    def store_session_for_multi_db
      Thread.current[:multi_db_session_get] = lambda { request.session }
      yield
      Thread.current[:multi_db_session_get] = nil
    end

    def self.current_session
      if getter = Thread.current[:multi_db_session_get]
        getter.call()
      else
        Thread.current
      end
    end

  end
end
