module LogStashLogger
  class Middleware
    def initialize(app)
      @app = app
    end

    def call(env)
      @app.call(env)
    ensure
      Rails.logger.commit if Rails.logger.respond_to? :commit
    end
  end
end