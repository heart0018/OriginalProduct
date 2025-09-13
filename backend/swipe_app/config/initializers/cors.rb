# config/initializers/cors.rb

allow_origins = ENV.fetch("CORS_ORIGINS", "").split(",").map(&:strip).reject(&:empty?)
default_origins = [ "http://127.0.0.1:5173" ]

Rails.application.config.middleware.insert_before 0, Rack::Cors do
  allow do
    origins(*(allow_origins.presence || default_origins))
    resource "/api/*",
      headers: :any,
      methods: [ :get, :post, :patch, :put, :delete, :options ],
      credentials: true,
      max_age: 600
  end
end
