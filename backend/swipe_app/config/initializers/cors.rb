# config/initializers/cors.rb

Rails.application.config.middleware.insert_before 0, Rack::Cors do
  allow do
    origins "*"  # ← 本番ではここを限定する（例: 'https://your-frontend.com'）

    resource "*",
      headers: :any,
      methods: [ :get, :post, :options ]
  end
end
