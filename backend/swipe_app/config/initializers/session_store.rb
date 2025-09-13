Rails.application.config.session_store :cookie_store,
  key: "_swipe_app_session",
  secure: Rails.env.production?,
  httponly: true,
  same_site: (ENV["CROSS_SITE_COOKIE"] == "true" ? :none : :lax),
  domain: ENV["COOKIE_DOMAIN"].presence
