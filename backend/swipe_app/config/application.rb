require_relative "boot"
require "rails/all"

Bundler.require(*Rails.groups)

module SwipeApp
  class Application < Rails::Application
    config.load_defaults 8.0
    config.api_only = true

    # APIでもCookie/Sessionを使う
    config.middleware.use ActionDispatch::Cookies
    config.middleware.use ActionDispatch::Session::CookieStore, key: "_swipe_app_session"
  end
end
