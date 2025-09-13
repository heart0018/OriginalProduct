class ApplicationController < ActionController::API
  include ActionController::Cookies
  include ActionController::RequestForgeryProtection
  # JSON API は CSRF 無効
  skip_forgery_protection if: -> { request.format.json? }
end
