require "googleauth"

class Api::V1::AuthController < ApplicationController
  # skip_before_action :verify_authenticity_token  # 不要

  def google
    id_token = params[:id_token].presence || params[:credential].to_s
    if id_token.blank?
      render json: { error: "id_tokenは必須です" }, status: :bad_request
      return
    end

    client_ids = ENV.fetch("GOOGLE_CLIENT_ID").split(",").map(&:strip)
    payload = Google::Auth::IDTokens.verify_oidc(id_token, aud: client_ids)

    user = User.find_or_create_by!(google_id: payload["sub"]) { |u| u.region = "未設定" }
    begin
      session[:user_id] = user.id
    rescue ActionDispatch::Request::Session::DisabledSessionError => e
      Rails.logger.error("auth#google session disabled: #{e.message}")
      # セッションが無効でも 200 は返す（後でミドルウェアを設定すること）
    end
    render json: user
  rescue Google::Auth::IDTokens::VerificationError => e
    Rails.logger.warn("auth#google verification failed: #{e.class} #{e.message}")
    render json: { error: "verification_failed", message: e.message }, status: :unauthorized
  rescue KeyError => e
    Rails.logger.error("auth#google env missing: #{e.message}")
    render json: { error: "server_misconfigured", message: "GOOGLE_CLIENT_ID 未設定" }, status: :internal_server_error
  rescue => e
    Rails.logger.error("auth#google failed: #{e.class} #{e.message}\n#{e.backtrace&.first(3)&.join("\n")}")
    render json: { error: "認証に失敗しました" }, status: :internal_server_error
  end
end
