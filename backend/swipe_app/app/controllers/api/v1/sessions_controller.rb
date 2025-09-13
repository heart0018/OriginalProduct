class Api::V1::SessionsController < ApplicationController
  # skip_before_action :verify_authenticity_token  # 不要

  def show
    if (uid = session[:user_id]) && (user = User.find_by(id: uid))
      render json: user
    else
      render json: { authenticated: false }, status: :unauthorized
    end
  end

  def destroy
    reset_session
    head :no_content
  end
end
