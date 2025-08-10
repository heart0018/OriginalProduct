class Api::V1::CardsController < ApplicationController
 def index
  user_location = if params[:lat].present? && params[:lng].present?
    [ params[:lat].to_f, params[:lng].to_f ]
  else
    nil
  end

  limit = (params[:limit] || 10).to_i
  offset = (params[:offset] || 0).to_i

  # 最大10件まで
  limit = [ limit, 10].min
  offset = [ offset, 0 ].max

  cards = Card.includes(:review_comments).limit(limit).offset(offset).order(:id)

  render json: cards.map { |card|
    card.to_frontend_json(user_location || [ 36.5689565, 140.0689112 ])
  }
end
  def show
    @card = Card.find(params[:id])

    user_location = if params[:lat].present? && params[:lng].present?
      [ params[:lat].to_f, params[:lng].to_f ]
    else
      nil
    end

    render json: @card.to_frontend_json([ 36.5689565, 140.0689112 ]) # ここは実際のユーザーの位置情報を使うべき
  end
end
