class Api::V1::CardsController < ApplicationController
 def index
  limit = (params[:limit] || 10).to_i
  offset = (params[:offset] || 0).to_i

  # 最大10件まで
  limit = [ limit, 10 ].min
  offset = [ offset, 0 ].max

  cards = Card.includes(:review_comments)

  # 地域フィルタ: region=関東 または region=関東,近畿 のように指定可能
  if params[:region].present?
    regions = params[:region].to_s.split(",").map { |s| s.strip }.reject(&:empty?)
    cards = cards.where(region: regions) if regions.any?
  end

  # 地域順ソート: sort=region を指定、任意で region_order をカンマ区切りで受け付け
  if params[:sort].to_s == "region"
    default_order = [
      "北海道",
      "東北",
      "関東",
      "中部",
      "近畿",
      "中国",
      "九州"
    ]
    region_order = if params[:region_order].present?
      params[:region_order].to_s.split(",").map { |s| s.strip }.reject(&:empty?)
    else
      default_order
    end

    # 未指定/不一致の地域とNULLは末尾へ
    quoted = region_order.map { |name| ActiveRecord::Base.connection.quote(name) }
    if quoted.any?
      # MySQL向け: CASE 式で安定ソート、その後 id 昇順
      case_sql = [ "CASE" ].tap do |sql|
        region_order.each_with_index do |name, idx|
          sql << "WHEN region = #{ActiveRecord::Base.connection.quote(name)} THEN #{idx}"
        end
        sql << "ELSE #{region_order.length} END"
      end.join(" ")
      cards = cards.order(Arel.sql(case_sql)).order(:id)
    else
      cards = cards.order(:id)
    end
  else
    cards = cards.order(:id)
  end

  cards = cards.limit(limit).offset(offset)

  render json: cards.map { |card|
    loc = if params[:lat].present? && params[:lng].present?
      [ params[:lat].to_f, params[:lng].to_f ]
    else
      nil
    end
    card.to_frontend_json(loc || [ 35.65856, 139.745461 ])
  }
 end
  def show
    @card = Card.find(params[:id])

  render json: @card.to_frontend_json([ 35.65856, 139.745461 ]) # ここは実際のユーザーの位置情報を使うべき
  end
end
