class Card < ApplicationRecord
  has_many :user_cards, dependent: :destroy
  has_many :users, through: :user_cards
  has_many :review_comments, dependent: :destroy

  validates :title, presence: true, length: { maximum: 128 }
  validates :rating, presence: true, numericality: { greater_than_or_equal_to: 0.0, less_than_or_equal_to: 5.0 }
  validates :review_count, presence: true, numericality: { greater_than_or_equal_to: 0 }
  validates :place_id, uniqueness: true, allow_blank: true
  validates :genre, length: { maximum: 32 }
  # DBの制約（varchar(1000)）に合わせる
  validates :image_url, length: { maximum: 1000 }
  validates :external_link, length: { maximum: 256 }
  validates :region, length: { maximum: 16 }
  validates :address, length: { maximum: 128 }

def to_frontend_json(user_location = nil)
  {
    id: id,
    title: title,
    type: self.genre,
    region: region,
    address: address,
    latitude: latitude,
    longitude: longitude,
    distance_km: user_location ? calc_distance_km(user_location) : nil,
    rating: rating,
    review_count: review_count,
  image_url: image_url,
    place_id: place_id,
    map_url: if title.present?
            "https://www.google.com/maps/search/?api=1&query=#{ERB::Util.url_encode("#{title} #{address}")}"
             else
            nil
             end,
  # includes(:review_comments) を活かすため、DBクエリを発行しない first(5) を使用
  reviews: review_comments.first(5).map { |review| { id: review.id, comment: review.comment } },
  # 互換のために review_comments キーも返す（クライアントがこちらを期待している場合に対応）
  review_comments: review_comments.first(5).map { |review| { id: review.id, comment: review.comment } }
  }
end


  private

  def calc_distance_km(user_location)
    return nil unless user_location && latitude && longitude

    user_lat, user_lng = user_location
    rad_per_deg = Math::PI / 180
    earth_rkm = 6371 # 地球の半径 (km)

    dlat_rad = (latitude - user_lat) * rad_per_deg
    dlng_rad = (longitude - user_lng) * rad_per_deg

    lat_rad = latitude * rad_per_deg
    user_lat_rad = user_lat * rad_per_deg

    a = Math.sin(dlat_rad / 2)**2 +
      Math.cos(lat_rad) * Math.cos(user_lat_rad) * Math.sin(dlng_rad / 2)**2
    c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    (earth_rkm * c).round(1)
  end
end
