import "../assets/styles/SwipeCard.css";
import type { Card } from "../types/Card";

type Props = {
  card: Card;
};

const SwipeCard = ({ card }: Props) => {
  return (
    <div className="swipe-card">
      {/* 画像 */}
      <div
        className="card-image"
        style={{ backgroundImage: `url(${card.image_url})` }}
      />

      {/* コンテンツ*/}
      <div className="card-content">
        <div className="card-info">
          <h2 className="place-name">{card.title}</h2>
          <div className="details">
            <span className="type">📍{card.distance_km}km</span>
            <span className="rating">
              ⭐ {card.rating.toFixed(1)}({card.review_count}件)
            </span>
          </div>
          <div className="address">📌 {card.address}</div>
        </div>

        {/* レビュー */}
        <div className="reviews">
          {card.reviews.slice(0, 5).map((review, index) => (
            <div key={index} className="review">
              <div className="review-text">{review.comment}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default SwipeCard;
