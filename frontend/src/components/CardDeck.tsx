import { createRef, useEffect, useMemo, useState } from "react";
import TinderCard from "react-tinder-card";
import "../assets/styles/CardDeck.css";
import type { Card } from "../types/Card";
import SwipeCard from "./SwipeCard";

const CardDeck = () => {
  const [cards, setCards] = useState<Card[]>([]);
  const [offset, setOffset] = useState(0);
  const LIMIT = 10;
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);

  // 各カードに個別のrefを作成
  const childRefs = useMemo(
    () => cards.map(() => createRef<any>()),
    [cards.length]
  );

  // 初回読み込み
  useEffect(() => {
    fetchCards();
  }, []);

  // 残枚数監視でプリフェッチ
  useEffect(() => {
    if (!loading && hasMore && cards.length <= 3) {
      fetchCards();
    }
  }, [cards.length, loading, hasMore]);

  const fetchCards = async () => {
    if (loading || !hasMore) return;

    setLoading(true);
    try {
      const res = await fetch(
        `http://localhost:3000/api/v1/cards?limit=${LIMIT}&offset=${offset}`
      );
      const data = await res.json();

      if (data.length === 0) {
        setHasMore(false);
        return;
      }

      setCards((prevCards) => {
        const existingIds = new Set(prevCards.map((card) => card.id));
        const newUniqueCards = data.filter((card) => !existingIds.has(card.id));
        return [...prevCards, ...newUniqueCards];
      });

      setOffset((prev) => prev + LIMIT);
    } catch (error) {
      console.error("カードの取得に失敗しました:", error);
    } finally {
      setLoading(false);
    }
  };

  //配列からカードを削除
  const handleSwipe = (direction: string, cardId: number) => {
    setCards((prevCards) => prevCards.filter((card) => card.id !== cardId));
  };

  const handleLike = () => {
    if (childRefs.length > 0) {
      const currentRef = childRefs[childRefs.length - 1];
      currentRef?.current?.swipe("right");
    }
  };

  const handleReject = () => {
    if (childRefs.length > 0) {
      const currentRef = childRefs[childRefs.length - 1];
      currentRef?.current?.swipe("left");
    }
  };

  const handleGo = () => {
    if (cards.length > 0) {
      const currentCard = cards[cards.length - 1];
      window.open(currentCard.map_url, "_blank");
    }
  };

  if (loading && cards.length === 0)
    return <div className="loading">読み込み中...</div>;
  if (cards.length === 0 && !hasMore)
    return <div className="finished">カードは以上です</div>;

  return (
    <div className="card-deck-container">
      <div className="card-deck">
        {cards.map((card, index) => (
          <TinderCard
            ref={childRefs[index]}
            className="tinder-card"
            key={card.id}
            onSwipe={(dir) => handleSwipe(dir, card.id)}
            preventSwipe={["up", "down"]}
            swipeRequirementType="position"
            swipeThreshold={30}
            flickOnSwipe={true}
          >
            <SwipeCard card={card} />
          </TinderCard>
        ))}
      </div>

      {/* ボタンコンテナ */}
      <div className="button-container">
        <button
          className="action-button nope-button"
          onClick={handleReject}
          title="Nope"
        >
          ✕
        </button>
        <button
          className="action-button go-button"
          onClick={handleGo}
          title="Go to location"
        >
          GO!
        </button>
        <button
          className="action-button like-button"
          onClick={handleLike}
          title="Like"
        >
          ♥
        </button>
      </div>

      {loading && (
        <div className="loading-indicator">次のカードを読み込み中...</div>
      )}
    </div>
  );
};

export default CardDeck;
