#!/usr/bin/env python3
"""
フロントエンド用JSON API テストスクリプト
データベースからカード情報を取得してJSON形式で出力
"""

import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import json
from typing import List, Dict, Optional
import math

# .envファイルを読み込み
load_dotenv()

class CardJsonGenerator:
    def __init__(self):
        """初期化"""
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }

    def connect_database(self):
        """データベース接続"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            if connection.is_connected():
                return connection
        except Error as e:
            print(f"❌ データベース接続エラー: {e}")
            return None

    def calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """2点間の距離を計算（ハヴァーサイン公式）"""
        # 地球の半径（km）
        R = 6371.0

        # 度をラジアンに変換
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        # 緯度と経度の差
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad

        # ハヴァーサイン公式
        a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

        distance = R * c
        return round(distance, 1)

    def get_card_with_reviews(self, card_id: int, user_lat: float = 35.6762, user_lon: float = 139.6503) -> Optional[Dict]:
        """指定されたカードIDの詳細情報とレビューを取得"""
        connection = self.connect_database()
        if not connection:
            return None

        try:
            cursor = connection.cursor(dictionary=True)

            # カード基本情報取得
            card_query = """
                SELECT
                    id,
                    title,
                    type,
                    region,
                    address,
                    latitude,
                    longitude,
                    rating,
                    review_count,
                    image_url,
                    external_link
                FROM cards
                WHERE id = %s
            """

            cursor.execute(card_query, (card_id,))
            card = cursor.fetchone()

            if not card:
                return None

            # レビュー取得
            review_query = """
                SELECT
                    comment as text,
                    created_at
                FROM review_comments
                WHERE card_id = %s
                ORDER BY created_at DESC
                LIMIT 5
            """

            cursor.execute(review_query, (card_id,))
            reviews = cursor.fetchall()

            # 距離計算
            distance_km = None
            if card['latitude'] and card['longitude']:
                distance_km = self.calculate_distance(
                    user_lat, user_lon,
                    float(card['latitude']), float(card['longitude'])
                )

            # JSON形式に整形
            result = {
                "id": card['id'],
                "title": card['title'],
                "type": card['type'],
                "region": card['region'],
                "address": card['address'].replace('日本、〒', '〒').replace('日本、', '') if card['address'] else "",
                "distance_km": distance_km,
                "rating": float(card['rating']) if card['rating'] else 0.0,
                "review_count": card['review_count'],
                "thumbnail_url": card['image_url'],
                "place_id": None,  # Google Places APIのplace_idは保存していない
                "reviews": []
            }

            # レビューデータを整形
            for i, review in enumerate(reviews):
                result['reviews'].append({
                    "author": f"ユーザー{i+1}",  # 匿名化
                    "text": review['text'][:200] + ("..." if len(review['text']) > 200 else ""),  # 200文字に制限
                    "rating": "N/A"  # レビューの個別評価は保存していない
                })

            return result

        except Error as e:
            print(f"❌ データベースエラー: {e}")
            return None

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def get_all_cards_summary(self, user_lat: float = 35.6762, user_lon: float = 139.6503) -> List[Dict]:
        """全カードの概要情報を取得"""
        connection = self.connect_database()
        if not connection:
            return []

        try:
            cursor = connection.cursor(dictionary=True)

            query = """
                SELECT
                    c.id,
                    c.title,
                    c.type,
                    c.region,
                    c.address,
                    c.latitude,
                    c.longitude,
                    c.rating,
                    c.review_count,
                    c.image_url,
                    COUNT(rc.id) as comment_count
                FROM cards c
                LEFT JOIN review_comments rc ON c.id = rc.card_id
                GROUP BY c.id
                ORDER BY c.rating DESC, c.review_count DESC
            """

            cursor.execute(query)
            cards = cursor.fetchall()

            result = []
            for card in cards:
                # 距離計算
                distance_km = None
                if card['latitude'] and card['longitude']:
                    distance_km = self.calculate_distance(
                        user_lat, user_lon,
                        float(card['latitude']), float(card['longitude'])
                    )

                result.append({
                    "id": card['id'],
                    "title": card['title'],
                    "type": card['type'],
                    "region": card['region'],
                    "address": card['address'].replace('日本、〒', '〒').replace('日本、', '') if card['address'] else "",
                    "distance_km": distance_km,
                    "rating": float(card['rating']) if card['rating'] else 0.0,
                    "review_count": card['review_count'],
                    "thumbnail_url": card['image_url'],
                    "comment_count": card['comment_count']
                })

            return result

        except Error as e:
            print(f"❌ データベースエラー: {e}")
            return []

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def main():
    """メイン関数"""
    generator = CardJsonGenerator()

    print("🔍 フロントエンド用JSON API テスト")
    print("=" * 50)

    # 全カードの概要を取得
    print("\n📊 全カード概要:")
    all_cards = generator.get_all_cards_summary()
    print(json.dumps(all_cards, ensure_ascii=False, indent=2))

    print("\n" + "=" * 50)

    # 1件目の詳細情報を取得
    if all_cards:
        card_id = all_cards[0]['id']
        print(f"\n🔍 カードID {card_id} の詳細情報:")
        card_detail = generator.get_card_with_reviews(card_id)
        print(json.dumps(card_detail, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
