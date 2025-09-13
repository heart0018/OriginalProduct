#!/usr/bin/env python3
"""
レビューコメント取得システム
既存のスポットデータからレビューを取得して本番環境に保存
"""

import os
import json
import requests
import time
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

class ReviewCollector:
    """レビューコメント収集クラス"""

    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'charset': 'utf8mb4'
        }
        self.api_usage = 0
        self.collected_reviews = 0

    def get_place_reviews(self, place_id):
        """Place IDからレビューを取得"""
        if not place_id or not self.api_key:
            return []

        try:
            url = "https://maps.googleapis.com/maps/api/place/details/json"
            params = {
                'place_id': place_id,
                'fields': 'reviews',
                'language': 'ja',
                'key': self.api_key
            }

            response = requests.get(url, params=params, timeout=15)
            self.api_usage += 1

            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'OK':
                    return data.get('result', {}).get('reviews', [])
                else:
                    print(f"    ⚠️  API Status: {data.get('status')}")
            else:
                print(f"    ❌ HTTP Error: {response.status_code}")

        except requests.exceptions.Timeout:
            print(f"    ⏰ タイムアウト")
        except Exception as e:
            print(f"    ❌ エラー: {e}")

        return []

    def collect_all_reviews(self, batch_size=10):
        """全スポットのレビューを収集"""
        dev_connection = None
        prod_connection = None

        try:
            # 開発環境からplace_id取得
            dev_connection = mysql.connector.connect(
                **self.mysql_config,
                database='swipe_app_development'
            )
            dev_cursor = dev_connection.cursor()

            # 本番環境にレビュー保存
            prod_connection = mysql.connector.connect(
                **self.mysql_config,
                database='swipe_app_production'
            )
            prod_cursor = prod_connection.cursor()

            print("🚀 レビューコメント収集開始")
            print("💬 Google Places API → review_comments\n")

            # 既存のレビューコメントをクリア
            prod_cursor.execute("DELETE FROM review_comments")
            prod_connection.commit()
            print("🗑️  既存レビューコメントクリア完了")

            # place_idとcard_idのマッピング取得
            dev_cursor.execute("SELECT place_id, name FROM spots WHERE place_id IS NOT NULL")
            spots_data = dev_cursor.fetchall()

            prod_cursor.execute("SELECT id, place_id, title FROM cards WHERE place_id IS NOT NULL")
            cards_data = prod_cursor.fetchall()

            # place_id → card_id マッピング作成
            place_to_card = {}
            for card_id, place_id, title in cards_data:
                place_to_card[place_id] = card_id

            print(f"📊 レビュー収集対象: {len(spots_data)}件")
            print(f"🎯 カードマッピング: {len(place_to_card)}件")
            print()

            processed_count = 0

            for place_id, name in spots_data:
                if place_id not in place_to_card:
                    continue

                card_id = place_to_card[place_id]
                processed_count += 1

                print(f"🔄 処理中 ({processed_count}/{len(spots_data)}): {name}")
                print(f"   🆔 Place ID: {place_id}")

                # レビュー取得
                reviews = self.get_place_reviews(place_id)

                if reviews:
                    saved_count = 0
                    for review in reviews[:5]:  # 最大5件のレビュー
                        comment_text = review.get('text', '')
                        author_name = review.get('author_name', '匿名')
                        rating = review.get('rating', 0)
                        time_desc = review.get('relative_time_description', '')

                        if comment_text:
                            # レビューコメント保存
                            insert_query = """
                                INSERT INTO review_comments (
                                    comment, card_id, created_at, updated_at
                                ) VALUES (
                                    %s, %s, NOW(), NOW()
                                )
                            """

                            # コメントにメタ情報も含める
                            full_comment = f"評価: {rating}/5\n投稿者: {author_name}\n投稿時期: {time_desc}\n\n{comment_text}"

                            try:
                                prod_cursor.execute(insert_query, (full_comment, card_id))
                                saved_count += 1
                                self.collected_reviews += 1
                            except Exception as e:
                                print(f"      ❌ 保存エラー: {e}")

                    if saved_count > 0:
                        prod_connection.commit()
                        print(f"    ✅ レビュー保存: {saved_count}件")
                    else:
                        print(f"    ⚠️  保存可能なレビューなし")
                else:
                    print(f"    📭 レビューなし")

                # API制限対策
                time.sleep(0.5)

                # バッチ休憩
                if processed_count % batch_size == 0:
                    print(f"\n📈 進捗: {processed_count}/{len(spots_data)}件")
                    print(f"   💬 収集レビュー: {self.collected_reviews}件")
                    print(f"   🔧 API使用回数: {self.api_usage}回")
                    print(f"   😴 バッチ休憩中（3秒）...")
                    time.sleep(3)
                    print()

            print(f"🎉 レビュー収集完了!")
            print(f"   📊 処理件数: {processed_count}件")
            print(f"   💬 収集レビュー: {self.collected_reviews}件")
            print(f"   🔧 API使用回数: {self.api_usage}回")

            # 最終確認
            prod_cursor.execute("SELECT COUNT(*) FROM review_comments")
            final_count = prod_cursor.fetchone()[0]
            print(f"   🎯 DB保存レビュー: {final_count}件")

        except Exception as e:
            print(f"❌ 収集エラー: {e}")
            if prod_connection:
                prod_connection.rollback()

        finally:
            if dev_connection:
                dev_cursor.close()
                dev_connection.close()
            if prod_connection:
                prod_cursor.close()
                prod_connection.close()

def main():
    print("💬 レビューコメント収集システム")
    print("🎯 Google Places API → 本番環境\n")

    collector = ReviewCollector()
    collector.collect_all_reviews(batch_size=10)

if __name__ == "__main__":
    main()
