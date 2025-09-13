#!/usr/bin/env python3
"""
レビューコメント収集システム（制限解除版）
API制限を考慮して段階的にレビューを収集
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

    def get_place_reviews(self, place_id, place_name):
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
                status = data.get('status')

                if status == 'OK':
                    reviews = data.get('result', {}).get('reviews', [])
                    print(f"    ✅ レビュー取得: {len(reviews)}件")
                    return reviews
                elif status == 'OVER_QUERY_LIMIT':
                    print(f"    ❌ API制限達成 - 一時停止")
                    return 'LIMIT_REACHED'
                else:
                    print(f"    ⚠️  API Status: {status}")
            else:
                print(f"    ❌ HTTP Error: {response.status_code}")

        except requests.exceptions.Timeout:
            print(f"    ⏰ タイムアウト")
        except Exception as e:
            print(f"    ❌ エラー: {e}")

        return []

    def collect_reviews_batch(self, start_index=0, batch_size=20):
        """バッチ処理でレビューを収集"""
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

            print("💬 レビューコメント収集開始（バッチ処理）")
            print(f"📊 開始位置: {start_index}, バッチサイズ: {batch_size}\n")

            # place_idとcard_idのマッピング取得
            dev_cursor.execute("SELECT place_id, name FROM spots WHERE place_id IS NOT NULL ORDER BY created_at LIMIT %s OFFSET %s", (batch_size, start_index))
            spots_data = dev_cursor.fetchall()

            prod_cursor.execute("SELECT id, place_id, title FROM cards WHERE place_id IS NOT NULL")
            cards_data = prod_cursor.fetchall()

            # place_id → card_id マッピング作成
            place_to_card = {}
            for card_id, place_id, title in cards_data:
                place_to_card[place_id] = card_id

            print(f"📊 今回処理対象: {len(spots_data)}件")
            print()

            processed_count = 0

            for place_id, name in spots_data:
                if place_id not in place_to_card:
                    continue

                card_id = place_to_card[place_id]
                processed_count += 1

                print(f"🔄 処理中 ({processed_count}/{len(spots_data)}): {name}")
                print(f"   🆔 Card ID: {card_id}")

                # レビュー取得
                reviews = self.get_place_reviews(place_id, name)

                if reviews == 'LIMIT_REACHED':
                    print(f"\n⚠️  API制限に達しました。処理を停止します。")
                    print(f"   📊 処理済み: {processed_count}件")
                    print(f"   💬 収集レビュー: {self.collected_reviews}件")
                    break

                if reviews:
                    saved_count = 0
                    for review in reviews[:3]:  # 最大3件のレビュー
                        comment_text = review.get('text', '')
                        author_name = review.get('author_name', '匿名')
                        rating = review.get('rating', 0)
                        time_desc = review.get('relative_time_description', '')

                        if comment_text and len(comment_text.strip()) > 10:  # 意味のあるコメントのみ
                            # レビューコメント保存
                            insert_query = """
                                INSERT INTO review_comments (
                                    comment, card_id, created_at, updated_at
                                ) VALUES (
                                    %s, %s, NOW(), NOW()
                                )
                            """

                            # コメントにメタ情報も含める
                            full_comment = f"評価: {rating}/5 | 投稿者: {author_name} | 時期: {time_desc}\n\n{comment_text}"

                            try:
                                prod_cursor.execute(insert_query, (full_comment, card_id))
                                saved_count += 1
                                self.collected_reviews += 1
                            except Exception as e:
                                print(f"      ❌ 保存エラー: {e}")

                    if saved_count > 0:
                        prod_connection.commit()
                        print(f"    💬 レビュー保存: {saved_count}件")
                    else:
                        print(f"    📝 保存対象レビューなし")
                else:
                    print(f"    📭 レビューなし")

                # API制限対策（少し間隔を空ける）
                time.sleep(0.8)

                # 進捗表示
                if processed_count % 5 == 0:
                    print(f"\n📈 進捗: {processed_count}/{len(spots_data)}件")
                    print(f"   💬 収集レビュー: {self.collected_reviews}件")
                    print(f"   🔧 API使用回数: {self.api_usage}回")
                    print()

            print(f"\n🎉 バッチ処理完了!")
            print(f"   📊 処理件数: {processed_count}件")
            print(f"   💬 収集レビュー: {self.collected_reviews}件")
            print(f"   🔧 API使用回数: {self.api_usage}回")

            # 最終確認
            prod_cursor.execute("SELECT COUNT(*) FROM review_comments")
            final_count = prod_cursor.fetchone()[0]
            print(f"   🎯 DB総レビュー数: {final_count}件")

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
    print("💬 レビューコメント収集システム（制限対応版）")
    print("🎯 段階的レビュー収集\n")

    collector = ReviewCollector()
    # 最初の20件から開始
    collector.collect_reviews_batch(start_index=0, batch_size=20)

if __name__ == "__main__":
    main()
