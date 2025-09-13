#!/usr/bin/env python3
"""
既存データ画像永続化スクリプト
photo_reference から実際の画像URLを取得してデータベースに保存
これでユーザーのスワイプがAPI制限に依存しなくなる
"""

import os
import json
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
from utils.request_guard import get_photo_direct_url

load_dotenv()

class ImageUrlPersistence:
    """画像URL永続化クラス"""

    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }

        # フォールバック画像マッピング
        self.fallback_images = {
            '温泉': "https://images.unsplash.com/photo-1544161515-4ab6ce6db874?w=400&h=300&fit=crop",
            'relax_onsen': "https://images.unsplash.com/photo-1544161515-4ab6ce6db874?w=400&h=300&fit=crop",
            'relax_park': "https://images.unsplash.com/photo-1441974231531-c6227db76b6e?w=400&h=300&fit=crop",
            'relax_cafe': "https://images.unsplash.com/photo-1501339847302-ac426a4a7cbb?w=400&h=300&fit=crop",
            'relax_sauna': "https://images.unsplash.com/photo-1571902943202-507ec2618e8f?w=400&h=300&fit=crop",
            'relax_walk': "https://images.unsplash.com/photo-1551698618-1dfe5d97d256?w=400&h=300&fit=crop"
        }

    def get_permanent_image_url(self, photo_reference: str) -> str:
        """photo_referenceから永続的な画像URLを取得"""
        if not photo_reference or not self.api_key:
            return None

        try:
            # 30日キャッシュ＋同日リトライ抑止
            url = get_photo_direct_url(photo_reference, maxwidth=400, ttl_sec=60*60*24*30)
            if url:
                print(f"    ✅ 永続URL取得成功")
                return url
            print(f"    ❌ 永続URL取得失敗")
            return None
        except Exception as e:
            print(f"    ❌ エラー: {e}")
            return None

    def process_existing_spots(self, limit=None):
        """既存スポットの画像を永続化"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()

            print("🚀 既存データ画像永続化開始\n")

            # 未処理スポット取得
            query = """
                SELECT id, name, category, region, photos
                FROM spots
                WHERE photos IS NOT NULL
                AND JSON_LENGTH(photos) > 0
                AND (image_urls IS NULL OR JSON_LENGTH(image_urls) = 0)
                ORDER BY created_at DESC
            """

            if limit:
                query += f" LIMIT {limit}"

            cursor.execute(query)
            spots_to_process = cursor.fetchall()

            print(f"📊 処理対象: {len(spots_to_process)}件")

            processed_count = 0
            success_count = 0
            api_usage = 0

            for spot_id, name, category, region, photos_json in spots_to_process:
                print(f"\n🔄 処理中 ({processed_count + 1}/{len(spots_to_process)}): {name} ({region})")

                try:
                    photos = json.loads(photos_json)
                    permanent_urls = []

                    # 最大3枚の画像を処理
                    for i, photo in enumerate(photos[:3]):
                        photo_ref = photo.get('photo_reference')
                        if photo_ref:
                            print(f"    📸 画像 {i+1}/3 処理中...")
                            permanent_url = self.get_permanent_image_url(photo_ref)
                            api_usage += 1

                            if permanent_url:
                                permanent_urls.append({
                                    'url': permanent_url,
                                    'width': photo.get('width'),
                                    'height': photo.get('height')
                                })

                            # API制限対策（0.5秒待機）
                            time.sleep(0.5)

                    # フォールバック画像URL
                    fallback_url = self.fallback_images.get(category, self.fallback_images.get('温泉'))

                    # データベース更新
                    update_query = """
                        UPDATE spots
                        SET image_urls = %s, fallback_image_url = %s
                        WHERE id = %s
                    """

                    image_urls_json = json.dumps(permanent_urls) if permanent_urls else None
                    cursor.execute(update_query, (image_urls_json, fallback_url, spot_id))
                    connection.commit()

                    if permanent_urls:
                        print(f"    ✅ 成功: {len(permanent_urls)}枚の永続URL保存")
                        success_count += 1
                    else:
                        print(f"    ⚠️  永続URL取得失敗、フォールバック設定")

                except json.JSONDecodeError:
                    print(f"    ❌ JSON解析エラー")
                except Exception as e:
                    print(f"    ❌ 処理エラー: {e}")

                processed_count += 1

                # 進捗表示
                if processed_count % 5 == 0:
                    print(f"\n📈 進捗: {processed_count}/{len(spots_to_process)}件 (成功: {success_count}件, API使用: {api_usage}回)")

                # API制限対策（10件ごとに長めの休憩）
                if processed_count % 10 == 0:
                    print("    😴 API制限対策で2秒休憩...")
                    time.sleep(2)

            print(f"\n🎉 画像永続化完了!")
            print(f"   📊 処理件数: {processed_count}件")
            print(f"   ✅ 永続URL成功: {success_count}件")
            print(f"   🔗 API使用回数: {api_usage}回")
            print(f"   💡 結果: ユーザーのスワイプがAPI制限に依存しなくなりました！")

            cursor.close()
            connection.close()

        except Exception as e:
            print(f"❌ 処理エラー: {e}")

def main():
    """メイン実行"""
    print("🚨 重要: この処理でユーザースワイプのAPI依存問題を解決します\n")

    processor = ImageUrlPersistence()

    # 最初は少量でテスト
    print("🧪 まずテスト実行（最初の5件）")
    processor.process_existing_spots(limit=5)

    print("\n📋 テスト結果を確認して、全件処理を実行しますか？ (y/n):")
    choice = input().lower().strip()

    if choice == 'y':
        print("\n🚀 全件処理開始...")
        processor.process_existing_spots()
    else:
        print("❌ 全件処理をキャンセルしました")

if __name__ == "__main__":
    main()
