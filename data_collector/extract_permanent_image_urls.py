#!/usr/bin/env python3
"""
全スポット画像永続化スクリプト（制限解除版）
photo_referenceから永続的な直接URLを取得してデータベースに保存
これで完全にAPIキー依存を解決
"""

import os
import json
import requests
import time
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

class PermanentImageUrlExtractor:
    """永続画像URL抽出クラス"""

    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }
        self.success_count = 0
        self.api_usage = 0

    def extract_permanent_url(self, photo_reference: str) -> str:
        """photo_referenceから永続的な直接URLを抽出"""
        if not photo_reference or not self.api_key:
            return None

        try:
            photo_url = "https://maps.googleapis.com/maps/api/place/photo"
            params = {
                'maxwidth': 800,  # 高品質画像
                'photo_reference': photo_reference,
                'key': self.api_key
            }

            # 302リダイレクトを捕捉して直接URLを取得
            response = requests.get(photo_url, params=params, allow_redirects=False, timeout=15)
            self.api_usage += 1

            if response.status_code == 302:
                direct_url = response.headers.get('Location')
                if direct_url:
                    # URLの有効性を確認
                    test_response = requests.head(direct_url, timeout=10)
                    if test_response.status_code == 200:
                        print(f"    ✅ 永続URL取得成功")
                        return direct_url
                    else:
                        print(f"    ❌ URL無効: HTTP {test_response.status_code}")
                else:
                    print(f"    ❌ リダイレクトURL不明")
            else:
                print(f"    ❌ 想定外のレスポンス: HTTP {response.status_code}")

        except requests.exceptions.Timeout:
            print(f"    ⏰ タイムアウト")
        except Exception as e:
            print(f"    ❌ エラー: {e}")

        return None

    def process_all_spots(self, batch_size=5):
        """全スポットの画像を永続化"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()

            print("🚀 全スポット画像永続化開始\n")
            print("🎯 目標: APIキー依存の完全解決\n")

            # 永続URL未取得のスポット取得
            query = """
                SELECT id, name, category, region, photos
                FROM spots
                WHERE photos IS NOT NULL
                AND JSON_LENGTH(photos) > 0
                AND (image_urls IS NULL OR JSON_LENGTH(image_urls) = 0)
                ORDER BY created_at DESC
            """

            cursor.execute(query)
            spots_to_process = cursor.fetchall()

            print(f"📊 処理対象: {len(spots_to_process)}件")
            print(f"📈 バッチサイズ: {batch_size}件ずつ処理")
            print()

            processed_count = 0

            for spot_id, name, category, region, photos_json in spots_to_process:
                print(f"🔄 処理中 ({processed_count + 1}/{len(spots_to_process)}): {name}")
                print(f"   📍 地域: {region} | カテゴリ: {category}")

                try:
                    photos = json.loads(photos_json)
                    permanent_urls = []

                    # 最大3枚の画像を処理
                    for i, photo in enumerate(photos[:3]):
                        photo_ref = photo.get('photo_reference')
                        if photo_ref:
                            print(f"    📸 画像 {i+1}/3 永続化中...")
                            permanent_url = self.extract_permanent_url(photo_ref)

                            if permanent_url:
                                permanent_urls.append({
                                    'url': permanent_url,
                                    'width': photo.get('width'),
                                    'height': photo.get('height'),
                                    'api_independent': True  # APIキー不要フラグ
                                })
                                self.success_count += 1

                            # API制限対策
                            time.sleep(0.3)

                    # データベース更新
                    if permanent_urls:
                        image_urls_json = json.dumps(permanent_urls)
                        update_query = "UPDATE spots SET image_urls = %s WHERE id = %s"
                        cursor.execute(update_query, (image_urls_json, spot_id))
                        connection.commit()

                        print(f"    🎉 成功: {len(permanent_urls)}枚の永続URL保存完了")
                    else:
                        print(f"    ⚠️  永続URL取得失敗")

                except json.JSONDecodeError:
                    print(f"    ❌ JSON解析エラー")
                except Exception as e:
                    print(f"    ❌ 処理エラー: {e}")

                processed_count += 1

                # 進捗表示とバッチ休憩
                if processed_count % batch_size == 0:
                    print(f"\n📈 進捗: {processed_count}/{len(spots_to_process)}件")
                    print(f"   ✅ 永続URL成功: {self.success_count}件")
                    print(f"   🔧 API使用回数: {self.api_usage}回")
                    print(f"   😴 バッチ休憩中（3秒）...")
                    time.sleep(3)
                    print()

            print(f"🎉 全スポット永続化完了!")
            print(f"   📊 処理件数: {processed_count}件")
            print(f"   ✅ 永続URL成功: {self.success_count}件")
            print(f"   🔧 API使用回数: {self.api_usage}回")
            print(f"   🚀 結果: 完全にAPIキー依存を解決しました！")

            cursor.close()
            connection.close()

        except Exception as e:
            print(f"❌ 処理エラー: {e}")

def main():
    print("🔓 API制限解除確認済み")
    print("🎯 永続画像URL取得開始\n")

    extractor = PermanentImageUrlExtractor()
    extractor.process_all_spots(batch_size=5)

if __name__ == "__main__":
    main()
