#!/usr/bin/env python3
"""
Google Places API 画像表示テスト
photo_reference から実際の画像URLを生成してテスト
"""

import os
import json
import requests
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

def test_photo_urls():
    """最新データの画像URL生成とテスト"""

    API_KEY = os.getenv('GOOGLE_API_KEY')
    if not API_KEY:
        print("❌ Google Places API キーが見つかりません")
        return

    try:
        # データベース接続
        connection = mysql.connector.connect(
            host='localhost',
            user='Haruto',
            password=os.getenv('MYSQL_PASSWORD'),
            database='swipe_app_development',
            charset='utf8mb4'
        )

        cursor = connection.cursor()

        print("🔍 画像URL生成テスト開始\n")

        # 最新の画像データを持つスポットを取得
        cursor.execute("""
            SELECT name, photos, region
            FROM spots
            WHERE photos IS NOT NULL
            AND JSON_LENGTH(photos) > 0
            ORDER BY created_at DESC
            LIMIT 5
        """)

        spots_with_photos = cursor.fetchall()

        if not spots_with_photos:
            print("❌ 画像データを持つスポットが見つかりません")
            return

        for name, photos_json, region in spots_with_photos:
            print(f"📍 テスト対象: {name} ({region})")

            try:
                photos = json.loads(photos_json)
                if photos and len(photos) > 0:
                    # 最初の写真のphoto_referenceを取得
                    photo_ref = photos[0].get('photo_reference')
                    if photo_ref:
                        # 画像URL生成（複数サイズでテスト）
                        sizes = [400, 800, 1600]

                        for size in sizes:
                            photo_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth={size}&photo_reference={photo_ref}&key={API_KEY}"

                            print(f"  📸 サイズ {size}px: テスト中...")

                            # HEAD リクエストで画像の存在確認
                            try:
                                response = requests.head(photo_url, timeout=10)
                                if response.status_code == 200:
                                    content_type = response.headers.get('content-type', '')
                                    if 'image' in content_type:
                                        print(f"     ✅ 成功 - Content-Type: {content_type}")
                                        print(f"     🔗 URL: {photo_url}")
                                    else:
                                        print(f"     ⚠️  画像ではない - Content-Type: {content_type}")
                                else:
                                    print(f"     ❌ HTTPエラー: {response.status_code}")
                            except requests.exceptions.Timeout:
                                print(f"     ⏰ タイムアウト")
                            except requests.exceptions.RequestException as e:
                                print(f"     ❌ リクエストエラー: {e}")
                    else:
                        print("  ❌ photo_reference が見つかりません")
                else:
                    print("  ❌ 写真データが空です")
            except json.JSONDecodeError:
                print("  ❌ 写真データのJSON解析に失敗")

            print()

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"❌ エラー: {e}")

if __name__ == "__main__":
    test_photo_urls()
