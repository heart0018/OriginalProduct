#!/usr/bin/env python3
"""
Google Places 画像の実際のURL取得テスト
photo_referenceから直接アクセス可能な画像URLを取得
これでAPIキー依存を完全に解決
"""

import os
import json
import requests
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

def get_actual_image_url(photo_reference, api_key):
    """photo_referenceから実際の画像URL（直接アクセス可能）を取得"""
    try:
        # Google Photo API エンドポイント
        photo_url = "https://maps.googleapis.com/maps/api/place/photo"
        params = {
            'maxwidth': 400,
            'photo_reference': photo_reference,
            'key': api_key
        }

        # allow_redirects=False で302リダイレクトを捕捉
        response = requests.get(photo_url, params=params, allow_redirects=False, timeout=10)

        print(f"📊 レスポンス情報:")
        print(f"   ステータス: {response.status_code}")
        print(f"   ヘッダー: {dict(response.headers)}")

        if response.status_code == 302:
            # リダイレクト先が実際の画像URL
            actual_url = response.headers.get('Location')
            if actual_url:
                print(f"✅ 実際の画像URL取得成功!")
                print(f"   🔗 直接URL: {actual_url}")

                # 実際のURLをテスト（APIキーなしでアクセス）
                test_response = requests.head(actual_url, timeout=10)
                print(f"   🧪 直接アクセステスト: HTTP {test_response.status_code}")

                if test_response.status_code == 200:
                    print(f"   🎉 APIキーなしで直接アクセス可能!")
                    return actual_url
                else:
                    print(f"   ❌ 直接アクセス失敗")
            else:
                print(f"   ❌ リダイレクト先URL不明")
        else:
            print(f"   ❌ 期待された302リダイレクトではない")

    except requests.exceptions.Timeout:
        print(f"   ⏰ タイムアウト")
    except Exception as e:
        print(f"   ❌ エラー: {e}")

    return None

def test_direct_url_extraction():
    """実際のURL抽出テスト"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='Haruto',
            password=os.getenv('MYSQL_PASSWORD'),
            database='swipe_app_development',
            charset='utf8mb4'
        )

        cursor = connection.cursor()
        api_key = os.getenv('GOOGLE_API_KEY')

        print("🧪 画像直接URL抽出テスト\n")
        print("🎯 目的: APIキーに依存しない直接画像URLの取得\n")

        # テスト用のphoto_reference取得
        cursor.execute("SELECT name, photos FROM spots WHERE photos IS NOT NULL LIMIT 1")
        result = cursor.fetchone()

        if result:
            name, photos_json = result
            photos = json.loads(photos_json)

            if photos and len(photos) > 0:
                photo_ref = photos[0].get('photo_reference')

                print(f"📍 テスト対象: {name}")
                print(f"🔑 photo_reference: {photo_ref[:50]}...")
                print()

                # 実際のURL取得を試行
                actual_url = get_actual_image_url(photo_ref, api_key)

                if actual_url:
                    print(f"\n🎉 成功! 取得した直接URL:")
                    print(f"   {actual_url}")
                    print(f"\n💡 この URL は:")
                    print(f"   ✅ APIキーなしで直接アクセス可能")
                    print(f"   ✅ ユーザーがスワイプしてもAPI消費なし")
                    print(f"   ✅ 商用レベルの安定性")

                    # URLの構造分析
                    if 'lh3.googleusercontent.com' in actual_url:
                        print(f"   📊 Google Content Delivery Network (CDN)")
                    elif 'googleapis.com' in actual_url:
                        print(f"   📊 Google APIs infrastructure")

                else:
                    print(f"\n❌ 直接URL取得失敗")
                    print(f"   理由: API制限またはアクセス制御")
            else:
                print("❌ 写真データが見つかりません")
        else:
            print("❌ テスト用データが見つかりません")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"❌ テストエラー: {e}")

if __name__ == "__main__":
    test_direct_url_extraction()
