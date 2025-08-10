#!/usr/bin/env python3
"""
Google Places API photo_reference テストスクリプト
実際のphoto_referenceを使って画像URL生成をテスト
"""

import os
import requests
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

def test_photo_url_generation():
    """photo_reference を使った画像URL生成をテスト"""
    google_api_key = os.getenv('GOOGLE_API_KEY')

    if not google_api_key:
        print("❌ GOOGLE_API_KEY が設定されていません")
        return

    # テスト用のplace_id（前野原温泉 さやの湯処）
    test_place_id = "ChIJN8F5rJKIGGARZfn7gNK3LFk"  # 仮のplace_id

    # Places API基本URL
    places_api_base = "https://maps.googleapis.com/maps/api/place"
    place_details_url = f"{places_api_base}/details/json"

    # 詳細情報を取得してphoto_referenceを取得
    params = {
        'place_id': test_place_id,
        'key': google_api_key,
        'language': 'ja',
        'fields': 'name,photos'
    }

    try:
        print("🔍 Place Details APIからphoto_referenceを取得中...")
        response = requests.get(place_details_url, params=params)
        response.raise_for_status()

        data = response.json()
        print(f"API Status: {data.get('status')}")

        if data.get('status') != 'OK':
            print(f"❌ API エラー: {data.get('status')} - {data.get('error_message', 'Unknown error')}")
            return

        result = data.get('result', {})
        photos = result.get('photos', [])

        if not photos:
            print("⚠️  写真が見つかりませんでした")
            return

        photo_ref = photos[0].get('photo_reference')
        print(f"📸 photo_reference: {photo_ref}")
        print(f"photo_reference長さ: {len(photo_ref)}文字")

        # 様々なサイズでURL生成をテスト
        test_sizes = [50, 100, 200, 400]

        for max_width in test_sizes:
            photo_url = f"{places_api_base}/photo?maxwidth={max_width}&photoreference={photo_ref}&key={google_api_key}"
            print(f"\n📐 maxwidth={max_width}px:")
            print(f"  URL長さ: {len(photo_url)}文字")
            print(f"  URL: {photo_url[:100]}..." if len(photo_url) > 100 else f"  URL: {photo_url}")

            # URL短縮のテスト - APIキーを一部のみ使用
            short_key = google_api_key[:20] + "..."
            short_url = f"{places_api_base}/photo?maxwidth={max_width}&photoreference={photo_ref}&key={short_key}"
            print(f"  短縮版長さ: {len(short_url)}文字")

        # 実際のPhoto URLにアクセスしてテスト
        test_url = f"{places_api_base}/photo?maxwidth=200&photoreference={photo_ref}&key={google_api_key}"
        print(f"\n🌐 実際のURL動作テスト:")
        print(f"URL: {test_url}")

        # HEADリクエストで画像が存在するかチェック
        head_response = requests.head(test_url)
        print(f"Status Code: {head_response.status_code}")
        print(f"Content-Type: {head_response.headers.get('Content-Type', 'N/A')}")

        if head_response.status_code == 200:
            print("✅ 画像URL が正常に動作しています")
        else:
            print("❌ 画像URL にアクセスできません")

    except requests.RequestException as e:
        print(f"❌ API リクエストエラー: {e}")
    except Exception as e:
        print(f"❌ 予期しないエラー: {e}")

if __name__ == "__main__":
    test_photo_url_generation()
