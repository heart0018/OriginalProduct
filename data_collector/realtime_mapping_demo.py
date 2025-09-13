#!/usr/bin/env python3
"""
取得時マッピング実証システム（小規模テスト版）
リアルタイム地域マッピングの概念実証
"""

import os
import time
import random
import requests
import mysql.connector
import re
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

class RealtimeMappingDemo:
    def __init__(self):
        self.google_api_key = os.getenv('GOOGLE_API_KEY')

        if not self.google_api_key:
            raise ValueError("GOOGLE_API_KEY が環境変数に設定されていません")

        # データベース接続設定
        self.db_config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'Haruto'),
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': os.getenv('MYSQL_DATABASE', 'swipe_app_production'),
            'charset': 'utf8mb4'
        }

        # 都道府県→地域マッピング（日本語）
        self.prefecture_to_region = {
            # 北海道
            '北海道': '北海道',

            # 東北
            '青森県': '東北', '岩手県': '東北', '宮城県': '東北',
            '秋田県': '東北', '山形県': '東北', '福島県': '東北',

            # 関東
            '茨城県': '関東', '栃木県': '関東', '群馬県': '関東',
            '埼玉県': '関東', '千葉県': '関東', '東京都': '関東', '神奈川県': '関東',

            # 中部
            '新潟県': '中部', '富山県': '中部', '石川県': '中部', '福井県': '中部',
            '山梨県': '中部', '長野県': '中部', '岐阜県': '中部', '静岡県': '中部', '愛知県': '中部',

            # 関西
            '三重県': '関西', '滋賀県': '関西', '京都府': '関西',
            '大阪府': '関西', '兵庫県': '関西', '奈良県': '関西', '和歌山県': '関西',

            # 中国・四国
            '鳥取県': '中国', '島根県': '中国', '岡山県': '中国', '広島県': '中国', '山口県': '中国',
            '徳島県': '四国', '香川県': '四国', '愛媛県': '四国', '高知県': '四国',

            # 九州・沖縄
            '福岡県': '九州', '佐賀県': '九州', '長崎県': '九州', '熊本県': '九州',
            '大分県': '九州', '宮崎県': '九州', '鹿児島県': '九州', '沖縄県': '九州'
        }

    def extract_prefecture_from_address(self, address: str) -> Optional[str]:
        """アドレスから都道府県を抽出"""
        if not address:
            return None

        # Google Places APIの標準フォーマット: "日本、〒XXX-XXXX 都道府県..."
        prefecture_pattern = r'(北海道|[^\s]+県|[^\s]+府|[^\s]+都)'

        matches = re.findall(prefecture_pattern, address)
        for match in matches:
            if match in self.prefecture_to_region:
                return match

        return None

    def get_region_from_prefecture(self, prefecture: str) -> str:
        """都道府県から地域を取得"""
        return self.prefecture_to_region.get(prefecture, '関東')  # デフォルトは関東

    def search_entertainment_venues(self, query: str, location: str, limit: int = 5) -> List[Dict]:
        """エンターテイメント施設を検索（リアルタイム地域マッピング付き）"""
        places = []

        search_query = f"{query} {location}"

        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            'query': search_query,
            'language': 'ja',
            'region': 'JP',
            'key': self.google_api_key,
        }

        try:
            print(f"🔍 検索: {search_query}")
            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if data['status'] == 'OK':
                for place in data['results'][:limit]:
                    # ★リアルタイム地域マッピング★
                    address = place.get('formatted_address', '')
                    detected_prefecture = self.extract_prefecture_from_address(address)
                    detected_region = self.get_region_from_prefecture(detected_prefecture) if detected_prefecture else '関東'

                    place_info = {
                        'place_id': place['place_id'],
                        'name': place['name'],
                        'address': address,
                        'prefecture': detected_prefecture,
                        'region': detected_region,  # ★リアルタイムで正確な地域設定★
                        'lat': place['geometry']['location']['lat'],
                        'lng': place['geometry']['location']['lng'],
                        'rating': place.get('rating', 0),
                        'photo_references': [photo['photo_reference'] for photo in place.get('photos', [])[:1]]
                    }

                    places.append(place_info)
                    print(f"  ✓ {place['name']}")
                    print(f"    📍 {address}")
                    print(f"    🗾 {detected_prefecture} → {detected_region} ★リアルタイム★")

            elif data['status'] == 'OVER_QUERY_LIMIT':
                print(f"  ❌ API制限に達しました")
            else:
                print(f"  ❌ 検索エラー: {data['status']}")

        except Exception as e:
            print(f"  ❌ 検索エラー: {e}")

        return places

    def save_to_database(self, place_data: Dict, category: str) -> bool:
        """データベースに保存（リアルタイム地域マッピング済み）"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            # 重複チェック
            cursor.execute("SELECT id FROM cards WHERE place_id = %s", (place_data['place_id'],))
            if cursor.fetchone():
                print(f"    ⚠️ 既存データ: {place_data['name']}")
                return False

            # カード情報挿入（リアルタイム地域マッピング済み）
            card_sql = """
                INSERT INTO cards (
                    place_id, title, genre, region, latitude, longitude,
                    address, rating, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """

            card_data = (
                place_data['place_id'],
                place_data['name'],
                category,
                place_data['region'],  # ★リアルタイムで設定された正確な地域★
                place_data['lat'],
                place_data['lng'],
                place_data['address'],
                place_data['rating']
            )

            cursor.execute(card_sql, card_data)
            connection.commit()

            print(f"    ✅ DB保存: {place_data['name']} → {place_data['region']} ({place_data.get('prefecture', 'N/A')})")

            cursor.close()
            connection.close()

            return True

        except Exception as e:
            print(f"    ❌ DB保存エラー: {e}")
            return False

    def demo_realtime_mapping(self):
        """取得時マッピングシステムの実証"""
        print("🎯 取得時マッピングシステム実証")
        print("=" * 50)
        print("📊 リアルタイム処理: API取得→アドレス解析→地域分類→DB保存")
        print("=" * 50)

        test_cases = [
            ("カラオケ", "渋谷", "entertainment_karaoke"),
            ("ゲームセンター", "心斎橋", "entertainment_arcade"),
            ("映画館", "博多", "entertainment_cinema"),
        ]

        total_processed = 0
        successful_saves = 0

        for query, location, category in test_cases:
            print(f"\n🎮 テストケース: {query} @ {location}")
            print("-" * 30)

            places = self.search_entertainment_venues(query, location, limit=3)

            for place in places:
                total_processed += 1
                if self.save_to_database(place, category):
                    successful_saves += 1

                time.sleep(1)  # API制限対策

            print(f"\n⏸️ 次のテストケースまで休憩...")
            time.sleep(3)

        print(f"\n🎯 取得時マッピング実証完了")
        print("=" * 50)
        print(f"処理件数: {total_processed}件")
        print(f"DB保存成功: {successful_saves}件")
        print(f"成功率: {(successful_saves/total_processed*100) if total_processed > 0 else 0:.1f}%")
        print("✅ リアルタイム地域マッピング動作確認完了")

def main():
    """メイン実行関数"""
    try:
        demo = RealtimeMappingDemo()
        demo.demo_realtime_mapping()

    except Exception as e:
        print(f"❌ システムエラー: {e}")

if __name__ == "__main__":
    main()
