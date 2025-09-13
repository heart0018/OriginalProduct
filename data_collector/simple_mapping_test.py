#!/usr/bin/env python3
"""
取得時マッピング検証テスト（簡易版）
API制限を考慮して少量データで検証
"""

import os
import sys
import requests
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import json
from typing import List, Dict, Optional
import time
import re

# .envファイルを読み込み
load_dotenv()

class SimpleEntertainmentTest:
    def __init__(self):
        """初期化"""
        self.google_api_key = os.getenv('GOOGLE_API_KEY')
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_production',
            'charset': 'utf8mb4'
        }

        # API設定
        self.places_api_base = "https://maps.googleapis.com/maps/api/place"
        self.text_search_url = f"{self.places_api_base}/textsearch/json"
        self.place_details_url = f"{self.places_api_base}/details/json"

        # 都道府県→地域マッピング（日本語版）
        self.prefecture_to_region = {
            '北海道': '北海道',
            '青森県': '東北', '岩手県': '東北', '宮城県': '東北',
            '秋田県': '東北', '山形県': '東北', '福島県': '東北',
            '東京都': '関東', '茨城県': '関東', '栃木県': '関東',
            '群馬県': '関東', '埼玉県': '関東', '千葉県': '関東', '神奈川県': '関東',
            '新潟県': '中部', '富山県': '中部', '石川県': '中部', '福井県': '中部',
            '山梨県': '中部', '長野県': '中部', '岐阜県': '中部',
            '静岡県': '中部', '愛知県': '中部',
            '京都府': '関西', '大阪府': '関西', '三重県': '関西',
            '滋賀県': '関西', '兵庫県': '関西', '奈良県': '関西', '和歌山県': '関西',
            '鳥取県': '中国', '島根県': '中国', '岡山県': '中国',
            '広島県': '中国', '山口県': '中国',
            '徳島県': '中国', '香川県': '中国',
            '愛媛県': '中国', '高知県': '中国',
            '福岡県': '九州', '佐賀県': '九州', '長崎県': '九州',
            '大分県': '九州', '熊本県': '九州', '宮崎県': '九州',
            '鹿児島県': '九州', '沖縄県': '九州'
        }

    def extract_prefecture_from_address(self, address):
        """住所から都道府県を抽出"""
        if not address:
            return None

        # パターン1: 日本、〒XXX-XXXX 都道府県
        pattern1 = r'日本、〒[0-9]{3}-[0-9]{4}\s+([^市区町村]+?[県都府])'
        match1 = re.search(pattern1, address)
        if match1:
            return match1.group(1)

        # パターン2: 都道府県を直接検索（最長マッチ）
        found_prefectures = []
        for prefecture in self.prefecture_to_region.keys():
            if prefecture in address:
                found_prefectures.append(prefecture)

        if found_prefectures:
            return max(found_prefectures, key=len)

        return None

    def get_region_from_address(self, address):
        """住所から地域を判定（日本語）"""
        prefecture = self.extract_prefecture_from_address(address)
        if prefecture:
            return self.prefecture_to_region.get(prefecture)
        return None

    def simple_search(self, query: str, max_results: int = 5) -> List[Dict]:
        """シンプルな検索（制限対策）"""
        print(f"🔍 検索: '{query}' (制限: {max_results}件)")

        params = {
            'query': query,
            'key': self.google_api_key,
            'language': 'ja',
            'region': 'jp'
        }

        try:
            response = requests.get(self.text_search_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == 'OK':
                results = data.get('results', [])[:max_results]
                print(f"✅ 取得: {len(results)}件")
                return results
            else:
                print(f"❌ 検索失敗: {data.get('status')}")
                return []

        except Exception as e:
            print(f"❌ 検索エラー: {e}")
            return []

    def save_simple_data(self, place_data: Dict, genre: str) -> Optional[int]:
        """簡単なデータ保存（取得時マッピング適用）"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()

            # 住所から地域を自動判定（取得時マッピング！）
            address = place_data.get('formatted_address', '')
            region = self.get_region_from_address(address)

            if not region:
                print(f"⚠️ 地域判定失敗: {place_data.get('name')} - {address}")
                region = 'その他'

            # 重複チェック
            cursor.execute(
                'SELECT id FROM cards WHERE title = %s AND genre = %s',
                (place_data['name'], genre)
            )

            if cursor.fetchone():
                print(f"⏸️ 重複スキップ: {place_data['name']}")
                return None

            # 現在のスキーマに合わせた保存
            card_query = '''
            INSERT INTO cards (genre, title, rating, review_count, region, address,
                             place_id, latitude, longitude, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            '''

            # 位置情報取得
            geometry = place_data.get('geometry', {})
            location = geometry.get('location', {})
            latitude = location.get('lat')
            longitude = location.get('lng')

            card_values = (
                genre,
                place_data['name'][:128],  # 長さ制限
                place_data.get('rating'),
                place_data.get('user_ratings_total', 0),
                region,  # 🎯 取得時にマッピング済み！
                address[:128],  # 長さ制限
                place_data.get('place_id', '')[:128],
                latitude,
                longitude
            )

            cursor.execute(card_query, card_values)
            card_id = cursor.lastrowid

            connection.commit()
            print(f"✅ 保存完了: {place_data['name']} (地域: {region}) → ID: {card_id}")
            return card_id

        except Error as e:
            print(f"❌ DB保存エラー: {e}")
            return None
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

    def test_mapping_system(self):
        """取得時マッピングシステムのテスト"""
        print("🧪 取得時マッピングシステム テスト開始\n")

        # テスト用クエリ（API制限を考慮して少数）
        test_queries = [
            ('ゲームセンター 札幌', 'play_arcade'),
            ('カラオケ 東京', 'play_karaoke'),
            ('ゲーセン 大阪', 'play_arcade')
        ]

        total_saved = 0

        for query, genre in test_queries:
            print(f"\n📋 テスト: {query} ({genre})")

            # 検索実行
            places = self.simple_search(query, max_results=2)

            for place in places:
                saved_id = self.save_simple_data(place, genre)
                if saved_id:
                    total_saved += 1

                time.sleep(2)  # API制限対策

        print(f"\n📊 テスト結果: {total_saved}件保存完了")

        # 保存されたデータの確認
        self.verify_saved_data()

    def verify_saved_data(self):
        """保存されたデータの地域マッピング確認"""
        print("\n🔍 保存データの地域マッピング確認:")

        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()

            cursor.execute('''
                SELECT title, genre, region, address
                FROM cards
                WHERE genre IN ('play_arcade', 'play_karaoke')
                ORDER BY created_at DESC
                LIMIT 10
            ''')

            results = cursor.fetchall()

            for title, genre, region, address in results:
                # 住所から期待される地域を再計算
                expected_region = self.get_region_from_address(address)

                status = "✅" if region == expected_region else "❌"
                print(f"{status} {title[:30]}...")
                print(f"   ジャンル: {genre}")
                print(f"   保存地域: {region} | 期待地域: {expected_region}")
                print(f"   住所: {address[:50]}...")
                print()

            cursor.close()
            connection.close()

        except Error as e:
            print(f"❌ 確認エラー: {e}")

def main():
    """メイン実行"""
    tester = SimpleEntertainmentTest()
    tester.test_mapping_system()

if __name__ == '__main__':
    main()
