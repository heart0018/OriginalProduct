#!/usr/bin/env python3
"""
リアルタイム地域マッピング付きエンターテイメント収集システム
取得時にアドレスから都道府県を抽出し、即座に正確な地域に分類してDB保存
"""

import os
import time
import json
import random
import requests
import mysql.connector
import re
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv
from utils.request_guard import (
    get_json,
    already_fetched_place,
    mark_fetched_place,
    get_photo_direct_url,
)

load_dotenv()

class RealtimeEntertainmentCollector:
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
            '徳島県': '中国', '香川県': '中国', '愛媛県': '中国', '高知県': '中国',

            # 九州・沖縄
            '福岡県': '九州', '佐賀県': '九州', '長崎県': '九州', '熊本県': '九州',
            '大分県': '九州', '宮崎県': '九州', '鹿児島県': '九州', '沖縄県': '九州'
        }

        # エンターテイメントカテゴリ設定
        self.entertainment_categories = {
            'entertainment_arcade': ['ゲームセンター', 'アミューズメント施設'],
            'entertainment_karaoke': ['カラオケ', 'カラオケボックス'],
            'entertainment_bowling': ['ボウリング', 'ボウリング場'],
            'entertainment_cinema': ['映画館', 'シネマ'],
            'entertainment_sports': ['スポッチャ', 'ラウンドワン', 'バッティングセンター']
        }

        # 地域別検索キーワード
        self.region_keywords = {
            '北海道': ['札幌', '函館', '旭川', '帯広'],
            '東北': ['仙台', '青森', '盛岡', '秋田', '山形', '福島'],
            '関東': ['東京', '横浜', '埼玉', '千葉', '茨城', '栃木', '群馬'],
            '中部': ['名古屋', '新潟', '金沢', '富山', '長野', '静岡'],
            '関西': ['大阪', '京都', '神戸', '奈良', '和歌山'],
            '中国': ['広島', '岡山', '山口', '鳥取', '島根', '高松', '松山', '高知', '徳島'],
            '九州': ['福岡', '熊本', '鹿児島', '長崎', '大分', '宮崎', '那覇']
        }

    def extract_prefecture_from_address(self, address: str) -> Optional[str]:
        """アドレスから都道府県を抽出"""
        if not address:
            return None

        # Google Places APIの標準フォーマット: "日本、〒XXX-XXXX 都道府県..."
        # または単純に都道府県名が含まれている場合
        prefecture_pattern = r'(北海道|[^\s]+県|[^\s]+府|[^\s]+都)'

        matches = re.findall(prefecture_pattern, address)
        for match in matches:
            if match in self.prefecture_to_region:
                return match

        return None

    def get_region_from_prefecture(self, prefecture: str) -> str:
        """都道府県から地域を取得"""
        return self.prefecture_to_region.get(prefecture, '関東')  # デフォルトは関東

    def search_places(self, query: str, region: str) -> List[Dict]:
        """Places APIで検索（リアルタイム地域マッピング付き）"""
        places = []

        for keyword in self.region_keywords.get(region, ['日本']):
            search_query = f"{query} {keyword}"

            url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
            params = {
                'query': search_query,
                'language': 'ja',
                'region': 'JP',
                'key': self.google_api_key,
            }

            try:
                print(f"  🔍 検索中: {search_query}")
                # 7日キャッシュ
                data = get_json(url, params, ttl_sec=60*60*24*7)

                if data['status'] == 'OK':
                    for place in data['results']:
                        # リアルタイム地域マッピング
                        address = place.get('formatted_address', '')
                        detected_prefecture = self.extract_prefecture_from_address(address)
                        detected_region = self.get_region_from_prefecture(detected_prefecture) if detected_prefecture else region

                        place_info = {
                            'place_id': place['place_id'],
                            'name': place['name'],
                            'address': address,
                            'prefecture': detected_prefecture,
                            'region': detected_region,  # リアルタイムで正確な地域設定
                            'lat': place['geometry']['location']['lat'],
                            'lng': place['geometry']['location']['lng'],
                            'rating': place.get('rating', 0),
                            'photo_references': [photo['photo_reference'] for photo in place.get('photos', [])[:3]]
                        }

                        places.append(place_info)
                        print(f"    ✓ {place['name']} → {detected_prefecture} → {detected_region}")

                elif data['status'] == 'OVER_QUERY_LIMIT':
                    print(f"  ❌ API制限に達しました")
                    break
                else:
                    print(f"  ❌ 検索エラー: {data['status']}")

                # レート制限対策
                time.sleep(random.uniform(1, 2))

            except Exception as e:
                print(f"  ❌ 検索エラー: {e}")
                continue

        return places

    def get_place_details_with_reviews(self, place_id: str) -> Optional[Dict]:
        """プレイス詳細とレビューを取得"""
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            'place_id': place_id,
            'fields': 'reviews,formatted_phone_number,website,opening_hours',
            'language': 'ja',
            'key': self.google_api_key,
        }

        try:
            if already_fetched_place(place_id):
                return None
            data = get_json(url, params, ttl_sec=60*60*24*30)

            if data['status'] == 'OK':
                result = data.get('result') if isinstance(data, dict) else None
                if result:
                    mark_fetched_place(place_id)
                return result
            else:
                print(f"    ❌ 詳細取得エラー: {data['status']}")
                return None

        except Exception as e:
            print(f"    ❌ 詳細取得エラー: {e}")
            return None

    def get_permanent_image_url(self, photo_reference: str) -> Optional[str]:
        """永続的画像URLを取得"""
        if not photo_reference:
            return None

        try:
            return get_photo_direct_url(photo_reference, maxwidth=800, ttl_sec=60*60*24*30)
        except Exception as e:
            print(f"    ❌ 画像URL取得エラー: {e}")
            return None

    def save_to_database(self, place_data: Dict, category: str) -> bool:
        """データベースに保存（リアルタイム地域マッピング済み）"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            # 重複チェック
            cursor.execute("SELECT id FROM cards WHERE place_id = %s", (place_data['place_id'],))
            if cursor.fetchone():
                print(f"    ⚠️ 重複スキップ: {place_data['name']}")
                return False

            # メイン画像URL取得
            main_image_url = None
            if place_data['photo_references']:
                main_image_url = self.get_permanent_image_url(place_data['photo_references'][0])

            # Genre統一マッピング（詳細カテゴリは preserved）
            unified_genre = 'entertainment'  # すべてのエンターテイメントカテゴリを統一
            detailed_category = category  # 元の詳細カテゴリを保持

            # カード情報挿入（統一genre + 詳細カテゴリ + リアルタイム地域マッピング済み）
            card_sql = """
                INSERT INTO cards (
                    place_id, title, genre, detailed_category, region, latitude, longitude,
                    address, image_url, rating, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """

            card_data = (
                place_data['place_id'],
                place_data['name'],
                unified_genre,  # 統一genre: 'entertainment'
                detailed_category,  # 詳細カテゴリ: 'entertainment_karaoke' など
                place_data['region'],  # リアルタイムで設定された正確な地域
                place_data['lat'],
                place_data['lng'],
                place_data['address'],
                main_image_url,
                place_data['rating']
            )

            cursor.execute(card_sql, card_data)
            card_id = cursor.lastrowid

            print(f"    ✅ DB保存完了: {place_data['name']} → {place_data['region']} ({place_data.get('prefecture', 'N/A')})")
            print(f"    🏷️ Genre: {unified_genre} | 詳細: {detailed_category}")

            connection.commit()
            cursor.close()
            connection.close()

            return True

        except Exception as e:
            print(f"    ❌ DB保存エラー: {e}")
            return False

    def collect_entertainment_category(self, category: str, target_per_region: int = 20):
        """エンターテイメントカテゴリを収集（リアルタイム地域マッピング）"""
        print(f"\n🎮 {category} 収集開始")
        print("=" * 50)

        queries = self.entertainment_categories.get(category, [category])
        total_collected = 0

        for region in self.region_keywords.keys():
            print(f"\n📍 {region}地方での収集開始")
            region_collected = 0

            for query in queries:
                if region_collected >= target_per_region:
                    break

                print(f"\n🔍 {query} @ {region}")
                places = self.search_places(query, region)

                for place in places[:target_per_region]:
                    if region_collected >= target_per_region:
                        break

                    # リアルタイムマッピングで既に正確な地域が設定済み
                    if self.save_to_database(place, category):
                        region_collected += 1
                        total_collected += 1

                    time.sleep(random.uniform(0.5, 1.0))

            print(f"  ✅ {region}: {region_collected}件収集")
            time.sleep(2)  # 地域間の待機

        print(f"\n🎯 {category} 収集完了: {total_collected}件")
        return total_collected

    def collect_all_entertainment(self, target_per_category_region: int = 15):
        """全エンターテイメントカテゴリを収集"""
        print("🎮 リアルタイム地域マッピング付きエンターテイメント収集システム")
        print("=" * 60)
        print("📊 取得時マッピング: アドレス→都道府県→地域 (即座に正確分類)")
        print("=" * 60)

        total_collected = 0
        results = {}

        for category in self.entertainment_categories.keys():
            collected = self.collect_entertainment_category(category, target_per_category_region)
            results[category] = collected
            total_collected += collected

            print(f"\n⏸️ カテゴリ間休憩...")
            time.sleep(5)

        print(f"\n🎯 全エンターテイメント収集完了")
        print("=" * 50)
        for category, count in results.items():
            print(f"{category}: {count}件")
        print(f"総計: {total_collected}件")

        return results

def main():
    """メイン実行関数"""
    try:
        collector = RealtimeEntertainmentCollector()

        # エンターテイメント全カテゴリ収集（リアルタイム地域マッピング）
        results = collector.collect_all_entertainment(target_per_category_region=10)

        print("\n🎮 リアルタイム地域マッピング収集完了!")
        print("✅ 取得時に正確な地域分類でDB保存済み")

    except Exception as e:
        print(f"❌ システムエラー: {e}")

if __name__ == "__main__":
    main()
