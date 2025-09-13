#!/usr/bin/env python3
"""
リアルタイムグルメデータ収集システム
- 細かい料理ジャンルを直接genreカラムに保存
- リアルタイム都道府県→地域マッピング
- Google Places API使用
"""

import requests
import mysql.connector
import time
import os
import re
from dotenv import load_dotenv
from utils.request_guard import (
    get_json,
    already_fetched_place,
    mark_fetched_place,
)

# 環境変数読み込み
load_dotenv()

class RealtimeGourmetCollector:
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY environment variable is required")

        # DB接続情報
        self.db_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_production',
            'charset': 'utf8mb4'
        }

        # グルメカテゴリ定義（genreに直接保存）
        self.gourmet_categories = {
            'gourmet_yoshoku': {
                'search_terms': ['洋食', 'イタリアン', 'フレンチ', 'ステーキ'],
                'locations': ['新宿', '渋谷', '銀座', '六本木', '表参道']
            },
            'gourmet_washoku': {
                'search_terms': ['和食', '日本料理', '懐石', '割烹', '寿司'],
                'locations': ['新宿', '渋谷', '銀座', '赤坂', '築地']
            },
            'gourmet_chinese': {
                'search_terms': ['中華', '中国料理', '四川料理', '広東料理', '北京料理'],
                'locations': ['新宿', '池袋', '横浜中華街', '上野', '赤坂']
            },
            'gourmet_bar': {
                'search_terms': ['バー', 'Bar', 'ワインバー', 'カクテルバー', 'ウイスキーバー'],
                'locations': ['新宿', '六本木', '銀座', '渋谷', '表参道']
            },
            'gourmet_izakaya': {
                'search_terms': ['居酒屋', '個人店 居酒屋', '地元 居酒屋', '隠れ家 居酒屋'],
                'locations': ['新宿', '渋谷', '池袋', '上野', '品川']
            }
        }

        # 都道府県→地域マッピング（リアルタイム用）
        self.prefecture_to_region = {
            # 北海道
            '北海道': 'hokkaido',

            # 東北
            '青森': 'tohoku', '岩手': 'tohoku', '宮城': 'tohoku',
            '秋田': 'tohoku', '山形': 'tohoku', '福島': 'tohoku',

            # 関東
            '茨城': 'kanto', '栃木': 'kanto', '群馬': 'kanto',
            '埼玉': 'kanto', '千葉': 'kanto', '東京': 'kanto', '神奈川': 'kanto',

            # 中部
            '新潟': 'chubu', '富山': 'chubu', '石川': 'chubu',
            '福井': 'chubu', '山梨': 'chubu', '長野': 'chubu',
            '岐阜': 'chubu', '静岡': 'chubu', '愛知': 'chubu',

            # 関西
            '三重': 'kansai', '滋賀': 'kansai', '京都': 'kansai',
            '大阪': 'kansai', '兵庫': 'kansai', '奈良': 'kansai', '和歌山': 'kansai',

            # 中国・四国（統合）
            '鳥取': 'chugoku_shikoku', '島根': 'chugoku_shikoku', '岡山': 'chugoku_shikoku',
            '広島': 'chugoku_shikoku', '山口': 'chugoku_shikoku',
            '徳島': 'chugoku_shikoku', '香川': 'chugoku_shikoku',
            '愛媛': 'chugoku_shikoku', '高知': 'chugoku_shikoku',

            # 九州・沖縄
            '福岡': 'kyushu_okinawa', '佐賀': 'kyushu_okinawa', '長崎': 'kyushu_okinawa',
            '熊本': 'kyushu_okinawa', '大分': 'kyushu_okinawa', '宮崎': 'kyushu_okinawa',
            '鹿児島': 'kyushu_okinawa', '沖縄': 'kyushu_okinawa'
        }

    def extract_prefecture_realtime(self, address):
        """住所から都道府県をリアルタイム抽出"""
        if not address:
            return None

        # 都道府県パターンをマッチング
        for prefecture in self.prefecture_to_region.keys():
            if prefecture in address:
                return prefecture

        # 特殊ケース処理
        if '東京都' in address:
            return '東京'
        elif '京都府' in address:
            return '京都'
        elif '大阪府' in address:
            return '大阪'

        return None

    def get_region_from_address(self, address):
        """住所から地域を取得（リアルタイム）"""
        prefecture = self.extract_prefecture_realtime(address)
        if prefecture:
            return self.prefecture_to_region.get(prefecture)
        return None

    def search_places(self, query, location="東京"):
        """Google Places Text Search"""
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            'query': f"{query} {location}",
            'key': self.api_key,
            'language': 'ja',
            'region': 'jp'
        }

        try:
            return get_json(url, params, ttl_sec=60*60*24*7)
        except Exception as e:
            print(f"❌ 検索エラー: {e}")
            return None

    def get_place_details(self, place_id):
        """Google Places Place Details"""
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            'place_id': place_id,
            'fields': 'name,formatted_address,rating,user_ratings_total,photos,reviews,formatted_phone_number,website,opening_hours',
            'key': self.api_key,
            'language': 'ja'
        }

        try:
            if already_fetched_place(place_id):
                return {}
            data = get_json(url, params, ttl_sec=60*60*24*30)
            result = data.get('result', {}) if isinstance(data, dict) else {}
            mark_fetched_place(place_id)
            return result
        except Exception as e:
            print(f"❌ 詳細取得エラー: {e}")
            return {}

    def save_to_database(self, spot_data):
        """データベースに保存"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            # 重複チェック
            cursor.execute(
                "SELECT id FROM cards WHERE title = %s AND address = %s",
                (spot_data['title'], spot_data['address'])
            )

            if cursor.fetchone():
                print(f"  ⚠️ 重複スキップ: {spot_data['title']}")
                return False

            # カード情報を挿入
            insert_card_query = """
                INSERT INTO cards (title, address, rating, review_count, genre, region, phone, website, opening_hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            card_values = (
                spot_data['title'],
                spot_data['address'],
                spot_data['rating'],
                spot_data['review_count'],
                spot_data['genre'],  # 細かいジャンルを直接保存
                spot_data['region'],
                spot_data.get('phone'),
                spot_data.get('website'),
                spot_data.get('opening_hours')
            )

            cursor.execute(insert_card_query, card_values)
            card_id = cursor.lastrowid

            # レビューコメントを挿入
            if spot_data.get('reviews'):
                for review in spot_data['reviews']:
                    cursor.execute(
                        "INSERT INTO review_comments (card_id, comment) VALUES (%s, %s)",
                        (card_id, review)
                    )

            connection.commit()
            print(f"  ✅ 保存完了: {spot_data['title']} (ID: {card_id})")
            return True

        except Exception as e:
            print(f"  ❌ 保存エラー: {e}")
            return False
        finally:
            if 'connection' in locals():
                cursor.close()
                connection.close()

    def collect_category(self, genre, config, max_items=5):
        """特定グルメカテゴリのデータ収集"""
        print(f"\n🍽️ カテゴリ: {genre}")
        print("=" * 50)

        collected = 0

        for search_term in config['search_terms']:
            if collected >= max_items:
                break

            for location in config['locations']:
                if collected >= max_items:
                    break

                print(f"🔍 検索中: {search_term} {location}")

                # Places検索
                search_results = self.search_places(search_term, location)
                if not search_results or 'results' not in search_results:
                    continue

                for place in search_results['results']:
                    if collected >= max_items:
                        break

                    # 基本情報取得
                    name = place.get('name', '')
                    address = place.get('formatted_address', '')
                    rating = place.get('rating', 0)
                    user_ratings_total = place.get('user_ratings_total', 0)
                    place_id = place.get('place_id', '')

                    if not all([name, address, place_id]):
                        continue

                    # リアルタイム地域判定
                    region = self.get_region_from_address(address)
                    if not region:
                        print(f"  ❌ 地域判定失敗: {name}")
                        continue

                    # 詳細情報取得
                    details = self.get_place_details(place_id)

                    # レビューコメント取得
                    reviews = []
                    if details.get('reviews'):
                        reviews = [review.get('text', '') for review in details['reviews'][:5]]

                    # データ構造作成
                    spot_data = {
                        'title': name,
                        'address': address,
                        'rating': rating,
                        'review_count': user_ratings_total,
                        'genre': genre,  # 細かいジャンルを直接genreに保存
                        'region': region,
                        'phone': details.get('formatted_phone_number'),
                        'website': details.get('website'),
                        'opening_hours': str(details.get('opening_hours', {}).get('weekday_text', [])),
                        'reviews': reviews
                    }

                    # 保存実行
                    if self.save_to_database(spot_data):
                        collected += 1
                        print(f"  📍 {name} -> 地域: {region}")

                    # API制限対策
                    time.sleep(0.1)

        print(f"📊 {genre} 収集完了: {collected}件")
        return collected

    def run_collection(self, target_categories=None, items_per_category=5):
        """グルメデータ収集実行"""
        print("🍽️ リアルタイムグルメデータ収集システム")
        print("=" * 60)
        print(f"📊 目的: 細かい料理ジャンルを直接genreに保存")
        print("=" * 60)

        if target_categories is None:
            target_categories = list(self.gourmet_categories.keys())

        total_collected = 0

        for genre in target_categories:
            if genre not in self.gourmet_categories:
                print(f"❌ 未知のカテゴリ: {genre}")
                continue

            config = self.gourmet_categories[genre]
            collected = self.collect_category(genre, config, items_per_category)
            total_collected += collected

            # API制限対策
            time.sleep(1)

        print(f"\n🎉 グルメデータ収集完了!")
        print(f"✅ 総収集件数: {total_collected}件")
        print(f"✅ Genre保存方式: 細かいジャンル直接保存")

        return total_collected

if __name__ == "__main__":
    import sys

    collector = RealtimeGourmetCollector()

    # コマンドライン引数処理
    if len(sys.argv) > 1:
        target_category = sys.argv[1]
        if target_category in collector.gourmet_categories:
            collector.run_collection([target_category], items_per_category=10)
        else:
            print(f"❌ 無効なカテゴリ: {target_category}")
            print(f"✅ 有効なカテゴリ: {list(collector.gourmet_categories.keys())}")
    else:
        # 全カテゴリ収集
        collector.run_collection(items_per_category=5)
