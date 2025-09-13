#!/usr/bin/env python3
"""
遊びジャンル収集テストシステム（取得時マッピング検証）
ゲーセン・カラオケを各地域10件ずつ収集（合計80件）
取得時に地域マッピングを実行してDBに保存
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

class PlayEntertainmentCollector:
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

        # 地域別検索都市（各地域から代表都市を選択）
        self.regional_cities = {
            '北海道': ['札幌', '函館', '旭川'],
            '東北': ['仙台', '青森', '盛岡', '山形', '秋田', '福島'],
            '関東': ['東京', '横浜', '千葉', '大宮', '宇都宮', '前橋', '水戸'],
            '中部': ['名古屋', '金沢', '富山', '福井', '甲府', '長野', '岐阜', '静岡', '新潟'],
            '関西': ['大阪', '京都', '神戸', '奈良', '和歌山', '津'],
            '中国': ['広島', '岡山', '松江', '鳥取', '山口'],
            '九州': ['福岡', '熊本', '鹿児島', '長崎', '大分', '宮崎', '佐賀']
        }

        # 都道府県→地域マッピング（日本語版）
        self.prefecture_to_region = {
            # 北海道地方
            '北海道': '北海道',

            # 東北地方
            '青森県': '東北', '岩手県': '東北', '宮城県': '東北',
            '秋田県': '東北', '山形県': '東北', '福島県': '東北',

            # 関東地方
            '東京都': '関東', '茨城県': '関東', '栃木県': '関東',
            '群馬県': '関東', '埼玉県': '関東', '千葉県': '関東', '神奈川県': '関東',

            # 中部地方
            '新潟県': '中部', '富山県': '中部', '石川県': '中部', '福井県': '中部',
            '山梨県': '中部', '長野県': '中部', '岐阜県': '中部',
            '静岡県': '中部', '愛知県': '中部',

            # 関西地方
            '京都府': '関西', '大阪府': '関西', '三重県': '関西',
            '滋賀県': '関西', '兵庫県': '関西', '奈良県': '関西', '和歌山県': '関西',
            '京都': '関西',  # 京都の省略形対応

            # 中国地方
            '鳥取県': '中国', '島根県': '中国', '岡山県': '中国',
            '広島県': '中国', '山口県': '中国',

            # 四国地方 → 中国地方に統合
            '徳島県': '中国', '香川県': '中国',
            '愛媛県': '中国', '高知県': '中国',

            # 九州地方
            '福岡県': '九州', '佐賀県': '九州', '長崎県': '九州',
            '大分県': '九州', '熊本県': '九州', '宮崎県': '九州',
            '鹿児島県': '九州', '沖縄県': '九州'
        }

        # 遊びジャンル定義
        self.entertainment_categories = {
            'play_arcade': {
                'terms': ['ゲームセンター', 'アミューズメント', 'ゲーセン', 'アーケード'],
                'genre': 'play_arcade'
            },
            'play_karaoke': {
                'terms': ['カラオケ', 'カラオケボックス', 'KTV'],
                'genre': 'play_karaoke'
            }
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

        # パターン2: 日本、都道府県（郵便番号なし）
        pattern2 = r'日本、([^市区町村]+?[県都府])'
        match2 = re.search(pattern2, address)
        if match2:
            return match2.group(1)

        # パターン3: 都道府県を直接検索（最長マッチ）
        found_prefectures = []
        for prefecture in self.prefecture_to_region.keys():
            if prefecture in address:
                found_prefectures.append(prefecture)

        # 最長の県名を返す
        if found_prefectures:
            return max(found_prefectures, key=len)

        return None

    def get_region_from_address(self, address):
        """住所から地域を判定（日本語）"""
        prefecture = self.extract_prefecture_from_address(address)
        if prefecture:
            return self.prefecture_to_region.get(prefecture)
        return None

    def search_places(self, query: str, target_count: int = 10) -> List[Dict]:
        """Places APIでスポット検索"""
        all_results = []
        next_page_token = None

        print(f"🔍 検索中: '{query}' (目標: {target_count}件)")

        while len(all_results) < target_count:
            params = {
                'query': query,
                'key': self.google_api_key,
                'language': 'ja',
                'region': 'jp'
            }

            if next_page_token:
                params['pagetoken'] = next_page_token
                time.sleep(2)  # ページトークン使用時は少し待機

            try:
                response = requests.get(self.text_search_url, params=params)
                response.raise_for_status()
                data = response.json()

                if 'results' in data:
                    for place in data['results']:
                        if len(all_results) >= target_count:
                            break
                        all_results.append(place)

                # 次のページがあるかチェック
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    break

            except Exception as e:
                print(f"❌ 検索エラー: {e}")
                break

        print(f"✅ 取得完了: {len(all_results)}件")
        return all_results[:target_count]

    def get_place_details(self, place_id: str) -> Optional[Dict]:
        """詳細情報とレビューを取得"""
        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'language': 'ja',
            'fields': 'name,formatted_address,formatted_phone_number,website,rating,user_ratings_total,reviews,photos,geometry'
        }

        try:
            response = requests.get(self.place_details_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get('status') == 'OK':
                return data.get('result')
            else:
                print(f"❌ 詳細取得失敗: {data.get('status')}")
                return None

        except Exception as e:
            print(f"❌ 詳細取得エラー: {e}")
            return None

    def extract_permanent_image_url(self, photo_reference: str) -> Optional[str]:
        """永続的な画像URLを抽出"""
        if not photo_reference:
            return None

        photo_url = f"https://maps.googleapis.com/maps/api/place/photo"
        params = {
            'photoreference': photo_reference,
            'maxwidth': 800,
            'key': self.google_api_key
        }

        try:
            response = requests.get(photo_url, params=params, allow_redirects=False)
            if response.status_code == 302:
                return response.headers.get('Location')
        except Exception as e:
            print(f"❌ 画像URL取得エラー: {e}")

        return None

    def save_to_database(self, place_data: Dict) -> Optional[int]:
        """データベースに保存（取得時マッピング適用）"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()

            # 住所から地域を自動判定
            address = place_data.get('formatted_address', '')
            region = self.get_region_from_address(address)

            if not region:
                print(f"⚠️ 地域判定失敗: {place_data.get('name')} - {address}")
                region = 'その他'  # デフォルト値

            # 重複チェック
            cursor.execute(
                'SELECT id FROM cards WHERE title = %s AND address = %s',
                (place_data['name'], address)
            )

            if cursor.fetchone():
                print(f"⏸️ 重複スキップ: {place_data['name']}")
                return None

            # メイン情報を挿入
            card_query = '''
            INSERT INTO cards (title, description, address, phone, website, rating,
                             genre, region, latitude, longitude, image_url, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            '''

            # 画像URL取得
            image_url = None
            if place_data.get('photos'):
                photo_ref = place_data['photos'][0].get('photo_reference')
                image_url = self.extract_permanent_image_url(photo_ref)

            # 位置情報取得
            geometry = place_data.get('geometry', {})
            location = geometry.get('location', {})
            latitude = location.get('lat')
            longitude = location.get('lng')

            card_values = (
                place_data['name'],
                f"評価: {place_data.get('rating', 'N/A')} ({place_data.get('user_ratings_total', 0)}件の評価)",
                address,
                place_data.get('formatted_phone_number'),
                place_data.get('website'),
                place_data.get('rating'),
                place_data.get('genre'),
                region,  # 取得時にマッピング済み！
                latitude,
                longitude,
                image_url
            )

            cursor.execute(card_query, card_values)
            card_id = cursor.lastrowid

            # レビューコメント保存
            reviews = place_data.get('reviews', [])
            for review in reviews[:5]:  # 最大5件のレビュー
                comment_query = '''
                INSERT INTO review_comments (card_id, comment, rating, created_at)
                VALUES (%s, %s, %s, NOW())
                '''

                cursor.execute(comment_query, (
                    card_id,
                    review.get('text', ''),
                    review.get('rating', 0)
                ))

            connection.commit()
            print(f"✅ 保存完了: {place_data['name']} (地域: {region})")
            return card_id

        except Error as e:
            print(f"❌ DB保存エラー: {e}")
            return None
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

    def collect_entertainment_data(self):
        """遊びジャンルデータ収集メイン処理"""
        print("🎮 遊びジャンル収集開始（取得時マッピング検証）\n")

        total_collected = 0
        results_summary = {}

        for category, config in self.entertainment_categories.items():
            print(f"\n📋 カテゴリ: {config['genre']}")
            category_total = 0
            results_summary[config['genre']] = {}

            for region, cities in self.regional_cities.items():
                print(f"\n🗾 地域: {region}")
                region_collected = 0
                target_per_region = 10

                for term in config['terms']:
                    if region_collected >= target_per_region:
                        break

                    for city in cities:
                        if region_collected >= target_per_region:
                            break

                        query = f"{term} {city}"
                        places = self.search_places(query, target_count=3)

                        for place in places:
                            if region_collected >= target_per_region:
                                break

                            # 詳細情報取得
                            details = self.get_place_details(place['place_id'])
                            if details:
                                details['genre'] = config['genre']
                                card_id = self.save_to_database(details)

                                if card_id:
                                    region_collected += 1
                                    category_total += 1
                                    total_collected += 1

                        time.sleep(1)  # API制限対策

                results_summary[config['genre']][region] = region_collected
                print(f"   ✅ {region}: {region_collected}件収集")

        # 結果サマリー
        print(f"\n📊 収集結果サマリー:")
        print(f"総収集件数: {total_collected}件\n")

        for genre, regions in results_summary.items():
            print(f"🎮 {genre}:")
            for region, count in regions.items():
                print(f"   {region}: {count}件")
            print()

def main():
    """メイン実行"""
    if len(sys.argv) > 1:
        print("🎮 遊びジャンル収集システム（取得時マッピング検証）")
        print("使用方法: python test_entertainment_mapping.py")
        return

    collector = PlayEntertainmentCollector()
    collector.collect_entertainment_data()

if __name__ == '__main__':
    main()
