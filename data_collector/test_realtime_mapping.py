#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
取得時マッピング検証スクリプト - 遊びジャンル（ゲーセン・カラオケ）
各地域10件ずつ、合計80件のデータを収集して取得時マッピングをテスト
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

class GameEntertainmentCollector:
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

        # 地域マッピング（取得時使用）
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

        # 検索対象都市（各地域から代表都市）
        self.target_cities = {
            '北海道': ['札幌', '函館'],
            '東北': ['仙台', '青森'],
            '関東': ['東京', '横浜'],
            '中部': ['名古屋', '金沢'],
            '関西': ['大阪', '京都'],
            '中国': ['広島', '岡山'],
            '九州': ['福岡', '長崎']
        }

        # 遊びジャンルの検索クエリ
        self.game_categories = {
            'game_arcade': {
                'terms': ['ゲームセンター', 'アーケード', 'ゲーセン', 'ゲーム', 'クレーンゲーム'],
                'target_per_region': 5
            },
            'game_karaoke': {
                'terms': ['カラオケ', 'カラオケボックス', 'カラオケ館', 'ビッグエコー', 'カラオケルーム'],
                'target_per_region': 5
            }
        }

    def extract_prefecture_from_address(self, address):
        """住所から都道府県を抽出（取得時マッピング用）"""
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
        """住所から地域を判定（取得時マッピング）"""
        prefecture = self.extract_prefecture_from_address(address)
        if prefecture:
            return self.prefecture_to_region.get(prefecture, '不明')
        return '不明'

    def search_places(self, query: str, location: str = None) -> List[Dict]:
        """Google Places APIで場所を検索"""
        try:
            params = {
                'query': f"{query} {location}" if location else query,
                'key': self.google_api_key,
                'language': 'ja',
                'region': 'jp'
            }

            response = requests.get(self.text_search_url, params=params)

            if response.status_code == 200:
                data = response.json()
                return data.get('results', [])
            else:
                print(f"❌ API検索エラー: {response.status_code}")
                return []

        except Exception as e:
            print(f"❌ 検索エラー: {e}")
            return []

    def get_place_details(self, place_id: str) -> Optional[Dict]:
        """Place IDから詳細情報を取得"""
        try:
            params = {
                'place_id': place_id,
                'key': self.google_api_key,
                'fields': 'place_id,name,formatted_address,geometry,rating,user_ratings_total,reviews,photos,formatted_phone_number,website,opening_hours',
                'language': 'ja'
            }

            response = requests.get(self.place_details_url, params=params)

            if response.status_code == 200:
                return response.json().get('result', {})
            else:
                print(f"❌ 詳細取得エラー: {response.status_code}")
                return None

        except Exception as e:
            print(f"❌ 詳細取得エラー: {e}")
            return None

    def extract_permanent_image_url(self, photo_reference: str) -> Optional[str]:
        """写真参照から永続的なURLを抽出"""
        try:
            # Google Photos APIを使用して写真を取得
            photo_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference={photo_reference}&key={self.google_api_key}"

            # HEAD requestで実際のURLを取得
            response = requests.head(photo_url, allow_redirects=True)
            return response.url if response.status_code == 200 else None

        except Exception as e:
            print(f"❌ 画像URL取得エラー: {e}")
            return None

    def save_to_database(self, place_data: Dict, genre: str, region: str) -> Optional[int]:
        """データベースに保存（取得時マッピング済み）"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()

            # 重複チェック
            cursor.execute('SELECT id FROM cards WHERE title = %s AND address = %s',
                         (place_data['name'], place_data.get('formatted_address', '')))

            if cursor.fetchone():
                print(f"⏸️ 重複スキップ: {place_data['name']}")
                cursor.close()
                connection.close()
                return None

            # メインデータ挿入
            insert_query = '''
                INSERT INTO cards (
                    title, description, genre, region, address,
                    latitude, longitude, image_url, rating,
                    phone_number, website, opening_hours, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            '''

            # 画像URL処理
            image_url = None
            if place_data.get('photos'):
                photo_ref = place_data['photos'][0].get('photo_reference')
                if photo_ref:
                    image_url = self.extract_permanent_image_url(photo_ref)

            # 緯度経度取得
            location = place_data.get('geometry', {}).get('location', {})
            latitude = location.get('lat')
            longitude = location.get('lng')

            # 営業時間処理
            opening_hours = None
            if place_data.get('opening_hours', {}).get('weekday_text'):
                opening_hours = '\n'.join(place_data['opening_hours']['weekday_text'])

            values = (
                place_data['name'],
                f"評価: {place_data.get('rating', 'N/A')} ({place_data.get('user_ratings_total', 0)}件の評価)",
                genre,
                region,  # 取得時マッピング済みの地域
                place_data.get('formatted_address', ''),
                latitude,
                longitude,
                image_url,
                place_data.get('rating'),
                place_data.get('formatted_phone_number'),
                place_data.get('website'),
                opening_hours
            )

            cursor.execute(insert_query, values)
            card_id = cursor.lastrowid

            # レビューコメント保存
            if place_data.get('reviews'):
                for review in place_data['reviews'][:5]:  # 最大5件
                    if review.get('text'):
                        cursor.execute(
                            'INSERT INTO review_comments (card_id, comment, created_at) VALUES (%s, %s, NOW())',
                            (card_id, review['text'])
                        )

            connection.commit()
            print(f"✅ 保存完了: {place_data['name']} → {region}")

            cursor.close()
            connection.close()
            return card_id

        except Error as e:
            print(f"❌ DB保存エラー: {e}")
            return None

    def collect_game_entertainment_data(self):
        """遊びジャンルデータ収集メイン処理"""
        print("🎮 遊びジャンル収集開始（取得時マッピング検証）\n")

        total_collected = 0
        collection_summary = {}

        for region, cities in self.target_cities.items():
            collection_summary[region] = {}
            print(f"📍 {region}地域の収集開始...")

            for genre, config in self.game_categories.items():
                collected_count = 0
                target = config['target_per_region']

                print(f"  🎯 {genre} - 目標: {target}件")

                for term in config['terms']:
                    if collected_count >= target:
                        break

                    for city in cities:
                        if collected_count >= target:
                            break

                        query = f"{term} {city}"
                        print(f"    🔍 検索中: {query}")

                        places = self.search_places(query, city)

                        for place in places[:3]:  # 各検索で最大3件
                            if collected_count >= target:
                                break

                            # 詳細情報取得
                            details = self.get_place_details(place['place_id'])
                            if not details:
                                continue

                            # 住所から地域を判定（取得時マッピング）
                            address = details.get('formatted_address', '')
                            detected_region = self.get_region_from_address(address)

                            # 期待地域と実際の地域が一致するかチェック
                            if detected_region == region:
                                card_id = self.save_to_database(details, genre, detected_region)
                                if card_id:
                                    collected_count += 1
                                    total_collected += 1
                                    print(f"      ✅ {details['name']} (地域: {detected_region})")
                            else:
                                print(f"      ⚠️ 地域不一致: {details['name']} 期待={region}, 実際={detected_region}")

                        time.sleep(0.5)  # レート制限対策

                collection_summary[region][genre] = collected_count
                print(f"  📊 {genre}: {collected_count}/{target}件収集完了\n")

            print(f"📍 {region}地域完了\n")

        # 結果サマリー
        print("🎯 取得時マッピング検証結果")
        print("=" * 50)

        for region, genres in collection_summary.items():
            total_region = sum(genres.values())
            print(f"{region}: {total_region}件")
            for genre, count in genres.items():
                print(f"  {genre}: {count}件")

        print(f"\n📊 総計: {total_collected}件")
        print("✨ 取得時マッピング検証完了！")

def main():
    collector = GameEntertainmentCollector()
    collector.collect_game_entertainment_data()

if __name__ == '__main__':
    main()
