#!/usr/bin/env python3
"""
東北地方リラックスカテゴリスポット自動取得スクリプト（修正版）
中部地方で動作確認済みのロジックを東北地方用に調整
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

# .envファイルを読み込み
load_dotenv()

class TohokuRelaxDataCollector:
    def __init__(self):
        """初期化"""
        self.google_api_key = os.getenv('GOOGLE_API_KEY')
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }

        # API設定
        self.places_api_base = "https://maps.googleapis.com/maps/api/place"
        self.text_search_url = f"{self.places_api_base}/textsearch/json"
        self.place_details_url = f"{self.places_api_base}/details/json"

        # 東北地方の県リスト
        self.tohoku_prefectures = ['青森', '岩手', '宮城', '秋田', '山形', '福島']

    def _setup_mysql_connection(self):
        """MySQL接続をセットアップ"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            if connection.is_connected():
                print(f"✅ MySQLに接続しました: {self.mysql_config['database']}")
                return connection
        except Error as e:
            print(f"❌ MySQL接続エラー: {e}")
            return None

    def _search_places(self, query: str) -> List[Dict]:
        """Google Places APIで場所を検索"""
        print(f"検索中: {query}")

        params = {
            'query': query,
            'key': self.google_api_key,
            'language': 'ja',
            'region': 'jp'
        }

        try:
            response = requests.get(self.text_search_url, params=params)
            data = response.json()

            if data.get('status') == 'OK':
                results = data.get('results', [])
                print(f"  検索結果: {len(results)}件")
                return results
            else:
                print(f"  APIエラー: {data.get('status')} - {query}")
                return []

        except Exception as e:
            print(f"  検索エラー ({query}): {e}")
            return []

    def _get_place_details(self, place_id: str) -> Optional[Dict]:
        """場所の詳細情報を取得"""
        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'language': 'ja',
            'fields': 'place_id,name,formatted_address,geometry,rating,user_ratings_total,price_level,formatted_phone_number,website,opening_hours,photos,types,vicinity,plus_code'
        }

        try:
            response = requests.get(self.place_details_url, params=params)
            data = response.json()

            if data.get('status') == 'OK':
                result = data.get('result')
                print(f"  詳細取得成功: {result.get('name')} (place_id: {result.get('place_id')})")
                return result
            else:
                print(f"  詳細取得エラー: {data.get('status')} - {place_id}")
                return None

        except Exception as e:
            print(f"  詳細取得例外 ({place_id}): {e}")
            return None

    def _save_to_database(self, connection, places: List[Dict], category: str):
        """データベースに保存"""
        if not places:
            print("保存するデータがありません")
            return 0

        cursor = connection.cursor()

        insert_query = """
        INSERT INTO spots (
            place_id, name, category, address, latitude, longitude, rating,
            user_ratings_total, price_level, phone_number, website, opening_hours,
            photos, types, vicinity, plus_code, region
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            address = VALUES(address),
            rating = VALUES(rating),
            user_ratings_total = VALUES(user_ratings_total),
            phone_number = VALUES(phone_number),
            website = VALUES(website),
            opening_hours = VALUES(opening_hours),
            updated_at = CURRENT_TIMESTAMP
        """

        saved_count = 0
        for i, place in enumerate(places):
            try:
                # データ検証を追加
                place_id = place.get('place_id')
                name = place.get('name')

                # place_id の検証
                if not place_id:
                    print(f"  警告: place_id が空です - {name} (データ {i+1})")
                    continue

                # name の検証
                if not name:
                    print(f"  警告: name が空です - place_id: {place_id} (データ {i+1})")
                    continue

                print(f"  保存中 ({i+1}/{len(places)}): {name}")

                # 写真情報の処理
                photos = []
                if 'photos' in place:
                    for photo in place['photos'][:5]:  # 最大5枚
                        photo_reference = photo.get('photo_reference')
                        if photo_reference:
                            photos.append({
                                'photo_reference': photo_reference,
                                'height': photo.get('height'),
                                'width': photo.get('width')
                            })

                # 営業時間の処理
                opening_hours = None
                if 'opening_hours' in place:
                    opening_hours = json.dumps(place['opening_hours'], ensure_ascii=False)

                # 座標の取得
                location = place.get('geometry', {}).get('location', {})
                latitude = location.get('lat')
                longitude = location.get('lng')

                data = (
                    place_id,
                    name,
                    category,
                    place.get('formatted_address'),
                    latitude,
                    longitude,
                    place.get('rating'),
                    place.get('user_ratings_total'),
                    place.get('price_level'),
                    place.get('formatted_phone_number'),
                    place.get('website'),
                    opening_hours,
                    json.dumps(photos, ensure_ascii=False) if photos else None,
                    json.dumps(place.get('types', []), ensure_ascii=False),
                    place.get('vicinity'),
                    place.get('plus_code', {}).get('compound_code') if place.get('plus_code') else None,
                    'tohoku'
                )

                cursor.execute(insert_query, data)
                saved_count += 1
                print(f"    ✅ 保存成功")

            except Exception as e:
                print(f"    ❌ 保存エラー: {e} - {place.get('name', 'Unknown')}")
                print(f"    エラー詳細: {type(e).__name__}")
                if hasattr(e, 'errno'):
                    print(f"    MySQL Error Code: {e.errno}")
                continue

        connection.commit()
        cursor.close()
        print(f"カテゴリ {category}: {saved_count}件保存完了")
        return saved_count

    def collect_onsen_test(self):
        """温泉カテゴリのテスト収集"""
        print("=== 東北地方温泉カテゴリ収集テスト ===\n")

        # データベース接続
        connection = self._setup_mysql_connection()
        if not connection:
            return

        try:
            # 東北地方用クエリ
            test_queries = [
                "温泉 宮城県",
                "温泉 青森県",
                "温泉 岩手県",
                "温泉 秋田県",
                "温泉 山形県",
                "温泉 福島県"
            ]

            all_places = []

            for query in test_queries:
                # 基本検索
                search_results = self._search_places(query)

                for result in search_results[:3]:  # 各クエリから最大3件
                    place_id = result.get('place_id')
                    if place_id:
                        # 詳細情報取得
                        detailed_place = self._get_place_details(place_id)
                        if detailed_place:
                            # 東北地方チェック
                            address = detailed_place.get('formatted_address', '')
                            if any(pref in address for pref in self.tohoku_prefectures):
                                all_places.append(detailed_place)

                        time.sleep(0.5)  # API制限対策

                time.sleep(1)

            # 重複除去
            unique_places = []
            seen_place_ids = set()

            for place in all_places:
                place_id = place.get('place_id')
                if place_id and place_id not in seen_place_ids:
                    unique_places.append(place)
                    seen_place_ids.add(place_id)

            print(f"\n取得件数: {len(unique_places)}件")

            # データベースに保存
            if unique_places:
                saved_count = self._save_to_database(connection, unique_places, 'relax_onsen')
                print(f"\n✅ 東北地方完了: {saved_count}件保存")
            else:
                print("\n❌ 保存するデータがありません")

        finally:
            if connection.is_connected():
                connection.close()
                print("✅ データベース接続クローズ")

def main():
    collector = TohokuRelaxDataCollector()
    collector.collect_onsen_test()

if __name__ == "__main__":
    main()
