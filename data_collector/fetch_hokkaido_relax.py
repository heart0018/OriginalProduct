#!/usr/bin/env python3
"""
北海道リラックスカテゴリスポット自動取得スクリプト
Google Places APIを使用して北海道の温泉・公園・サウナ・カフェ・散歩コースデータを取得し、MySQLに保存する
各ジャンル20件ずつ
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
from utils.request_guard import (
    get_json,
    already_fetched_place,
    mark_fetched_place,
    get_photo_direct_url,
)

# .envファイルを読み込み
load_dotenv()

class HokkaidoRelaxDataCollector:
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

        # 北海道のみ
        self.hokkaido_prefectures = ['北海道']

        # 検索設定（リラックスカテゴリ・北海道全域対応）
        self.search_categories = {
            'relax_onsen': {
                'base_terms': [
                    "温泉", "銭湯", "スーパー銭湯", "天然温泉", "日帰り温泉",
                    "温泉施設", "入浴施設", "岩盤浴"
                ],
                'queries': self._generate_regional_queries([
                    "温泉", "銭湯", "スーパー銭湯", "天然温泉", "日帰り温泉",
                    "温泉施設", "入浴施設", "岩盤浴"
                ]),
                'keywords': ['温泉', '銭湯', 'スパ', 'spa', 'hot spring', 'bath house', '入浴', '岩盤浴'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 20
            },
            'relax_park': {
                'base_terms': [
                    "公園", "都市公園", "緑地", "運動公園", "道立公園",
                    "自然公園", "森林公園", "総合公園", "国営公園"
                ],
                'queries': self._generate_regional_queries([
                    "公園", "都市公園", "緑地", "運動公園", "道立公園",
                    "自然公園", "森林公園", "総合公園", "国営公園"
                ]),
                'keywords': ['公園', 'park', '緑地', '運動場', 'スポーツ', '広場', '散歩', '遊歩道'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 20
            },
            'relax_sauna': {
                'base_terms': [
                    "サウナ", "サウナ施設", "個室サウナ", "フィンランドサウナ",
                    "ロウリュ", "サウナ&スパ", "岩盤浴", "テントサウナ",
                    "外気浴", "水風呂", "サウナラウンジ", "サ活", "高温サウナ",
                    "低温サウナ", "ととのい", "整い", "発汗", "サウナカフェ"
                ],
                'queries': self._generate_regional_queries([
                    "サウナ", "サウナ施設", "個室サウナ", "フィンランドサウナ",
                    "ロウリュ", "サウナ&スパ", "岩盤浴", "テントサウナ",
                    "外気浴", "水風呂", "サウナラウンジ", "サ活", "高温サウナ",
                    "低温サウナ", "ととのい", "整い", "発汗", "サウナカフェ"
                ]),
                'keywords': ['サウナ', 'sauna', 'ロウリュ', '岩盤浴', 'テント', '外気浴', '水風呂', '整', 'ととの', '発汗', 'サ活'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 20
            },
            'relax_cafe': {
                'base_terms': [
                    "カフェ", "コーヒーショップ", "動物カフェ", "猫カフェ",
                    "ドッグカフェ", "古民家カフェ", "隠れ家カフェ", "喫茶店",
                    "カフェラウンジ", "ブックカフェ"
                ],
                'queries': self._generate_regional_queries([
                    "カフェ", "コーヒーショップ", "動物カフェ", "猫カフェ",
                    "ドッグカフェ", "古民家カフェ", "隠れ家カフェ", "喫茶店",
                    "カフェラウンジ", "ブックカフェ"
                ]),
                'keywords': ['カフェ', 'cafe', 'coffee', 'コーヒー', '喫茶', '動物', '猫', '犬', 'ブック'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 20
            },
            'relax_walking': {
                'base_terms': [
                    "散歩コース", "ウォーキングコース", "遊歩道", "散策路",
                    "プロムナード", "歩道", "散歩道", "ハイキングコース",
                    "トレイル", "自然歩道"
                ],
                'queries': self._generate_regional_queries([
                    "散歩コース", "ウォーキングコース", "遊歩道", "散策路",
                    "プロムナード", "歩道", "散歩道", "ハイキングコース",
                    "トレイル", "自然歩道"
                ]),
                'keywords': ['散歩', 'ウォーキング', '遊歩道', '散策', 'プロムナード', 'トレイル', 'ハイキング'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 20
            }
        }

        self.total_target_count = 100  # 全体の取得目標件数（各カテゴリ20件ずつ）

    def _generate_regional_queries(self, base_terms: List[str]) -> List[str]:
        """北海道全域の検索クエリを生成"""
        queries = []

        # 基本用語に北海道を組み合わせ
        for term in base_terms:
            queries.extend([
                f"{term} 北海道",
                f"北海道 {term}",
                f"{term} 札幌",
                f"{term} 函館",
                f"{term} 旭川",
                f"{term} 釧路",
                f"{term} 帯広",
                f"{term} 小樽"
            ])

        return queries

    def _setup_mysql_connection(self):
        """MySQL接続をセットアップ"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            if connection.is_connected():
                print(f"MySQLに接続しました: {self.mysql_config['database']}")
                return connection
        except Error as e:
            print(f"MySQL接続エラー: {e}")
            return None

    def _create_tables_if_not_exists(self, connection):
        """必要なテーブルを作成"""
        cursor = connection.cursor()

        # spotsテーブル作成
        create_spots_table = """
        CREATE TABLE IF NOT EXISTS spots (
            id INT AUTO_INCREMENT PRIMARY KEY,
            place_id VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(500) NOT NULL,
            category VARCHAR(100) NOT NULL,
            address TEXT,
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            rating DECIMAL(3, 2),
            user_ratings_total INT,
            price_level INT,
            phone_number VARCHAR(50),
            website VARCHAR(1000),
            opening_hours JSON,
            photos JSON,
            types JSON,
            vicinity TEXT,
            plus_code VARCHAR(50),
            region VARCHAR(50) DEFAULT 'hokkaido',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_place_id (place_id),
            INDEX idx_category (category),
            INDEX idx_region (region)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

        cursor.execute(create_spots_table)
        connection.commit()
        cursor.close()
        print("テーブル作成完了")

    def _get_existing_place_ids(self, connection, category: str = None) -> set:
        """既存のplace_idを取得"""
        cursor = connection.cursor()
        if category:
            query = "SELECT place_id FROM spots WHERE category = %s AND region = 'hokkaido'"
            cursor.execute(query, (category,))
        else:
            query = "SELECT place_id FROM spots WHERE region = 'hokkaido'"
            cursor.execute(query)

        existing_ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return existing_ids

    def _search_places(self, query: str, category: str) -> List[Dict]:
        """Google Places APIで場所を検索"""
        all_results = []
        next_page_token = None

        for attempt in range(3):  # 最大3ページまで取得
            params = {
                'query': query,
                'key': self.google_api_key,
                'language': 'ja',
                'region': 'jp'
            }

            if next_page_token:
                params['pagetoken'] = next_page_token

            try:
                data = get_json(self.text_search_url, params, ttl_sec=60*60*24*7)

                if data.get('status') == 'OK':
                    results = data.get('results', [])
                    filtered_results = self._filter_results(results, category)
                    all_results.extend(filtered_results)

                    next_page_token = data.get('next_page_token')
                    if not next_page_token or len(all_results) >= 60:  # 十分な結果が得られた場合
                        break

                    # next_page_tokenがある場合は少し待機
                    time.sleep(2)
                else:
                    print(f"APIエラー: {data.get('status')} - {query}")
                    break

            except Exception as e:
                print(f"検索エラー ({query}): {e}")
                break

        return all_results

    def _filter_results(self, results: List[Dict], category: str) -> List[Dict]:
        """検索結果をフィルタリング"""
        if category not in self.search_categories:
            return results

        category_config = self.search_categories[category]
        keywords = category_config.get('keywords', [])
        exclude_types = category_config.get('exclude_types', [])

        filtered = []
        for place in results:
            # 除外タイプチェック
            place_types = place.get('types', [])
            if any(excluded in place_types for excluded in exclude_types):
                continue

            # キーワードチェック（名前や住所に含まれているか）
            name = place.get('name', '').lower()
            vicinity = place.get('vicinity', '').lower()

            # 北海道が含まれているかチェック
            address_text = f"{name} {vicinity}".lower()

            has_hokkaido_location = '北海道' in address_text
            has_keyword = any(keyword.lower() in address_text for keyword in keywords)

            if has_hokkaido_location and (has_keyword or not keywords):
                filtered.append(place)

        return filtered

    def _get_place_details(self, place_id: str) -> Optional[Dict]:
        """場所の詳細情報を取得"""
        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'language': 'ja',
            'fields': 'name,formatted_address,geometry,rating,user_ratings_total,price_level,formatted_phone_number,website,opening_hours,photos,types,vicinity,plus_code'
        }

        try:
            if already_fetched_place(place_id):
                return None
            data = get_json(self.place_details_url, params, ttl_sec=60*60*24*30)

            if data.get('status') == 'OK':
                result = data.get('result')
                mark_fetched_place(place_id)
                return result
            else:
                print(f"詳細取得エラー: {data.get('status')} - {place_id}")
                return None

        except Exception as e:
            print(f"詳細取得例外 ({place_id}): {e}")
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
                    'hokkaido'
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

    def collect_category_data(self, category: str, connection):
        """特定のカテゴリのデータを収集"""
        if category not in self.search_categories:
            print(f"未知のカテゴリ: {category}")
            return

        category_config = self.search_categories[category]
        target_count = category_config['target_count']

        print(f"\n=== {category} カテゴリの収集開始 ===")

        # 既存データのチェック
        existing_place_ids = self._get_existing_place_ids(connection, category)
        print(f"既存データ: {len(existing_place_ids)}件")

        all_places = []
        queries = category_config['queries']

        # クエリを実行
        for i, query in enumerate(queries):
            if len(all_places) >= target_count * 2:  # 十分な候補が集まったら停止
                break

            print(f"検索中 ({i+1}/{len(queries)}): {query}")

            results = self._search_places(query, category)

            for place in results:
                place_id = place.get('place_id')
                if place_id and place_id not in existing_place_ids:
                    # 詳細情報を取得
                    detailed_place = self._get_place_details(place_id)
                    if detailed_place:
                        all_places.append(detailed_place)
                        existing_place_ids.add(place_id)

            time.sleep(1)  # API制限対策

        # 重複除去
        unique_places = []
        seen_place_ids = set()

        for place in all_places:
            place_id = place.get('place_id')
            if place_id not in seen_place_ids:
                unique_places.append(place)
                seen_place_ids.add(place_id)

        # 最終的に目標件数に調整
        unique_places = unique_places[:target_count]

        print(f"取得件数: {len(unique_places)}件")

        # データベースに保存
        if unique_places:
            saved_count = self._save_to_database(connection, unique_places, category)
            print(f"データベース保存完了: {saved_count}件")
        else:
            print("保存するデータがありません")

    def get_stats(self, connection):
        """収集状況の統計を表示"""
        cursor = connection.cursor()

        print("\n=== 北海道リラックスカテゴリ収集状況 ===")

        # カテゴリ別統計
        for category in self.search_categories.keys():
            query = "SELECT COUNT(*) FROM spots WHERE category = %s AND region = 'hokkaido'"
            cursor.execute(query, (category,))
            count = cursor.fetchone()[0]
            target = self.search_categories[category]['target_count']
            print(f"{category}: {count}/{target}件 ({count/target*100:.1f}%)")

        # 総計
        query = "SELECT COUNT(*) FROM spots WHERE region = 'hokkaido'"
        cursor.execute(query)
        total = cursor.fetchone()[0]
        print(f"\n総計: {total}/{self.total_target_count}件")

        cursor.close()

    def run_collection(self, category: str = None):
        """データ収集を実行"""
        print("北海道リラックスカテゴリデータ収集を開始します")

        # MySQL接続
        connection = self._setup_mysql_connection()
        if not connection:
            return

        try:
            # テーブル作成
            self._create_tables_if_not_exists(connection)

            # カテゴリ指定がある場合はそのカテゴリのみ実行
            categories = [category] if category else list(self.search_categories.keys())

            for cat in categories:
                if cat not in self.search_categories:
                    print(f"無効なカテゴリ: {cat}")
                    continue

                self.collect_category_data(cat, connection)

                # 進捗確認
                self.get_stats(connection)

                # カテゴリ間の待機
                if cat != categories[-1]:
                    print("次のカテゴリまで30秒待機...")
                    time.sleep(30)

            print("\n=== 収集完了 ===")
            self.get_stats(connection)

        except Exception as e:
            print(f"実行エラー: {e}")
        finally:
            if connection.is_connected():
                connection.close()
                print("MySQL接続を閉じました")

def main():
    """メイン関数"""
    collector = HokkaidoRelaxDataCollector()

    if len(sys.argv) > 1:
        category = sys.argv[1]
        collector.run_collection(category)
    else:
        collector.run_collection()

if __name__ == "__main__":
    main()
