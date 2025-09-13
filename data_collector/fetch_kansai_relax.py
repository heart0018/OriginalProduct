#!/usr/bin/env python3
"""
関西地方リラックスカテゴリスポット自動取得スクリプト
Google Places APIを使用して関西地方の温泉・公園・サウナ・カフェ・散歩コースデータを取得し、MySQLに保存する
各ジャンル20件ずつ、都道府県で均等配分
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

class KansaiRelaxDataCollector:
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

        # 関西地方の都府県リスト
        self.kansai_prefectures = ['大阪', '兵庫', '京都', '奈良', '滋賀', '和歌山']

        # 検索設定（リラックスカテゴリ・関西全域対応）
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
                    "公園", "都市公園", "緑地", "運動公園", "府立公園", "県立公園",
                    "自然公園", "森林公園", "総合公園", "国営公園"
                ],
                'queries': self._generate_regional_queries([
                    "公園", "都市公園", "緑地", "運動公園", "府立公園", "県立公園",
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
        """関西全域の検索クエリを生成"""
        queries = []

        # 各都府県 × 各基本用語の組み合わせを生成
        for prefecture in self.kansai_prefectures:
            for term in base_terms:
                queries.append(f"{term} {prefecture}")

        # 関西全域での一般的な検索も追加
        for term in base_terms:
            queries.extend([
                f"{term} 関西",
                f"{term} 関西地方",
                f"関西 {term}",
                f"{term} 近畿",
                f"近畿 {term}"
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
            region VARCHAR(50) DEFAULT 'kansai',
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
            query = "SELECT place_id FROM spots WHERE category = %s AND region = 'kansai'"
            cursor.execute(query, (category,))
        else:
            query = "SELECT place_id FROM spots WHERE region = 'kansai'"
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

            # 関西地方の都府県が含まれているかチェック
            address_text = f"{name} {vicinity}".lower()
            kansai_prefectures_check = ['大阪', '兵庫', '京都', '奈良', '滋賀', '和歌山']

            has_kansai_location = any(pref in address_text for pref in kansai_prefectures_check)
            has_keyword = any(keyword.lower() in address_text for keyword in keywords)

            if has_kansai_location and (has_keyword or not keywords):
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
        for place in places:
            try:
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
                    place.get('place_id'),
                    place.get('name'),
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
                    'kansai'
                )

                cursor.execute(insert_query, data)
                saved_count += 1

            except Exception as e:
                print(f"保存エラー: {e} - {place.get('name', 'Unknown')}")
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

        # 各都府県での均等配分を目指す
        prefecture_targets = {pref: target_count // len(self.kansai_prefectures) for pref in self.kansai_prefectures}
        remaining = target_count % len(self.kansai_prefectures)

        # 余りを最初の都府県に配分
        for i, pref in enumerate(self.kansai_prefectures):
            if i < remaining:
                prefecture_targets[pref] += 1

        print(f"都府県別目標件数: {prefecture_targets}")

        prefecture_counts = {pref: 0 for pref in self.kansai_prefectures}

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

        # 重複除去と都府県別配分
        unique_places = []
        seen_place_ids = set()

        # 各都府県から均等に選択
        for pref in self.kansai_prefectures:
            pref_places = []
            for place in all_places:
                address = place.get('formatted_address', '')
                if pref in address and place.get('place_id') not in seen_place_ids:
                    pref_places.append(place)
                    seen_place_ids.add(place.get('place_id'))

            # この都府県から目標件数まで選択
            selected = pref_places[:prefecture_targets[pref]]
            unique_places.extend(selected)
            prefecture_counts[pref] = len(selected)

        # 目標に達していない場合は追加
        if len(unique_places) < target_count:
            additional_needed = target_count - len(unique_places)
            additional_places = []

            for place in all_places:
                if len(additional_places) >= additional_needed:
                    break
                if place.get('place_id') not in seen_place_ids:
                    additional_places.append(place)
                    seen_place_ids.add(place.get('place_id'))

            unique_places.extend(additional_places)

        # 最終的に目標件数に調整
        unique_places = unique_places[:target_count]

        print(f"最終的な都府県別件数: {prefecture_counts}")
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

        print("\n=== 関西地方リラックスカテゴリ収集状況 ===")

        # カテゴリ別統計
        for category in self.search_categories.keys():
            query = "SELECT COUNT(*) FROM spots WHERE category = %s AND region = 'kansai'"
            cursor.execute(query, (category,))
            count = cursor.fetchone()[0]
            target = self.search_categories[category]['target_count']
            print(f"{category}: {count}/{target}件 ({count/target*100:.1f}%)")

        # 都府県別統計
        print("\n都府県別統計:")
        for pref in self.kansai_prefectures:
            query = "SELECT COUNT(*) FROM spots WHERE address LIKE %s AND region = 'kansai'"
            cursor.execute(query, (f'%{pref}%',))
            count = cursor.fetchone()[0]
            print(f"{pref}: {count}件")

        # 総計
        query = "SELECT COUNT(*) FROM spots WHERE region = 'kansai'"
        cursor.execute(query)
        total = cursor.fetchone()[0]
        print(f"\n総計: {total}/{self.total_target_count}件")

        cursor.close()

    def run_collection(self, category: str = None):
        """データ収集を実行"""
        print("関西地方リラックスカテゴリデータ収集を開始します")

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
    collector = KansaiRelaxDataCollector()

    if len(sys.argv) > 1:
        category = sys.argv[1]
        collector.run_collection(category)
    else:
        collector.run_collection()

if __name__ == "__main__":
    main()
