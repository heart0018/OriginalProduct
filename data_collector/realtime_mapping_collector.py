#!/usr/bin/env python3
"""
取得時マッピングシステム - 新規データ収集用
Google Places APIからデータを取得し、即座に正確な地域マッピングを行ってDB保存
エンターテイメントカテゴリ（ゲームセンター・カラオケ）での検証から開始
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
from utils.request_guard import (
    get_json,
    already_fetched_place,
    mark_fetched_place,
    get_photo_direct_url,
)

# .envファイルを読み込み
load_dotenv()

class RealtimeMappingCollector:
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

        # 新規カテゴリ設定（エンターテイメント系から開始）
        self.search_categories = {
            'entertainment_arcade': {
                'genre': 'entertainment_arcade',
                'base_terms': [
                    "ゲームセンター", "アミューズメント", "アーケード", "ゲーム施設",
                    "UFOキャッチャー", "プリクラ", "音ゲー", "太鼓の達人",
                    "ガンダム", "beatmania", "DDR", "アミューズメントパーク"
                ],
                'keywords': ['ゲーム', 'アミューズメント', 'アーケード', 'プリクラ', 'UFO', '音ゲー'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 100
            },
            'entertainment_karaoke': {
                'genre': 'entertainment_karaoke',
                'base_terms': [
                    "カラオケ", "カラオケボックス", "カラオケ館", "ビッグエコー",
                    "カラオケの鉄人", "まねきねこ", "カラオケ喫茶", "個室カラオケ",
                    "パーティールーム", "歌ルーム", "カラオケバー"
                ],
                'keywords': ['カラオケ', 'karaoke', 'カラ', '歌', 'ボックス', '個室'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 100
            }
        }

    def extract_prefecture_from_address(self, address: str) -> Optional[str]:
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

    def get_region_from_address(self, address: str) -> str:
        """住所から地域を判定（取得時マッピング）"""
        prefecture = self.extract_prefecture_from_address(address)
        if prefecture:
            region = self.prefecture_to_region.get(prefecture)
            if region:
                print(f"    🎯 取得時マッピング: {prefecture} → {region}")
                return region

        print(f"    ⚠️ 地域判定不可: {address[:50]}...")
        return 'その他'

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

    def _get_existing_place_ids(self, connection, genre: str = None) -> set:
        """既存のgoogle_place_idを取得"""
        cursor = connection.cursor()
        if genre:
            query = "SELECT google_place_id FROM cards WHERE genre = %s AND google_place_id IS NOT NULL"
            cursor.execute(query, (genre,))
        else:
            query = "SELECT google_place_id FROM cards WHERE google_place_id IS NOT NULL"
            cursor.execute(query)

        existing_ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
        return existing_ids

    def _search_places(self, query: str, category: str) -> List[Dict]:
        """Google Places APIで場所を検索"""
        all_results = []
        next_page_token = None

        for attempt in range(2):  # 最大2ページまで取得
            params = {
                'query': query,
                'key': self.google_api_key,
                'language': 'ja',
                'region': 'jp'
            }

            if next_page_token:
                params['pagetoken'] = next_page_token

            try:
                # 7日キャッシュ
                data = get_json(self.text_search_url, params, ttl_sec=60*60*24*7)

                if data.get('status') == 'OK':
                    results = data.get('results', [])
                    filtered_results = self._filter_results(results, category)
                    all_results.extend(filtered_results)

                    next_page_token = data.get('next_page_token')
                    if not next_page_token or len(all_results) >= 40:
                        break

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

            # キーワードチェック
            name = place.get('name', '').lower()
            vicinity = place.get('vicinity', '').lower()
            address_text = f"{name} {vicinity}".lower()

            has_keyword = any(keyword.lower() in address_text for keyword in keywords)

            if has_keyword or not keywords:
                filtered.append(place)

        return filtered

    def _get_place_details(self, place_id: str) -> Optional[Dict]:
        """場所の詳細情報を取得"""
        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'language': 'ja',
            'fields': 'name,formatted_address,geometry,rating,user_ratings_total,price_level,formatted_phone_number,website,opening_hours,photos,types,vicinity,plus_code,reviews'
        }

        try:
            # 直近取得済みならスキップ
            if already_fetched_place(place_id):
                return None
            data = get_json(self.place_details_url, params, ttl_sec=60*60*24*30)

            if data.get('status') == 'OK':
                result = data.get('result') if isinstance(data, dict) else None
                if result:
                    mark_fetched_place(place_id)
                return result
            else:
                print(f"詳細取得エラー: {data.get('status')} - {place_id}")
                return None

        except Exception as e:
            print(f"詳細取得例外 ({place_id}): {e}")
            return None

    def _get_permanent_image_url(self, photo_reference: str) -> Optional[str]:
        """画像の永続URLを取得"""
        if not photo_reference:
            return None

        try:
            # 30日キャッシュの直接URL
            return get_photo_direct_url(photo_reference, maxwidth=800, ttl_sec=60*60*24*30)
        except Exception as e:
            print(f"    画像URL取得エラー: {e}")
            return None

    def _save_to_database(self, connection, places: List[Dict], category: str):
        """データベースに保存（取得時マッピング適用）"""
        if not places:
            print("保存するデータがありません")
            return 0

        cursor = connection.cursor()
        category_config = self.search_categories[category]
        genre = category_config['genre']

        insert_card_query = """
        INSERT INTO cards (
            title, description, genre, image_url, latitude, longitude,
            address, phone, website, rating, region, google_place_id,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            NOW(), NOW()
        )
        """

        insert_comment_query = """
        INSERT INTO review_comments (
            card_id, comment, created_at, updated_at
        ) VALUES (
            %s, %s, NOW(), NOW()
        )
        """

        saved_count = 0
        for i, place in enumerate(places):
            try:
                name = place.get('name')
                place_id = place.get('place_id')
                address = place.get('formatted_address', '')

                if not name or not place_id:
                    continue

                print(f"  保存中 ({i+1}/{len(places)}): {name}")

                # 🎯 取得時マッピング実行
                region = self.get_region_from_address(address)

                # 座標の取得
                location = place.get('geometry', {}).get('location', {})
                latitude = location.get('lat')
                longitude = location.get('lng')

                # 永続画像URL取得
                image_url = None
                photos = place.get('photos', [])
                if photos:
                    photo_reference = photos[0].get('photo_reference')
                    if photo_reference:
                        image_url = self._get_permanent_image_url(photo_reference)
                        print(f"    📸 永続画像URL取得: {'成功' if image_url else '失敗'}")

                # カード保存
                card_data = (
                    name,
                    place.get('vicinity', ''),  # description
                    genre,
                    image_url,
                    latitude,
                    longitude,
                    address,
                    place.get('formatted_phone_number'),
                    place.get('website'),
                    place.get('rating'),
                    region,  # 🎯 取得時マッピング結果を使用
                    place_id
                )

                cursor.execute(insert_card_query, card_data)
                card_id = cursor.lastrowid

                # レビューコメント保存
                reviews = place.get('reviews', [])
                review_count = 0
                for review in reviews[:5]:  # 最大5件
                    comment = review.get('text')
                    if comment and len(comment.strip()) > 10:
                        cursor.execute(insert_comment_query, (card_id, comment))
                        review_count += 1

                print(f"    ✅ 保存成功 (地域: {region}, レビュー: {review_count}件)")
                saved_count += 1

            except Exception as e:
                print(f"    ❌ 保存エラー: {e} - {place.get('name', 'Unknown')}")
                continue

        connection.commit()
        cursor.close()
        print(f"カテゴリ {genre}: {saved_count}件保存完了")
        return saved_count

    def collect_category_data(self, category: str, connection, target_count: int = None):
        """特定のカテゴリのデータを収集（取得時マッピング）"""
        if category not in self.search_categories:
            print(f"未知のカテゴリ: {category}")
            return

        category_config = self.search_categories[category]
        target_count = target_count or category_config['target_count']
        genre = category_config['genre']

        print(f"\n=== {genre} カテゴリの収集開始（取得時マッピング） ===")

        # 既存データのチェック
        existing_place_ids = self._get_existing_place_ids(connection, genre)
        print(f"既存データ: {len(existing_place_ids)}件")

        all_places = []
        base_terms = category_config['base_terms']

        # 全国規模での検索
        for i, term in enumerate(base_terms):
            if len(all_places) >= target_count * 2:
                break

            queries = [
                f"{term} 日本",
                f"{term} 関東",
                f"{term} 関西",
                f"{term} 東京",
                f"{term} 大阪",
                f"{term} 名古屋"
            ]

            for query in queries:
                print(f"検索中 ({i+1}/{len(base_terms)}): {query}")

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

                if len(all_places) >= target_count * 1.5:
                    break

        # 重複除去
        unique_places = []
        seen_place_ids = set()

        for place in all_places:
            place_id = place.get('place_id')
            if place_id not in seen_place_ids:
                unique_places.append(place)
                seen_place_ids.add(place_id)

        # 目標件数に調整
        unique_places = unique_places[:target_count]

        print(f"取得件数: {len(unique_places)}件")

        # 🎯 取得時マッピングでデータベースに保存
        if unique_places:
            saved_count = self._save_to_database(connection, unique_places, category)
            print(f"データベース保存完了: {saved_count}件")
        else:
            print("保存するデータがありません")

    def get_stats(self, connection):
        """収集状況の統計を表示"""
        cursor = connection.cursor()

        print("\n=== 取得時マッピングシステム収集状況 ===")

        # カテゴリ別統計
        for category_config in self.search_categories.values():
            genre = category_config['genre']
            query = "SELECT COUNT(*) FROM cards WHERE genre = %s"
            cursor.execute(query, (genre,))
            count = cursor.fetchone()[0]
            target = category_config['target_count']
            print(f"{genre}: {count}/{target}件 ({count/target*100:.1f}%)")

        # 地域別統計
        print("\n地域別統計:")
        query = """
        SELECT region, COUNT(*)
        FROM cards
        WHERE genre IN ('entertainment_arcade', 'entertainment_karaoke')
        GROUP BY region
        ORDER BY region
        """
        cursor.execute(query)
        regions = cursor.fetchall()

        for region, count in regions:
            print(f"{region}: {count}件")

        cursor.close()

    def run_collection(self, category: str = None, target_count: int = None):
        """データ収集を実行（取得時マッピング）"""
        print("🎯 取得時マッピングシステム - データ収集開始")

        # MySQL接続
        connection = self._setup_mysql_connection()
        if not connection:
            return

        try:
            # カテゴリ指定がある場合はそのカテゴリのみ実行
            categories = [category] if category else list(self.search_categories.keys())

            for cat in categories:
                if cat not in self.search_categories:
                    print(f"無効なカテゴリ: {cat}")
                    continue

                self.collect_category_data(cat, connection, target_count)

                # 進捗確認
                self.get_stats(connection)

                # カテゴリ間の待機
                if cat != categories[-1]:
                    print("次のカテゴリまで30秒待機...")
                    time.sleep(30)

            print("\n=== 取得時マッピング収集完了 ===")
            self.get_stats(connection)

        except Exception as e:
            print(f"実行エラー: {e}")
        finally:
            if connection.is_connected():
                connection.close()
                print("MySQL接続を閉じました")

def main():
    """メイン関数"""
    collector = RealtimeMappingCollector()

    if len(sys.argv) > 1:
        category = sys.argv[1]
        target_count = int(sys.argv[2]) if len(sys.argv) > 2 else None
        collector.run_collection(category, target_count)
    else:
        collector.run_collection()

if __name__ == "__main__":
    main()
