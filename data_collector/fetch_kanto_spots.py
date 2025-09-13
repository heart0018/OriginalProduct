#!/usr/bin/env python3
"""
多カテゴリスポット自動取得スクリプト
Google Places APIを使用して東京の温泉・公園・サウナ・カフェデータを取得し、MySQLに保存する
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

class MultiCategoryDataCollector:
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

        # 関東地方の都県リスト
        self.kanto_prefectures = ['東京', '神奈川', '千葉', '埼玉', '茨城', '栃木', '群馬']

        # 検索設定（カテゴリ別・関東全域対応）
        self.search_categories = {
            'relax_onsen': {
                'queries': self._generate_regional_queries([
                    "温泉", "銭湯", "スーパー銭湯", "天然温泉", "日帰り温泉", 
                    "温泉施設", "入浴施設", "岩盤浴"
                ]),
                'keywords': ['温泉', '銭湯', 'スパ', 'spa', 'hot spring', 'bath house', '入浴', '岩盤浴'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 100
            },
            'active_park': {
                'queries': self._generate_regional_queries([
                    "公園", "都市公園", "緑地", "運動公園", "県立公園", 
                    "自然公園", "森林公園", "総合公園", "散歩コース"
                ]),
                'keywords': ['公園', 'park', '緑地', '運動場', 'スポーツ', '広場', '散歩', '遊歩道'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 100
            },
            'active_sauna': {
                'queries': self._generate_regional_queries([
                    "サウナ", "サウナ施設", "個室サウナ", "フィンランドサウナ", 
                    "ロウリュ", "サウナ&スパ", "岩盤浴"
                ]),
                'keywords': ['サウナ', 'sauna', 'ロウリュ', '岩盤浴'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 100
            },
            'relax_cafe': {
                'queries': self._generate_regional_queries([
                    "カフェ", "コーヒーショップ", "動物カフェ", "猫カフェ", 
                    "ドッグカフェ", "古民家カフェ", "隠れ家カフェ", "喫茶店"
                ]),
                'keywords': ['カフェ', 'cafe', 'coffee', 'コーヒー', '喫茶', '動物', '猫', '犬'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 100
            }
        }

        self.total_target_count = 400  # 全体の取得目標件数（各カテゴリ100件ずつ）

    def _generate_regional_queries(self, base_terms: List[str]) -> List[str]:
        """関東全域の検索クエリを生成"""
        queries = []
        
        # 各都県 × 各基本用語の組み合わせを生成
        for prefecture in self.kanto_prefectures:
            for term in base_terms:
                queries.append(f"{term} {prefecture}")
        
        # 関東全域での一般的な検索も追加
        for term in base_terms:
            queries.extend([
                f"{term} 関東",
                f"{term} 関東地方",
                f"関東 {term}"
            ])
        
        return queries

    def validate_config(self):
        """設定の検証"""
        if not self.google_api_key:
            raise ValueError("GOOGLE_API_KEY が .env ファイルに設定されていません")
        if not self.mysql_config['password']:
            raise ValueError("MYSQL_PASSWORD が .env ファイルに設定されていません")

        print("✅ 設定の検証が完了しました")

    def search_places(self, query: str, location: str = "", radius: int = 100000) -> List[Dict]:
        """Google Places APIでテキスト検索（関東全域対応）"""
        # 位置情報がない場合は関東地方の中心付近を使用
        if not location:
            params = {
                'query': query,
                'key': self.google_api_key,
                'language': 'ja',
                'region': 'jp'
            }
        else:
            params = {
                'query': f"{query} {location}",
                'key': self.google_api_key,
                'language': 'ja',
                'region': 'jp'
            }

        try:
            print(f"🔍 検索中: {query}")
            data = get_json(self.text_search_url, params, ttl_sec=60*60*24*7)

            if data.get('status') != 'OK':
                if data.get('status') != 'ZERO_RESULTS':  # 結果なしは正常なケースとして扱う
                    print(f"⚠️  検索エラー: {data.get('status')} - {data.get('error_message', 'Unknown error')}")
                return []

            results = data.get('results', [])
            
            # 関東地方の住所を持つ結果のみフィルタリング
            kanto_results = []
            for result in results:
                address = result.get('formatted_address', '')
                if any(prefecture in address for prefecture in self.kanto_prefectures):
                    kanto_results.append(result)
            
            print(f"📍 関東地方内: {len(kanto_results)}件の候補を発見")
            return kanto_results

        except requests.RequestException as e:
            print(f"❌ API リクエストエラー: {e}")
            return []

    def get_place_details(self, place_id: str) -> Optional[Dict]:
        """場所の詳細情報を取得"""
        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'language': 'ja',
            'fields': 'name,formatted_address,rating,user_ratings_total,photos,url,types,geometry,opening_hours,reviews'
        }

        try:
            if already_fetched_place(place_id):
                return None
            data = get_json(self.place_details_url, params, ttl_sec=60*60*24*30)

            if data.get('status') != 'OK':
                print(f"⚠️  詳細取得エラー: {data.get('status')}")
                return None

            return data.get('result')

        except requests.RequestException as e:
            print(f"❌ 詳細取得エラー: {e}")
            return None

    def get_photo_url(self, photo_reference: str, max_width: int = 200) -> str:
        """キャッシュされた直リンクを返す（無ければ空文字）"""
        direct = get_photo_direct_url(photo_reference, maxwidth=max_width, ttl_sec=60*60*24*30)
        return direct or ""

    def validate_place_id(self, place_id: str) -> bool:
        """place_idの有効性をチェック"""
        if not place_id:
            return False

        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'fields': 'place_id'  # 最小限のフィールドのみ取得
        }

        try:
            response = requests.get(self.place_details_url, params=params)
            response.raise_for_status()

            data = response.json()

            if data.get('status') == 'OK':
                print(f"  ✅ place_id有効: {place_id[:20]}...")
                return True
            else:
                print(f"  ❌ place_id無効: {place_id[:20]}... (status: {data.get('status')})")
                return False

        except requests.RequestException as e:
            print(f"  ❌ place_id検証エラー: {e}")
            return False

    def is_japanese_text(self, text: str) -> bool:
        """テキストが日本語かどうかを判定"""
        if not text:
            return False

        # ひらがな、カタカナ、漢字の文字数をカウント
        japanese_chars = 0
        total_chars = len(text.replace(' ', '').replace('\n', ''))

        if total_chars == 0:
            return False

        for char in text:
            # ひらがな: U+3040-U+309F
            # カタカナ: U+30A0-U+30FF
            # 漢字: U+4E00-U+9FAF
            if ('\u3040' <= char <= '\u309F' or
                '\u30A0' <= char <= '\u30FF' or
                '\u4E00' <= char <= '\u9FAF'):
                japanese_chars += 1

        # 日本語文字が全体の30%以上なら日本語とみなす
        return (japanese_chars / total_chars) >= 0.3

    def extract_japanese_reviews(self, reviews: List[Dict], max_count: int = 10) -> List[Dict]:
        """日本語レビューを抽出・ソート"""
        if not reviews:
            return []

        japanese_reviews = []

        for review in reviews:
            text = review.get('text', '')
            if self.is_japanese_text(text):
                japanese_reviews.append({
                    'text': text,
                    'rating': review.get('rating', 0),
                    'time': review.get('time', 0),  # Unixタイムスタンプ
                    'author_name': review.get('author_name', ''),
                    'relative_time_description': review.get('relative_time_description', '')
                })

        # 新しい順にソート（timeの降順）
        japanese_reviews.sort(key=lambda x: x['time'], reverse=True)

        # 最大件数に制限
        return japanese_reviews[:max_count]

    def filter_places_by_category(self, places: List[Dict], category: str) -> List[Dict]:
        """カテゴリ別に施設をフィルタリング"""
        if category not in self.search_categories:
            return []

        category_config = self.search_categories[category]
        keywords = category_config['keywords']
        exclude_types = category_config['exclude_types']

        filtered = []

        for place in places:
            # 名前にカテゴリ関連キーワードが含まれているかチェック
            name = place.get('name', '').lower()
            types = place.get('types', [])

            # キーワードチェック
            has_keyword = any(keyword in name for keyword in keywords)

            # 除外タイプチェック
            has_exclude_type = any(exc_type in types for exc_type in exclude_types)

            if has_keyword and not has_exclude_type:
                filtered.append(place)

        return filtered

    def format_place_data(self, place: Dict, category: str, details: Optional[Dict] = None) -> Dict:
        """場所データを整形"""
        # 基本情報
        name = place.get('name', '')
        address = place.get('formatted_address', '')
        rating = place.get('rating', 0.0)
        review_count = place.get('user_ratings_total', 0)

        # 詳細情報があれば使用
        if details:
            name = details.get('name', name)
            address = details.get('formatted_address', address)
            rating = details.get('rating', rating)
            review_count = details.get('user_ratings_total', review_count)

        # 長さ制限を適用
        name = name[:128] if name else ''
        address = address[:128] if address else ''

        # 座標情報取得
        latitude = None
        longitude = None
        geometry = (details or place).get('geometry', {})
        if 'location' in geometry:
            location = geometry['location']
            latitude = location.get('lat')
            longitude = location.get('lng')
            print(f"  📍 座標: ({latitude}, {longitude})")

        # 写真URL取得
        image_url = None
        photos = (details or place).get('photos', [])
        if photos and len(photos) > 0:
            photo_ref = photos[0].get('photo_reference')
            if photo_ref:
                # 画像URL生成（データベース制限を1000文字に拡張したので、長いURLも保存可能）
                full_url = self.get_photo_url(photo_ref, max_width=200)

                # URLの長さを1000文字に制限（データベース制限に合わせて）
                if len(full_url) <= 1000:
                    image_url = full_url
                    print(f"  📸 画像URL生成成功: {len(full_url)}文字")
                else:
                    # 極端に長い場合のみスキップ
                    print(f"  ⚠️  画像URLが極端に長いためスキップ: {len(full_url)}文字")
            else:
                print(f"  ⚠️  photo_referenceが見つかりません")

        # Google Maps URL取得
        external_link = (details or {}).get('url', '')
        if not external_link and place.get('place_id'):
            external_link = f"https://maps.google.com/?place_id={place.get('place_id')}"

        # URLの長さを256文字に制限
        if len(external_link) > 256:
            # place_idベースのURLに変更
            if place.get('place_id'):
                external_link = f"https://maps.google.com/?place_id={place.get('place_id')}"
                if len(external_link) > 256:
                    external_link = external_link[:256]
            else:
                external_link = external_link[:256]

        # レビューデータ取得
        reviews = []
        if details and 'reviews' in details:
            reviews = self.extract_japanese_reviews(details['reviews'], max_count=10)
            print(f"  💬 日本語レビュー: {len(reviews)}件")

        return {
            'genre': category,  # カテゴリを動的に設定（カラム名をgenreに変更）
            'title': name,
            'rating': float(rating) if rating else 0.0,
            'review_count': int(review_count) if review_count else 0,
            'image_url': image_url,
            'external_link': external_link,
            'region': '関東',  # 東京は関東地区
            'address': address,
            'latitude': latitude,   # 座標情報を追加
            'longitude': longitude, # 座標情報を追加
            'place_id': place.get('place_id'),  # Google Places API のplace_idを追加
            'reviews': reviews  # レビューデータを追加
        }

    def connect_database(self):
        """データベース接続"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            if connection.is_connected():
                print("✅ データベース接続成功")
                return connection
        except Error as e:
            print(f"❌ データベース接続エラー: {e}")
            return None

    def save_to_database(self, places_data: List[Dict]):
        """データベースに保存"""
        connection = self.connect_database()
        if not connection:
            return False

        try:
            cursor = connection.cursor()

            # 重複チェック用クエリ（place_idベースに変更）
            check_query = "SELECT id FROM cards WHERE place_id = %s"

            # カード挿入クエリ
            insert_card_query = """
                INSERT INTO cards (genre, title, rating, review_count, image_url, external_link, region, address, latitude, longitude, place_id, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            """

            # レビュー挿入クエリ
            insert_review_query = """
                INSERT INTO review_comments (comment, card_id, created_at, updated_at)
                VALUES (%s, %s, NOW(), NOW())
            """

            inserted_count = 0
            duplicate_count = 0
            total_reviews_inserted = 0

            for place_data in places_data:
                # place_idの有効性チェック
                if not place_data.get('place_id'):
                    print(f"⚠️  place_idが見つかりません: {place_data['title']}")
                    duplicate_count += 1
                    continue

                # 重複チェック（place_idベース）
                cursor.execute(check_query, (place_data['place_id'],))
                existing = cursor.fetchone()

                if existing:
                    print(f"⚠️  スキップ（重複）: {place_data['title']} (place_id: {place_data['place_id']})")
                    duplicate_count += 1
                    continue

                # カードデータ挿入
                card_values = (
                    place_data['genre'],
                    place_data['title'],
                    place_data['rating'],
                    place_data['review_count'],
                    place_data['image_url'],
                    place_data['external_link'],
                    place_data['region'],
                    place_data['address'],
                    place_data['latitude'],
                    place_data['longitude'],
                    place_data['place_id']
                )

                cursor.execute(insert_card_query, card_values)
                card_id = cursor.lastrowid  # 挿入されたカードのIDを取得

                # レビューデータ挿入
                reviews = place_data.get('reviews', [])
                reviews_inserted = 0

                for review in reviews:
                    review_text = review['text']
                    # コメントの長さ制限（TEXTフィールドなので65535文字まで可能だが、実用的な長さに制限）
                    if len(review_text) > 1000:
                        review_text = review_text[:997] + "..."

                    cursor.execute(insert_review_query, (review_text, card_id))
                    reviews_inserted += 1

                inserted_count += 1
                total_reviews_inserted += reviews_inserted
                print(f"✅ 保存完了: {place_data['title']} (レビュー{reviews_inserted}件)")

            connection.commit()
            print(f"\n📊 保存結果: {inserted_count}件挿入, {duplicate_count}件重複スキップ")
            print(f"💬 レビュー保存結果: {total_reviews_inserted}件挿入")

            return True

        except Error as e:
            print(f"❌ データベースエラー: {e}")
            connection.rollback()
            return False

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("✅ データベース接続終了")

    def collect_data(self):
        """メインのデータ収集処理（関東全域・全カテゴリ対応）"""
        print("🚀 関東全域多カテゴリデータ収集開始")
        print(f"🎯 目標取得件数: 合計{self.total_target_count}件")
        print("📋 対象カテゴリ:")
        for category, config in self.search_categories.items():
            print(f"  • {category}: {config['target_count']}件")
        print(f"🌏 対象地域: {', '.join(self.kanto_prefectures)}")

        # 設定検証
        self.validate_config()

        all_formatted_data = []

        # 各カテゴリでデータ収集
        for category, category_config in self.search_categories.items():
            print(f"\n🔍 【{category}】カテゴリ データ収集開始 (目標: {category_config['target_count']}件)")

            unique_places = {}  # place_idで重複除去
            processed_queries = 0

            # 各検索クエリで検索実行
            for query in category_config['queries']:
                if len(unique_places) >= category_config['target_count']:
                    print(f"🎯 {category} 目標件数に達しました！")
                    break

                places = self.search_places(query)
                processed_queries += 1

                # カテゴリ別にフィルタリング
                filtered_places = self.filter_places_by_category(places, category)

                # 重複除去（place_idベース）
                new_additions = 0
                for place in filtered_places:
                    place_id = place.get('place_id')
                    if place_id and place_id not in unique_places:
                        unique_places[place_id] = place
                        new_additions += 1

                print(f"💡 {category} クエリ{processed_queries}: +{new_additions}件 (累計: {len(unique_places)}件)")

                # API呼び出し制限対策
                time.sleep(1.5)  # より長めの待機時間

                # 進捗表示（10クエリごと）
                if processed_queries % 10 == 0:
                    print(f"📊 {category} 進捗: {processed_queries}クエリ処理済み, {len(unique_places)}件収集済み")

            # 上位N件を選択
            selected_places = list(unique_places.values())[:category_config['target_count']]
            print(f"\n📋 {category} 最終選択された施設: {len(selected_places)}件")

            # 詳細情報取得とデータ整形
            category_data = []
            for i, place in enumerate(selected_places, 1):
                print(f"\n🔍 {category} 詳細取得中 ({i}/{len(selected_places)}): {place.get('name')}")

                # place_idの有効性をチェック
                place_id = place.get('place_id')
                if not place_id:
                    print(f"  ⚠️  place_idが見つかりません: {place.get('name')}")
                    continue

                if not self.validate_place_id(place_id):
                    print(f"  ⚠️  無効なplace_idのためスキップ: {place.get('name')}")
                    continue

                # 詳細情報取得
                details = None
                if place_id:
                    details = self.get_place_details(place_id)
                    time.sleep(0.8)  # API制限対策（少し長めに）

                # データ整形（カテゴリを渡す）
                formatted_place = self.format_place_data(place, category, details)
                category_data.append(formatted_place)

                print(f"  📍 {formatted_place['title']} ({formatted_place['address'][:30]}...)")
                print(f"  ⭐ 評価: {formatted_place['rating']} ({formatted_place['review_count']}件)")

                # 進捗表示（20件ごと）
                if i % 20 == 0:
                    print(f"📊 {category} 詳細取得進捗: {i}/{len(selected_places)}件完了")

            # カテゴリデータを全体に追加
            all_formatted_data.extend(category_data)
            print(f"✅ {category} カテゴリ完了: {len(category_data)}件追加")

        # データベースに保存
        print(f"\n💾 データベース保存開始... (合計{len(all_formatted_data)}件)")
        success = self.save_to_database(all_formatted_data)

        if success:
            print(f"\n🎉 関東全域データ収集完了！")
            print(f"📊 総取得件数: {len(all_formatted_data)}件")

            # カテゴリ別集計
            category_counts = {}
            prefecture_counts = {}
            for item in all_formatted_data:
                cat = item['genre']  # typeからgenreに変更
                category_counts[cat] = category_counts.get(cat, 0) + 1
                
                # 都県別集計
                address = item.get('address', '')
                for prefecture in self.kanto_prefectures:
                    if prefecture in address:
                        prefecture_counts[prefecture] = prefecture_counts.get(prefecture, 0) + 1
                        break

            print("📈 カテゴリ別集計:")
            for cat, count in category_counts.items():
                print(f"  • {cat}: {count}件")
                
            print("🌏 都県別集計:")
            for prefecture, count in sorted(prefecture_counts.items()):
                print(f"  • {prefecture}: {count}件")
        else:
            print(f"\n❌ データ保存に失敗しました")

        return success

def main():
    """メイン関数"""
    try:
        collector = MultiCategoryDataCollector()
        collector.collect_data()

    except KeyboardInterrupt:
        print("\n⚠️  処理が中断されました")
    except Exception as e:
        print(f"\n❌ 予期しないエラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
