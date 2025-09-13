#!/usr/bin/env python3
"""
メガリラックス収集システム
2800件のデータを効率的に収集 (レビューコメント・永続画像URL付き)
"""

import os
import sys
import json
import time
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime
from utils.request_guard import (
    get_json,
    already_fetched_place,
    mark_fetched_place,
    get_photo_direct_url,
)

# 環境変数読み込み
load_dotenv()

class MegaRelaxCollector:
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_PLACES_API_KEY')
        if not self.api_key:
            raise ValueError("GOOGLE_PLACES_API_KEY not found in environment variables")

        self.db_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }

        # 地域設定
        self.regions = {
            'hokkaido': {
                'name': '北海道',
                'query_area': '北海道 札幌 函館 旭川'
            },
            'tohoku': {
                'name': '東北',
                'query_area': '仙台 青森 盛岡 秋田 山形 福島'
            },
            'kanto': {
                'name': '関東',
                'query_area': '東京 横浜 千葉 埼玉 茨城 栃木 群馬'
            },
            'chubu': {
                'name': '中部',
                'query_area': '名古屋 金沢 富山 福井 山梨 長野 岐阜 静岡 新潟'
            },
            'kansai': {
                'name': '関西',
                'query_area': '大阪 京都 神戸 奈良 和歌山 滋賀'
            },
            'chugoku_shikoku': {
                'name': '中国四国',
                'query_area': '広島 岡山 山口 島根 鳥取 高松 松山 高知 徳島'
            },
            'kyushu_okinawa': {
                'name': '九州沖縄',
                'query_area': '福岡 北九州 熊本 鹿児島 宮崎 大分 佐賀 長崎 那覇'
            }
        }

        # カテゴリー設定
        self.categories = {
            'parks': {
                'name': '公園',
                'queries': [
                    '公園',
                    'パーク',
                    '都市公園',
                    '国営公園',
                    '県立公園',
                    '市民公園',
                    '自然公園',
                    '森林公園',
                    '植物園',
                    '動物園'
                ]
            },
            'sauna': {
                'name': 'サウナ',
                'queries': [
                    'サウナ',
                    'スパ',
                    '岩盤浴',
                    'ウェルネス',
                    'リラクゼーション施設',
                    'スーパー銭湯',
                    '健康ランド',
                    'デイスパ',
                    'フィンランドサウナ',
                    'ロウリュ'
                ]
            },
            'cafe': {
                'name': 'カフェ',
                'queries': [
                    'カフェ',
                    'コーヒーショップ',
                    '喫茶店',
                    'カフェレストラン',
                    'ブックカフェ',
                    'アートカフェ',
                    'オーガニックカフェ',
                    'スペシャルティコーヒー',
                    'テラスカフェ',
                    '隠れ家カフェ'
                ]
            },
            'walking_courses': {
                'name': '散歩コース',
                'queries': [
                    'ウォーキングコース',
                    '散歩道',
                    '遊歩道',
                    'プロムナード',
                    '散策路',
                    'ハイキングコース',
                    '自然歩道',
                    '川沿い散歩道',
                    '公園散歩道',
                    '街歩きコース'
                ]
            }
        }

    def get_permanent_image_url(self, photo_reference):
        """photo_referenceから永続画像URLを取得"""
        if not photo_reference:
            return None

        try:
            return get_photo_direct_url(photo_reference, maxwidth=400, ttl_sec=60*60*24*30)

        except Exception as e:
            print(f"⚠️ 画像URL取得エラー: {e}")
            return None

    def get_place_reviews(self, place_id):
        """レビューコメントを取得"""
        try:
            url = f"https://maps.googleapis.com/maps/api/place/details/json"
            params = {
                'place_id': place_id,
                'fields': 'reviews',
                'key': self.api_key,
                'language': 'ja'
            }

            data = get_json(url, params, ttl_sec=60*60*24*30)

            if data.get('status') == 'OK' and 'result' in data:
                reviews = data['result'].get('reviews', [])
                review_comments = []

                for review in reviews:
                    comment = review.get('text', '').strip()
                    if comment and len(comment) >= 10:  # 最低10文字以上
                        review_data = {
                            'text': comment,
                            'rating': review.get('rating', 0),
                            'author': review.get('author_name', ''),
                            'time': review.get('time', 0)
                        }
                        review_comments.append(review_data)

                return review_comments[:5]  # 最大5件

            return []

        except Exception as e:
            print(f"⚠️ レビュー取得エラー: {e}")
            return []

    def search_places(self, region_key, category_key, target_count=100):
        """指定地域・カテゴリーでスポットを検索"""
        region = self.regions[region_key]
        category = self.categories[category_key]

        print(f"\n🔍 {region['name']} - {category['name']} 収集開始 (目標: {target_count}件)")

        collected_places = []
        used_place_ids = set()

        for query_term in category['queries']:
            if len(collected_places) >= target_count:
                break

            print(f"  📍 検索: {query_term} in {region['query_area']}")

            # テキスト検索実行
            search_query = f"{query_term} {region['query_area']}"
            url = "https://maps.googleapis.com/maps/api/place/textsearch/json"

            params = {
                'query': search_query,
                'key': self.api_key,
                'language': 'ja',
                'region': 'jp'
            }

            try:
                data = get_json(url, params, ttl_sec=60*60*24*7)

                if data.get('status') != 'OK':
                    print(f"    ⚠️ API Error: {data.get('status', 'Unknown')}")
                    continue

                results = data.get('results', [])
                print(f"    ✅ {len(results)}件の候補発見")

                for result in results:
                    if len(collected_places) >= target_count:
                        break

                    place_id = result.get('place_id')
                    if place_id in used_place_ids:
                        continue

                    used_place_ids.add(place_id)

                    # 詳細情報取得
                    place_details = self.get_place_details(place_id)
                    if place_details:

                        # カテゴリー情報追加
                        place_details['region'] = region_key
                        place_details['category'] = f"relax_{category_key}"

                        collected_places.append(place_details)
                        print(f"    ✅ {place_details['name']} (レビュー: {len(place_details.get('reviews', []))}件)")

                # API制限対策
                time.sleep(0.1)

            except Exception as e:
                print(f"    ❌ 検索エラー: {e}")
                continue

        print(f"🎯 {region['name']} - {category['name']}: {len(collected_places)}件収集完了")
        return collected_places

    def get_place_details(self, place_id):
        """詳細情報とレビューを取得（キャッシュ・重複抑止付き）"""
        try:
            if already_fetched_place(place_id):
                return None

            url = f"https://maps.googleapis.com/maps/api/place/details/json"
            params = {
                'place_id': place_id,
                'fields': 'name,formatted_address,geometry,photos,rating,types,website,formatted_phone_number,opening_hours,reviews',
                'key': self.api_key,
                'language': 'ja'
            }

            data = get_json(url, params, ttl_sec=60*60*24*30)

            if data.get('status') == 'OK' and 'result' in data:
                result = data['result']

                # 画像URL取得
                image_urls = []
                photos = result.get('photos', [])
                for photo in photos[:3]:  # 最大3枚
                    photo_ref = photo.get('photo_reference')
                    if photo_ref:
                        permanent_url = self.get_permanent_image_url(photo_ref)
                        if permanent_url:
                            image_urls.append(permanent_url)

                # フォールバック画像
                if not image_urls:
                    image_urls.append("https://images.unsplash.com/photo-1566073771259-6a8506099945?ixlib=rb-4.0.3&auto=format&fit=crop&w=800&q=80")

                place_data = {
                    'place_id': place_id,
                    'name': result.get('name', ''),
                    'address': result.get('formatted_address', ''),
                    'latitude': result.get('geometry', {}).get('location', {}).get('lat'),
                    'longitude': result.get('geometry', {}).get('location', {}).get('lng'),
                    'rating': result.get('rating', 0),
                    'image_urls': json.dumps(image_urls),
                    'website': result.get('website', ''),
                    'phone': result.get('formatted_phone_number', ''),
                    'types': json.dumps(result.get('types', [])),
                    'reviews': result.get('reviews', [])
                }

                mark_fetched_place(place_id)
                return place_data

            return None

        except Exception as e:
            print(f"⚠️ 詳細取得エラー: {e}")
            return None

    def save_to_database(self, places):
        """データベースに保存"""
        if not places:
            return

        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            insert_query = """
            INSERT INTO spots (
                place_id, name, address, latitude, longitude,
                rating, image_urls, category, region,
                website, phone, types, review_comments, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                address = VALUES(address),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                rating = VALUES(rating),
                image_urls = VALUES(image_urls),
                website = VALUES(website),
                phone = VALUES(phone),
                types = VALUES(types),
                review_comments = VALUES(review_comments)
            """

            saved_count = 0
            for place in places:
                try:
                    # レビューコメントをJSON形式で保存
                    reviews_json = json.dumps(place.get('reviews', []), ensure_ascii=False)

                    values = (
                        place['place_id'],
                        place['name'],
                        place['address'],
                        place['latitude'],
                        place['longitude'],
                        place['rating'],
                        place['image_urls'],
                        place['category'],
                        place['region'],
                        place['website'],
                        place['phone'],
                        place['types'],
                        reviews_json,
                        datetime.now()
                    )

                    cursor.execute(insert_query, values)
                    saved_count += 1

                except Exception as e:
                    print(f"⚠️ 保存エラー: {place.get('name', 'Unknown')} - {e}")
                    continue

            connection.commit()
            print(f"💾 データベース保存完了: {saved_count}件")

            cursor.close()
            connection.close()

        except Exception as e:
            print(f"❌ データベースエラー: {e}")

    def collect_region_category(self, region_key, category_key, target_count=100):
        """特定地域・カテゴリーの収集実行"""
        places = self.search_places(region_key, category_key, target_count)
        self.save_to_database(places)
        return len(places)

    def collect_all(self):
        """全地域・全カテゴリーの完全収集"""
        print("🚀 メガリラックス収集開始！")
        print("目標: 2800件 (4カテゴリー × 100件 × 7地域)")
        print("=" * 60)

        total_collected = 0

        for region_key in self.regions.keys():
            for category_key in self.categories.keys():
                print(f"\n📍 {self.regions[region_key]['name']} - {self.categories[category_key]['name']}")

                try:
                    count = self.collect_region_category(region_key, category_key, 100)
                    total_collected += count

                    print(f"✅ 完了: {count}件")
                    print(f"📊 累計: {total_collected}件 / 2800件")

                    # 進捗表示
                    progress = (total_collected / 2800) * 100
                    print(f"📈 進捗: {progress:.1f}%")

                    # API制限対策
                    time.sleep(1)

                except Exception as e:
                    print(f"❌ エラー: {e}")
                    continue

        print("\n" + "=" * 60)
        print(f"🎉 メガリラックス収集完了！")
        print(f"📊 総収集数: {total_collected}件")
        print(f"🎯 達成率: {(total_collected / 2800) * 100:.1f}%")

    def get_current_stats(self):
        """現在の収集状況を確認"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            print("📊 現在の収集状況")
            print("=" * 50)

            # 地域別・カテゴリー別集計
            cursor.execute("""
                SELECT region, category, COUNT(*)
                FROM spots
                GROUP BY region, category
                ORDER BY region, category
            """)

            results = cursor.fetchall()
            total = 0

            for region, category, count in results:
                print(f"{region} - {category}: {count}件")
                total += count

            print(f"\n総計: {total}件 / 2800件")
            print(f"達成率: {(total / 2800) * 100:.1f}%")

            cursor.close()
            connection.close()

        except Exception as e:
            print(f"❌ 統計取得エラー: {e}")

def main():
    if len(sys.argv) < 2:
        print("使用方法:")
        print("  python3 mega_relax_collector.py all                    # 全収集")
        print("  python3 mega_relax_collector.py <region> <category>    # 個別収集")
        print("  python3 mega_relax_collector.py stats                  # 統計表示")
        print("")
        print("地域: hokkaido, tohoku, kanto, chubu, kansai, chugoku_shikoku, kyushu_okinawa")
        print("カテゴリー: parks, sauna, cafe, walking_courses")
        return

    collector = MegaRelaxCollector()

    if sys.argv[1] == 'all':
        collector.collect_all()
    elif sys.argv[1] == 'stats':
        collector.get_current_stats()
    elif len(sys.argv) == 3:
        region = sys.argv[1]
        category = sys.argv[2]
        collector.collect_region_category(region, category)
    else:
        print("❌ 引数が不正です")

if __name__ == "__main__":
    main()
