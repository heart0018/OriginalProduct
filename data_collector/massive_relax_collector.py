#!/usr/bin/env python3
"""
大規模リラックスカテゴリー収集システム
4カテゴリー × 100データ × 7地域 = 2800件収集
永続画像URL対応 + API制限管理
"""

import os
import json
import requests
import time
import mysql.connector
from dotenv import load_dotenv
from datetime import datetime
from utils.request_guard import (
    get_json,
    get_photo_direct_url,
)

load_dotenv()

class MassiveRelaxCollector:
    """大規模リラックスカテゴリー収集クラス"""

    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }

        # 収集対象設定
        self.categories = {
            'parks': {
                'japanese': '公園',
                'search_terms': ['公園', 'park', '緑地', '広場', '遊園地']
            },
            'sauna': {
                'japanese': 'サウナ',
                'search_terms': ['サウナ', 'sauna', 'スパ', '岩盤浴']
            },
            'cafe': {
                'japanese': 'カフェ',
                'search_terms': ['カフェ', 'cafe', 'coffee', 'コーヒー']
            },
            'walking_courses': {
                'japanese': '散歩コース',
                'search_terms': ['散歩道', 'ウォーキングコース', '遊歩道', 'プロムナード', '散策路']
            }
        }

        self.regions = {
            'hokkaido': {
                'name': '北海道',
                'cities': ['札幌', '函館', '旭川', '帯広', '釧路', '北見', '小樽']
            },
            'tohoku': {
                'name': '東北',
                'cities': ['仙台', '青森', '盛岡', '秋田', '山形', '福島', '郡山']
            },
            'kanto': {
                'name': '関東',
                'cities': ['東京', '横浜', '埼玉', '千葉', '茨城', '栃木', '群馬']
            },
            'chubu': {
                'name': '中部',
                'cities': ['名古屋', '静岡', '新潟', '富山', '金沢', '福井', '山梨', '長野', '岐阜']
            },
            'kansai': {
                'name': '関西',
                'cities': ['大阪', '京都', '神戸', '奈良', '和歌山', '滋賀']
            },
            'chugoku_shikoku': {
                'name': '中国・四国',
                'cities': ['広島', '岡山', '山口', '鳥取', '島根', '高松', '松山', '高知', '徳島']
            },
            'kyushu_okinawa': {
                'name': '九州・沖縄',
                'cities': ['福岡', '北九州', '熊本', '鹿児島', '長崎', '大分', '宮崎', '佐賀', '那覇']
            }
        }

        self.api_usage = 0
        self.collected_spots = 0

    def search_places(self, query, region, city, category_key):
        """Google Places APIで場所を検索"""
        if not self.api_key:
            return []

        try:
            url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
            full_query = f"{query} {city}"

            params = {
                'query': full_query,
                'language': 'ja',
                'key': self.api_key
            }

            data = get_json(url, params, ttl_sec=60*60*24*7)
                status = data.get('status')
                if status == 'OK':
                    places = data.get('results', [])
                    print(f"      ✅ 検索結果: {len(places)}件")
                    return places
                elif status == 'OVER_QUERY_LIMIT':
                    print(f"      ❌ API制限達成")
                    return 'LIMIT_REACHED'
                else:
                    print(f"      ⚠️  Status: {status}")

        except Exception as e:
            print(f"      ❌ 検索エラー: {e}")

        return []

    def extract_permanent_image_url(self, photo_reference):
        """photo_referenceから永続的な直接URLを取得"""
        if not photo_reference or not self.api_key:
            return None

        try:
            direct_url = get_photo_direct_url(photo_reference, maxwidth=800, ttl_sec=60*60*24*30)
            return direct_url

        except Exception as e:
            print(f"        画像URL取得エラー: {e}")

        return None

    def get_fallback_image(self, category_key, name):
        """Unsplashフォールバック画像URL取得"""
        category_keywords = {
            'parks': 'park nature green',
            'sauna': 'sauna spa relaxation',
            'cafe': 'cafe coffee interior',
            'walking_courses': 'walking path nature trail'
        }

        keyword = category_keywords.get(category_key, 'nature')
        return f"https://source.unsplash.com/800x600/?{keyword}"

    def save_spot(self, place, region, category_key):
        """スポット情報をデータベースに保存"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()

            # 重複チェック
            place_id = place.get('place_id')
            if place_id:
                cursor.execute("SELECT id FROM spots WHERE place_id = %s", (place_id,))
                if cursor.fetchone():
                    print(f"        ⚠️  重複スキップ: {place.get('name', 'Unknown')}")
                    cursor.close()
                    connection.close()
                    return False

            # 永続画像URL取得
            permanent_urls = []
            fallback_url = self.get_fallback_image(category_key, place.get('name', ''))

            photos = place.get('photos', [])
            if photos:
                for photo in photos[:3]:  # 最大3枚
                    photo_ref = photo.get('photo_reference')
                    if photo_ref:
                        permanent_url = self.extract_permanent_image_url(photo_ref)
                        if permanent_url:
                            permanent_urls.append({
                                'url': permanent_url,
                                'width': photo.get('width'),
                                'height': photo.get('height'),
                                'api_independent': True
                            })
                        time.sleep(0.2)  # API制限対策

            # データ準備
            geometry = place.get('geometry', {})
            location = geometry.get('location', {})

            spot_data = {
                'place_id': place_id,
                'name': place.get('name'),
                'category': f"relax_{category_key}",
                'address': place.get('formatted_address'),
                'latitude': location.get('lat'),
                'longitude': location.get('lng'),
                'rating': place.get('rating'),
                'user_ratings_total': place.get('user_ratings_total'),
                'price_level': place.get('price_level'),
                'photos': json.dumps(photos) if photos else None,
                'types': json.dumps(place.get('types', [])),
                'vicinity': place.get('vicinity'),
                'plus_code': place.get('plus_code', {}).get('global_code'),
                'region': region,
                'image_urls': json.dumps(permanent_urls) if permanent_urls else None,
                'fallback_image_url': fallback_url
            }

            # データベース挿入
            insert_query = """
                INSERT INTO spots (
                    place_id, name, category, address, latitude, longitude,
                    rating, user_ratings_total, price_level, photos, types,
                    vicinity, plus_code, region, image_urls, fallback_image_url,
                    created_at, updated_at
                ) VALUES (
                    %(place_id)s, %(name)s, %(category)s, %(address)s, %(latitude)s, %(longitude)s,
                    %(rating)s, %(user_ratings_total)s, %(price_level)s, %(photos)s, %(types)s,
                    %(vicinity)s, %(plus_code)s, %(region)s, %(image_urls)s, %(fallback_image_url)s,
                    NOW(), NOW()
                )
            """

            cursor.execute(insert_query, spot_data)
            connection.commit()

            self.collected_spots += 1
            print(f"        💾 保存完了: {place.get('name')}")
            print(f"        🖼️  永続画像: {len(permanent_urls)}件")

            cursor.close()
            connection.close()
            return True

        except Exception as e:
            print(f"        ❌ 保存エラー: {e}")
            return False

    def collect_category_region(self, category_key, region_key, target_count=100):
        """特定カテゴリー・地域の収集"""
        category = self.categories[category_key]
        region = self.regions[region_key]

        print(f"\n🎯 収集開始: {category['japanese']} × {region['name']}")
        print(f"   目標: {target_count}件")

        collected_in_this_session = 0

        for city in region['cities']:
            if collected_in_this_session >= target_count:
                break

            print(f"   🏙️  都市: {city}")

            for search_term in category['search_terms']:
                if collected_in_this_session >= target_count:
                    break

                print(f"     🔍 検索: {search_term}")

                places = self.search_places(search_term, region_key, city, category_key)

                if places == 'LIMIT_REACHED':
                    print(f"     ⚠️  API制限達成 - 処理停止")
                    return collected_in_this_session

                if places:
                    for place in places:
                        if collected_in_this_session >= target_count:
                            break

                        success = self.save_spot(place, region_key, category_key)
                        if success:
                            collected_in_this_session += 1

                        time.sleep(0.3)  # API制限対策

                time.sleep(1)  # 検索間隔

        print(f"   🎉 完了: {collected_in_this_session}件収集")
        return collected_in_this_session

    def collect_all_massive(self):
        """全カテゴリー・全地域の大規模収集"""
        print("🚀 大規模リラックスカテゴリー収集開始")
        print("🎯 目標: 4カテゴリー × 100件 × 7地域 = 2800件")
        print("🖼️  永続画像URL対応済み\n")

        total_collected = 0

        for category_key, category in self.categories.items():
            print(f"\n📂 カテゴリー: {category['japanese']} ({category_key})")
            category_total = 0

            for region_key, region in self.regions.items():
                collected = self.collect_category_region(category_key, region_key, 100)
                category_total += collected
                total_collected += collected

                print(f"   📊 {region['name']}: {collected}件")
                print(f"   🔧 API使用: {self.api_usage}回")

                # 大量処理の休憩
                if self.api_usage % 50 == 0:
                    print(f"   😴 API制限対策休憩（10秒）...")
                    time.sleep(10)

            print(f"🎯 {category['japanese']} 完了: {category_total}件")

        print(f"\n🎉 大規模収集完了!")
        print(f"   📊 総収集数: {total_collected}件")
        print(f"   🔧 API使用: {self.api_usage}回")
        print(f"   🖼️  全て永続画像URL対応済み")

def main():
    print("🏗️  大規模リラックスカテゴリー収集システム")
    print("💫 APIキー依存なし永続画像システム")
    print("⚡ 2800件データ収集\n")

    collector = MassiveRelaxCollector()
    collector.collect_all_massive()

if __name__ == "__main__":
    main()
