#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全リラックスカテゴリー大規模収集システム
公園・サウナ・カフェ・散歩コース × 100件 × 7地域 = 2800件
レビューコメント・永続画像URL付き完全版
"""

import requests
import mysql.connector
import json
import time
import os
import random
from datetime import datetime
from dotenv import load_dotenv
from utils.request_guard import get_json, get_photo_direct_url, already_fetched_place, mark_fetched_place

load_dotenv()

class RelaxCategoryCollector:
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')  # .envファイルのキー名に合わせて修正
        self.base_url = "https://maps.googleapis.com/maps/api/place"

        # DB接続設定（本番環境）
        self.db_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_production',  # 本番データベースに変更
            'charset': 'utf8mb4'
        }

        # 地域定義
        self.regions = {
            'hokkaido': {
                'name': '北海道',
                'center': {'lat': 43.0642, 'lng': 141.3469},
                'cities': ['札幌', '函館', '旭川', '釧路', '帯広', '北見', '小樽', '室蘭']
            },
            'tohoku': {
                'name': '東北',
                'center': {'lat': 38.2682, 'lng': 140.8694},
                'cities': ['仙台', '青森', '盛岡', '秋田', '山形', '福島', '八戸', '郡山']
            },
            'kanto': {
                'name': '関東',
                'center': {'lat': 35.6762, 'lng': 139.6503},
                'cities': ['東京', '横浜', '千葉', 'さいたま', '宇都宮', '前橋', '水戸', '川崎']
            },
            'chubu': {
                'name': '中部',
                'center': {'lat': 36.2048, 'lng': 138.2529},
                'cities': ['名古屋', '金沢', '富山', '福井', '長野', '甲府', '岐阜', '静岡']
            },
            'kansai': {
                'name': '関西',
                'center': {'lat': 34.6937, 'lng': 135.5023},
                'cities': ['大阪', '京都', '神戸', '奈良', '和歌山', '津', '大津', '堺']
            },
            'chugoku_shikoku': {
                'name': '中国',
                'center': {'lat': 34.3853, 'lng': 132.4553},
                'cities': ['広島', '岡山', '松江', '鳥取', '山口', '高松', '松山', '高知']
            },
            'kyushu_okinawa': {
                'name': '九州',
                'center': {'lat': 31.7683, 'lng': 131.0023},
                'cities': ['福岡', '長崎', '熊本', '大分', '宮崎', '鹿児島', '佐賀', '那覇']
            }
        }

        # カテゴリー定義
        self.categories = {
            'parks': {
                'name': '公園',
                'keywords': ['公園', 'パーク', '庭園', '植物園', '動物園', '森林公園']
            },
            'sauna': {
                'name': 'サウナ',
                'keywords': ['サウナ', 'スパ', '銭湯', '健康ランド', 'スーパー銭湯']
            },
            'cafe': {
                'name': 'カフェ',
                'keywords': ['カフェ', 'コーヒー', '喫茶店', 'コーヒーショップ']
            },
            'walking_courses': {
                'name': '散歩コース',
                'keywords': ['散歩道', 'ウォーキングコース', '遊歩道', '散策路', 'プロムナード']
            }
        }

    def connect_db(self):
        """データベース接続"""
        return mysql.connector.connect(**self.db_config)

    def search_places(self, region_key, category_key, target_count=100):
        """指定地域・カテゴリーでスポット検索"""
        region = self.regions[region_key]
        category = self.categories[category_key]

        print(f"\n🔍 {region['name']}の{category['name']}を検索中...")

        collected_places = []
        seen_place_ids = set()

        # 各都市 + キーワード組み合わせで検索
        for city in region['cities']:
            if len(collected_places) >= target_count:
                break

            for keyword in category['keywords']:
                if len(collected_places) >= target_count:
                    break

                query = f"{city} {keyword}"
                print(f"  検索: {query}")

                places = self._text_search(query, region['center'])

                for place in places:
                    if len(collected_places) >= target_count:
                        break

                    if place['place_id'] not in seen_place_ids:
                        seen_place_ids.add(place['place_id'])
                        collected_places.append(place)
                        print(f"    ✓ {place['name']} (総計: {len(collected_places)})")

                # API制限対策
                time.sleep(0.1)

        print(f"✅ {region['name']}の{category['name']}: {len(collected_places)}件収集完了")
        return collected_places

    def _text_search(self, query, location=None):
        """テキスト検索API"""
        url = f"{self.base_url}/textsearch/json"

        params = {
            'query': query,
            'key': self.api_key,
            'language': 'ja',
            'region': 'jp'
        }

        if location:
            params['location'] = f"{location['lat']},{location['lng']}"
            params['radius'] = 50000  # 50km

        try:
            data = get_json(url, params, ttl_sec=60*60*24*7)

            if data['status'] == 'OK':
                return data.get('results', [])
            else:
                print(f"    ⚠️ API警告: {data.get('status')}")
                return []

        except Exception as e:
            print(f"    ❌ 検索エラー: {e}")
            return []

    def get_place_details(self, place_id):
        """詳細情報取得（レビュー含む）"""
        url = f"{self.base_url}/details/json"

        params = {
            'place_id': place_id,
            'key': self.api_key,
            'fields': 'name,formatted_address,geometry,photos,rating,user_ratings_total,reviews,website,formatted_phone_number,opening_hours',
            'language': 'ja',
            'reviews_sort': 'newest'
        }

        try:
            if already_fetched_place(place_id):
                return {}
            data = get_json(url, params, ttl_sec=60*60*24*30)

            if data['status'] == 'OK':
                return data.get('result', {})
            else:
                print(f"    ⚠️ 詳細取得警告: {data.get('status')}")
                return {}

        except Exception as e:
            print(f"    ❌ 詳細取得エラー: {e}")
            return {}

    def extract_permanent_image_url(self, photo_reference):
        """永続画像URL抽出"""
        if not photo_reference:
            return None

        try:
            location = get_photo_direct_url(photo_reference, maxwidth=800, ttl_sec=60*60*24*30)
            return location

        except Exception as e:
            print(f"    ❌ 画像URL抽出エラー: {e}")

    return None

    def save_to_database(self, region_key, category_key, places_data):
        """データベース保存（本番cardsテーブル用）"""
        region_label = self.regions.get(region_key, {}).get('name', region_key)
        print(f"\n💾 {region_label}の{self.categories[category_key]['name']}をDB保存中...")

        connection = self.connect_db()
        cursor = connection.cursor()

        saved_count = 0

        for place_data in places_data:
            try:
                # 重複チェック
                cursor.execute(
                    "SELECT id FROM cards WHERE place_id = %s",
                    (place_data['place_id'],)
                )

                if cursor.fetchone():
                    print(f"  ⏭️ スキップ（重複）: {place_data['name']}")
                    continue

                # レビューコメント保存用の準備
                reviews = place_data.get('reviews', [])
                review_text = ""
                if reviews:
                    # 最初のレビューをテキストとして保存
                    review_text = reviews[0].get('text', '')[:500]  # 500文字制限

                # 外部リンク生成
                place_id = place_data['place_id']
                external_link = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

                # 画像URL（最初の1枚を使用）
                image_urls = place_data.get('image_urls', [])
                image_url = image_urls[0] if image_urls else ""

                # データ挿入（cardsテーブル用）
                insert_query = """
                INSERT INTO cards (
                    genre, title, rating, review_count, image_url, external_link,
                    region, address, place_id, latitude, longitude, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                cursor.execute(insert_query, (
                    "relax",  # relax系は'genre'を'relax'に統一
                    place_data['name'],       # title
                    place_data.get('rating'),
                    place_data.get('user_ratings_total', 0),  # review_count
                    image_url,
                    external_link,
                    region_label,  # 日本語地域名を保存
                    place_data.get('address', ''),
                    place_data['place_id'],
                    place_data.get('latitude'),
                    place_data.get('longitude'),
                    datetime.now(),
                    datetime.now()
                ))

                card_id = cursor.lastrowid

                # レビューコメントを別テーブルに保存
                if reviews:
                    for review in reviews[:5]:  # 最大5件のレビュー
                        review_insert = """
                        INSERT INTO review_comments (
                            card_id, comment, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s)
                        """

                        cursor.execute(review_insert, (
                            card_id,
                            review.get('text', '')[:1000],  # 1000文字制限
                            datetime.now(),
                            datetime.now()
                        ))

                saved_count += 1
                print(f"  ✅ 保存: {place_data['name']} (レビュー: {len(reviews)}件)")

            except Exception as e:
                print(f"  ❌ 保存エラー: {place_data.get('name', '不明')} - {e}")

        connection.commit()
        cursor.close()
        connection.close()

        print(f"✅ {saved_count}件をデータベースに保存完了")
        return saved_count

    def collect_region_category(self, region_key, category_key, target_count=100):
        """地域・カテゴリー別収集"""
        print(f"\n🎯 {self.regions[region_key]['name']} × {self.categories[category_key]['name']} 収集開始")

        # 基本検索
        places = self.search_places(region_key, category_key, target_count)

        if not places:
            print("❌ スポットが見つかりませんでした")
            return 0

        # 詳細情報・レビュー・画像収集
        print(f"\n📋 詳細情報・レビュー・画像収集中...")
        enriched_places = []

        for i, place in enumerate(places):
            print(f"  処理中 ({i+1}/{len(places)}): {place['name']}")

            # 詳細情報取得
            details = self.get_place_details(place['place_id'])

            # データ統合
            enriched_place = {
                'place_id': place['place_id'],
                'name': place['name'],
                'address': details.get('formatted_address', place.get('formatted_address', '')),
                'latitude': None,
                'longitude': None,
                'rating': details.get('rating'),
                'user_ratings_total': details.get('user_ratings_total', 0),
                'phone': details.get('formatted_phone_number', ''),
                'website': details.get('website', ''),
                'opening_hours': details.get('opening_hours', {}),
                'image_urls': [],
                'reviews': []
            }

            # 座標設定
            if details.get('geometry', {}).get('location'):
                location = details['geometry']['location']
                enriched_place['latitude'] = location['lat']
                enriched_place['longitude'] = location['lng']
            elif place.get('geometry', {}).get('location'):
                location = place['geometry']['location']
                enriched_place['latitude'] = location['lat']
                enriched_place['longitude'] = location['lng']

            # 画像URL収集（永続URL）
            photos = details.get('photos', [])
            for photo in photos[:3]:  # 最大3枚
                photo_ref = photo.get('photo_reference')
                if photo_ref:
                    permanent_url = self.extract_permanent_image_url(photo_ref)
                    if permanent_url:
                        enriched_place['image_urls'].append(permanent_url)
                        print(f"    🖼️ 画像URL取得済み")

            # レビュー収集
            reviews = details.get('reviews', [])
            for review in reviews:
                if len(review.get('text', '')) >= 10:  # 短すぎるレビューは除外
                    review_data = {
                        'author_name': review.get('author_name', '匿名'),
                        'rating': review.get('rating'),
                        'text': review.get('text', ''),
                        'time': review.get('time'),
                        'relative_time_description': review.get('relative_time_description', '')
                    }
                    enriched_place['reviews'].append(review_data)

            print(f"    ✅ 画像: {len(enriched_place['image_urls'])}件, レビュー: {len(enriched_place['reviews'])}件")
            enriched_places.append(enriched_place)

            # API制限対策
            time.sleep(0.2)

        # データベース保存
        saved_count = self.save_to_database(region_key, category_key, enriched_places)

        return saved_count

    def collect_all_categories(self):
        """全カテゴリー・全地域収集"""
        print("🚀 全リラックスカテゴリー大規模収集開始！")
        print(f"目標: {len(self.categories)}カテゴリー × 100件 × {len(self.regions)}地域 = {len(self.categories) * 100 * len(self.regions)}件\n")

        total_collected = 0
        results = {}

        for category_key in self.categories.keys():
            results[category_key] = {}

            for region_key in self.regions.keys():
                collected = self.collect_region_category(region_key, category_key, 100)
                results[category_key][region_key] = collected
                total_collected += collected

                print(f"📊 進捗: {total_collected}件 / {len(self.categories) * 100 * len(self.regions)}件")

                # 地域間の待機時間
                time.sleep(1)

        # 最終レポート
        print(f"\n🎉 全収集完了！")
        print(f"総収集件数: {total_collected}件")

        print("\n📊 詳細結果:")
        for category_key, category_results in results.items():
            category_total = sum(category_results.values())
            print(f"\n{self.categories[category_key]['name']}: {category_total}件")
            for region_key, count in category_results.items():
                print(f"  {self.regions[region_key]['name']}: {count}件")

def main():
    collector = RelaxCategoryCollector()
    collector.collect_all_categories()

if __name__ == "__main__":
    main()
