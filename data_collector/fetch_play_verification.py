#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
遊びジャンル収集・取得時マッピング検証システム
ゲームセンター・カラオケを各地域10件ずつ収集（合計80件）
"""

import requests
import mysql.connector
import json
import time
import re
import os
from dotenv import load_dotenv

class PlayVerificationCollector:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('GOOGLE_PLACES_API_KEY')

        # 都道府県→地域マッピング（日本語版）
        self.prefecture_to_region = {
            # 北海道
            '北海道': '北海道',

            # 東北
            '青森県': '東北', '岩手県': '東北', '宮城県': '東北',
            '秋田県': '東北', '山形県': '東北', '福島県': '東北',

            # 関東
            '東京都': '関東', '茨城県': '関東', '栃木県': '関東',
            '群馬県': '関東', '埼玉県': '関東', '千葉県': '関東', '神奈川県': '関東',

            # 中部
            '新潟県': '中部', '富山県': '中部', '石川県': '中部', '福井県': '中部',
            '山梨県': '中部', '長野県': '中部', '岐阜県': '中部',
            '静岡県': '中部', '愛知県': '中部',

            # 関西
            '京都府': '関西', '大阪府': '関西', '三重県': '関西',
            '滋賀県': '関西', '兵庫県': '関西', '奈良県': '関西', '和歌山県': '関西',
            '京都': '関西',  # 省略形対応

            # 中国・四国
            '鳥取県': '中国', '島根県': '中国', '岡山県': '中国',
            '広島県': '中国', '山口県': '中国',
            '徳島県': '中国', '香川県': '中国',
            '愛媛県': '中国', '高知県': '中国',

            # 九州
            '福岡県': '九州', '佐賀県': '九州', '長崎県': '九州',
            '大分県': '九州', '熊本県': '九州', '宮崎県': '九州',
            '鹿児島県': '九州', '沖縄県': '九州'
        }

        # 検索対象地域と都市
        self.search_regions = {
            '北海道': ['札幌', '函館', '旭川'],
            '東北': ['仙台', '青森', '盛岡', '秋田', '山形', '福島'],
            '関東': ['東京', '横浜', '千葉', '大宮', '宇都宮', '前橋', '水戸'],
            '中部': ['名古屋', '金沢', '長野', '静岡', '富山', '新潟', '甲府'],
            '関西': ['大阪', '京都', '神戸', '奈良', '和歌山', '大津'],
            '中国': ['広島', '岡山', '松江', '鳥取', '山口', '高松', '松山'],
            '九州': ['福岡', '長崎', '熊本', '鹿児島', '大分', '宮崎', '那覇']
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

        if found_prefectures:
            return max(found_prefectures, key=len)

        return None

    def get_region_from_address(self, address):
        """住所から地域を判定（日本語版）"""
        prefecture = self.extract_prefecture_from_address(address)
        if prefecture:
            return self.prefecture_to_region.get(prefecture)
        return None

    def search_places(self, query, city, region):
        """Google Places APIで場所を検索"""
        base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"

        params = {
            'query': f'{query} {city}',
            'key': self.api_key,
            'language': 'ja',
            'region': 'jp'
        }

        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ 検索エラー ({city}): {e}")
            return None

    def get_place_details(self, place_id):
        """詳細情報とレビューを取得"""
        detail_url = "https://maps.googleapis.com/maps/api/place/details/json"

        params = {
            'place_id': place_id,
            'key': self.api_key,
            'language': 'ja',
            'fields': 'name,formatted_address,geometry,rating,review,photo,place_id,opening_hours'
        }

        try:
            response = requests.get(detail_url, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ 詳細取得エラー: {e}")
            return None

    def get_permanent_image_url(self, photo_reference):
        """永続的な画像URLを取得"""
        if not photo_reference:
            return None

        photo_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=800&photoreference={photo_reference}&key={self.api_key}"

        try:
            response = requests.get(photo_url, allow_redirects=False)
            if response.status_code == 302:
                return response.headers.get('Location')
        except Exception as e:
            print(f"⚠️ 画像URL取得失敗: {e}")

        return None

    def save_to_database(self, place_data):
        """データベースに保存"""
        try:
            connection = mysql.connector.connect(
                host='localhost',
                user='Haruto',
                password=os.getenv('MYSQL_PASSWORD'),
                database='swipe_app_production',
                charset='utf8mb4'
            )

            cursor = connection.cursor()

            # カード情報を保存
            card_query = """
            INSERT INTO cards (title, description, image_url, latitude, longitude, address, region, genre, place_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(card_query, (
                place_data['title'],
                place_data['description'],
                place_data['image_url'],
                place_data['latitude'],
                place_data['longitude'],
                place_data['address'],
                place_data['region'],  # 取得時にマッピング済み
                place_data['genre'],
                place_data['place_id']
            ))

            card_id = cursor.lastrowid

            # レビューコメントを保存
            if place_data.get('reviews'):
                comment_query = """
                INSERT INTO review_comments (card_id, comment)
                VALUES (%s, %s)
                """

                for review in place_data['reviews']:
                    cursor.execute(comment_query, (card_id, review))

            connection.commit()
            cursor.close()
            connection.close()

            return card_id

        except Exception as e:
            print(f"❌ DB保存エラー: {e}")
            return None

    def collect_play_data(self, genre, query_keywords, target_per_region=10):
        """遊びデータを収集"""
        print(f'🎮 {genre}データ収集開始 (各地域{target_per_region}件)')

        total_collected = 0
        region_counts = {}

        for region, cities in self.search_regions.items():
            print(f'\n=== {region}地域 ===')
            region_counts[region] = 0
            collected_place_ids = set()

            for city in cities:
                if region_counts[region] >= target_per_region:
                    break

                for keyword in query_keywords:
                    if region_counts[region] >= target_per_region:
                        break

                    print(f'🔍 検索中: {keyword} in {city}')

                    search_result = self.search_places(keyword, city, region)
                    if not search_result or 'results' not in search_result:
                        continue

                    for place in search_result['results'][:5]:  # 上位5件
                        if region_counts[region] >= target_per_region:
                            break

                        place_id = place.get('place_id')
                        if place_id in collected_place_ids:
                            continue

                        collected_place_ids.add(place_id)

                        # 詳細情報取得
                        details = self.get_place_details(place_id)
                        if not details or 'result' not in details:
                            continue

                        detail = details['result']
                        address = detail.get('formatted_address', '')

                        # 取得時に地域マッピング実行
                        detected_region = self.get_region_from_address(address)
                        if not detected_region:
                            print(f'⚠️ 地域判定失敗: {address}')
                            detected_region = region  # フォールバック

                        # 地域マッピング結果表示
                        mapping_status = '✅' if detected_region == region else '🔄'
                        print(f'  {mapping_status} {detail.get("name", "名前不明")} → {detected_region}')

                        # 画像URL取得
                        image_url = None
                        if detail.get('photos'):
                            photo_ref = detail['photos'][0].get('photo_reference')
                            image_url = self.get_permanent_image_url(photo_ref)

                        # レビュー収集
                        reviews = []
                        if detail.get('reviews'):
                            for review in detail['reviews'][:5]:
                                if review.get('text'):
                                    reviews.append(review['text'])

                        # データ構造作成
                        place_data = {
                            'title': detail.get('name', '名前不明'),
                            'description': f'{genre}施設です。',
                            'image_url': image_url,
                            'latitude': detail.get('geometry', {}).get('location', {}).get('lat'),
                            'longitude': detail.get('geometry', {}).get('location', {}).get('lng'),
                            'address': address,
                            'region': detected_region,  # マッピング済み地域
                            'genre': f'play_{genre}',
                            'place_id': place_id,
                            'reviews': reviews
                        }

                        # データベース保存
                        card_id = self.save_to_database(place_data)
                        if card_id:
                            region_counts[region] += 1
                            total_collected += 1
                            print(f'    ✅ 保存完了 (ID: {card_id})')

                        time.sleep(0.1)  # API制限対策

                    time.sleep(0.2)

            print(f'{region}: {region_counts[region]}件収集')

        print(f'\n📊 {genre}収集完了: 総計{total_collected}件')
        for region, count in region_counts.items():
            print(f'  {region}: {count}件')

        return total_collected

def main():
    collector = PlayVerificationCollector()

    print('🎯 取得時マッピング検証 - 遊びジャンル収集')
    print('目標: 各地域10件 × 8地域 = 80件\n')

    # ゲームセンター収集
    arcade_total = collector.collect_play_data(
        'arcade',
        ['ゲームセンター', 'ゲーセン', 'アミューズメント'],
        target_per_region=5
    )

    time.sleep(2)

    # カラオケ収集
    karaoke_total = collector.collect_play_data(
        'karaoke',
        ['カラオケ', 'カラオケボックス', 'ビッグエコー', 'カラオケ館'],
        target_per_region=5
    )

    total = arcade_total + karaoke_total
    print(f'\n🎊 検証用データ収集完了!')
    print(f'📊 総収集件数: {total}件')
    print(f'   ゲームセンター: {arcade_total}件')
    print(f'   カラオケ: {karaoke_total}件')

if __name__ == '__main__':
    main()
