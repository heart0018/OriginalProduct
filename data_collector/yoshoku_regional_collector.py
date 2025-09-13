#!/usr/bin/env python3
"""
洋食ジャンル地域別大量収集システム
- 7地域 × 最大100件 = 最大700件
- 地域ごとに洋食データを収集
- 件数が足りない地域は自然停止
"""

import requests
import mysql.connector
import time
import os
import re
from dotenv import load_dotenv
from utils.request_guard import (
    get_json,
    already_fetched_place,
    mark_fetched_place,
)

# 環境変数読み込み
load_dotenv()

class YoshokuRegionalCollector:
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY environment variable is required")

        # DB接続情報
        self.db_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_production',
            'charset': 'utf8mb4'
        }

        # 地域別主要都市定義
        self.regional_cities = {
            'hokkaido': [
                '札幌', '函館', '旭川', '釧路', '帯広', '北見', '小樽', '室蘭',
                '苫小牧', '稚内', '網走', '名寄', '千歳', '恵庭', '石狩'
            ],
            'tohoku': [
                '仙台', '青森', '盛岡', '秋田', '山形', '福島', '八戸', '弘前',
                'いわき', '郡山', '酒田', '米沢', '会津若松', '一関', '大船渡'
            ],
            'kanto': [
                '東京', '横浜', '川崎', '千葉', 'さいたま', '宇都宮', '前橋', '水戸',
                '新宿', '渋谷', '池袋', '銀座', '六本木', '表参道', '恵比寿',
                '船橋', '柏', '川口', '越谷', '所沢', '高崎', 'つくば'
            ],
            'chubu': [
                '名古屋', '新潟', '金沢', '富山', '福井', '甲府', '長野', '岐阜', '静岡',
                '浜松', '豊田', '岡崎', '一宮', '春日井', '長岡', '上越', '高岡',
                '松本', '上田', '沼津', '富士', '藤枝'
            ],
            'kansai': [
                '大阪', '京都', '神戸', '奈良', '和歌山', '大津', '津',
                '梅田', '難波', '天王寺', '京都駅', '河原町', '三宮', '姫路',
                '堺', '東大阪', '枚方', '豊中', '吹田', '高槻', '茨木'
            ],
            'chugoku_shikoku': [
                '広島', '岡山', '山口', '鳥取', '松江', '高松', '松山', '高知',
                '福山', '倉敷', '下関', '宇部', '徳島', '今治', '新居浜',
                '丸亀', '坂出', '四国中央', '西条', '大洲'
            ],
            'kyushu_okinawa': [
                '福岡', '北九州', '熊本', '鹿児島', '長崎', '大分', '宮崎', '佐賀', '那覇',
                '久留米', '飯塚', '直方', '田川', '柳川', '大牟田', '筑後',
                '佐世保', '諫早', '大村', '別府', '中津', '日田', '都城'
            ]
        }

        # 洋食検索キーワード（豊富に）
        self.yoshoku_keywords = [
            '洋食', 'イタリアン', 'フレンチ', 'ステーキ', 'ハンバーグ',
            'パスタ', 'ピザ', 'ビストロ', 'トラットリア', 'オステリア',
            'フランス料理', 'イタリア料理', '西洋料理', 'European',
            'カジュアルフレンチ', 'イタリアンレストラン', 'フレンチレストラン',
            'ステーキハウス', 'グリル料理', 'ワインバー', '洋風レストラン'
        ]

        # 都道府県→地域マッピング
        self.prefecture_to_region = {
            '北海道': 'hokkaido',
            '青森': 'tohoku', '岩手': 'tohoku', '宮城': 'tohoku',
            '秋田': 'tohoku', '山形': 'tohoku', '福島': 'tohoku',
            '茨城': 'kanto', '栃木': 'kanto', '群馬': 'kanto',
            '埼玉': 'kanto', '千葉': 'kanto', '東京': 'kanto', '神奈川': 'kanto',
            '新潟': 'chubu', '富山': 'chubu', '石川': 'chubu',
            '福井': 'chubu', '山梨': 'chubu', '長野': 'chubu',
            '岐阜': 'chubu', '静岡': 'chubu', '愛知': 'chubu',
            '三重': 'kansai', '滋賀': 'kansai', '京都': 'kansai',
            '大阪': 'kansai', '兵庫': 'kansai', '奈良': 'kansai', '和歌山': 'kansai',
            '鳥取': 'chugoku_shikoku', '島根': 'chugoku_shikoku', '岡山': 'chugoku_shikoku',
            '広島': 'chugoku_shikoku', '山口': 'chugoku_shikoku',
            '徳島': 'chugoku_shikoku', '香川': 'chugoku_shikoku',
            '愛媛': 'chugoku_shikoku', '高知': 'chugoku_shikoku',
            '福岡': 'kyushu_okinawa', '佐賀': 'kyushu_okinawa', '長崎': 'kyushu_okinawa',
            '熊本': 'kyushu_okinawa', '大分': 'kyushu_okinawa', '宮崎': 'kyushu_okinawa',
            '鹿児島': 'kyushu_okinawa', '沖縄': 'kyushu_okinawa'
        }

    def extract_prefecture_realtime(self, address):
        """住所から都道府県をリアルタイム抽出"""
        if not address:
            return None

        for prefecture in self.prefecture_to_region.keys():
            if prefecture in address:
                return prefecture

        if '東京都' in address:
            return '東京'
        elif '京都府' in address:
            return '京都'
        elif '大阪府' in address:
            return '大阪'

        return None

    def get_region_from_address(self, address):
        """住所から地域を取得"""
        prefecture = self.extract_prefecture_realtime(address)
        if prefecture:
            return self.prefecture_to_region.get(prefecture)
        return None

    def search_places(self, query, location):
        """Google Places Text Search"""
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            'query': f"{query} {location}",
            'key': self.api_key,
            'language': 'ja',
            'region': 'jp'
        }

        try:
            # 7日キャッシュで重複検索APIを抑止
            return get_json(url, params, ttl_sec=60*60*24*7)
        except Exception as e:
            print(f"❌ 検索エラー: {e}")
            return None

    def get_place_details(self, place_id):
        """Google Places Place Details"""
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            'place_id': place_id,
            'fields': 'name,formatted_address,rating,user_ratings_total,photos,reviews,formatted_phone_number,website,opening_hours',
            'key': self.api_key,
            'language': 'ja'
        }

        try:
            if already_fetched_place(place_id):
                return {}
            data = get_json(url, params, ttl_sec=60*60*24*30)
            result = data.get('result', {}) if isinstance(data, dict) else {}
            mark_fetched_place(place_id)
            return result
        except Exception as e:
            print(f"❌ 詳細取得エラー: {e}")
            return {}

    def save_to_database(self, spot_data):
        """データベースに保存"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            # 重複チェック
            cursor.execute(
                "SELECT id FROM cards WHERE title = %s AND address = %s",
                (spot_data['title'], spot_data['address'])
            )

            if cursor.fetchone():
                return False

            # カード情報を挿入
            insert_card_query = """
                INSERT INTO cards (title, address, rating, review_count, genre, region, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            """

            card_values = (
                spot_data['title'],
                spot_data['address'],
                spot_data['rating'],
                spot_data['review_count'],
                'gourmet_yoshoku',  # 洋食ジャンル
                spot_data['region']
            )

            cursor.execute(insert_card_query, card_values)
            card_id = cursor.lastrowid

            # レビューコメントを挿入
            if spot_data.get('reviews'):
                for review in spot_data['reviews']:
                    cursor.execute(
                        "INSERT INTO review_comments (card_id, comment, created_at, updated_at) VALUES (%s, %s, NOW(), NOW())",
                        (card_id, review)
                    )

            connection.commit()
            return True

        except Exception as e:
            print(f"  ❌ 保存エラー: {e}")
            return False
        finally:
            if 'connection' in locals():
                cursor.close()
                connection.close()

    def collect_region(self, region, target_count=100):
        """特定地域の洋食データ収集"""
        print(f"\n🍽️ 地域: {region.upper()}")
        print("=" * 60)
        print(f"📊 目標: {target_count}件")

        cities = self.regional_cities.get(region, [])
        collected = 0
        processed_places = set()  # 重複防止

        for keyword in self.yoshoku_keywords:
            if collected >= target_count:
                break

            for city in cities:
                if collected >= target_count:
                    break

                print(f"🔍 検索中: {keyword} {city} ({collected}/{target_count})")

                # Places検索
                search_results = self.search_places(keyword, city)
                if not search_results or 'results' not in search_results:
                    continue

                for place in search_results['results']:
                    if collected >= target_count:
                        break

                    place_id = place.get('place_id', '')
                    if place_id in processed_places:
                        continue
                    processed_places.add(place_id)

                    # 基本情報取得
                    name = place.get('name', '')
                    address = place.get('formatted_address', '')
                    rating = place.get('rating', 0)
                    user_ratings_total = place.get('user_ratings_total', 0)

                    if not all([name, address, place_id]):
                        continue

                    # 地域判定（収集対象地域かチェック）
                    detected_region = self.get_region_from_address(address)
                    if detected_region != region:
                        continue

                    # 詳細情報取得
                    details = self.get_place_details(place_id)

                    # レビューコメント取得
                    reviews = []
                    if details.get('reviews'):
                        reviews = [review.get('text', '') for review in details['reviews'][:5]]

                    # データ構造作成
                    spot_data = {
                        'title': name,
                        'address': address,
                        'rating': rating,
                        'review_count': user_ratings_total,
                        'region': region,
                        'reviews': reviews
                    }

                    # 保存実行
                    if self.save_to_database(spot_data):
                        collected += 1
                        print(f"  ✅ {name}")

                    # API制限対策（ガード導入後は控えめ）
                    time.sleep(0.05)

                # 検索間隔（キャッシュ前提で軽め）
                time.sleep(0.1)

        print(f"📊 {region} 洋食収集完了: {collected}件")
        return collected

    def run_full_yoshoku_collection(self):
        """全地域洋食データ収集実行"""
        print("🍽️ 洋食ジャンル全国収集システム")
        print("=" * 70)
        print("📊 目標: 7地域 × 最大100件 = 最大700件")
        print("📋 ジャンル: gourmet_yoshoku")
        print("=" * 70)

        results = {}
        total_collected = 0

        for region in self.regional_cities.keys():
            collected = self.collect_region(region, target_count=100)
            results[region] = collected
            total_collected += collected

            # 地域間の休憩
            print(f"⏱️ 次の地域まで3秒休憩...")
            time.sleep(3)

        # 結果サマリー
        print(f"\n🎉 洋食データ収集完了!")
        print("=" * 50)
        print("📊 地域別収集結果:")
        for region, count in results.items():
            print(f"  {region}: {count}件")
        print("=" * 50)
        print(f"✅ 総収集件数: {total_collected}件")
        print(f"✅ 達成率: {total_collected/700*100:.1f}%")

        return results

if __name__ == "__main__":
    import sys

    collector = YoshokuRegionalCollector()

    if len(sys.argv) > 1:
        # 特定地域のみ
        region = sys.argv[1]
        if region in collector.regional_cities:
            collector.collect_region(region, target_count=100)
        else:
            print(f"❌ 無効な地域: {region}")
            print(f"✅ 有効な地域: {list(collector.regional_cities.keys())}")
    else:
        # 全地域収集
        collector.run_full_yoshoku_collection()
