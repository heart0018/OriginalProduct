#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import mysql.connector
import os
import time
import re
from dotenv import load_dotenv
from utils.request_guard import get_json, already_fetched_place, mark_fetched_place

# 環境変数の読み込み
load_dotenv()

# Google Places API設定
API_KEY = os.getenv('GOOGLE_API_KEY')
BASE_URL = "https://maps.googleapis.com/maps/api/place"

# データベース接続設定
DB_CONFIG = {
    'host': 'localhost',
    'user': 'Haruto',
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': 'swipe_app_production',
    'charset': 'utf8mb4'
}

# 地域定義と主要都市
REGIONS = {
    'hokkaido': ['札幌', '函館', '旭川', '釧路', '帯広', '北見'],
    'tohoku': ['仙台', '青森', '盛岡', '秋田', '山形', '福島'],
    'kanto': ['東京', '横浜', '川崎', '千葉', 'さいたま', '宇都宮'],
    'chubu': ['名古屋', '新潟', '金沢', '富山', '福井', '甲府'],
    'kansai': ['大阪', '京都', '神戸', '奈良', '和歌山', '滋賀'],
    'chugoku_shikoku': ['広島', '岡山', '山口', '松山', '高松', '高知'],
    'kyushu_okinawa': ['福岡', '長崎', '熊本', '鹿児島', '那覇', '宮崎']
}

# バー・居酒屋検索キーワード
ALCOHOL_KEYWORDS = [
    # バー系
    'バー', 'bar', 'カクテルバー', 'ワインバー', 'ウイスキーバー',
    'スポーツバー', 'ダイニングバー', 'ビアバー', '立ち飲み',

    # 居酒屋系
    '居酒屋', 'izakaya', '串焼き', '焼き鳥', '炉端焼き', '魚民',
    '鳥貴族', 'とりあえず吾平', '白木屋', '笑笑', '庄や',

    # 専門店
    '日本酒バー', '焼酎バー', 'ビール専門店', 'クラフトビール',
    '大衆酒場', '赤ちょうちん', '立ち呑み',

    # スタイル別
     'おでん屋', 'もつ焼き', '串カツ', 'せんべろ', '角打ち'
]

# 都道府県マッピング
PREFECTURE_MAPPING = {
    # 北海道
    '北海道': 'hokkaido',

    # 東北
    '青森県': 'tohoku', '岩手県': 'tohoku', '宮城県': 'tohoku',
    '秋田県': 'tohoku', '山形県': 'tohoku', '福島県': 'tohoku',

    # 関東
    '茨城県': 'kanto', '栃木県': 'kanto', '群馬県': 'kanto',
    '埼玉県': 'kanto', '千葉県': 'kanto', '東京都': 'kanto', '神奈川県': 'kanto',

    # 中部
    '新潟県': 'chubu', '富山県': 'chubu', '石川県': 'chubu', '福井県': 'chubu',
    '山梨県': 'chubu', '長野県': 'chubu', '岐阜県': 'chubu', '静岡県': 'chubu', '愛知県': 'chubu',

    # 関西
    '三重県': 'kansai', '滋賀県': 'kansai', '京都府': 'kansai',
    '大阪府': 'kansai', '兵庫県': 'kansai', '奈良県': 'kansai', '和歌山県': 'kansai',

    # 中国・四国
    '鳥取県': 'chugoku_shikoku', '島根県': 'chugoku_shikoku', '岡山県': 'chugoku_shikoku',
    '広島県': 'chugoku_shikoku', '山口県': 'chugoku_shikoku',
    '徳島県': 'chugoku_shikoku', '香川県': 'chugoku_shikoku', '愛媛県': 'chugoku_shikoku', '高知県': 'chugoku_shikoku',

    # 九州・沖縄
    '福岡県': 'kyushu_okinawa', '佐賀県': 'kyushu_okinawa', '長崎県': 'kyushu_okinawa',
    '熊本県': 'kyushu_okinawa', '大分県': 'kyushu_okinawa', '宮崎県': 'kyushu_okinawa',
    '鹿児島県': 'kyushu_okinawa', '沖縄県': 'kyushu_okinawa'
}

def extract_region_from_address(address):
    """住所から地域を抽出"""
    if not address:
        return 'unknown'

    for prefecture, region in PREFECTURE_MAPPING.items():
        if prefecture in address:
            return region

    return 'unknown'

def search_places(query, location=None):
    """Google Places APIで場所を検索"""
    url = f"{BASE_URL}/textsearch/json"
    params = {
        'query': query,
        'key': API_KEY,
        'language': 'ja',
        'region': 'jp',
        'type': 'restaurant|bar|establishment'
    }

    if location:
        params['location'] = location
        params['radius'] = 50000

    try:
        return get_json(url, params, ttl_sec=60*60*24*7)  # 7日キャッシュ
    except Exception as e:
        print(f"  ❌ API エラー: {e}")
        return None

def get_place_details(place_id):
    """場所の詳細情報を取得"""
    url = f"{BASE_URL}/details/json"
    params = {
        'place_id': place_id,
        'key': API_KEY,
        'language': 'ja',
        'fields': 'name,formatted_address,rating,user_ratings_total,reviews,geometry'
    }

    try:
        if already_fetched_place(place_id):
            return None
        data = get_json(url, params, ttl_sec=60*60*24*30)  # 30日キャッシュ
        mark_fetched_place(place_id)
        return data
    except Exception as e:
        print(f"  ❌ 詳細取得エラー: {e}")
        return None

def check_duplicate(cursor, title, address):
    """重複チェック"""
    cursor.execute("""
        SELECT COUNT(*) FROM cards
        WHERE title = %s AND address = %s
    """, (title, address))

    return cursor.fetchone()[0] > 0

def save_to_database(place_data, region):
    """データベースに保存"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # 重複チェック
        if check_duplicate(cursor, place_data['title'], place_data['address']):
            cursor.close()
            connection.close()
            return False

        # cardsテーブルに挿入
        insert_card_query = """
            INSERT INTO cards (title, address, rating, review_count, genre, region, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        """

        cursor.execute(insert_card_query, (
            place_data['title'],
            place_data['address'],
            place_data['rating'],
            place_data['review_count'],
            'gourmet_alcohol',  # 新しいジャンル
            region
        ))

        card_id = cursor.lastrowid

        # レビューコメントを挿入
        if place_data['reviews']:
            insert_review_query = """
                INSERT INTO review_comments (card_id, comment, created_at, updated_at)
                VALUES (%s, %s, NOW(), NOW())
            """

            for review in place_data['reviews']:
                cursor.execute(insert_review_query, (card_id, review))

        connection.commit()
        cursor.close()
        connection.close()
        return True

    except Exception as e:
        print(f"  ❌ DB保存エラー: {e}")
        return False

def collect_region_data(region_name):
    """指定地域のバー・居酒屋データを収集"""
    print(f"\n🍺 地域: {region_name.upper()}")
    print("=" * 60)
    print(f"📊 目標: 100件")

    cities = REGIONS[region_name]
    collected_count = 0
    target_per_region = 100

    for keyword in ALCOHOL_KEYWORDS:
        if collected_count >= target_per_region:
            break

        for city in cities:
            if collected_count >= target_per_region:
                break

            query = f"{keyword} {city}"
            print(f"🔍 検索中: {query} ({collected_count}/{target_per_region})")

            # API検索
            results = search_places(query)
            if not results or 'results' not in results:
                continue

            # 各結果を処理
            for place in results['results'][:5]:  # 各検索で最大5件
                if collected_count >= target_per_region:
                    break

                # 詳細情報取得
                details = get_place_details(place['place_id'])
                if not details or 'result' not in details:
                    continue

                result = details['result']

                # 基本情報抽出
                title = result.get('name', '')
                address = result.get('formatted_address', '')
                rating = result.get('rating', 0.0)
                review_count = result.get('user_ratings_total', 0)

                if not title or not address:
                    continue

                # 住所から地域を抽出して検証
                detected_region = extract_region_from_address(address)
                if detected_region != region_name and detected_region != 'unknown':
                    continue  # 対象地域外はスキップ

                # レビューコメント取得
                reviews = []
                if 'reviews' in result:
                    reviews = [review.get('text', '') for review in result['reviews'][:5]]

                # データ構造作成
                place_data = {
                    'title': title,
                    'address': address,
                    'rating': rating,
                    'review_count': review_count,
                    'reviews': reviews
                }

                # データベース保存
                if save_to_database(place_data, region_name):
                    print(f"  ✅ {title}")
                    collected_count += 1
                else:
                    print(f"  ⚠️ スキップ: {title}")

                time.sleep(0.1)  # API制限対策

            time.sleep(0.5)  # 都市間休憩

        time.sleep(1)  # キーワード間休憩

    print(f"📊 {region_name} バー・居酒屋収集完了: {collected_count}件")
    return collected_count

def main():
    """メイン実行関数"""
    print("🍺 バー・居酒屋ジャンル全国収集システム")
    print("=" * 70)
    print("📊 目標: 7地域 × 最大100件 = 最大700件")
    print("📋 ジャンル: gourmet_alcohol")
    print("=" * 70)

    total_collected = 0

    for region in REGIONS.keys():
        collected = collect_region_data(region)
        total_collected += collected

        print("⏱️ 次の地域まで3秒休憩...")
        time.sleep(3)

    print(f"\n🎉 全地域収集完了!")
    print(f"📊 総収集件数: {total_collected}件")
    print("🍺 gourmet_alcohol ジャンルのデータ収集が完了しました!")

if __name__ == "__main__":
    main()
