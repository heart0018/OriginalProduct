#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ランダムジャンル × 8地域 × 20件 = 160件 収集ランナー
- 既存のリクエストガード(get_json/Details重複抑止/Photo直URLキャッシュ)を利用
- グルメ系ジャンルの中から1つランダム選択
- 地域は8分割: 北海道 / 東北 / 関東 / 中部 / 関西 / 中国 / 四国 / 九州沖縄
- 各地域ごとに主要都市×キーワードでTextSearchし、Detailsを取得してcardsに保存
"""

import os
import time
import random
import mysql.connector
from dotenv import load_dotenv
from typing import Dict, List, Optional
from utils.request_guard import get_json, already_fetched_place, mark_fetched_place

# .env 読み込み
load_dotenv()

PLACES_TEXT_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

class RandomGenre8RegionRunner:
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GOOGLE_PLACES_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY")
        if not self.api_key:
            raise ValueError("Google API Key not set (GOOGLE_API_KEY/GOOGLE_PLACES_API_KEY/GOOGLE_MAPS_API_KEY)")

        self.db_conf = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_production',
            'charset': 'utf8mb4'
        }
        if not self.db_conf['password']:
            raise ValueError('MYSQL_PASSWORD not set')

        # 8地域の主要都市
        self.regions: Dict[str, Dict] = {
            'hokkaido': {
                'name': '北海道',
                'cities': ['札幌', '函館', '旭川', '釧路', '帯広', '北見', '小樽', '室蘭']
            },
            'tohoku': {
                'name': '東北',
                'cities': ['仙台', '青森', '盛岡', '秋田', '山形', '福島', '八戸', '郡山']
            },
            'kanto': {
                'name': '関東',
                'cities': ['東京', '横浜', '千葉', 'さいたま', '宇都宮', '前橋', '水戸', '川崎']
            },
            'chubu': {
                'name': '中部',
                'cities': ['名古屋', '新潟', '金沢', '富山', '福井', '甲府', '長野', '岐阜', '静岡']
            },
            'kansai': {
                'name': '関西',
                'cities': ['大阪', '京都', '神戸', '奈良', '和歌山', '大津', '津']
            },
            'chugoku': {
                'name': '中国',
                'cities': ['広島', '岡山', '山口', '鳥取', '松江']
            },
            'shikoku': {
                'name': '四国',
                'cities': ['高松', '松山', '高知', '徳島']
            },
            'kyushu_okinawa': {
                'name': '九州沖縄',
                'cities': ['福岡', '北九州', '熊本', '鹿児島', '長崎', '大分', '宮崎', '佐賀', '那覇']
            }
        }

        # 住所→8地域の簡易マッピング（都道府県の一部省略形に対応）
        self.pref_to_region = {
            # 北海道
            '北海道': 'hokkaido',

            # 東北
            '青森': 'tohoku', '岩手': 'tohoku', '宮城': 'tohoku', '秋田': 'tohoku', '山形': 'tohoku', '福島': 'tohoku',

            # 関東
            '茨城': 'kanto', '栃木': 'kanto', '群馬': 'kanto', '埼玉': 'kanto', '千葉': 'kanto', '東京': 'kanto', '神奈川': 'kanto',

            # 中部
            '新潟': 'chubu', '富山': 'chubu', '石川': 'chubu', '福井': 'chubu', '山梨': 'chubu', '長野': 'chubu', '岐阜': 'chubu', '静岡': 'chubu', '愛知': 'chubu',

            # 関西
            '三重': 'kansai', '滋賀': 'kansai', '京都': 'kansai', '大阪': 'kansai', '兵庫': 'kansai', '奈良': 'kansai', '和歌山': 'kansai',

            # 中国
            '鳥取': 'chugoku', '島根': 'chugoku', '岡山': 'chugoku', '広島': 'chugoku', '山口': 'chugoku',

            # 四国
            '徳島': 'shikoku', '香川': 'shikoku', '愛媛': 'shikoku', '高知': 'shikoku',

            # 九州・沖縄
            '福岡': 'kyushu_okinawa', '佐賀': 'kyushu_okinawa', '長崎': 'kyushu_okinawa', '熊本': 'kyushu_okinawa', '大分': 'kyushu_okinawa', '宮崎': 'kyushu_okinawa', '鹿児島': 'kyushu_okinawa', '沖縄': 'kyushu_okinawa'
        }

        # ジャンル候補（グルメ系）: 既存リアルタイムコレクタのカテゴリを採用
        # キーワードは呼び出し時にRealtimeGourmetCollectorから取得
        from realtime_gourmet_collector import RealtimeGourmetCollector  # lazy import
        self.gourmet_source = RealtimeGourmetCollector()
        self.available_genres = list(self.gourmet_source.gourmet_categories.keys())

    def _region_from_address(self, address: str) -> Optional[str]:
        if not address:
            return None
        # 代表的な表記ゆれ
        if '東京都' in address:
            return 'kanto'
        if '京都府' in address:
            return 'kansai'
        if '大阪府' in address:
            return 'kansai'
        for pref, reg in self.pref_to_region.items():
            if pref in address:
                return reg
        return None

    def _search(self, query: str) -> List[Dict]:
        params = {
            'query': query,
            'key': self.api_key,
            'language': 'ja',
            'region': 'jp'
        }
        try:
            data = get_json(PLACES_TEXT_URL, params, ttl_sec=60*60*24*7)
            if data.get('status') != 'OK':
                return []
            return data.get('results', [])
        except Exception:
            return []

    def _details(self, place_id: str) -> Dict:
        if already_fetched_place(place_id):
            return {}
        params = {
            'place_id': place_id,
            'fields': 'name,formatted_address,rating,user_ratings_total,photos,reviews,formatted_phone_number,website,opening_hours,geometry',
            'key': self.api_key,
            'language': 'ja'
        }
        try:
            data = get_json(PLACES_DETAILS_URL, params, ttl_sec=60*60*24*30)
            res = data.get('result', {}) if isinstance(data, dict) else {}
            mark_fetched_place(place_id)
            return res
        except Exception:
            return {}

    def _connect_db(self):
        return mysql.connector.connect(**self.db_conf)

    def _save_card(self, card: Dict, reviews: List[str]) -> bool:
        """cardsテーブルへ保存（place_id重複スキップ + レビューコメント保存）
        既存スキーマに合わせ、phone/website/opening_hoursは保存しない。
        カラム: genre,title,rating,review_count,image_url,external_link,region,address,latitude,longitude,place_id,created_at,updated_at
        """
        conn = self._connect_db()
        try:
            cur = conn.cursor()
            # 既存チェック(place_id優先)
            cur.execute("SELECT id FROM cards WHERE place_id=%s", (card['place_id'],))
            if cur.fetchone():
                return False

            insert_card = (
                "INSERT INTO cards (genre,title,rating,review_count,image_url,external_link,region,address,latitude,longitude,place_id,created_at,updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())"
            )
            insert_rev = "INSERT INTO review_comments (comment,card_id,created_at,updated_at) VALUES (%s,%s,NOW(),NOW())"

            cur.execute(insert_card, (
                card.get('genre'),
                card.get('title'),
                card.get('rating', 0.0),
                card.get('review_count', 0),
                card.get('image_url'),
                card.get('external_link'),
                card.get('region'),
                card.get('address'),
                card.get('latitude'),
                card.get('longitude'),
                card.get('place_id'),
            ))
            card_id = cur.lastrowid

            for txt in reviews[:5]:
                if not txt:
                    continue
                if len(txt) > 1000:
                    txt = txt[:997] + '...'
                cur.execute(insert_rev, (txt, card_id))

            conn.commit()
            return True
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def collect(self, chosen_genre: Optional[str] = None, per_region: int = 20) -> Dict[str, int]:
        """
        8地域を順に処理し、各地域でper_region件を目標に収集して保存。
        戻り値: 地域ごとの保存件数
        """
        # ジャンル決定
        if not chosen_genre:
            chosen_genre = random.choice(self.available_genres)
        if chosen_genre not in self.available_genres:
            raise ValueError(f"Unknown genre: {chosen_genre}")

        search_terms = self.gourmet_source.gourmet_categories[chosen_genre]['search_terms']
        print(f"\n🎯 選択ジャンル: {chosen_genre}  / 用語: {len(search_terms)}個")

        saved_counts: Dict[str, int] = {}

        for region_key, reg in self.regions.items():
            target = per_region
            collected = 0
            seen_place_ids = set()
            print(f"\n🗾 地域: {reg['name']} ({region_key}) 目標 {target}件")

            for term in search_terms:
                if collected >= target:
                    break
                for city in reg['cities']:
                    if collected >= target:
                        break

                    query = f"{term} {city}"
                    results = self._search(query)
                    if not results:
                        continue

                    for place in results:
                        if collected >= target:
                            break
                        pid = place.get('place_id')
                        if not pid or pid in seen_place_ids:
                            continue
                        seen_place_ids.add(pid)

                        # 住所から地域確認
                        addr = place.get('formatted_address', '')
                        resolved_region = self._region_from_address(addr)
                        if resolved_region != region_key:
                            continue

                        # 詳細取得
                        details = self._details(pid)
                        # 画像URLは省略（Photo直URLは別途キャッシュ管理。ここでは外部リンク重視）
                        name = details.get('name') or place.get('name')
                        address = details.get('formatted_address') or addr
                        rating = details.get('rating', place.get('rating', 0.0) or 0.0)
                        review_count = details.get('user_ratings_total', place.get('user_ratings_total', 0) or 0)
                        website = details.get('website')
                        phone = details.get('formatted_phone_number')
                        opening_hours = None
                        if details.get('opening_hours') and isinstance(details['opening_hours'], dict):
                            opening_hours = str(details['opening_hours'].get('weekday_text', []))

                        # 座標
                        lat = lon = None
                        geom = details.get('geometry') or place.get('geometry') or {}
                        if geom and geom.get('location'):
                            lat = geom['location'].get('lat')
                            lon = geom['location'].get('lng')

                        # 外部リンク（place_idベースの安定URL）
                        external_link = f"https://maps.google.com/?place_id={pid}"

                        card = {
                            'genre': chosen_genre,
                            'title': (name or '')[:128],
                            'rating': float(rating) if rating else 0.0,
                            'review_count': int(review_count) if review_count else 0,
                            'image_url': None,
                            'external_link': external_link[:256],
                            'region': region_key,
                            'address': (address or '')[:128],
                            'latitude': lat,
                            'longitude': lon,
                            'place_id': pid,
                            'phone': phone,
                            'website': website,
                            'opening_hours': opening_hours,
                        }

                        # レビュー（テキストのみ、最大5件）
                        reviews = []
                        for rv in (details.get('reviews') or [])[:5]:
                            txt = rv.get('text', '').strip()
                            if txt and len(txt) >= 5:
                                reviews.append(txt)

                        if self._save_card(card, reviews):
                            collected += 1
                            print(f"  ✅ 保存: {card['title']}  ({collected}/{target})")
                        else:
                            # 重複など
                            pass

                        # 軽い間隔（QPS/並列はguard側で制御）
                        time.sleep(0.05)

            saved_counts[region_key] = collected
            print(f"📊 地域完了: {reg['name']} → {collected}件")
            time.sleep(0.5)

        total = sum(saved_counts.values())
        print(f"\n🎉 合計保存: {total}件 (期待値: {per_region * len(self.regions)})")
        return saved_counts


def main():
    import argparse
    parser = argparse.ArgumentParser(description='ランダムジャンル×8地域収集ランナー')
    parser.add_argument('--genre', help='固定ジャンルキーを指定（例: gourmet_chinese）', default=None)
    parser.add_argument('--per-region', type=int, default=20, help='各地域の目標件数')
    args = parser.parse_args()

    runner = RandomGenre8RegionRunner()
    runner.collect(chosen_genre=args.genre, per_region=args.per_region)


if __name__ == '__main__':
    main()
