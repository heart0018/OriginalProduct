#!/usr/bin/env python3
"""
東北地方多カテゴリスポット自動取得スクリプト
Google Places APIを使用して東北の温泉・公園・サウナ・カフェデータを取得し、MySQLに保存する
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

class TohokuDataCollector:
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

        # 東北地方の県リスト
        self.tohoku_prefectures = ['青森', '岩手', '宮城', '秋田', '山形', '福島']

        # 検索設定（カテゴリ別・東北全域対応）
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
                'target_count': 100
            },
            'active_park': {
                'base_terms': [
                    "公園", "都市公園", "緑地", "運動公園", "県立公園",
                    "自然公園", "森林公園", "総合公園", "散歩コース"
                ],
                'queries': self._generate_regional_queries([
                    "公園", "都市公園", "緑地", "運動公園", "県立公園",
                    "自然公園", "森林公園", "総合公園", "散歩コース"
                ]),
                'keywords': ['公園', 'park', '緑地', '運動場', 'スポーツ', '広場', '散歩', '遊歩道'],
                'exclude_types': ['lodging', 'hotel'],
                'target_count': 100
            },
            'active_sauna': {
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
                'target_count': 100
            },
            'relax_cafe': {
                'base_terms': [
                    "カフェ", "コーヒーショップ", "動物カフェ", "猫カフェ",
                    "ドッグカフェ", "古民家カフェ", "隠れ家カフェ", "喫茶店"
                ],
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
        """東北全域の検索クエリを生成"""
        queries = []

        # 各県 × 各基本用語の組み合わせを生成
        for prefecture in self.tohoku_prefectures:
            for term in base_terms:
                queries.append(f"{term} {prefecture}")

        # 東北全域での一般的な検索も追加
        for term in base_terms:
            queries.extend([
                f"{term} 東北",
                f"{term} 東北地方",
                f"東北 {term}"
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
        """Google Places APIでテキスト検索（東北全域対応）"""
        # 位置情報がない場合は東北地方の中心付近を使用
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

            # 東北地方の住所を持つ結果のみフィルタリング
            tohoku_results = []
            for result in results:
                address = result.get('formatted_address', '')
                if any(prefecture in address for prefecture in self.tohoku_prefectures):
                    tohoku_results.append(result)

            print(f"📍 東北地方内: {len(tohoku_results)}件の候補を発見")
            return tohoku_results

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
            data = get_json(self.place_details_url, params, ttl_sec=60*60*24*30)

            if data.get('status') == 'OK':
                print(f"  ✅ place_id有効: {place_id[:20]}...")
                return True
            else:
                print(f"  ❌ place_id無効: {place_id[:20]}... (status: {data.get('status')})")
                return False

        except Exception as e:
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
            types = place.get('types', []) or []

            # キーワードチェック
            has_keyword = any(k in name for k in keywords)

            # active_sauna 緩和: types に spa / gym / health / establishment があり、名前にサウナ系断片("サウナ"/"整"/"ととの") があれば許容
            if not has_keyword and category == 'active_sauna':
                sauna_frag = any(frag in name for frag in ['サウナ', '整', 'ととの'])
                type_hint = any(t in types for t in ['spa', 'gym', 'health', 'establishment'])
                if sauna_frag and type_hint:
                    has_keyword = True

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
            'region': '東北',  # 東北地区
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

    def _get_existing_counts(self, category: str):
        """指定カテゴリの既存総数と県別カウントを取得"""
        connection = self.connect_database()
        total = 0
        prefect_counts = {p: 0 for p in self.tohoku_prefectures}
        if not connection:
            return total, prefect_counts
        try:
            cur = connection.cursor()
            # 総数
            cur.execute("SELECT COUNT(*) FROM cards WHERE genre=%s AND region='東北'", (category,))
            total = cur.fetchone()[0]
            # 住所を取得して県判定
            cur.execute("SELECT address FROM cards WHERE genre=%s AND region='東北'", (category,))
            for (addr,) in cur.fetchall():
                if not addr:
                    continue
                for pref in self.tohoku_prefectures:
                    if pref in addr:
                        prefect_counts[pref] += 1
                        break
        finally:
            if connection.is_connected():
                cur.close()
                connection.close()
        return total, prefect_counts

    def collect_data(self, category: Optional[str] = None):
        """均等配分アルゴリズムでのデータ収集（県ごとのクォータ厳守）
        category を指定した場合、そのカテゴリのみ不足分を追加収集(トップアップ)する"""
        print("🚀 東北全域多カテゴリデータ収集開始 (均等/トップアップモード)")
        self.validate_config()

        ZERO_GAIN_LIMIT = 5
        REALLOC_ALLOW_DIFF = 1
        EXTRA_ONSEN_EXPAND_TERMS = [
            "健康ランド", "温浴", "温浴施設", "スパリゾート", "リゾート温泉",
            "源泉かけ流し", "日帰り入浴", "温泉センター"
        ]
        SAUNA_EXPAND_TERMS = ["テントサウナ", "外気浴", "水風呂", "ととのい", "整い", "高温サウナ", "低温サウナ", "サウナラウンジ", "サ活", "発汗"]

        def build_prefecture_quotas(total: int) -> Dict[str, int]:
            base = total // len(self.tohoku_prefectures)
            rem = total % len(self.tohoku_prefectures)
            quotas = {}
            for i, pref in enumerate(self.tohoku_prefectures):
                quotas[pref] = base + (1 if i < rem else 0)
            return quotas

        # 対象カテゴリリスト
        categories = [category] if category else list(self.search_categories.keys())

        all_formatted = []

        for cat in categories:
            if cat not in self.search_categories:
                print(f"⚠️ 未知カテゴリ: {cat} スキップ")
                continue
            cfg = self.search_categories[cat]
            full_target = cfg['target_count']

            # 既存数取得（トップアップ用途）
            existing_total, existing_pref_counts = self._get_existing_counts(cat)
            existing_place_ids = self._load_existing_place_ids()  # 追加: 既存除外
            if existing_total >= full_target:
                print(f"✅ {cat}: 既に目標{full_target}件に達しているためスキップ")
                continue
            remaining_target = full_target - existing_total
            print(f"\n🔍 {cat}: 既存 {existing_total}/{full_target} → 追加取得目標 {remaining_target}件")

            # 既存分を考慮した県別不足計算 (理想=full_targetを均等割当)
            ideal_full = build_prefecture_quotas(full_target)
            deficits = {p: max(0, ideal_full[p] - existing_pref_counts.get(p, 0)) for p in self.tohoku_prefectures}
            total_deficit = sum(deficits.values())

            # 追加クォータ決定
            if total_deficit == 0:
                # 理論上均等 → 残り件数を均等割り
                quotas = build_prefecture_quotas(remaining_target)
            else:
                # 不足が多い県を優先しつつ remaining_target を配分
                quotas = {p: 0 for p in self.tohoku_prefectures}
                need = remaining_target
                ordered = sorted(deficits.items(), key=lambda x: x[1], reverse=True)
                # ラウンドロビンで deficit 消化
                while need > 0:
                    progress = 0
                    for pref, deficit in ordered:
                        if need <= 0:
                            break
                        if quotas[pref] >= deficit:
                            continue
                        quotas[pref] += 1
                        need -= 1
                        progress += 1
                    if progress == 0:
                        break
                # 念のため: 未割当があれば均等配分
                if need > 0:
                    tmp = build_prefecture_quotas(need)
                    for p, v in tmp.items():
                        quotas[p] += v
            # 0割当県は収集ループでスキップされる
            print(f"🧮 県別追加クォータ: {quotas}")

            prefecture_query_map: Dict[str, List[str]] = {}
            for pref in self.tohoku_prefectures:
                if quotas.get(pref, 0) <= 0:
                    prefecture_query_map[pref] = []
                    continue
                qlist = [f"{term} {pref}" for term in cfg['base_terms']]
                prefecture_query_map[pref] = qlist

            collected_places: Dict[str, Dict] = {}
            counts = {p: 0 for p in self.tohoku_prefectures}
            exhausted = {p: quotas.get(p, 0) == 0 for p in self.tohoku_prefectures}
            zero_gain_streak = {p: 0 for p in self.tohoku_prefectures}

            rounds = 0
            target = remaining_target  # 以降この変数で不足分ターゲットを扱う
            while sum(counts.values()) < target and not all(exhausted.values()):
                rounds += 1
                for pref in self.tohoku_prefectures:
                    if counts[pref] >= quotas.get(pref, 0):
                        continue
                    if exhausted[pref]:
                        continue
                    if not prefecture_query_map[pref]:
                        exhausted[pref] = True
                        continue
                    query = prefecture_query_map[pref].pop(0)
                    places = self.search_places(query)
                    filtered = self.filter_places_by_category(places, cat)
                    added = 0
                    for place in filtered:
                        if counts[pref] >= quotas[pref]:
                            break
                        place_id = place.get('place_id')
                        address = place.get('formatted_address', '')
                        if not place_id or place_id in collected_places or place_id in existing_place_ids:
                            continue
                        if pref not in address:
                            continue
                        collected_places[place_id] = place
                        counts[pref] += 1
                        added += 1
                    if added == 0:
                        zero_gain_streak[pref] += 1
                    else:
                        zero_gain_streak[pref] = 0
                    print(f"🔁 R{rounds} {pref} {query}: +{added} (追加累計 {counts[pref]}/{quotas[pref]}) streak={zero_gain_streak[pref]}")
                    time.sleep(0.6)
                    if zero_gain_streak[pref] >= ZERO_GAIN_LIMIT and counts[pref] < quotas[pref]:
                        print(f"  ⛔ {pref} 連続0件{ZERO_GAIN_LIMIT}回で打ち切り (不足 {quotas[pref]-counts[pref]})")
                        exhausted[pref] = True
                        continue
                    if counts[pref] >= quotas[pref]:
                        continue
                    if not prefecture_query_map[pref]:
                        extra_terms = cfg['keywords'][:3]
                        if cat == 'relax_onsen' and pref in ['青森', '秋田', '山形']:
                            extra_terms = list(dict.fromkeys(extra_terms + EXTRA_ONSEN_EXPAND_TERMS))
                        if cat == 'active_sauna':
                            extra_terms = list(dict.fromkeys(extra_terms + SAUNA_EXPAND_TERMS))
                        regenerated = [f"{t} {pref}" for t in extra_terms]
                        prefecture_query_map[pref] = regenerated
                        if not regenerated:
                            exhausted[pref] = True
                # 内側for終わり
                if rounds > 100:
                    print("⚠️ ラウンド上限到達。打ち切り。")
                    break

            total_collected = sum(counts.values())
            deficit = target - total_collected
            if deficit > 0:
                print(f"⚠️ 追加目標未達 (一次収集): {total_collected}/{target} 不足 {deficit}件 → 再配分フェーズ")
                realloc_rounds = 0
                for pref in self.tohoku_prefectures:
                    if exhausted[pref]:
                        continue
                    allowed = quotas.get(pref, 0) + REALLOC_ALLOW_DIFF - counts[pref]
                    if allowed <= 0:
                        continue
                    if not prefecture_query_map[pref]:
                        base_extra = cfg['keywords'][:5]
                        if cat == 'relax_onsen' and pref in ['青森', '秋田', '山形']:
                            base_extra = list(dict.fromkeys(base_extra + EXTRA_ONSEN_EXPAND_TERMS))
                        if cat == 'active_sauna':
                            base_extra = list(dict.fromkeys(base_extra + SAUNA_EXPAND_TERMS))
                        prefecture_query_map[pref] = [f"{t} {pref}" for t in base_extra]
                while deficit > 0 and realloc_rounds < 50:
                    realloc_rounds += 1
                    progress = 0
                    for pref in self.tohoku_prefectures:
                        if deficit <= 0:
                            break
                        if counts[pref] >= quotas.get(pref, 0) + REALLOC_ALLOW_DIFF:
                            continue
                        if not prefecture_query_map[pref]:
                            continue
                        query = prefecture_query_map[pref].pop(0)
                        places = self.search_places(query)
                        filtered = self.filter_places_by_category(places, cat)
                        for place in filtered:
                            if deficit <= 0:
                                break
                            if counts[pref] >= quotas.get(pref, 0) + REALLOC_ALLOW_DIFF:
                                break
                            pid = place.get('place_id')
                            addr = place.get('formatted_address', '')
                            if not pid or pid in collected_places or pid in existing_place_ids:
                                continue
                            if pref not in addr:
                                continue
                            collected_places[pid] = place
                            counts[pref] += 1
                            deficit -= 1
                            progress += 1
                        print(f"  ♻ 再配分R{realloc_rounds} {pref} {query}: 現在 {counts[pref]} / 上限 {quotas.get(pref,0) + REALLOC_ALLOW_DIFF} 残り不足 {deficit}")
                        time.sleep(0.4)
                    if progress == 0:
                        print("  ⛔ 再配分進捗なし → 打ち切り")
                        break
                if deficit > 0:
                    print(f"⚠️ 再配分後も不足: {deficit}件 (今回追加 {total_collected - (target - (full_target - existing_total))}件)")
                else:
                    print("✅ 再配分で追加目標充足")

            # ここから active_sauna 専用第二フェーズ（不足継続時）
            if cat == 'active_sauna' and deficit > 0:
                print(f"🔥 active_sauna 第二フェーズ突入: まだ {deficit}件不足 (緩和探索)")
                SECOND_PHASE_TERMS = [
                    "セルフロウリュ", "アウトドアサウナ", "薪サウナ", "貸切サウナ", "プライベートサウナ",
                    "サウナテント", "本格サウナ", "サウナ 小規模", "サウナ スパ", "整いスペース",
                    "健康ランド サウナ", "スパ サウナ", "リラクゼーション サウナ"
                ]
                # 不足県のみ
                deficit_prefs = [p for p in self.tohoku_prefectures if quotas.get(p,0) > 0 and counts[p] < quotas[p] + REALLOC_ALLOW_DIFF]
                if not deficit_prefs:
                    deficit_prefs = self.tohoku_prefectures  # 念のため
                rounds2 = 0
                existing_place_ids = self._load_existing_place_ids()
                while deficit > 0 and rounds2 < 40:
                    rounds2 += 1
                    progress2 = 0
                    for pref in deficit_prefs:
                        if deficit <= 0:
                            break
                        # 緩和上限: quotas[pref] + REALLOC_ALLOW_DIFF まで
                        if counts[pref] >= quotas.get(pref,0) + REALLOC_ALLOW_DIFF:
                            continue
                        # クエリ生成: SECOND_PHASE_TERMS から1件ずつ (ラウンドロビン)
                        term = SECOND_PHASE_TERMS[rounds2 % len(SECOND_PHASE_TERMS)]
                        query = f"{term} {pref}"
                        places = self.search_places(query)
                        if not places:
                            continue
                        # 緩和フィルタ: 元フィルタ + (名前にサウナ/整/ととの/スパ/健康ランド/岩盤浴) か types に spa/health/bath があれば
                        for place in places:
                            if deficit <= 0:
                                break
                            place_id = place.get('place_id')
                            if not place_id or place_id in existing_place_ids or place_id in collected_places:
                                continue
                            addr = place.get('formatted_address', '')
                            if pref not in addr:
                                continue
                            name_low = (place.get('name','') or '').lower()
                            types = place.get('types', []) or []
                            name_hit = any(k in name_low for k in ['サウナ','整','ととの','スパ','健康','岩盤'])
                            type_hit = any(t in types for t in ['spa','health','gym','bath','establishment'])
                            if not (name_hit or type_hit):
                                continue
                            collected_places[place_id] = place
                            counts[pref] += 1
                            deficit -= 1
                            progress2 += 1
                        print(f"  🔍 第二R{rounds2} {pref} {query}: 進捗 {counts[pref]}/{quotas.get(pref,0)+REALLOC_ALLOW_DIFF} 残り不足 {deficit}")
                        time.sleep(0.5)
                    if progress2 == 0:
                        print("  ⛔ 第二フェーズ進捗なし → 打ち切り")
                        break
                if deficit > 0:
                    print(f"⚠️ 第二フェーズ後も不足: {deficit}件 (これ以上は新規place_id枯渇の可能性)" )
                else:
                    print("✅ 第二フェーズで不足解消")

            # 詳細取得（追加分のみ）
            print(f"📦 {cat} 追加分 詳細取得開始: {min(sum(counts.values()), target)}件")
            category_data = []
            for i, (pid, place) in enumerate(list(collected_places.items()), 1):
                if i > target:
                    break
                if pid in existing_place_ids:
                    continue
                print(f"  ({i}/{target}) {place.get('name')} 詳細取得")
                if not self.validate_place_id(pid):
                    continue
                details = self.get_place_details(pid)
                time.sleep(0.7)
                formatted = self.format_place_data(place, cat, details)
                category_data.append(formatted)
                if i % 20 == 0:
                    print(f"    進捗: {i}/{target}")

            print("📊 追加後 県別増加件数:")
            for pref in self.tohoku_prefectures:
                inc = counts[pref]
                if inc > 0:
                    print(f"  • {pref}: +{inc}")
            all_formatted.extend(category_data)
            print(f"✅ {cat} 追加完了: {len(category_data)}件 (DB挿入時に重複除外の可能性あり)")

        print(f"\n💾 保存処理: 今回追加 {len(all_formatted)}件")
        if all_formatted:
            self.save_to_database(all_formatted)
        else:
            print("ℹ️ 追加対象なし (保存スキップ)")
        print("🎉 指定カテゴリ処理終了")
        return True

    def _load_existing_place_ids(self) -> set:
        """全cardsのplace_idを読み込み重複除外用集合を返す"""
        connection = self.connect_database()
        ids = set()
        if not connection:
            return ids
        try:
            cur = connection.cursor()
            cur.execute("SELECT place_id FROM cards WHERE place_id IS NOT NULL")
            for (pid,) in cur.fetchall():
                if pid:
                    ids.add(pid)
        finally:
            if connection.is_connected():
                cur.close()
                connection.close()
        return ids

def main():
    """メイン関数"""
    try:
        collector = TohokuDataCollector()
        # CLI引数 --category <name>
        cat = None
        if '--category' in sys.argv:
            idx = sys.argv.index('--category')
            if idx + 1 < len(sys.argv):
                cat = sys.argv[idx + 1]
        collector.collect_data(category=cat)
    except KeyboardInterrupt:
        print("\n⚠️  処理が中断されました")
    except Exception as e:
        print(f"\n❌ 予期しないエラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
