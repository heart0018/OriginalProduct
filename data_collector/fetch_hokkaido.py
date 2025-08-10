#!/usr/bin/env python3
"""
北海道多カテゴリスポット自動取得スクリプト
Google Places APIを使用して北海道の温泉・公園・サウナ・カフェデータを取得し、MySQLに保存する
(関東版と全く同じ仕様 / 均等配分=単一県なのでそのまま / トップアップ / 再配分 / サウナ第二フェーズ 対応)
"""

import os
import sys
import requests
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from typing import List, Dict, Optional
import time

# .env読み込み
load_dotenv()

class HokkaidoDataCollector:
    def __init__(self):
        self.google_api_key = os.getenv('GOOGLE_API_KEY')
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }
        self.places_api_base = 'https://maps.googleapis.com/maps/api/place'
        self.text_search_url = f"{self.places_api_base}/textsearch/json"
        self.place_details_url = f"{self.places_api_base}/details/json"

        # 北海道のみ（均等配分ロジックはそのまま利用）
        self.prefectures = ['北海道']

        self.search_categories = {
            'relax_onsen': {
                'base_terms': [
                    '温泉','銭湯','スーパー銭湯','天然温泉','日帰り温泉',
                    '温泉施設','入浴施設','岩盤浴'
                ],
                'queries': self._generate_regional_queries([
                    '温泉','銭湯','スーパー銭湯','天然温泉','日帰り温泉',
                    '温泉施設','入浴施設','岩盤浴'
                ]),
                'keywords': ['温泉','銭湯','スパ','spa','hot spring','bath house','入浴','岩盤浴'],
                'exclude_types': ['lodging','hotel'],
                'target_count': 100
            },
            'active_park': {
                'base_terms': [
                    '公園','都市公園','緑地','運動公園','道立公園',
                    '自然公園','森林公園','総合公園','散歩コース'
                ],
                'queries': self._generate_regional_queries([
                    '公園','都市公園','緑地','運動公園','道立公園',
                    '自然公園','森林公園','総合公園','散歩コース'
                ]),
                'keywords': ['公園','park','緑地','運動場','スポーツ','広場','散歩','遊歩道'],
                'exclude_types': ['lodging','hotel'],
                'target_count': 100
            },
            'active_sauna': {
                'base_terms': [
                    'サウナ','サウナ施設','個室サウナ','フィンランドサウナ',
                    'ロウリュ','サウナ&スパ','岩盤浴','テントサウナ',
                    '外気浴','水風呂','サウナラウンジ','サ活','高温サウナ',
                    '低温サウナ','ととのい','整い','発汗','サウナカフェ'
                ],
                'queries': self._generate_regional_queries([
                    'サウナ','サウナ施設','個室サウナ','フィンランドサウナ',
                    'ロウリュ','サウナ&スパ','岩盤浴','テントサウナ',
                    '外気浴','水風呂','サウナラウンジ','サ活','高温サウナ',
                    '低温サウナ','ととのい','整い','発汗','サウナカフェ'
                ]),
                'keywords': ['サウナ','sauna','ロウリュ','岩盤浴','テント','外気浴','水風呂','整','ととの','発汗','サ活'],
                'exclude_types': ['lodging','hotel'],
                'target_count': 100
            },
            'relax_cafe': {
                'base_terms': [
                    'カフェ','コーヒーショップ','動物カフェ','猫カフェ',
                    'ドッグカフェ','古民家カフェ','隠れ家カフェ','喫茶店'
                ],
                'queries': self._generate_regional_queries([
                    'カフェ','コーヒーショップ','動物カフェ','猫カフェ',
                    'ドッグカフェ','古民家カフェ','隠れ家カフェ','喫茶店'
                ]),
                'keywords': ['カフェ','cafe','coffee','コーヒー','喫茶','動物','猫','犬'],
                'exclude_types': ['lodging','hotel'],
                'target_count': 100
            }
        }
        self.total_target_count = 400

    def _generate_regional_queries(self, base_terms: List[str]) -> List[str]:
        queries = []
        for pref in self.prefectures:
            for term in base_terms:
                queries.append(f"{term} {pref}")
        for term in base_terms:
            queries.extend([
                f"{term} 北海道",
                f"北海道 {term}"
            ])
        return queries

    def validate_config(self):
        if not self.google_api_key:
            raise ValueError('GOOGLE_API_KEY 未設定')
        if not self.mysql_config['password']:
            raise ValueError('MYSQL_PASSWORD 未設定')
        print('✅ 設定OK')

    def search_places(self, query: str) -> List[Dict]:
        params = {
            'query': query,
            'key': self.google_api_key,
            'language': 'ja',
            'region': 'jp'
        }
        try:
            print(f"🔍 検索: {query}")
            r = requests.get(self.text_search_url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            if data.get('status') != 'OK':
                if data.get('status') != 'ZERO_RESULTS':
                    print(f"⚠️ 検索エラー {data.get('status')}")
                return []
            results = data.get('results', [])
            filtered = []
            for res in results:
                addr = res.get('formatted_address','')
                if any(pref in addr for pref in self.prefectures):
                    filtered.append(res)
            print(f"📍 北海道内候補 {len(filtered)}件")
            return filtered
        except requests.RequestException as e:
            print(f"❌ 検索失敗: {e}")
            return []

    def get_place_details(self, place_id: str) -> Optional[Dict]:
        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'language': 'ja',
            'fields': 'name,formatted_address,rating,user_ratings_total,photos,url,types,geometry,opening_hours,reviews'
        }
        try:
            r = requests.get(self.place_details_url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            if data.get('status') != 'OK':
                print(f"⚠️ 詳細NG {data.get('status')}")
                return None
            return data.get('result')
        except requests.RequestException as e:
            print(f"❌ 詳細取得失敗: {e}")
            return None

    def get_photo_url(self, photo_reference: str, max_width: int = 200) -> str:
        return f"{self.places_api_base}/photo?maxwidth={max_width}&photo_reference={photo_reference}&key={self.google_api_key}"

    def validate_place_id(self, place_id: str) -> bool:
        if not place_id:
            return False
        params = {'place_id': place_id,'key': self.google_api_key,'fields': 'place_id'}
        try:
            r = requests.get(self.place_details_url, params=params, timeout=10)
            r.raise_for_status()
            d = r.json()
            ok = d.get('status') == 'OK'
            print(f"  {'✅' if ok else '❌'} place_id検証 {place_id[:20]}...")
            return ok
        except requests.RequestException:
            print('  ❌ place_id検証通信失敗')
            return False

    def is_japanese_text(self, text: str) -> bool:
        if not text:
            return False
        stripped = text.replace(' ','').replace('\n','')
        if not stripped:
            return False
        jp = 0
        for ch in stripped:
            if ('\u3040' <= ch <= '\u309F') or ('\u30A0' <= ch <= '\u30FF') or ('\u4E00' <= ch <= '\u9FAF'):
                jp += 1
        return jp/len(stripped) >= 0.3

    def extract_japanese_reviews(self, reviews: List[Dict], max_count: int = 10) -> List[Dict]:
        if not reviews:
            return []
        jr = []
        for rv in reviews:
            txt = rv.get('text','')
            if self.is_japanese_text(txt):
                jr.append({
                    'text': txt,
                    'rating': rv.get('rating',0),
                    'time': rv.get('time',0),
                    'author_name': rv.get('author_name',''),
                    'relative_time_description': rv.get('relative_time_description','')
                })
        jr.sort(key=lambda x: x['time'], reverse=True)
        return jr[:max_count]

    def filter_places_by_category(self, places: List[Dict], category: str) -> List[Dict]:
        if category not in self.search_categories:
            return []
        cfg = self.search_categories[category]
        keywords = cfg['keywords']
        exclude = cfg['exclude_types']
        out = []
        for p in places:
            name = (p.get('name','') or '').lower()
            types = p.get('types',[]) or []
            has_kw = any(k in name for k in keywords)
            if not has_kw and category == 'active_sauna':
                sauna_frag = any(f in name for f in ['サウナ','整','ととの'])
                type_hint = any(t in types for t in ['spa','gym','health','establishment'])
                if sauna_frag and type_hint:
                    has_kw = True
            if not has_kw:
                continue
            if any(ex in types for ex in exclude):
                continue
            out.append(p)
        return out

    def format_place_data(self, place: Dict, category: str, details: Optional[Dict]=None) -> Dict:
        src = details or place
        name = src.get('name', place.get('name',''))[:128]
        address = src.get('formatted_address', place.get('formatted_address',''))[:128]
        rating = float(src.get('rating', place.get('rating',0.0)) or 0.0)
        review_count = int(src.get('user_ratings_total', place.get('user_ratings_total',0)) or 0)
        latitude = longitude = None
        geom = src.get('geometry',{})
        if 'location' in geom:
            latitude = geom['location'].get('lat')
            longitude = geom['location'].get('lng')
            print(f"  📍 ({latitude},{longitude})")
        image_url = None
        photos = src.get('photos',[]) or []
        if photos:
            ref = photos[0].get('photo_reference')
            if ref:
                url = self.get_photo_url(ref, max_width=200)
                if len(url) <= 1000:
                    image_url = url
                    print(f"  📸 画像OK len={len(url)}")
        external_link = src.get('url','')
        pid = place.get('place_id')
        if not external_link and pid:
            external_link = f"https://maps.google.com/?place_id={pid}"
        if len(external_link) > 256 and pid:
            external_link = f"https://maps.google.com/?place_id={pid}"[:256]
        reviews = []
        if details and 'reviews' in details:
            reviews = self.extract_japanese_reviews(details['reviews'], max_count=10)
            print(f"  💬 JPレビュー {len(reviews)}件")
        return {
            'genre': category,
            'title': name,
            'rating': rating,
            'review_count': review_count,
            'image_url': image_url,
            'external_link': external_link,
            'region': '北海道',
            'address': address,
            'latitude': latitude,
            'longitude': longitude,
            'place_id': pid,
            'reviews': reviews
        }

    def connect_database(self):
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            if conn.is_connected():
                print('✅ DB接続')
                return conn
        except Error as e:
            print(f'❌ DB接続失敗: {e}')
        return None

    def save_to_database(self, rows: List[Dict]):
        conn = self.connect_database()
        if not conn:
            return False
        try:
            cur = conn.cursor()
            check_q = 'SELECT id FROM cards WHERE place_id=%s'
            insert_card = (
                'INSERT INTO cards (genre,title,rating,review_count,image_url,external_link,region,address,latitude,longitude,place_id,created_at,updated_at) '
                'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())'
            )
            insert_rev = 'INSERT INTO review_comments (comment,card_id,created_at,updated_at) VALUES (%s,%s,NOW(),NOW())'
            ins = dup = rev_sum = 0
            for r in rows:
                pid = r.get('place_id')
                if not pid:
                    dup += 1
                    continue
                cur.execute(check_q,(pid,))
                if cur.fetchone():
                    dup += 1
                    continue
                cur.execute(insert_card,(
                    r['genre'],r['title'],r['rating'],r['review_count'],r['image_url'],r['external_link'],
                    r['region'],r['address'],r['latitude'],r['longitude'],r['place_id']
                ))
                card_id = cur.lastrowid
                added_rev = 0
                for rev in r.get('reviews',[]):
                    txt = rev['text']
                    if len(txt) > 1000:
                        txt = txt[:997]+'...'
                    cur.execute(insert_rev,(txt,card_id))
                    added_rev += 1
                rev_sum += added_rev
                ins += 1
                print(f"✅ 保存: {r['title']} (レビュー{added_rev})")
            conn.commit()
            print(f"\n📊 挿入 {ins} / 重複 {dup}  レビュー {rev_sum}")
            return True
        except Error as e:
            print(f'❌ DBエラー: {e}')
            conn.rollback()
            return False
        finally:
            if conn.is_connected():
                cur.close(); conn.close(); print('✅ DB切断')

    def _load_existing_place_ids(self) -> set:
        conn = self.connect_database()
        ids = set()
        if not conn:
            return ids
        try:
            cur = conn.cursor()
            cur.execute('SELECT place_id FROM cards WHERE place_id IS NOT NULL')
            for (pid,) in cur.fetchall():
                if pid: ids.add(pid)
        finally:
            if conn.is_connected():
                cur.close(); conn.close()
        return ids

    def _get_existing_counts(self, category: str):
        conn = self.connect_database()
        total = 0
        pref_counts = {p:0 for p in self.prefectures}
        if not conn:
            return total, pref_counts
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM cards WHERE genre=%s AND region='北海道'", (category,))
            total = cur.fetchone()[0]
            cur.execute("SELECT address FROM cards WHERE genre=%s AND region='北海道'", (category,))
            for (addr,) in cur.fetchall():
                if not addr:
                    continue
                if '北海道' in addr:
                    pref_counts['北海道'] += 1
        finally:
            if conn.is_connected():
                cur.close(); conn.close()
        return total, pref_counts

    def collect_data(self, category: Optional[str]=None):
        print('🚀 北海道 多カテゴリ収集開始')
        self.validate_config()
        ZERO_GAIN_LIMIT = 5
        REALLOC_ALLOW_DIFF = 1
        EXTRA_ONSEN = ['健康ランド','温浴','温浴施設','スパリゾート','リゾート温泉','源泉かけ流し','日帰り入浴','温泉センター']
        SAUNA_EXPAND = ['テントサウナ','外気浴','水風呂','ととのい','整い','高温サウナ','低温サウナ','サウナラウンジ','サ活','発汗']
        categories = [category] if category else list(self.search_categories.keys())
        all_rows = []
        for cat in categories:
            if cat not in self.search_categories:
                print(f'⚠️ 未知カテゴリ {cat}')
                continue
            cfg = self.search_categories[cat]
            full_target = cfg['target_count']
            existing_total, existing_pref = self._get_existing_counts(cat)
            existing_ids = self._load_existing_place_ids()
            if existing_total >= full_target:
                print(f"✅ {cat} 既に {full_target}件到達 スキップ")
                continue
            remaining = full_target - existing_total
            print(f"\n🔍 {cat}: 既存 {existing_total}/{full_target} → 追加 {remaining}")
            # 単一県なので quota=remaining
            quotas = {'北海道': remaining}
            print(f"🧮 追加クォータ: {quotas}")
            query_map = {'北海道':[f"{t} 北海道" for t in cfg['base_terms']]}
            collected = {}
            counts = {'北海道':0}
            exhausted = {'北海道': quotas['北海道']==0}
            zero_streak = {'北海道':0}
            rounds=0
            target = remaining
            while sum(counts.values()) < target and not all(exhausted.values()):
                rounds += 1
                pref = '北海道'
                if counts[pref] >= quotas[pref] or exhausted[pref]:
                    break
                if not query_map[pref]:
                    exhausted[pref] = True
                    break
                q = query_map[pref].pop(0)
                places = self.search_places(q)
                filtered = self.filter_places_by_category(places, cat)
                added=0
                for p in filtered:
                    if counts[pref] >= quotas[pref]:
                        break
                    pid = p.get('place_id')
                    addr = p.get('formatted_address','')
                    if not pid or pid in collected or pid in existing_ids:
                        continue
                    if '北海道' not in addr:
                        continue
                    collected[pid]=p
                    counts[pref]+=1
                    added+=1
                if added==0:
                    zero_streak[pref]+=1
                else:
                    zero_streak[pref]=0
                print(f"🔁 R{rounds} {pref} {q}: +{added} ({counts[pref]}/{quotas[pref]}) streak={zero_streak[pref]}")
                time.sleep(0.6)
                if zero_streak[pref] >= ZERO_GAIN_LIMIT and counts[pref] < quotas[pref]:
                    print(f"  ⛔ 連続0件{ZERO_GAIN_LIMIT}回 打ち切り")
                    exhausted[pref]=True
                if counts[pref] < quotas[pref] and not query_map[pref]:
                    extra_terms = cfg['keywords'][:3]
                    if cat=='relax_onsen':
                        extra_terms = list(dict.fromkeys(extra_terms + EXTRA_ONSEN))
                    if cat=='active_sauna':
                        extra_terms = list(dict.fromkeys(extra_terms + SAUNA_EXPAND))
                    query_map[pref] = [f"{t} 北海道" for t in extra_terms]
            deficit = target - sum(counts.values())
            if cat=='active_sauna' and deficit>0:
                print(f"🔥 active_sauna 第二フェーズ 不足 {deficit}")
                SECOND = ['セルフロウリュ','アウトドアサウナ','薪サウナ','貸切サウナ','プライベートサウナ','サウナテント','本格サウナ','サウナ 小規模','サウナ スパ','整いスペース','健康ランド サウナ','スパ サウナ','リラクゼーション サウナ']
                r2=0
                while deficit>0 and r2<40:
                    r2+=1
                    term = SECOND[r2 % len(SECOND)]
                    q = f"{term} 北海道"
                    places = self.search_places(q)
                    for p in places:
                        if deficit<=0:
                            break
                        pid = p.get('place_id')
                        if not pid or pid in collected or pid in existing_ids:
                            continue
                        addr = p.get('formatted_address','')
                        if '北海道' not in addr:
                            continue
                        name_low = (p.get('name','') or '').lower()
                        types = p.get('types',[]) or []
                        if not (any(k in name_low for k in ['サウナ','整','ととの','スパ','健康','岩盤']) or any(t in types for t in ['spa','health','gym','bath','establishment'])):
                            continue
                        collected[pid]=p
                        counts['北海道']+=1
                        deficit-=1
                    print(f"  🔍 第二R{r2} {q} 進捗 {counts['北海道']}/{quotas['北海道']} 残 deficit {deficit}")
                    time.sleep(0.5)
                    if r2>60: break
                if deficit>0:
                    print(f"⚠️ 第二フェーズ後も不足 {deficit}")
                else:
                    print('✅ 第二フェーズ充足')
            # 詳細取得
            target_fetch = min(counts['北海道'], target)
            print(f"📦 {cat} 詳細取得 {target_fetch}件")
            cat_rows=[]
            i=0
            for pid, place in list(collected.items()):
                if i>=target_fetch: break
                if pid in existing_ids: continue
                print(f"  ({i+1}/{target_fetch}) {place.get('name')} 詳細")
                if not self.validate_place_id(pid):
                    continue
                details = self.get_place_details(pid)
                time.sleep(0.7)
                row = self.format_place_data(place, cat, details)
                cat_rows.append(row)
                i+=1
            print(f"✅ {cat} 追加準備 {len(cat_rows)}件")
            all_rows.extend(cat_rows)
        print(f"\n💾 保存対象 {len(all_rows)}件")
        if all_rows:
            self.save_to_database(all_rows)
        else:
            print('ℹ️ 追加なし')
        print('🎉 処理完了')
        return True

def main():
    try:
        collector = HokkaidoDataCollector()
        cat = None
        if '--category' in sys.argv:
            idx = sys.argv.index('--category')
            if idx+1 < len(sys.argv):
                cat = sys.argv[idx+1]
        collector.collect_data(category=cat)
    except KeyboardInterrupt:
        print('\n⚠️ 中断')
    except Exception as e:
        print(f"\n❌ 予期せぬエラー: {e}")
        import traceback; traceback.print_exc()

if __name__ == '__main__':
    main()
