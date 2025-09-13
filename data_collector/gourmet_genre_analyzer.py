#!/usr/bin/env python3
"""
グルメカテゴリAPI調査スクリプト
各ジャンルのAPIレスポンスとgenre情報を確認（DB保存なし）
"""

import os
import time
import random
import requests
import json
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()

class GourmetGenreAnalyzer:
    def __init__(self):
        self.google_api_key = os.getenv('GOOGLE_API_KEY')

        if not self.google_api_key:
            raise ValueError("GOOGLE_API_KEY が環境変数に設定されていません")

        # グルメカテゴリ設定
        self.gourmet_categories = {
            'gourmet_yoshoku': ['洋食', '洋食レストラン', 'イタリアン', 'フレンチ'],
            'gourmet_washoku': ['和食', '和食レストラン', '日本料理', '懐石'],
            'gourmet_chinese': ['中華', '中華料理', '中国料理', '四川料理'],
            'gourmet_bar': ['Bar', 'バー', 'ワインバー', 'カクテルバー'],
            'gourmet_izakaya': ['居酒屋', '個人店 居酒屋', '小規模 居酒屋', '地元 居酒屋']
        }

        # 検索エリア（東京中心）
        self.search_areas = ['渋谷', '新宿', '池袋', '銀座', '六本木']

    def search_places_api(self, query: str, location: str = '東京') -> List[Dict]:
        """Places APIでグルメスポットを検索"""
        search_query = f"{query} {location}"

        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {
            'query': search_query,
            'language': 'ja',
            'region': 'JP',
            'type': 'restaurant',  # レストランタイプ指定
            'key': self.google_api_key,
        }

        try:
            print(f"🔍 検索中: {search_query}")
            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            if data['status'] == 'OK':
                return data.get('results', [])
            elif data['status'] == 'OVER_QUERY_LIMIT':
                print(f"  ❌ API制限に達しました")
                return []
            else:
                print(f"  ❌ 検索エラー: {data['status']}")
                return []

        except Exception as e:
            print(f"  ❌ 検索エラー: {e}")
            return []

    def extract_place_info(self, place: Dict) -> Dict:
        """Places APIレスポンスから重要情報を抽出"""
        return {
            'place_id': place.get('place_id'),
            'name': place.get('name'),
            'types': place.get('types', []),
            'rating': place.get('rating', 0),
            'price_level': place.get('price_level'),
            'formatted_address': place.get('formatted_address', ''),
            'business_status': place.get('business_status'),
            'user_ratings_total': place.get('user_ratings_total', 0)
        }

    def analyze_place_types(self, types: List[str]) -> Dict:
        """place typesからカテゴリ分析"""
        # Google Places APIの主要types
        restaurant_types = [
            'restaurant', 'food', 'establishment', 'point_of_interest',
            'meal_takeaway', 'meal_delivery', 'cafe', 'bar', 'night_club'
        ]

        cuisine_types = [
            'japanese_restaurant', 'chinese_restaurant', 'italian_restaurant',
            'french_restaurant', 'american_restaurant', 'korean_restaurant'
        ]

        found_restaurant_types = [t for t in types if t in restaurant_types]
        found_cuisine_types = [t for t in types if t in cuisine_types]
        other_types = [t for t in types if t not in restaurant_types and t not in cuisine_types]

        return {
            'restaurant_types': found_restaurant_types,
            'cuisine_types': found_cuisine_types,
            'other_types': other_types,
            'all_types': types
        }

    def investigate_gourmet_genres(self):
        """グルメカテゴリのgenre調査を実行"""
        print("🍽️ グルメカテゴリAPI調査システム")
        print("=" * 60)
        print("📊 目的: APIレスポンスのgenre/types情報確認")
        print("📊 対象: 洋食・和食・中華・Bar・居酒屋")
        print("=" * 60)

        all_results = {}

        for category, queries in self.gourmet_categories.items():
            print(f"\n🍽️ カテゴリ: {category}")
            print("-" * 40)

            category_results = []

            # 各クエリで検索（最大5件まで）
            collected_count = 0
            for query in queries:
                if collected_count >= 5:
                    break

                # ランダムな地域で検索
                location = random.choice(self.search_areas)
                places = self.search_places_api(query, location)

                for place in places:
                    if collected_count >= 5:
                        break

                    place_info = self.extract_place_info(place)
                    type_analysis = self.analyze_place_types(place_info['types'])

                    result = {
                        'category': category,
                        'query': query,
                        'place_info': place_info,
                        'type_analysis': type_analysis
                    }

                    category_results.append(result)
                    collected_count += 1

                    # 結果表示
                    print(f"  ✓ {place_info['name']}")
                    print(f"    📍 {place_info['formatted_address'][:50]}...")
                    print(f"    ⭐ 評価: {place_info['rating']} ({place_info['user_ratings_total']}件)")
                    print(f"    🏷️ Types: {', '.join(place_info['types'][:3])}...")
                    print(f"    🍽️ 料理Types: {type_analysis['cuisine_types']}")
                    print()

                # API制限対策
                time.sleep(random.uniform(1, 2))

            all_results[category] = category_results
            print(f"  📊 {category} 収集完了: {len(category_results)}件")

            # カテゴリ間の休憩
            time.sleep(3)

        return all_results

    def analyze_genre_patterns(self, results: Dict):
        """収集結果からgenreパターンを分析"""
        print(f"\n📊 グルメgenre分析結果")
        print("=" * 50)

        total_items = 0
        all_types = set()
        cuisine_type_frequency = {}

        for category, items in results.items():
            print(f"\n🍽️ {category} 分析:")

            category_types = set()
            for item in items:
                place_info = item['place_info']
                type_analysis = item['type_analysis']

                # 全typesを収集
                all_types.update(place_info['types'])
                category_types.update(place_info['types'])

                # 料理タイプの頻度
                for cuisine_type in type_analysis['cuisine_types']:
                    cuisine_type_frequency[cuisine_type] = cuisine_type_frequency.get(cuisine_type, 0) + 1

                total_items += 1

            print(f"  件数: {len(items)}件")
            print(f"  主要types: {list(category_types)[:5]}")

        print(f"\n🎯 全体分析結果:")
        print(f"  総収集件数: {total_items}件")
        print(f"  ユニークtypes数: {len(all_types)}件")

        print(f"\n🍽️ 料理タイプ頻度:")
        for cuisine_type, count in sorted(cuisine_type_frequency.items(), key=lambda x: x[1], reverse=True):
            print(f"  {cuisine_type}: {count}件")

        print(f"\n📋 提案するgenre統一システム:")
        print("  🍽️ 大カテゴリ: 'gourmet'")
        print("  📝 詳細カテゴリ:")
        for category in self.gourmet_categories.keys():
            print(f"    - {category}")

        return {
            'total_items': total_items,
            'unique_types': len(all_types),
            'cuisine_frequency': cuisine_type_frequency,
            'all_types': list(all_types)
        }

def main():
    """メイン実行関数"""
    try:
        analyzer = GourmetGenreAnalyzer()

        # グルメカテゴリ調査実行
        results = analyzer.investigate_gourmet_genres()

        # genre分析
        analysis = analyzer.analyze_genre_patterns(results)

        print("\n🎉 グルメgenre調査完了!")
        print("✅ APIレスポンス確認完了")
        print("✅ genre統一方針決定")

    except Exception as e:
        print(f"❌ 調査エラー: {e}")

if __name__ == "__main__":
    main()
