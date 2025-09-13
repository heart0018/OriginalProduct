#!/usr/bin/env python3
"""
リラックスカテゴリデータ収集のデバッグスクリプト
API レスポンスと place_id の流れを詳細に確認
"""

import os
import json
import requests
from dotenv import load_dotenv

# 環境変数読み込み
load_dotenv()

class RelaxCollectionDebugger:
    def __init__(self):
        self.google_api_key = os.getenv('GOOGLE_API_KEY')
        self.text_search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        self.place_details_url = "https://maps.googleapis.com/maps/api/place/details/json"

    def debug_search_flow(self):
        """検索からデータベース保存までの流れをデバッグ"""
        print("=== Google Places API デバッグ開始 ===\n")

        # テスト用クエリ
        test_query = "温泉 愛知県"
        print(f"テストクエリ: {test_query}")

        # 1. Text Search API テスト
        print("\n1. Text Search API レスポンス確認:")
        search_results = self._test_text_search(test_query)

        if not search_results:
            print("検索結果が取得できませんでした")
            return

        # 2. 最初の結果で Place Details API テスト
        first_result = search_results[0]
        place_id = first_result.get('place_id')

        print(f"\n2. 最初の結果の place_id: {place_id}")
        print(f"   名前: {first_result.get('name')}")
        print(f"   住所: {first_result.get('formatted_address', 'N/A')}")

        if place_id:
            print("\n3. Place Details API レスポンス確認:")
            detailed_place = self._test_place_details(place_id)

            if detailed_place:
                print("\n4. 詳細情報の place_id 確認:")
                detailed_place_id = detailed_place.get('place_id')
                print(f"   詳細情報の place_id: {detailed_place_id}")
                print(f"   place_id が一致: {place_id == detailed_place_id}")

                print("\n5. データベース保存用データの確認:")
                self._check_database_data(detailed_place)
            else:
                print("詳細情報が取得できませんでした")
        else:
            print("place_id が見つかりませんでした")

    def _test_text_search(self, query):
        """Text Search API をテスト"""
        params = {
            'query': query,
            'key': self.google_api_key,
            'language': 'ja',
            'region': 'jp'
        }

        try:
            response = requests.get(self.text_search_url, params=params)
            data = response.json()

            print(f"   ステータス: {data.get('status')}")

            if data.get('status') == 'OK':
                results = data.get('results', [])
                print(f"   結果数: {len(results)}")

                if results:
                    first_result = results[0]
                    print(f"   最初の結果:")
                    print(f"     place_id: {first_result.get('place_id')}")
                    print(f"     名前: {first_result.get('name')}")
                    print(f"     タイプ: {first_result.get('types', [])}")

                return results
            else:
                print(f"   エラー: {data.get('error_message', 'Unknown error')}")
                return []

        except Exception as e:
            print(f"   例外エラー: {e}")
            return []

    def _test_place_details(self, place_id):
        """Place Details API をテスト"""
        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'language': 'ja',
            'fields': 'place_id,name,formatted_address,geometry,rating,user_ratings_total,price_level,formatted_phone_number,website,opening_hours,photos,types,vicinity,plus_code'
        }

        try:
            response = requests.get(self.place_details_url, params=params)
            data = response.json()

            print(f"   ステータス: {data.get('status')}")

            if data.get('status') == 'OK':
                result = data.get('result')
                print(f"   詳細情報取得成功:")
                print(f"     place_id: {result.get('place_id')}")
                print(f"     名前: {result.get('name')}")
                print(f"     住所: {result.get('formatted_address')}")
                print(f"     座標: {result.get('geometry', {}).get('location')}")

                return result
            else:
                print(f"   エラー: {data.get('error_message', 'Unknown error')}")
                return None

        except Exception as e:
            print(f"   例外エラー: {e}")
            return None

    def _check_database_data(self, place):
        """データベース保存用のデータ構造を確認"""
        print("   データベース保存予定の値:")

        # 実際の保存処理と同じロジック
        place_id = place.get('place_id')
        name = place.get('name')
        address = place.get('formatted_address')
        location = place.get('geometry', {}).get('location', {})
        latitude = location.get('lat')
        longitude = location.get('lng')

        print(f"     place_id: {place_id} (型: {type(place_id)})")
        print(f"     name: {name} (型: {type(name)})")
        print(f"     address: {address} (型: {type(address)})")
        print(f"     latitude: {latitude} (型: {type(latitude)})")
        print(f"     longitude: {longitude} (型: {type(longitude)})")

        # None チェック
        if place_id is None:
            print("     ❌ place_id が None です！")
        else:
            print("     ✅ place_id は有効です")

        if name is None:
            print("     ❌ name が None です！")
        else:
            print("     ✅ name は有効です")

def main():
    debugger = RelaxCollectionDebugger()
    debugger.debug_search_flow()

if __name__ == "__main__":
    main()
