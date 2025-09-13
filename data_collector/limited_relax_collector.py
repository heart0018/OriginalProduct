#!/usr/bin/env python3
"""
制限対応版リラックスカテゴリデータ収集スクリプト
Places API制限（250req/min, 200req/day）に完全対応
"""

import os
import sys
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import json
from typing import List, Dict, Optional
import time
from api_limit_manager import LimitedPlacesAPIClient

# .envファイルを読み込み
load_dotenv()

class LimitedRelaxDataCollector:
    def __init__(self, region_name: str, prefectures: List[str]):
        """初期化"""
        self.region_name = region_name
        self.prefectures = prefectures
        self.api_client = LimitedPlacesAPIClient()

        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }

    def _setup_mysql_connection(self):
        """MySQL接続をセットアップ"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            if connection.is_connected():
                print(f"✅ MySQLに接続しました: {self.mysql_config['database']}")
                return connection
        except Error as e:
            print(f"❌ MySQL接続エラー: {e}")
            return None

    def _save_to_database(self, connection, places: List[Dict], category: str):
        """データベースに保存"""
        if not places:
            print("保存するデータがありません")
            return 0

        cursor = connection.cursor()

        insert_query = """
        INSERT INTO spots (
            place_id, name, category, address, latitude, longitude, rating,
            user_ratings_total, price_level, phone_number, website, opening_hours,
            photos, types, vicinity, plus_code, region
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            address = VALUES(address),
            rating = VALUES(rating),
            user_ratings_total = VALUES(user_ratings_total),
            phone_number = VALUES(phone_number),
            website = VALUES(website),
            opening_hours = VALUES(opening_hours),
            updated_at = CURRENT_TIMESTAMP
        """

        saved_count = 0
        for i, place in enumerate(places):
            try:
                # データ検証を追加
                place_id = place.get('place_id')
                name = place.get('name')

                # place_id の検証
                if not place_id:
                    print(f"  警告: place_id が空です - {name} (データ {i+1})")
                    continue

                # name の検証
                if not name:
                    print(f"  警告: name が空です - place_id: {place_id} (データ {i+1})")
                    continue

                print(f"  保存中 ({i+1}/{len(places)}): {name}")

                # 写真情報の処理
                photos = []
                if 'photos' in place:
                    for photo in place['photos'][:5]:  # 最大5枚
                        photo_reference = photo.get('photo_reference')
                        if photo_reference:
                            photos.append({
                                'photo_reference': photo_reference,
                                'height': photo.get('height'),
                                'width': photo.get('width')
                            })

                # 営業時間の処理
                opening_hours = None
                if 'opening_hours' in place:
                    opening_hours = json.dumps(place['opening_hours'], ensure_ascii=False)

                # 座標の取得
                location = place.get('geometry', {}).get('location', {})
                latitude = location.get('lat')
                longitude = location.get('lng')

                data = (
                    place_id,
                    name,
                    category,
                    place.get('formatted_address'),
                    latitude,
                    longitude,
                    place.get('rating'),
                    place.get('user_ratings_total'),
                    place.get('price_level'),
                    place.get('formatted_phone_number'),
                    place.get('website'),
                    opening_hours,
                    json.dumps(photos, ensure_ascii=False) if photos else None,
                    json.dumps(place.get('types', []), ensure_ascii=False),
                    place.get('vicinity'),
                    place.get('plus_code', {}).get('compound_code') if place.get('plus_code') else None,
                    self.region_name
                )

                cursor.execute(insert_query, data)
                saved_count += 1
                print(f"    ✅ 保存成功")

            except Exception as e:
                print(f"    ❌ 保存エラー: {e} - {place.get('name', 'Unknown')}")
                print(f"    エラー詳細: {type(e).__name__}")
                if hasattr(e, 'errno'):
                    print(f"    MySQL Error Code: {e.errno}")
                continue

        connection.commit()
        cursor.close()
        print(f"カテゴリ {category}: {saved_count}件保存完了")
        return saved_count

    def collect_with_limits(self, category: str, target_per_prefecture: int = 3):
        """制限対応でカテゴリデータを収集"""
        print(f"=== {self.region_name}地方{category}カテゴリ収集開始 ===")
        print(f"目標: 各県{target_per_prefecture}件 x {len(self.prefectures)}県 = {target_per_prefecture * len(self.prefectures)}件")
        print(f"API制限: 250req/min, 200req/day")
        print()

        # データベース接続
        connection = self._setup_mysql_connection()
        if not connection:
            return

        try:
            all_places = []

            for i, prefecture in enumerate(self.prefectures):
                print(f"\n--- {prefecture}県の収集 ({i+1}/{len(self.prefectures)}) ---")

                # 県別のクエリ
                queries = [
                    f"{category} {prefecture}県",
                    f"{prefecture} {category}"
                ]

                prefecture_places = []

                for query in queries:
                    # API制限チェック済みの検索
                    results, success = self.api_client.search_places(query)

                    if not success:
                        print(f"⚠️  検索に失敗しました: {query}")
                        # 制限に到達した場合は収集を停止
                        if self.api_client.limit_manager.daily_limit_reached:
                            print("🚨 日次制限に到達しました。収集を停止します。")
                            break
                        continue

                    # 結果を処理
                    for result in results[:target_per_prefecture]:  # 県ごとに制限
                        place_id = result.get('place_id')
                        if place_id:
                            # 詳細情報取得（制限チェック済み）
                            detailed_place, detail_success = self.api_client.get_place_details(place_id)

                            if not detail_success:
                                if self.api_client.limit_manager.daily_limit_reached:
                                    print("🚨 日次制限に到達しました。収集を停止します。")
                                    break
                                continue

                            if detailed_place:
                                # 地域チェック
                                address = detailed_place.get('formatted_address', '')
                                if any(pref in address for pref in self.prefectures):
                                    prefecture_places.append(detailed_place)
                                    if len(prefecture_places) >= target_per_prefecture:
                                        break

                    # API制限チェック
                    if self.api_client.limit_manager.daily_limit_reached:
                        break

                    time.sleep(0.5)  # 県間の少し長めの待機

                print(f"{prefecture}県: {len(prefecture_places)}件取得")
                all_places.extend(prefecture_places)

                # 使用状況表示
                print(f"   {self.api_client.get_usage_summary()}")

                # 制限チェック
                if self.api_client.limit_manager.daily_limit_reached:
                    print("🚨 日次制限に到達しました。今日の収集を終了します。")
                    break

            # 重複除去
            unique_places = []
            seen_place_ids = set()

            for place in all_places:
                place_id = place.get('place_id')
                if place_id and place_id not in seen_place_ids:
                    unique_places.append(place)
                    seen_place_ids.add(place_id)

            print(f"\n=== 収集結果 ===")
            print(f"重複除去後: {len(unique_places)}件")
            print(self.api_client.get_usage_summary())

            # データベースに保存
            if unique_places:
                saved_count = self._save_to_database(connection, unique_places, category)
                print(f"\n✅ {self.region_name}地方完了: {saved_count}件保存")
                return saved_count
            else:
                print("\n❌ 保存するデータがありません")
                return 0

        finally:
            if connection.is_connected():
                connection.close()
                print("✅ データベース接続クローズ")


def test_limited_collection():
    """制限対応収集のテスト"""
    # 北海道でテスト（小規模）
    hokkaido_collector = LimitedRelaxDataCollector(
        region_name="hokkaido",
        prefectures=["北海道"]
    )

    # 温泉カテゴリを少量収集
    result = hokkaido_collector.collect_with_limits("温泉", target_per_prefecture=2)

    if result > 0:
        print(f"\n🎉 テスト成功: {result}件のデータを収集しました")
    else:
        print("\n⚠️  テストでデータを収集できませんでした")


if __name__ == "__main__":
    test_limited_collection()
