#!/usr/bin/env python3
"""
座標データ移行修正スクリプト
既存のcardsテーブルに緯度・経度データを追加更新
"""

import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

class CoordinateUpdater:
    """座標データ更新クラス"""

    def __init__(self):
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'charset': 'utf8mb4'
        }

    def update_coordinates(self):
        """spotsからcardsに座標データを更新"""

        dev_connection = None
        prod_connection = None

        try:
            # 開発環境接続
            dev_connection = mysql.connector.connect(
                **self.mysql_config,
                database='swipe_app_development'
            )
            dev_cursor = dev_connection.cursor()

            # 本番環境接続
            prod_connection = mysql.connector.connect(
                **self.mysql_config,
                database='swipe_app_production'
            )
            prod_cursor = prod_connection.cursor()

            print("🗺️  座標データ更新開始")
            print("📍 spots → cards 座標移行\n")

            # 開発環境から座標データ取得
            dev_cursor.execute("""
                SELECT place_id, latitude, longitude, name
                FROM spots
                WHERE place_id IS NOT NULL
                AND latitude IS NOT NULL
                AND longitude IS NOT NULL
                ORDER BY created_at ASC
            """)

            coordinates_data = dev_cursor.fetchall()
            print(f"📊 座標データ取得: {len(coordinates_data)}件")

            updated_count = 0

            for place_id, latitude, longitude, name in coordinates_data:
                # cardsテーブルで対応するレコードを更新
                update_query = """
                    UPDATE cards
                    SET latitude = %s, longitude = %s
                    WHERE place_id = %s
                """

                try:
                    prod_cursor.execute(update_query, (latitude, longitude, place_id))
                    if prod_cursor.rowcount > 0:
                        updated_count += 1
                        if updated_count % 10 == 0:
                            print(f"   📈 進捗: {updated_count}/{len(coordinates_data)}件")
                    else:
                        print(f"   ⚠️  対応するcard未発見: {name}")

                except Exception as e:
                    print(f"   ❌ 更新エラー ({name}): {e}")

            # コミット
            prod_connection.commit()

            print(f"\n🎉 座標データ更新完了!")
            print(f"   ✅ 更新成功: {updated_count}/{len(coordinates_data)}件")

            # 更新結果確認
            prod_cursor.execute("SELECT COUNT(*) FROM cards WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
            final_count = prod_cursor.fetchone()[0]
            print(f"   📊 本番環境座標データ: {final_count}件")

            # 完全性確認
            prod_cursor.execute("SELECT COUNT(*) FROM cards")
            total_cards = prod_cursor.fetchone()[0]
            completeness = (final_count / total_cards * 100) if total_cards > 0 else 0
            print(f"   📈 座標データ完全率: {completeness:.1f}%")

            # サンプル確認
            print(f"\n📍 更新後サンプル（3件）:")
            prod_cursor.execute("""
                SELECT title, latitude, longitude, region
                FROM cards
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                LIMIT 3
            """)

            samples = prod_cursor.fetchall()
            for title, lat, lng, region in samples:
                print(f"   {title} ({region})")
                print(f"     緯度: {lat}, 経度: {lng}")

            print(f"\n🚀 本番環境座標データ更新が完了しました！")

        except Exception as e:
            print(f"❌ 更新エラー: {e}")
            if prod_connection:
                prod_connection.rollback()

        finally:
            if dev_connection:
                dev_cursor.close()
                dev_connection.close()
            if prod_connection:
                prod_cursor.close()
                prod_connection.close()

def main():
    print("🔧 座標データ移行修正")
    print("🎯 開発環境 → 本番環境\n")

    updater = CoordinateUpdater()
    updater.update_coordinates()

if __name__ == "__main__":
    main()
