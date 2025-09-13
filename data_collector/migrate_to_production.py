#!/usr/bin/env python3
"""
データ移行スクリプト: development.spots → production.cards
永続化済みの画像URLも含めて完全移行
"""

import os
import json
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

class DataMigrator:
    """開発環境から本番環境へのデータ移行"""

    def __init__(self):
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'charset': 'utf8mb4'
        }

    def migrate_spots_to_cards(self):
        """spotsテーブルからcardsテーブルへの移行"""

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

            print("🚀 データ移行開始")
            print("📂 開発環境 → 本番環境")
            print("🗂️  spots → cards\n")

            # 開発環境からデータ取得
            dev_cursor.execute("""
                SELECT
                    place_id,
                    name,
                    category,
                    address,
                    rating,
                    user_ratings_total,
                    website,
                    region,
                    image_urls,
                    fallback_image_url
                FROM spots
                WHERE name IS NOT NULL
                ORDER BY created_at ASC
            """)

            spots_data = dev_cursor.fetchall()
            print(f"📊 移行対象: {len(spots_data)}件")

            # 既存のcardsデータをクリア（念のため）
            prod_cursor.execute("DELETE FROM cards")
            prod_connection.commit()
            print("🗑️  既存cardsデータクリア完了")

            migrated_count = 0

            for spot in spots_data:
                (place_id, name, category, address, rating,
                 user_ratings_total, website, region,
                 image_urls_json, fallback_image_url) = spot

                # 画像URL決定（永続URL優先）
                image_url = fallback_image_url  # デフォルトはフォールバック

                if image_urls_json:
                    try:
                        image_urls = json.loads(image_urls_json)
                        if image_urls and len(image_urls) > 0:
                            # 永続URLが最優先
                            first_image = image_urls[0]
                            if 'url' in first_image:
                                image_url = first_image['url']
                    except json.JSONDecodeError:
                        pass  # フォールバックURLを使用

                # カテゴリ名正規化
                genre = self.normalize_category(category)

                # 外部リンク（websiteがあればそれを、なければGoogle検索）
                external_link = website if website else f"https://www.google.com/search?q={name}"

                # cardsテーブルに挿入
                insert_query = """
                    INSERT INTO cards (
                        genre, title, rating, review_count,
                        image_url, external_link, region,
                        address, place_id, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                    )
                """

                values = (
                    genre,
                    name,
                    rating if rating else 0.0,
                    user_ratings_total if user_ratings_total else 0,
                    image_url,
                    external_link,
                    region,
                    address,
                    place_id
                )

                try:
                    prod_cursor.execute(insert_query, values)
                    migrated_count += 1

                    if migrated_count % 10 == 0:
                        print(f"   📈 進捗: {migrated_count}/{len(spots_data)}件")

                except Exception as e:
                    print(f"   ❌ エラー ({name}): {e}")

            # コミット
            prod_connection.commit()

            print(f"\n🎉 移行完了!")
            print(f"   ✅ 成功: {migrated_count}/{len(spots_data)}件")

            # 移行結果確認
            prod_cursor.execute("SELECT COUNT(*) FROM cards")
            final_count = prod_cursor.fetchone()[0]
            print(f"   📊 本番環境cards件数: {final_count}件")

            # 地域別確認
            print(f"\n📍 地域別移行結果:")
            prod_cursor.execute("SELECT region, COUNT(*) FROM cards GROUP BY region ORDER BY region")
            region_counts = prod_cursor.fetchall()
            for region, count in region_counts:
                print(f"   {region}: {count}件")

            print(f"\n🚀 本番環境へのデータ移行が完了しました！")

        except Exception as e:
            print(f"❌ 移行エラー: {e}")
            if prod_connection:
                prod_connection.rollback()

        finally:
            if dev_connection:
                dev_cursor.close()
                dev_connection.close()
            if prod_connection:
                prod_cursor.close()
                prod_connection.close()

    def normalize_category(self, category):
        """カテゴリ名の正規化"""
        if not category:
            return "relax"

        # カテゴリマッピング
        category_map = {
            "温泉": "relax",
            "relax_onsen": "relax",
            "relax_onsen_test": "relax",
            "onsen": "relax"
        }

        return category_map.get(category, "relax")

def main():
    print("🔄 本番環境データ移行")
    print("💫 永続URL込みで完全移行\n")

    migrator = DataMigrator()
    migrator.migrate_spots_to_cards()

if __name__ == "__main__":
    main()
