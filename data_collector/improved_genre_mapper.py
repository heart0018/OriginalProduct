#!/usr/bin/env python3
"""
Genre統一システム修正版
詳細カテゴリを正確に保持しつつ、大カテゴリに統一
"""

import os
import mysql.connector
from typing import Dict, List, Tuple
from dotenv import load_dotenv

load_dotenv()

class ImprovedGenreMapper:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'Haruto'),
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': os.getenv('MYSQL_DATABASE', 'swipe_app_production'),
            'charset': 'utf8mb4'
        }

        # 詳細→大カテゴリマッピング
        self.genre_mapping = {
            'relax_onsen': ('relax', 'relax_onsen'),
            'relax_cafe': ('relax', 'relax_cafe'),
            'relax_parks': ('relax', 'relax_parks'),
            'relax_sauna': ('relax', 'relax_sauna'),
            'relax_walking_courses': ('relax', 'relax_walking_courses'),
            'entertainment_arcade': ('entertainment', 'entertainment_arcade'),
            'entertainment_karaoke': ('entertainment', 'entertainment_karaoke'),
            'entertainment_bowling': ('entertainment', 'entertainment_bowling'),
            'entertainment_cinema': ('entertainment', 'entertainment_cinema'),
            'entertainment_sports': ('entertainment', 'entertainment_sports')
        }

    def add_detailed_category_column(self):
        """detailed_categoryカラムを追加"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            print("🔧 detailed_categoryカラム追加")
            print("=" * 40)

            # カラム存在確認
            cursor.execute("SHOW COLUMNS FROM cards LIKE 'detailed_category'")
            if cursor.fetchone():
                print("✅ detailed_categoryカラム既存")
            else:
                cursor.execute("""
                    ALTER TABLE cards
                    ADD COLUMN detailed_category VARCHAR(64) NULL
                    AFTER genre
                """)
                connection.commit()
                print("✅ detailed_categoryカラム追加完了")

            cursor.close()
            connection.close()
            return True

        except Exception as e:
            print(f"❌ カラム追加エラー: {e}")
            return False

    def analyze_and_map_genres(self):
        """現在のgenreを分析し、マッピング実行"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            print("\\n📊 Genre分析とマッピング実行")
            print("=" * 50)

            # 現在のgenre分布確認
            cursor.execute("SELECT genre, COUNT(*) FROM cards WHERE genre IS NOT NULL GROUP BY genre ORDER BY COUNT(*) DESC")
            current_genres = cursor.fetchall()

            print("📋 現在のgenre分布:")
            for genre, count in current_genres:
                if genre in self.genre_mapping:
                    unified, detailed = self.genre_mapping[genre]
                    print(f"  {genre}: {count}件 → {unified} ({detailed})")
                else:
                    print(f"  {genre}: {count}件 (変更なし)")

            # 実際のマッピング実行
            total_updated = 0
            for original_genre, (unified_genre, detailed_category) in self.genre_mapping.items():
                cursor.execute("SELECT COUNT(*) FROM cards WHERE genre = %s", (original_genre,))
                count = cursor.fetchone()[0]

                if count > 0:
                    print(f"\\n🔄 処理中: {original_genre} → {unified_genre}")

                    # 元のgenreを詳細カテゴリに保存し、genreを統一
                    cursor.execute("""
                        UPDATE cards
                        SET detailed_category = %s, genre = %s
                        WHERE genre = %s
                    """, (detailed_category, unified_genre, original_genre))

                    updated = cursor.rowcount
                    total_updated += updated
                    print(f"  ✅ 更新完了: {updated}件")

            connection.commit()
            print(f"\\n🎯 合計更新件数: {total_updated}件")

            cursor.close()
            connection.close()
            return True

        except Exception as e:
            print(f"❌ マッピングエラー: {e}")
            return False

    def verify_mapping_results(self):
        """マッピング結果を検証"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            print("\\n✅ マッピング結果検証")
            print("=" * 40)

            # 統一後のgenre分布
            cursor.execute("SELECT genre, COUNT(*) FROM cards WHERE genre IS NOT NULL GROUP BY genre ORDER BY COUNT(*) DESC")
            unified_genres = cursor.fetchall()

            print("🎯 統一後のgenre分布:")
            for genre, count in unified_genres:
                print(f"  {genre}: {count}件")

            # 詳細カテゴリ分布
            cursor.execute("SELECT detailed_category, COUNT(*) FROM cards WHERE detailed_category IS NOT NULL GROUP BY detailed_category ORDER BY COUNT(*) DESC")
            detailed_categories = cursor.fetchall()

            print("\\n📋 詳細カテゴリ分布:")
            for category, count in detailed_categories:
                print(f"  {category}: {count}件")

            # サンプルデータ
            print("\\n📄 統一システムサンプル:")
            cursor.execute("""
                SELECT title, genre, detailed_category, region
                FROM cards
                WHERE genre IN ('relax', 'entertainment')
                ORDER BY created_at DESC
                LIMIT 5
            """)
            samples = cursor.fetchall()

            for title, genre, detailed_category, region in samples:
                print(f"  ✓ {title}")
                print(f"    大カテゴリ: {genre} | 詳細: {detailed_category} | 地域: {region}")

            cursor.close()
            connection.close()
            return True

        except Exception as e:
            print(f"❌ 検証エラー: {e}")
            return False

    def execute_full_mapping(self):
        """完全なgenre統一マッピングを実行"""
        print("🎯 改良版Genre統一マッピングシステム")
        print("=" * 60)
        print("📊 relax系 → 'relax' + detailed_category保持")
        print("📊 entertainment系 → 'entertainment' + detailed_category保持")
        print("=" * 60)

        # 1. カラム追加
        if not self.add_detailed_category_column():
            return False

        # 2. マッピング実行
        if not self.analyze_and_map_genres():
            return False

        # 3. 結果検証
        if not self.verify_mapping_results():
            return False

        print("\\n🎉 Genre統一マッピング完了!")
        print("✅ 大カテゴリ: relax/entertainment で統一")
        print("✅ 詳細カテゴリ: detailed_categoryで完全保持")
        print("✅ 新規システム対応完了")

        return True

def main():
    """メイン実行"""
    try:
        mapper = ImprovedGenreMapper()
        mapper.execute_full_mapping()

    except Exception as e:
        print(f"❌ システムエラー: {e}")

if __name__ == "__main__":
    main()
