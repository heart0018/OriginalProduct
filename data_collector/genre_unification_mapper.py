#!/usr/bin/env python3
"""
Genre統一マッピングシステム
詳細カテゴリを保持しつつ、genreを大カテゴリ（relax/entertainment）に統一
"""

import os
import mysql.connector
from typing import Dict, List, Tuple
from dotenv import load_dotenv

load_dotenv()

class GenreUnificationMapper:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'Haruto'),
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': os.getenv('MYSQL_DATABASE', 'swipe_app_production'),
            'charset': 'utf8mb4'
        }

        # 詳細カテゴリ→大カテゴリマッピング
        self.category_mapping = {
            # Relaxカテゴリ
            'relax_onsen': 'relax',
            'relax_cafe': 'relax',
            'relax_parks': 'relax',
            'relax_sauna': 'relax',
            'relax_walking_courses': 'relax',
            'relax': 'relax',

            # Entertainmentカテゴリ
            'entertainment_arcade': 'entertainment',
            'entertainment_karaoke': 'entertainment',
            'entertainment_bowling': 'entertainment',
            'entertainment_cinema': 'entertainment',
            'entertainment_sports': 'entertainment',

            # その他（必要に応じて追加）
            'gourmet_restaurant': 'gourmet',
            'gourmet_cafe': 'gourmet',
            'activity_sports': 'activity',
            'activity_outdoor': 'activity'
        }

        # 逆マッピング（詳細カテゴリ名の正規化）
        self.detailed_category_names = {
            'relax_onsen': '温泉',
            'relax_cafe': 'カフェ',
            'relax_parks': '公園',
            'relax_sauna': 'サウナ',
            'relax_walking_courses': '散歩コース',
            'entertainment_arcade': 'ゲームセンター',
            'entertainment_karaoke': 'カラオケ',
            'entertainment_bowling': 'ボウリング',
            'entertainment_cinema': '映画館',
            'entertainment_sports': 'スポーツ施設'
        }

    def check_table_schema(self) -> bool:
        """データベーステーブルスキーマを確認し、必要に応じてカラムを追加"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            print("📊 テーブルスキーマ確認")
            print("=" * 40)

            # カラム存在確認
            cursor.execute("DESCRIBE cards")
            columns = cursor.fetchall()

            existing_columns = [col[0] for col in columns]
            print(f"既存カラム: {', '.join(existing_columns)}")

            # detailed_categoryカラムの確認・追加
            if 'detailed_category' not in existing_columns:
                print("\\n➕ detailed_categoryカラムを追加します...")
                cursor.execute("""
                    ALTER TABLE cards
                    ADD COLUMN detailed_category VARCHAR(64) NULL
                    AFTER genre
                """)
                connection.commit()
                print("✅ detailed_categoryカラム追加完了")
            else:
                print("✅ detailed_categoryカラム既存")

            cursor.close()
            connection.close()
            return True

        except Exception as e:
            print(f"❌ スキーマ確認エラー: {e}")
            return False

    def analyze_current_state(self) -> Dict:
        """現在のgenre状況を分析"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            print("\\n🔍 現在の状況分析")
            print("=" * 40)

            # 現在のgenre分布
            cursor.execute("SELECT genre, COUNT(*) FROM cards WHERE genre IS NOT NULL GROUP BY genre ORDER BY COUNT(*) DESC")
            current_genres = cursor.fetchall()

            analysis = {
                'current_genres': current_genres,
                'total_records': sum([count for _, count in current_genres]),
                'unification_plan': {}
            }

            print("📋 現在のgenre分布:")
            for genre, count in current_genres:
                unified_genre = self.category_mapping.get(genre, 'other')
                print(f"  {genre}: {count}件 → {unified_genre}")

                if unified_genre not in analysis['unification_plan']:
                    analysis['unification_plan'][unified_genre] = 0
                analysis['unification_plan'][unified_genre] += count

            print("\\n🎯 統一後の予想分布:")
            for unified_genre, total_count in analysis['unification_plan'].items():
                print(f"  {unified_genre}: {total_count}件")

            cursor.close()
            connection.close()
            return analysis

        except Exception as e:
            print(f"❌ 分析エラー: {e}")
            return {}

    def execute_genre_unification(self, dry_run: bool = True) -> bool:
        """genre統一を実行"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            print(f"\\n{'🔄 DRY RUN: ' if dry_run else '⚡ 実行: '}Genre統一処理")
            print("=" * 50)

            total_updated = 0

            for detailed_genre, unified_genre in self.category_mapping.items():
                # 該当レコード数確認
                cursor.execute("SELECT COUNT(*) FROM cards WHERE genre = %s", (detailed_genre,))
                count = cursor.fetchone()[0]

                if count > 0:
                    print(f"📝 {detailed_genre} → {unified_genre} ({count}件)")

                    if not dry_run:
                        # detailed_categoryに元のgenreを保存
                        cursor.execute("""
                            UPDATE cards
                            SET detailed_category = %s, genre = %s
                            WHERE genre = %s
                        """, (detailed_genre, unified_genre, detailed_genre))

                        total_updated += cursor.rowcount
                        print(f"  ✅ 更新完了: {cursor.rowcount}件")
                    else:
                        print(f"  📋 更新予定: {count}件")
                        total_updated += count

            if not dry_run:
                connection.commit()
                print(f"\\n🎯 Genre統一完了: {total_updated}件更新")
            else:
                print(f"\\n📊 DRY RUN完了: {total_updated}件更新予定")

            cursor.close()
            connection.close()
            return True

        except Exception as e:
            print(f"❌ 統一処理エラー: {e}")
            return False

    def verify_unification(self) -> bool:
        """統一結果を検証"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            print("\\n✅ 統一結果検証")
            print("=" * 40)

            # 統一後のgenre分布
            cursor.execute("SELECT genre, COUNT(*) FROM cards WHERE genre IS NOT NULL GROUP BY genre ORDER BY COUNT(*) DESC")
            unified_genres = cursor.fetchall()

            print("🎯 統一後のgenre分布:")
            for genre, count in unified_genres:
                print(f"  {genre}: {count}件")

            # detailed_categoryの分布
            cursor.execute("SELECT detailed_category, COUNT(*) FROM cards WHERE detailed_category IS NOT NULL GROUP BY detailed_category ORDER BY COUNT(*) DESC")
            detailed_categories = cursor.fetchall()

            print("\\n📋 詳細カテゴリ分布:")
            for category, count in detailed_categories:
                category_name = self.detailed_category_names.get(category, category)
                print(f"  {category} ({category_name}): {count}件")

            # サンプルデータ表示
            print("\\n📄 サンプルデータ:")
            cursor.execute("""
                SELECT title, genre, detailed_category, region
                FROM cards
                WHERE genre IS NOT NULL AND detailed_category IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 5
            """)
            samples = cursor.fetchall()

            for title, genre, detailed_category, region in samples:
                category_name = self.detailed_category_names.get(detailed_category, detailed_category)
                print(f"  ✓ {title}")
                print(f"    大カテゴリ: {genre} | 詳細: {category_name} | 地域: {region}")

            cursor.close()
            connection.close()
            return True

        except Exception as e:
            print(f"❌ 検証エラー: {e}")
            return False

    def run_full_unification(self, execute: bool = False):
        """完全な統一処理を実行"""
        print("🎯 Genre統一マッピングシステム")
        print("=" * 60)
        print("📊 目的: 詳細カテゴリを保持しつつ、genreを大カテゴリに統一")
        print("=" * 60)

        # 1. スキーマ確認
        if not self.check_table_schema():
            return False

        # 2. 現状分析
        analysis = self.analyze_current_state()
        if not analysis:
            return False

        # 3. DRY RUN
        if not self.execute_genre_unification(dry_run=True):
            return False

        # 4. 実行確認
        if execute:
            print("\\n⚠️ 実際にデータベースを更新しますか？")
            print("詳細カテゴリは preserved されます")

            # 実行
            if self.execute_genre_unification(dry_run=False):
                # 5. 検証
                self.verify_unification()
                print("\\n🎉 Genre統一システム構築完了!")
                print("✅ 大カテゴリ: relax/entertainment で統一")
                print("✅ 詳細カテゴリ: detailed_categoryで保持")

        else:
            print("\\n📋 DRY RUN完了")
            print("実際に実行する場合は execute=True で再実行してください")

def main():
    """メイン実行関数"""
    try:
        mapper = GenreUnificationMapper()

        # DRY RUNのみ実行（安全のため）
        mapper.run_full_unification(execute=False)

        print("\\n" + "="*60)
        print("🚀 実際に実行する場合:")
        print("mapper.run_full_unification(execute=True)")

    except Exception as e:
        print(f"❌ システムエラー: {e}")

if __name__ == "__main__":
    main()
