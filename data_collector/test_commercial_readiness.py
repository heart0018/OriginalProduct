#!/usr/bin/env python3
"""
商業化向け最終確認テスト
永続化されたURLがAPIキーなしで正常に動作するかテスト
"""

import os
import json
import requests
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

class CommercialReadinessTest:
    """商業化準備完了テスト"""

    def __init__(self):
        self.mysql_config = {
            'host': 'localhost',
            'user': 'Haruto',
            'password': os.getenv('MYSQL_PASSWORD'),
            'database': 'swipe_app_development',
            'charset': 'utf8mb4'
        }

    def test_api_independence(self):
        """APIキー依存なしでの画像アクセステスト"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            cursor = connection.cursor()

            print("🚀 商業化準備完了テスト開始")
            print("🎯 目標: APIキーなしでの画像アクセス確認\n")

            # 永続URLを取得
            query = """
                SELECT id, name, image_urls
                FROM spots
                WHERE image_urls IS NOT NULL
                AND JSON_LENGTH(image_urls) > 0
                LIMIT 10
            """

            cursor.execute(query)
            test_spots = cursor.fetchall()

            print(f"📊 テスト対象: {len(test_spots)}件のスポット")
            print()

            success_count = 0
            total_urls = 0

            for spot_id, name, image_urls_json in test_spots:
                print(f"🔍 テスト中: {name}")

                try:
                    image_urls = json.loads(image_urls_json)

                    for i, image_data in enumerate(image_urls[:2]):  # 最大2枚テスト
                        url = image_data.get('url')
                        if url:
                            total_urls += 1
                            print(f"    📸 画像 {i+1}: APIキーなしアクセステスト...")

                            # APIキーなしでアクセステスト
                            response = requests.head(url, timeout=10)

                            if response.status_code == 200:
                                print(f"        ✅ 成功: HTTP 200")
                                success_count += 1
                            else:
                                print(f"        ❌ 失敗: HTTP {response.status_code}")

                except json.JSONDecodeError:
                    print(f"    ❌ JSON解析エラー")
                except Exception as e:
                    print(f"    ❌ エラー: {e}")

                print()

            # 結果レポート
            success_rate = (success_count / total_urls * 100) if total_urls > 0 else 0

            print("🎉 商業化準備完了テスト結果")
            print(f"   📊 テスト済URL: {total_urls}個")
            print(f"   ✅ 成功: {success_count}個")
            print(f"   📈 成功率: {success_rate:.1f}%")

            if success_rate >= 95:
                print(f"   🚀 商業化準備: 完全に準備完了！")
                print(f"   💰 ユーザーのスワイプでAPIキーを消費しません")
                print(f"   🏆 持続可能なビジネスモデルを実現しました！")
            else:
                print(f"   ⚠️  一部のURLに問題があります")

            cursor.close()
            connection.close()

        except Exception as e:
            print(f"❌ テストエラー: {e}")

def main():
    print("🔍 商業化準備完了の最終確認")
    print("💼 持続可能なビジネスモデルのテスト\n")

    tester = CommercialReadinessTest()
    tester.test_api_independence()

if __name__ == "__main__":
    main()
