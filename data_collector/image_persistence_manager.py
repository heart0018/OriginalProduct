#!/usr/bin/env python3
"""
Google Places 画像永続化システム
photo_reference から画像をダウンロードしてローカル保存し、
API制限に依存しない画像表示システムを構築
"""

import os
import json
import requests
import hashlib
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
from pathlib import Path
from utils.request_guard import get_photo_direct_url

load_dotenv()

class ImagePersistenceManager:
    """画像永続化管理クラス"""

    def __init__(self, storage_dir="/home/haruto/OriginalProdact/data_collector/images"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        self.api_key = os.getenv('GOOGLE_API_KEY')

        # 地域別ディレクトリ作成
        regions = ['hokkaido', 'tohoku', 'kanto', 'chubu', 'kansai', 'chugoku_shikoku', 'kyushu_okinawa']
        for region in regions:
            (self.storage_dir / region).mkdir(exist_ok=True)

    def generate_image_filename(self, spot_name, photo_ref, size=800):
        """一意な画像ファイル名生成"""
        # スポット名とphoto_referenceからハッシュ生成
        unique_string = f"{spot_name}_{photo_ref}_{size}"
        hash_object = hashlib.md5(unique_string.encode())
        return f"{hash_object.hexdigest()[:12]}_{size}.jpg"

    def download_and_save_image(self, spot_name, region, photo_ref, size=800):
        """画像をダウンロードしてローカル保存"""
        if not self.api_key:
            return None

        try:
            # 画像ファイル名とパス
            filename = self.generate_image_filename(spot_name, photo_ref, size)
            file_path = self.storage_dir / region / filename

            # 既存ファイルチェック
            if file_path.exists():
                return f"images/{region}/{filename}"

            # まずはキャッシュされた直リンク取得
            direct_url = get_photo_direct_url(photo_ref, maxwidth=size, ttl_sec=60*60*24*30)
            if not direct_url:
                print("   ⚠️ 直リンク未取得（Photo APIキャッシュ）：スキップ")
                return None

            # 画像ダウンロード
            response = requests.get(direct_url, timeout=30)

            if response.status_code == 200:
                content_type = response.headers.get('content-type', '')
                if 'image' in content_type:
                    # 画像保存
                    with open(file_path, 'wb') as f:
                        f.write(response.content)

                    print(f"✅ 画像保存成功: {spot_name} -> {filename}")
                    return f"images/{region}/{filename}"
                else:
                    print(f"❌ 画像形式エラー: {content_type}")
                    return None
            else:
                print(f"❌ ダウンロード失敗: HTTP {response.status_code}")
                return None

        except Exception as e:
            print(f"❌ 画像保存エラー: {e}")
            return None

    def process_existing_data(self, limit=None):
        """既存データの画像を一括処理"""
        try:
            connection = mysql.connector.connect(
                host='localhost',
                user='Haruto',
                password=os.getenv('MYSQL_PASSWORD'),
                database='swipe_app_development',
                charset='utf8mb4'
            )

            cursor = connection.cursor()

            print("🎯 既存データの画像永続化開始\n")

            # 画像データを持つスポット取得
            query = """
                SELECT id, name, region, photos
                FROM spots
                WHERE photos IS NOT NULL
                AND JSON_LENGTH(photos) > 0
                AND (local_image_url IS NULL OR local_image_url = '')
                ORDER BY created_at DESC
            """

            if limit:
                query += f" LIMIT {limit}"

            cursor.execute(query)
            spots_to_process = cursor.fetchall()

            print(f"📊 処理対象: {len(spots_to_process)}件")

            processed_count = 0
            success_count = 0

            for spot_id, name, region, photos_json in spots_to_process:
                print(f"\n🔄 処理中: {name} ({region})")

                try:
                    photos = json.loads(photos_json)
                    if photos and len(photos) > 0:
                        # 最初の写真を処理
                        photo_ref = photos[0].get('photo_reference')
                        if photo_ref:
                            # 画像ダウンロード&保存
                            local_url = self.download_and_save_image(name, region, photo_ref)

                            if local_url:
                                # データベース更新
                                update_query = "UPDATE spots SET local_image_url = %s WHERE id = %s"
                                cursor.execute(update_query, (local_url, spot_id))
                                connection.commit()
                                success_count += 1
                                print(f"   ✅ データベース更新完了")
                            else:
                                print(f"   ❌ 画像保存失敗")
                        else:
                            print(f"   ⚠️  photo_reference なし")
                    else:
                        print(f"   ⚠️  写真データなし")

                except json.JSONDecodeError:
                    print(f"   ❌ JSON解析エラー")

                processed_count += 1

                # 進捗表示
                if processed_count % 10 == 0:
                    print(f"\n📈 進捗: {processed_count}/{len(spots_to_process)}件 (成功: {success_count}件)")

            print(f"\n🎉 画像永続化完了!")
            print(f"   📊 処理件数: {processed_count}件")
            print(f"   ✅ 成功件数: {success_count}件")
            print(f"   📁 保存先: {self.storage_dir}")

            cursor.close()
            connection.close()

        except Exception as e:
            print(f"❌ 処理エラー: {e}")

def main():
    """メイン実行"""
    print("🚀 画像永続化システム起動\n")

    # データベーステーブル更新確認
    print("1️⃣  データベーステーブル更新が必要です:")
    print("   ALTER TABLE spots ADD COLUMN local_image_url VARCHAR(500);")
    print("\n2️⃣  実行しますか？ (y/n):")

    choice = input().lower().strip()
    if choice != 'y':
        print("❌ キャンセルされました")
        return

    manager = ImagePersistenceManager()

    # 最初は少量でテスト
    print("\n🧪 テスト実行（最初の5件）")
    manager.process_existing_data(limit=5)

if __name__ == "__main__":
    main()
