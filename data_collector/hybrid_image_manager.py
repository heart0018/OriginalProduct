#!/usr/bin/env python3
"""
ハイブリッド画像表示システム - 最適解
- Google Places: 新規データ収集時のみ
- メモリキャッシュ: 高速アクセス
- フォールバック: 外部フリー画像
- プレースホルダー: カテゴリ別デフォルト
"""

import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

class HybridImageManager:
    """ハイブリッド画像管理システム"""

    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')
        self.cache = {}
        self.api_quota_remaining = 50  # 保守的な制限

        # フォールバック画像マッピング
        self.fallback_images = {
            '温泉': "https://source.unsplash.com/400x300/?onsen,hotspring,spa",
            'relax_onsen': "https://source.unsplash.com/400x300/?onsen,hotspring,spa",
            'relax_park': "https://source.unsplash.com/400x300/?park,nature,garden",
            'relax_cafe': "https://source.unsplash.com/400x300/?cafe,coffee,cozy",
            'relax_sauna': "https://source.unsplash.com/400x300/?sauna,wellness",
            'relax_walk': "https://source.unsplash.com/400x300/?walking,path,nature"
        }

    def get_image_url(self, spot_data, prefer_google=False):
        """最適な画像URL取得"""
        spot_id = spot_data.get('id')
        category = spot_data.get('category')
        photos = spot_data.get('photos')

        # 1. キャッシュチェック
        cache_key = f"spot_{spot_id}"
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            if time.time() - cached['timestamp'] < 3600:  # 1時間キャッシュ
                return cached['url']

        # 2. Google Places API (クォータ内 & 優先指定時)
        if prefer_google and self.api_quota_remaining > 0 and photos:
            try:
                photos_data = json.loads(photos) if isinstance(photos, str) else photos
                if photos_data and len(photos_data) > 0:
                    photo_ref = photos_data[0].get('photo_reference')
                    if photo_ref and self.api_key:
                        google_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photo_reference={photo_ref}&key={self.api_key}"

                        # キャッシュ保存
                        self.cache[cache_key] = {
                            'url': google_url,
                            'timestamp': time.time(),
                            'source': 'google'
                        }

                        self.api_quota_remaining -= 1
                        return google_url
            except:
                pass

        # 3. フォールバック画像
        fallback_url = self.fallback_images.get(category, "https://picsum.photos/400/300")

        # キャッシュ保存
        self.cache[cache_key] = {
            'url': fallback_url,
            'timestamp': time.time(),
            'source': 'fallback'
        }

        return fallback_url

    def get_quota_status(self):
        """API制限状況取得"""
        return {
            'remaining': self.api_quota_remaining,
            'cache_size': len(self.cache),
            'status': 'healthy' if self.api_quota_remaining > 10 else 'limited'
        }

def demo_hybrid_system():
    """ハイブリッドシステムデモ"""
    print("🎯 ハイブリッド画像システムデモ\n")

    manager = HybridImageManager()

    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='Haruto',
            password=os.getenv('MYSQL_PASSWORD'),
            database='swipe_app_development',
            charset='utf8mb4'
        )

        cursor = connection.cursor()
        cursor.execute("SELECT id, name, category, photos FROM spots LIMIT 5")
        spots = cursor.fetchall()

        print("📊 画像URL生成テスト:")
        for spot_id, name, category, photos in spots:
            spot_data = {
                'id': spot_id,
                'category': category,
                'photos': photos
            }

            # フォールバック画像
            fallback_url = manager.get_image_url(spot_data, prefer_google=False)
            print(f"✅ {name} ({category})")
            print(f"   🖼️  URL: {fallback_url}")
            print(f"   📊 ソース: フォールバック")
            print()

        status = manager.get_quota_status()
        print(f"📈 システム状況:")
        print(f"   🔋 API残量: {status['remaining']}")
        print(f"   💾 キャッシュ: {status['cache_size']}件")
        print(f"   📊 ステータス: {status['status']}")

        cursor.close()
        connection.close()

    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    demo_hybrid_system()
