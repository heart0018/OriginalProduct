#!/usr/bin/env python3
"""
Google API Key とphoto URL長さのデバッグスクリプト
"""

import os
from dotenv import load_dotenv

load_dotenv()

google_api_key = os.getenv('GOOGLE_API_KEY')
print(f"Google API Key長さ: {len(google_api_key)}文字")
print(f"API Key (最初の20文字): {google_api_key[:20]}...")

# サンプルphoto_reference（一般的な長さ）
sample_photo_ref = "Aap_uEA7vb0DDYVJWEaX3O-AtYp77AAmFLF3R6z7W0vUGnz4"
places_api_base = "https://maps.googleapis.com/maps/api/place"

# 様々なURL生成方法をテスト
print("\n📐 URL長さテスト:")

# 通常のURL
normal_url = f"{places_api_base}/photo?maxwidth=100&photo_reference={sample_photo_ref}&key={google_api_key}"
print(f"通常URL: {len(normal_url)}文字")

# より短いパラメータ名
short_url = f"{places_api_base}/photo?maxwidth=50&photo_reference={sample_photo_ref}&key={google_api_key}"
print(f"幅50px: {len(short_url)}文字")

# 最短URL
shortest_url = f"{places_api_base}/photo?maxwidth=50&photo_reference={sample_photo_ref}&key={google_api_key}"
print(f"最短URL: {len(shortest_url)}文字")

print(f"\n制限: 256文字")
print(f"超過分: {len(normal_url) - 256}文字")

# APIキーの一部を省略したバージョン（実際には動作しない）
truncated_key = google_api_key[:30]
truncated_url = f"{places_api_base}/photo?maxwidth=50&photo_reference={sample_photo_ref}&key={truncated_key}"
print(f"APIキー省略版: {len(truncated_url)}文字")
