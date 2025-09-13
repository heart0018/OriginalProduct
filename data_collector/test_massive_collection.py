#!/usr/bin/env python3
"""
大規模収集のテスト実行
北海道公園×10件で動作確認
"""

import os
import sys
sys.path.append('/home/haruto/OriginalProdact/data_collector')

from massive_relax_collector import MassiveRelaxCollector

def test_small_collection():
    print("🧪 大規模収集システムテスト")
    print("📍 北海道 × 公園 × 10件")

    collector = MassiveRelaxCollector()

    # テスト収集
    collected = collector.collect_category_region('parks', 'hokkaido', 10)

    print(f"\n✅ テスト完了:")
    print(f"   収集件数: {collected}件")
    print(f"   API使用: {collector.api_usage}回")

    if collected >= 5:
        print("🎉 システム正常動作確認!")
        print("💚 大規模収集準備完了")
    else:
        print("⚠️  収集数が少ない - 設定要確認")

if __name__ == "__main__":
    test_small_collection()
