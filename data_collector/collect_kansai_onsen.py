#!/usr/bin/env python3
"""
近畿地方リラックスカテゴリ収集スクリプト
API制限対応版
"""

from limited_relax_collector import LimitedRelaxDataCollector

def collect_kansai_onsen():
    """近畿地方の温泉データ収集"""

    # 近畿地方の都府県
    kansai_prefectures = ['大阪', '京都', '兵庫', '奈良', '和歌山', '滋賀']

    collector = LimitedRelaxDataCollector(
        region_name="kansai",
        prefectures=kansai_prefectures
    )

    print("🌸 近畿地方温泉データ収集開始")
    print(f"対象府県: {', '.join(kansai_prefectures)}")
    print("目標: 各府県3件 × 6府県 = 18件")
    print()

    # 温泉データ収集
    result = collector.collect_with_limits("温泉", target_per_prefecture=3)

    if result > 0:
        print(f"\n🎉 近畿地方完了: {result}件の温泉データを収集しました")
        return True
    else:
        print(f"\n⚠️  近畿地方でデータを収集できませんでした")
        return False

if __name__ == "__main__":
    collect_kansai_onsen()
