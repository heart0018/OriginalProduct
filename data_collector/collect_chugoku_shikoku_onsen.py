#!/usr/bin/env python3
"""
中国・四国地方リラックスカテゴリ収集スクリプト
API制限対応版 - 道後温泉、湯原温泉など名湯を狙い撃ち
"""

from limited_relax_collector import LimitedRelaxDataCollector

def collect_chugoku_shikoku_onsen():
    """中国・四国地方の温泉データ収集"""

    # 中国・四国地方の県
    chugoku_shikoku_prefectures = [
        '岡山', '広島', '山口', '鳥取', '島根',  # 中国地方
        '徳島', '香川', '愛媛', '高知'           # 四国地方
    ]

    collector = LimitedRelaxDataCollector(
        region_name="chugoku_shikoku",
        prefectures=chugoku_shikoku_prefectures
    )

    print("🌊 中国・四国地方温泉データ収集開始")
    print("🎯 狙い撃ち有名温泉:")
    print("  ♨️  道後温泉(愛媛) - 日本最古の温泉")
    print("  ♨️  湯原温泉(岡山) - 露天風呂の名所")
    print("  ♨️  玉造温泉(島根) - 美肌の湯")
    print("  ♨️  宮島温泉(広島) - 世界遺産の温泉")
    print("  ♨️  大歩危温泉(徳島) - 渓谷美の温泉")
    print(f"対象県: {', '.join(chugoku_shikoku_prefectures)}")
    print("目標: 各県2件 × 9県 = 18件")
    print()

    # 温泉データ収集
    result = collector.collect_with_limits("温泉", target_per_prefecture=2)

    if result > 0:
        print(f"\n🎉 中国・四国地方完了: {result}件の温泉データを収集しました")
        print("💎 道後温泉、湯原温泉などの名湯データ獲得！")
        return True
    else:
        print(f"\n⚠️  中国・四国地方でデータを収集できませんでした")
        return False

if __name__ == "__main__":
    collect_chugoku_shikoku_onsen()
