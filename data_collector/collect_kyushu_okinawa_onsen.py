#!/usr/bin/env python3
"""
九州・沖縄地方リラックスカテゴリ収集スクリプト
API制限対応版 - 別府、由布院、指宿など温泉天国を制覇
"""

from limited_relax_collector import LimitedRelaxDataCollector

def collect_kyushu_okinawa_onsen():
    """九州・沖縄地方の温泉データ収集"""

    # 九州・沖縄地方の県
    kyushu_okinawa_prefectures = [
        '福岡', '佐賀', '長崎', '熊本', '大分', '宮崎', '鹿児島', '沖縄'
    ]

    collector = LimitedRelaxDataCollector(
        region_name="kyushu_okinawa",
        prefectures=kyushu_okinawa_prefectures
    )

    print("🌺 九州・沖縄地方温泉データ収集開始")
    print("🔥 温泉天国九州の名湯狙い撃ち:")
    print("  ♨️  別府温泉(大分) - 日本一の源泉数")
    print("  ♨️  由布院温泉(大分) - 洗練された温泉リゾート")
    print("  ♨️  指宿温泉(鹿児島) - 砂むし風呂で有名")
    print("  ♨️  嬉野温泉(佐賀) - 美肌の湯")
    print("  ♨️  黒川温泉(熊本) - 情緒ある温泉街")
    print("  ♨️  雲仙温泉(長崎) - 硫黄泉の名湯")
    print(f"対象県: {', '.join(kyushu_okinawa_prefectures)}")
    print("目標: 各県2-3件 × 8県 = 20件")
    print()

    # 温泉データ収集
    result = collector.collect_with_limits("温泉", target_per_prefecture=3)

    if result > 0:
        print(f"\n🎉 九州・沖縄地方完了: {result}件の温泉データを収集しました")
        print("🔥 別府、由布院、指宿など温泉天国制覇！")
        return True
    else:
        print(f"\n⚠️  九州・沖縄地方でデータを収集できませんでした")
        return False

if __name__ == "__main__":
    collect_kyushu_okinawa_onsen()
