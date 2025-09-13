#!/usr/bin/env python3
"""
全国リラックスカテゴリデータ収集統括スクリプト
全地域のリラックスカテゴリ（温泉・公園・サウナ・カフェ・散歩コース）を一括収集
"""

import os
import sys
import subprocess
import time
from datetime import datetime

def run_region_collector(script_name, region_name):
    """指定した地域のデータ収集スクリプトを実行"""
    print(f"\n{'='*50}")
    print(f"{region_name}のリラックスカテゴリデータ収集を開始")
    print(f"実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    try:
        result = subprocess.run([
            'python3', script_name
        ], capture_output=True, text=True, timeout=3600)  # 1時間でタイムアウト

        if result.returncode == 0:
            print(f"✅ {region_name}の収集が正常に完了しました")
            print("標準出力:")
            print(result.stdout)
        else:
            print(f"❌ {region_name}の収集でエラーが発生しました")
            print("標準エラー:")
            print(result.stderr)

    except subprocess.TimeoutExpired:
        print(f"⏰ {region_name}の収集がタイムアウトしました")
    except Exception as e:
        print(f"🚫 {region_name}の収集で例外が発生しました: {e}")

def main():
    """メイン関数"""
    print("🎯 全国リラックスカテゴリデータ収集を開始します")
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 収集する地域とスクリプトのマッピング
    regions = [
        ('fetch_hokkaido_relax.py', '北海道'),
        ('fetch_tohoku_relax.py', '東北'),
        ('fetch_kanto_relax.py', '関東'),
        ('fetch_chubu_relax.py', '中部'),
        ('fetch_kansai_relax.py', '関西'),
        ('fetch_chugoku_shikoku_relax.py', '中国・四国'),
        ('fetch_kyushu_okinawa_relax.py', '九州・沖縄')
    ]

    total_regions = len(regions)
    completed_regions = 0
    failed_regions = []

    for script_name, region_name in regions:
        # スクリプトファイルの存在確認
        if not os.path.exists(script_name):
            print(f"⚠️  スクリプトファイルが見つかりません: {script_name}")
            failed_regions.append(region_name)
            continue

        # 地域別データ収集実行
        run_region_collector(script_name, region_name)
        completed_regions += 1

        # 進捗表示
        progress = (completed_regions / total_regions) * 100
        print(f"\n📊 進捗: {completed_regions}/{total_regions} 地域完了 ({progress:.1f}%)")

        # 次の地域まで待機（APIレート制限対策）
        if completed_regions < total_regions:
            wait_time = 120  # 2分間待機
            print(f"⏳ 次の地域まで{wait_time}秒待機します...")
            time.sleep(wait_time)

    # 最終結果
    print(f"\n{'='*60}")
    print("🎉 全国リラックスカテゴリデータ収集が完了しました")
    print(f"完了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📈 成功: {completed_regions}地域")

    if failed_regions:
        print(f"❌ 失敗: {len(failed_regions)}地域")
        print(f"失敗地域: {', '.join(failed_regions)}")

    print(f"{'='*60}")

    # 収集目標の確認
    print("\n📋 収集目標:")
    print("各地域 × 各カテゴリ20件ずつ")
    print("- 温泉: 20件")
    print("- 公園: 20件")
    print("- サウナ: 20件")
    print("- カフェ: 20件")
    print("- 散歩コース: 20件")
    print("地域別合計: 100件")
    print(f"全国合計目標: {100 * 7}件")

if __name__ == "__main__":
    main()
