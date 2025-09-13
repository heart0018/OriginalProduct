#!/usr/bin/env python3
"""
API制限アラートのテスト
意図的に制限を低く設定してアラート機能をテスト
"""

from api_limit_manager import LimitedPlacesAPIClient, APILimitManager
import time

class TestLimitManager(APILimitManager):
    """テスト用の低い制限値"""
    def __init__(self):
        super().__init__()
        # テスト用に制限を低く設定
        self.REQUESTS_PER_MINUTE = 3  # 3req/min
        self.REQUESTS_PER_DAY = 10     # 10req/day
        self.MIN_REQUEST_INTERVAL = 60 / self.REQUESTS_PER_MINUTE  # 20秒

class TestLimitedPlacesAPIClient(LimitedPlacesAPIClient):
    """テスト用APIクライアント"""
    def __init__(self):
        super().__init__()
        self.limit_manager = TestLimitManager()

def test_rate_limit_alerts():
    """レート制限アラートのテスト"""
    print("=== API制限アラートテスト ===")
    print("テスト用制限: 3req/min, 10req/day\n")

    client = TestLimitedPlacesAPIClient()

    test_queries = [
        "温泉 東京", "温泉 大阪", "温泉 京都", "温泉 神奈川",
        "温泉 千葉", "温泉 埼玉", "温泉 茨城", "温泉 栃木"
    ]

    for i, query in enumerate(test_queries):
        print(f"\n--- テスト {i+1}: {query} ---")

        results, success = client.search_places(query)

        if success:
            print(f"✅ 成功: {len(results)}件の結果")
        else:
            print("❌ 制限またはエラーにより失敗")

        print(f"現在の状況: {client.get_usage_summary()}")

        # 日次制限に到達した場合は停止
        if client.limit_manager.daily_limit_reached:
            print("\n🚨 日次制限に到達したため、テストを終了します。")
            break

        # 少し待機
        time.sleep(1)

    print(f"\n=== テスト完了 ===")
    print(f"最終状況: {client.get_usage_summary()}")

if __name__ == "__main__":
    test_rate_limit_alerts()
