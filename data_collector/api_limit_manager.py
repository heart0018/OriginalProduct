#!/usr/bin/env python3
"""
API制限対応のデータ収集ベースクラス
Places APIの制限（250req/min, 200req/day）に対応したエラーハンドリング
"""

import os
import time
import json
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dotenv import load_dotenv

load_dotenv()

class APILimitManager:
    """Places API制限管理クラス"""

    def __init__(self):
        # API制限設定
        self.REQUESTS_PER_MINUTE = 250
        self.REQUESTS_PER_DAY = 200
        self.MIN_REQUEST_INTERVAL = 60 / self.REQUESTS_PER_MINUTE  # 0.24秒

        # リクエスト追跡
        self.request_times = []
        self.daily_request_count = 0
        self.last_reset_date = datetime.now().date()

        # 制限状態追跡
        self.is_rate_limited = False
        self.daily_limit_reached = False

    def reset_daily_counter_if_needed(self):
        """日付が変わった場合のカウンターリセット"""
        current_date = datetime.now().date()
        if current_date != self.last_reset_date:
            self.daily_request_count = 0
            self.last_reset_date = current_date
            self.daily_limit_reached = False
            print(f"🔄 日次カウンターをリセットしました ({current_date})")

    def can_make_request(self) -> Tuple[bool, str]:
        """リクエスト可能かチェック"""
        self.reset_daily_counter_if_needed()

        # 日次制限チェック
        if self.daily_request_count >= self.REQUESTS_PER_DAY:
            self.daily_limit_reached = True
            return False, f"❌ 日次制限に到達しました ({self.daily_request_count}/{self.REQUESTS_PER_DAY})"

        # 分次制限チェック
        now = time.time()
        # 1分以内のリクエストをカウント
        recent_requests = [t for t in self.request_times if now - t < 60]

        if len(recent_requests) >= self.REQUESTS_PER_MINUTE:
            self.is_rate_limited = True
            wait_time = 60 - (now - recent_requests[0])
            return False, f"⚠️  分次制限に到達しました。{wait_time:.1f}秒待機が必要です"

        return True, "OK"

    def wait_if_needed(self):
        """必要に応じて待機"""
        if self.request_times:
            time_since_last = time.time() - self.request_times[-1]
            if time_since_last < self.MIN_REQUEST_INTERVAL:
                wait_time = self.MIN_REQUEST_INTERVAL - time_since_last
                time.sleep(wait_time)

    def record_request(self):
        """リクエストを記録"""
        now = time.time()
        self.request_times.append(now)
        self.daily_request_count += 1

        # 1分以上古いリクエストを削除
        self.request_times = [t for t in self.request_times if now - t < 60]

        # レート制限状態をリセット
        self.is_rate_limited = False

    def get_status(self) -> Dict:
        """現在の状況を取得"""
        self.reset_daily_counter_if_needed()
        now = time.time()
        recent_requests = [t for t in self.request_times if now - t < 60]

        return {
            'daily_used': self.daily_request_count,
            'daily_limit': self.REQUESTS_PER_DAY,
            'daily_remaining': self.REQUESTS_PER_DAY - self.daily_request_count,
            'minute_used': len(recent_requests),
            'minute_limit': self.REQUESTS_PER_MINUTE,
            'minute_remaining': self.REQUESTS_PER_MINUTE - len(recent_requests),
            'is_rate_limited': self.is_rate_limited,
            'daily_limit_reached': self.daily_limit_reached
        }


class LimitedPlacesAPIClient:
    """制限対応Places APIクライアント"""

    def __init__(self):
        self.google_api_key = os.getenv('GOOGLE_API_KEY')
        self.text_search_url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        self.place_details_url = "https://maps.googleapis.com/maps/api/place/details/json"
        self.limit_manager = APILimitManager()

    def search_places(self, query: str) -> Tuple[List[Dict], bool]:
        """制限対応の場所検索"""
        # リクエスト可能かチェック
        can_request, message = self.limit_manager.can_make_request()
        if not can_request:
            self._show_limit_alert(message)
            return [], False

        # 待機が必要な場合は待機
        self.limit_manager.wait_if_needed()

        params = {
            'query': query,
            'key': self.google_api_key,
            'language': 'ja',
            'region': 'jp'
        }

        try:
            response = requests.get(self.text_search_url, params=params)
            self.limit_manager.record_request()

            data = response.json()
            status = data.get('status')

            # APIエラーの詳細ハンドリング
            if status == 'OVER_QUERY_LIMIT':
                self._show_quota_exceeded_alert()
                return [], False
            elif status == 'REQUEST_DENIED':
                self._show_request_denied_alert(data.get('error_message', ''))
                return [], False
            elif status == 'OK':
                results = data.get('results', [])
                self._show_request_success(query, len(results))
                return results, True
            else:
                print(f"⚠️  APIエラー: {status} - {query}")
                print(f"   エラーメッセージ: {data.get('error_message', 'なし')}")
                return [], False

        except Exception as e:
            print(f"❌ リクエストエラー ({query}): {e}")
            return [], False

    def get_place_details(self, place_id: str) -> Tuple[Optional[Dict], bool]:
        """制限対応の詳細情報取得"""
        # リクエスト可能かチェック
        can_request, message = self.limit_manager.can_make_request()
        if not can_request:
            self._show_limit_alert(message)
            return None, False

        # 待機が必要な場合は待機
        self.limit_manager.wait_if_needed()

        params = {
            'place_id': place_id,
            'key': self.google_api_key,
            'language': 'ja',
            'fields': 'place_id,name,formatted_address,geometry,rating,user_ratings_total,price_level,formatted_phone_number,website,opening_hours,photos,types,vicinity,plus_code'
        }

        try:
            response = requests.get(self.place_details_url, params=params)
            self.limit_manager.record_request()

            data = response.json()
            status = data.get('status')

            # APIエラーの詳細ハンドリング
            if status == 'OVER_QUERY_LIMIT':
                self._show_quota_exceeded_alert()
                return None, False
            elif status == 'REQUEST_DENIED':
                self._show_request_denied_alert(data.get('error_message', ''))
                return None, False
            elif status == 'OK':
                result = data.get('result')
                return result, True
            else:
                print(f"⚠️  詳細取得エラー: {status} - {place_id}")
                return None, False

        except Exception as e:
            print(f"❌ 詳細取得例外 ({place_id}): {e}")
            return None, False

    def _show_limit_alert(self, message: str):
        """制限アラート表示"""
        print("\n" + "="*60)
        print("🚨 API制限アラート 🚨")
        print("="*60)
        print(f"メッセージ: {message}")

        status = self.limit_manager.get_status()
        print(f"日次使用量: {status['daily_used']}/{status['daily_limit']} (残り: {status['daily_remaining']})")
        print(f"分次使用量: {status['minute_used']}/{status['minute_limit']} (残り: {status['minute_remaining']})")

        if status['daily_limit_reached']:
            print("📅 日次制限に到達しました。明日まで待機してください。")
        elif status['is_rate_limited']:
            print("⏰ 分次制限に到達しました。1分待機してから再試行してください。")

        print("="*60 + "\n")

    def _show_quota_exceeded_alert(self):
        """クォータ超過アラート"""
        print("\n" + "="*60)
        print("🚨 QUOTA EXCEEDED - API制限超過 🚨")
        print("="*60)
        print("Google Places APIのクォータ制限に到達しました。")
        print("- 分次制限: 250リクエスト/分")
        print("- 日次制限: 200リクエスト/日")
        print("しばらく待機してから再試行してください。")
        print("="*60 + "\n")

    def _show_request_denied_alert(self, error_message: str):
        """リクエスト拒否アラート"""
        print("\n" + "="*60)
        print("🚨 REQUEST DENIED - リクエスト拒否 🚨")
        print("="*60)
        print("APIリクエストが拒否されました。")
        print(f"エラーメッセージ: {error_message}")
        print("API設定や認証情報を確認してください。")
        print("="*60 + "\n")

    def _show_request_success(self, query: str, result_count: int):
        """リクエスト成功ログ"""
        status = self.limit_manager.get_status()
        print(f"✅ 検索成功: {query} ({result_count}件) "
              f"[日次: {status['daily_used']}/{status['daily_limit']}, "
              f"分次: {status['minute_used']}/{status['minute_limit']}]")

    def get_usage_summary(self) -> str:
        """使用量サマリー"""
        status = self.limit_manager.get_status()
        return (f"📊 API使用状況: "
                f"日次 {status['daily_used']}/{status['daily_limit']} "
                f"({status['daily_remaining']} 残り), "
                f"分次 {status['minute_used']}/{status['minute_limit']} "
                f"({status['minute_remaining']} 残り)")


def test_api_limits():
    """API制限テスト"""
    print("=== API制限対応テスト ===\n")

    client = LimitedPlacesAPIClient()

    # テスト検索
    test_queries = ["温泉 東京", "温泉 大阪", "温泉 北海道"]

    for query in test_queries:
        print(f"\n--- {query} のテスト ---")
        results, success = client.search_places(query)

        if success and results:
            # 最初の結果の詳細を取得
            first_result = results[0]
            place_id = first_result.get('place_id')
            if place_id:
                details, detail_success = client.get_place_details(place_id)
                if detail_success:
                    print(f"   詳細取得成功: {details.get('name')}")

        print(f"   {client.get_usage_summary()}")
        time.sleep(1)

    print(f"\n=== テスト完了 ===")
    print(client.get_usage_summary())


if __name__ == "__main__":
    test_api_limits()
