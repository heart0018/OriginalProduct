#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
住所解析による自動地域判定システム
Google Places APIのaddressから都道府県を抽出し、地域を自動判定
"""

import re
import mysql.connector
import os
from dotenv import load_dotenv

class AddressRegionAnalyzer:
    def __init__(self):
        # 都道府県→地域マッピング
        self.prefecture_to_region = {
            # 北海道
            '北海道': 'hokkaido',

            # 東北
            '青森県': 'tohoku', '岩手県': 'tohoku', '宮城県': 'tohoku',
            '秋田県': 'tohoku', '山形県': 'tohoku', '福島県': 'tohoku',

            # 関東
            '茨城県': 'kanto', '栃木県': 'kanto', '群馬県': 'kanto',
            '埼玉県': 'kanto', '千葉県': 'kanto', '東京都': 'kanto', '神奈川県': 'kanto',

            # 中部
            '新潟県': 'chubu', '富山県': 'chubu', '石川県': 'chubu', '福井県': 'chubu',
            '山梨県': 'chubu', '長野県': 'chubu', '岐阜県': 'chubu',
            '静岡県': 'chubu', '愛知県': 'chubu',

            # 関西
            '三重県': 'kansai', '滋賀県': 'kansai', '京都府': 'kansai', '京都': 'kansai',  # 京都の両パターンに対応
            '大阪府': 'kansai', '兵庫県': 'kansai', '奈良県': 'kansai', '和歌山県': 'kansai',

            # 中国・四国
            '鳥取県': 'chugoku_shikoku', '島根県': 'chugoku_shikoku',
            '岡山県': 'chugoku_shikoku', '広島県': 'chugoku_shikoku', '山口県': 'chugoku_shikoku',
            '徳島県': 'chugoku_shikoku', '香川県': 'chugoku_shikoku',
            '愛媛県': 'chugoku_shikoku', '高知県': 'chugoku_shikoku',

            # 九州・沖縄
            '福岡県': 'kyushu_okinawa', '佐賀県': 'kyushu_okinawa', '長崎県': 'kyushu_okinawa',
            '熊本県': 'kyushu_okinawa', '大分県': 'kyushu_okinawa', '宮崎県': 'kyushu_okinawa',
            '鹿児島県': 'kyushu_okinawa', '沖縄県': 'kyushu_okinawa'
        }

        # 地域名（日本語）
        self.region_names = {
            'hokkaido': '北海道',
            'tohoku': '東北',
            'kanto': '関東',
            'chubu': '中部',
            'kansai': '関西',
            'chugoku_shikoku': '中国四国',
            'kyushu_okinawa': '九州沖縄'
        }

    def extract_prefecture_from_address(self, address):
        """住所から都道府県を抽出"""
        if not address:
            return None

        # パターン1: 日本、〒XXX-XXXX 都道府県
        pattern1 = r'日本、〒[0-9]{3}-[0-9]{4}\s+([^市区町村]+?[県都府])'
        match1 = re.search(pattern1, address)
        if match1:
            return match1.group(1)

        # パターン2: 日本、都道府県（郵便番号なし）
        pattern2 = r'日本、([^市区町村]+?[県都府])'
        match2 = re.search(pattern2, address)
        if match2:
            return match2.group(1)

        # パターン3: 都道府県を直接検索（最長マッチ）
        found_prefectures = []
        for prefecture in self.prefecture_to_region.keys():
            if prefecture in address:
                found_prefectures.append(prefecture)

        # 最長の県名を返す（京都 < 京都府）
        if found_prefectures:
            return max(found_prefectures, key=len)

        return None

    def get_region_from_address(self, address):
        """住所から地域を判定"""
        prefecture = self.extract_prefecture_from_address(address)
        if prefecture:
            return self.prefecture_to_region.get(prefecture)
        return None

    def analyze_existing_data(self):
        """既存データの地域判定精度を分析"""
        load_dotenv()

        connection = mysql.connector.connect(
            host='localhost',
            user='Haruto',
            password=os.getenv('MYSQL_PASSWORD'),
            database='swipe_app_production',
            charset='utf8mb4'
        )

        cursor = connection.cursor()
        cursor.execute('SELECT id, title, address, region FROM cards WHERE address IS NOT NULL LIMIT 100')
        results = cursor.fetchall()

        print('🔍 既存データの地域判定精度分析\n')

        correct = 0
        incorrect = 0
        no_prefecture = 0

        for card_id, title, address, current_region in results:
            extracted_prefecture = self.extract_prefecture_from_address(address)
            predicted_region = self.get_region_from_address(address)

            if not extracted_prefecture:
                no_prefecture += 1
                print(f'❌ 県名抽出失敗: {title}')
                print(f'   住所: {address}')
                continue

            if predicted_region == current_region:
                correct += 1
            else:
                incorrect += 1
                print(f'⚠️ 地域判定相違: {title}')
                print(f'   住所: {address}')
                print(f'   抽出県名: {extracted_prefecture}')
                print(f'   現在地域: {current_region}')
                print(f'   予測地域: {predicted_region}')
                print()

        total = len(results)
        print(f'\n📊 分析結果:')
        print(f'   正解: {correct}/{total}件 ({correct/total*100:.1f}%)')
        print(f'   不正解: {incorrect}/{total}件 ({incorrect/total*100:.1f}%)')
        print(f'   県名抽出失敗: {no_prefecture}/{total}件 ({no_prefecture/total*100:.1f}%)')

        cursor.close()
        connection.close()

    def update_regions_by_address(self):
        """住所解析による地域情報の一括更新"""
        load_dotenv()

        connection = mysql.connector.connect(
            host='localhost',
            user='Haruto',
            password=os.getenv('MYSQL_PASSWORD'),
            database='swipe_app_production',
            charset='utf8mb4'
        )

        cursor = connection.cursor()
        cursor.execute('SELECT id, address, region FROM cards WHERE address IS NOT NULL')
        results = cursor.fetchall()

        print(f'🔄 {len(results)}件の地域情報を住所解析で更新中...\n')

        updated = 0
        failed = 0

        for card_id, address, current_region in results:
            predicted_region = self.get_region_from_address(address)

            if predicted_region and predicted_region != current_region:
                cursor.execute(
                    'UPDATE cards SET region = %s WHERE id = %s',
                    (predicted_region, card_id)
                )
                updated += 1
                print(f'✅ 更新: ID {card_id} {current_region} → {predicted_region}')
            elif not predicted_region:
                failed += 1

        connection.commit()
        cursor.close()
        connection.close()

        print(f'\n📊 更新完了:')
        print(f'   更新件数: {updated}件')
        print(f'   失敗件数: {failed}件')

def main():
    analyzer = AddressRegionAnalyzer()

    print('🎯 住所解析による地域判定システム\n')

    # テスト住所
    test_addresses = [
        '日本、〒509-0238 岐阜県可児市大森１７４８−１',
        '日本、〒413-0233 静岡県伊東市赤沢１７０−２',
        '日本、〒983-0013 宮城県仙台市宮城野区中野３丁目４−９',
        '日本、北海道札幌市中央区南３条西６丁目',
        '日本、東京都新宿区歌舞伎町１丁目'
    ]

    print('🧪 テスト実行:')
    for address in test_addresses:
        prefecture = analyzer.extract_prefecture_from_address(address)
        region = analyzer.get_region_from_address(address)
        print(f'住所: {address}')
        print(f'県名: {prefecture} → 地域: {region}')
        print()

    # 既存データ分析
    analyzer.analyze_existing_data()

if __name__ == '__main__':
    main()
