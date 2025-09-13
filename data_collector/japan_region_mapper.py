#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
住所解析による地域修正システム（日本標準7地域対応版）
"""

import re
import mysql.connector
import os
from dotenv import load_dotenv

class JapanRegionMapper:
    def __init__(self):
        # 都道府県→標準地域マッピング
        self.prefecture_to_region = {
            # 北海道地方
            '北海道': 'hokkaido',

            # 東北地方
            '青森県': 'tohoku', '岩手県': 'tohoku', '宮城県': 'tohoku',
            '秋田県': 'tohoku', '山形県': 'tohoku', '福島県': 'tohoku',

            # 関東地方
            '東京都': 'kanto', '茨城県': 'kanto', '栃木県': 'kanto',
            '群馬県': 'kanto', '埼玉県': 'kanto', '千葉県': 'kanto', '神奈川県': 'kanto',

            # 中部地方
            '新潟県': 'chubu', '富山県': 'chubu', '石川県': 'chubu', '福井県': 'chubu',
            '山梨県': 'chubu', '長野県': 'chubu', '岐阜県': 'chubu',
            '静岡県': 'chubu', '愛知県': 'chubu',

            # 近畿地方
            '京都府': 'kinki', '大阪府': 'kinki', '三重県': 'kinki',
            '滋賀県': 'kinki', '兵庫県': 'kinki', '奈良県': 'kinki', '和歌山県': 'kinki',
            '京都': 'kinki',  # 京都の省略形対応

            # 中国地方
            '鳥取県': 'chugoku', '島根県': 'chugoku', '岡山県': 'chugoku',
            '広島県': 'chugoku', '山口県': 'chugoku',

            # 四国地方 → 中国地方に統合
            '徳島県': 'chugoku', '香川県': 'chugoku',
            '愛媛県': 'chugoku', '高知県': 'chugoku',

            # 九州地方
            '福岡県': 'kyushu', '佐賀県': 'kyushu', '長崎県': 'kyushu',
            '大分県': 'kyushu', '熊本県': 'kyushu', '宮崎県': 'kyushu',
            '鹿児島県': 'kyushu', '沖縄県': 'kyushu'
        }

        # 地域名（日本語）
        self.region_names = {
            'hokkaido': '北海道',
            'tohoku': '東北',
            'kanto': '関東',
            'chubu': '中部',
            'kinki': '近畿',
            'chugoku': '中国',
            'kyushu': '九州'
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

        # 最長の県名を返す
        if found_prefectures:
            return max(found_prefectures, key=len)

        return None

    def get_region_from_address(self, address):
        """住所から地域を判定"""
        prefecture = self.extract_prefecture_from_address(address)
        if prefecture:
            return self.prefecture_to_region.get(prefecture)
        return None

    def update_all_regions(self):
        """全データの地域を標準7地域に修正"""
        load_dotenv()

        connection = mysql.connector.connect(
            host='localhost',
            user='Haruto',
            password=os.getenv('MYSQL_PASSWORD'),
            database='swipe_app_production',
            charset='utf8mb4'
        )

        cursor = connection.cursor()
        cursor.execute('SELECT id, title, address, region FROM cards WHERE address IS NOT NULL')
        results = cursor.fetchall()

        print(f'🔄 {len(results)}件のデータを標準7地域に修正中...\\n')

        updated = 0
        failed = 0
        no_change = 0

        region_updates = {}

        for card_id, title, address, current_region in results:
            predicted_region = self.get_region_from_address(address)

            if predicted_region:
                if predicted_region != current_region:
                    cursor.execute(
                        'UPDATE cards SET region = %s WHERE id = %s',
                        (predicted_region, card_id)
                    )
                    updated += 1

                    if predicted_region not in region_updates:
                        region_updates[predicted_region] = 0
                    region_updates[predicted_region] += 1

                    print(f'✅ 更新: {title[:30]}... {current_region} → {predicted_region}')
                else:
                    no_change += 1
            else:
                failed += 1
                print(f'❌ 失敗: {title[:30]}... (住所: {address[:50]}...)')

        connection.commit()
        cursor.close()
        connection.close()

        print(f'\\n📊 修正完了レポート:')
        print(f'   ✅ 更新件数: {updated}件')
        print(f'   ⏸️ 変更なし: {no_change}件')
        print(f'   ❌ 失敗件数: {failed}件')

        print(f'\\n🗾 更新後の地域分布:')
        for region, count in sorted(region_updates.items()):
            print(f'   {self.region_names[region]}: +{count}件')

    def show_final_distribution(self):
        """最終的な地域分布を表示"""
        load_dotenv()

        connection = mysql.connector.connect(
            host='localhost',
            user='Haruto',
            password=os.getenv('MYSQL_PASSWORD'),
            database='swipe_app_production',
            charset='utf8mb4'
        )

        cursor = connection.cursor()
        cursor.execute('SELECT region, COUNT(*) FROM cards GROUP BY region ORDER BY region')
        results = cursor.fetchall()

        print('\\n🎯 最終的な地域分布:')
        total = 0
        for region, count in results:
            region_name = self.region_names.get(region, region)
            print(f'   {region_name}({region}): {count}件')
            total += count

        print(f'\\n📊 総計: {total}件')

        cursor.close()
        connection.close()

def main():
    mapper = JapanRegionMapper()

    print('🗾 日本標準7地域マッピングシステム\\n')

    # 全データの地域修正
    mapper.update_all_regions()

    # 最終分布表示
    mapper.show_final_distribution()

if __name__ == '__main__':
    main()
