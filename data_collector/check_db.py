#!/usr/bin/env python3
import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv(dotenv_path='.env')

config = {
    'host': 'localhost',
    'user': 'Haruto',
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': 'swipe_app_development'
}

try:
    conn = mysql.connector.connect(**config)
    cur = conn.cursor()

    print("=== ジャンル別件数 ===")
    cur.execute("SELECT genre, COUNT(*) FROM cards GROUP BY genre ORDER BY genre")
    for genre, count in cur.fetchall():
        print(f"{genre:15s}: {count:3d}件")

    print("\n=== 総計 ===")
    cur.execute("SELECT COUNT(*) FROM cards")
    total = cur.fetchone()[0]
    print(f"総カード数: {total}件")

    cur.execute("SELECT COUNT(DISTINCT place_id) FROM cards WHERE place_id IS NOT NULL")
    unique = cur.fetchone()[0]
    print(f"ユニークplace_id: {unique}件")

    print("\n=== 最新追加5件 ===")
    cur.execute("SELECT genre, title, LEFT(address, 40), created_at FROM cards ORDER BY created_at DESC LIMIT 5")
    for row in cur.fetchall():
        print(f"{row[0]:12s} | {row[1]:25s} | {row[2]:40s} | {row[3]}")

    print("\n=== active_sauna 都県別分布 ===")
    cur.execute("SELECT address FROM cards WHERE genre='active_sauna'")
    prefectures = ['東京', '神奈川', '千葉', '埼玉', '茨城', '栃木', '群馬']
    pref_count = {p: 0 for p in prefectures}

    for (addr,) in cur.fetchall():
        if addr:
            for pref in prefectures:
                if pref in addr:
                    pref_count[pref] += 1
                    break

    for pref, count in pref_count.items():
        print(f"{pref}: {count}件")

    conn.close()

except Exception as e:
    print(f"エラー: {e}")
