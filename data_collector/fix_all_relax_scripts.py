#!/usr/bin/env python3
"""
全地域のリラックス収集スクリプトを修正するバッチスクリプト
place_id null エラーを解決する統一修正
"""

import os
import re

def fix_save_to_database_method(file_path):
    """_save_to_database メソッドを修正"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 既に修正済みかチェック
        if 'place_id が空です' in content:
            print(f"✅ {os.path.basename(file_path)}: 既に修正済み")
            return True

        # _save_to_database メソッドを探して置換
        old_pattern = r'''(\s+saved_count = 0\s+for place in places:\s+try:\s+# 写真情報の処理)'''

        new_code = '''        saved_count = 0
        for i, place in enumerate(places):
            try:
                # データ検証を追加
                place_id = place.get('place_id')
                name = place.get('name')

                # place_id の検証
                if not place_id:
                    print(f"  警告: place_id が空です - {name} (データ {i+1})")
                    continue

                # name の検証
                if not name:
                    print(f"  警告: name が空です - place_id: {place_id} (データ {i+1})")
                    continue

                print(f"  保存中 ({i+1}/{len(places)}): {name}")

                # 写真情報の処理'''

        # より具体的なパターンで検索・置換
        save_method_pattern = r'''(\s+saved_count = 0\s+for place in places:\s+try:\s+# 写真情報の処理)'''

        if re.search(save_method_pattern, content):
            content = re.sub(save_method_pattern, new_code, content)
        else:
            print(f"❌ {os.path.basename(file_path)}: パターンが見つかりません")
            return False

        # データ配列の place.get('place_id') を place_id に変更
        content = re.sub(r'place\.get\(\'place_id\'\),\s+place\.get\(\'name\'\),', 'place_id,\n                    name,', content)

        # エラーハンドリング部分を修正
        old_error_pattern = r'''except Exception as e:\s+print\(f"保存エラー: \{e\} - \{place\.get\('name', 'Unknown'\)\}"\)\s+continue'''
        new_error_code = '''except Exception as e:
                print(f"    ❌ 保存エラー: {e} - {place.get('name', 'Unknown')}")
                print(f"    エラー詳細: {type(e).__name__}")
                if hasattr(e, 'errno'):
                    print(f"    MySQL Error Code: {e.errno}")
                continue'''

        content = re.sub(old_error_pattern, new_error_code, content)

        # cursor.execute の後に成功メッセージを追加
        content = re.sub(r'(\s+cursor\.execute\(insert_query, data\)\s+saved_count \+= 1)', r'\1\n                print(f"    ✅ 保存成功")', content)

        # ファイルに書き戻し
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        print(f"✅ {os.path.basename(file_path)}: 修正完了")
        return True

    except Exception as e:
        print(f"❌ {os.path.basename(file_path)}: 修正エラー - {e}")
        return False

def main():
    """メイン処理"""
    script_dir = "/home/haruto/OriginalProdact/data_collector"

    # 修正対象ファイル
    relax_scripts = [
        "fetch_hokkaido_relax.py",
        "fetch_tohoku_relax.py",
        "fetch_kanto_relax.py",
        "fetch_kansai_relax.py",
        "fetch_chugoku_shikoku_relax.py",
        "fetch_kyushu_okinawa_relax.py"
    ]

    print("=== 全地域リラックス収集スクリプト修正開始 ===\n")

    success_count = 0

    for script in relax_scripts:
        file_path = os.path.join(script_dir, script)
        if os.path.exists(file_path):
            if fix_save_to_database_method(file_path):
                success_count += 1
        else:
            print(f"❌ {script}: ファイルが見つかりません")

    print(f"\n=== 修正完了: {success_count}/{len(relax_scripts)} ファイル ===")

if __name__ == "__main__":
    main()
