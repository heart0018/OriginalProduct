# データ自動取得スクリプト

温泉×東京の施設データをGoogle Places APIから取得し、MySQLデータベースに保存するスクリプトです。

## 📋 準備

### 1. 必要なライブラリのインストール

```bash
cd /home/haruto/OriginalProdact/data_collector
pip install -r requirements.txt
```

### 2. 環境変数の設定

`.env`ファイルに以下の情報が設定されていることを確認してください：

```
GOOGLE_API_KEY=your_google_api_key
MYSQL_PASSWORD=your_mysql_password
MYSQL_HOST=localhost
MYSQL_USER=Haruto
MYSQL_DATABASE=swipe_app_development
```

### 3. データベースの準備

MySQLサーバーが起動しており、`swipe_app_development`データベースが存在することを確認してください。

```bash
# MySQLにログイン
mysql -u Haruto -p

# データベース確認
SHOW DATABASES;
USE swipe_app_development;
SHOW TABLES;
```

## 🚀 実行方法

```bash
cd /home/haruto/OriginalProdact/data_collector
python fetch_onsen_tokyo.py
```

## 📊 取得データ

スクリプトは以下のデータを取得します：

- **対象**: 東京の温泉・銭湯・スーパー銭湯
- **件数**: 約10件（重複除去後）
- **取得項目**:
  - 施設名（title）
  - 評価（rating）
  - レビュー件数（review_count）
  - 画像URL（image_url）
  - Google Maps URL（external_link）
  - 地域（region: "関東"）
  - 住所（address）
  - タイプ（type: "relax_onsen"）
- **レビューデータ**:
  - 各施設につき最大10件の日本語レビュー
  - 新しい順にソート
  - 1000文字を超える場合は省略

## 🔍 検索キーワード

以下のキーワードで検索を行います：

1. "温泉 東京"
2. "銭湯 東京"
3. "スーパー銭湯 東京"
4. "天然温泉 東京"

## ✅ 処理フロー

1. Google Places API Text Searchで施設検索
2. 温泉・銭湯関連施設のフィルタリング
3. 重複除去（place_idベース）
4. Google Places API Place Detailsで詳細情報取得
5. **日本語レビューの抽出と整形**（最大10件）
6. データ整形
7. MySQLデータベースに保存（重複チェック付き）
   - `cards`テーブル：施設情報
   - `review_comments`テーブル：レビューデータ

## ⚠️ 注意事項

- Google Places APIには1日あたりのリクエスト制限があります
- 重複データは自動的にスキップされます
- API呼び出し間には適切な間隔を設けています
- エラーログは標準出力に表示されます

## 🐛 トラブルシューティング

### データベース接続エラー
- MySQLサーバーが起動しているか確認
- 認証情報（ユーザー名・パスワード）が正しいか確認
- データベースが存在するか確認

### API エラー
- Google Places APIキーが有効か確認
- APIの使用制限に達していないか確認
- インターネット接続を確認

### インストールエラー
- Pythonのバージョンが3.6以上か確認
- pipが最新版か確認
- 仮想環境の使用を推奨
