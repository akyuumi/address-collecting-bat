# YouTube Channel Collector

YouTube の人気動画からチャンネル情報を自動収集し、メールアドレスを抽出するバッチ処理ツールです。

## 機能

- YouTube Data API を使用して人気動画からチャンネル情報を取得
- チャンネル説明文からメールアドレスを自動抽出
- **GCS の最新 CSV ファイルをデータソースとして使用**
- CSV ファイルとしてエクスポート
- Google Cloud Storage への自動アップロード
- **新機能**: Slack 通知による新規チャンネルの差分通知

## データフロー

1. **GCS から最新 CSV ファイルを取得**

   - バケット内の `channels_*` ファイルから最新のものを検索
   - ローカルに一時ダウンロードして読み込み
   - 既存チャンネル ID をセットとして管理

2. **新規チャンネルの収集**

   - YouTube Data API で人気動画からチャンネル情報を取得
   - 既存チャンネルと重複しない新規チャンネルのみを処理

3. **データの更新と保存**

   - 新規チャンネル情報を既存データに追加
   - タイムスタンプ付きの新しい CSV ファイルを作成
   - GCS にアップロード

4. **Slack 通知**
   - 新規取得チャンネルの詳細を通知

## セットアップ

### 1. 依存関係のインストール

```bash
pip install -r requirements.txt
```

### 2. 環境変数の設定

`.env`ファイルを作成し、以下の環境変数を設定してください：

```bash
# YouTube Data API
YOUTUBE_API_KEY=your_youtube_api_key_here

# Google Cloud Storage
GCS_BUCKET_NAME=your_gcs_bucket_name
GCS_CREDENTIALS_JSON={"type": "service_account", ...}

# Slack通知（オプション）
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

### 3. カテゴリ設定

`config/category_ids.json`で取得対象の YouTube カテゴリを設定できます。

## 使用方法

### 手動実行

```bash
python main.py
```

### バッチ実行

```bash
# ビルドスクリプトを使用
./build.bash

# 実行スクリプトを使用
./execute.bash
```

## Slack 通知機能

### 設定方法

1. Slack ワークスペースで Incoming Webhook を設定
2. `.env`ファイルに`SLACK_WEBHOOK_URL`を追加

### 通知内容

- 新規取得チャンネル数
- 実行時刻
- 新規チャンネルの詳細情報（最大 10 件）
- チャンネル名、ID、メール、登録者数、再生回数、動画数

### 通知例

```
🎉 YouTubeチャンネル収集バッチ実行完了！

📊 実行結果
• 新規取得チャンネル数: 15件
• 実行時刻: 2024-01-15 10:30:00
• 総チャンネル数: 1,250件

📋 新規チャンネル一覧
1. チャンネル名
   • チャンネルID: `UCxxxxxxxx`
   • メール: example@example.com
   • 登録者数: 100,000
   • 総再生回数: 1,000,000
   • 動画数: 50

📁 CSVファイルはGCSにアップロードされました。
```

## CSV ファイル構造

### 出力される CSV ファイルの列

- `channel_id`: チャンネル ID
- `title`: チャンネル名
- `description`: チャンネル説明
- `email`: 抽出されたメールアドレス
- `subscriber_count`: 登録者数
- `view_count`: 総再生回数
- `video_count`: 動画数
- `fetched_at`: 取得日時

### ファイル命名規則

- 形式: `channels_YYYYMMDD_HHMMSS.csv`
- 例: `channels_20240115_103000.csv`

## GCS バケット構造

```
gs://your-bucket-name/
├── channels_20240115_103000.csv  # 最新ファイル
├── channels_20240114_103000.csv  # 前回ファイル
└── channels_20240113_103000.csv  # 過去ファイル
```

## ログ出力例

```
2024-01-15 10:30:00 - INFO - GCSから最新のCSVファイルを取得: channels_20240114_103000.csv
2024-01-15 10:30:01 - INFO - CSVファイルをローカルにダウンロード: temp_channels_20240114_103000.csv
2024-01-15 10:30:02 - INFO - 既存データを読み込みました。チャンネル数: 1235
2024-01-15 10:30:03 - INFO - 一時ファイルを削除しました: temp_channels_20240114_103000.csv
2024-01-15 10:30:04 - INFO - バッチ処理を開始します。既存チャンネル数: 1235
```

## 注意事項

- YouTube Data API のクォータ制限に注意してください
- 初回実行時は新規データとして開始されます
- Slack 通知は新規チャンネルがある場合のみ送信されます
- GCS の認証情報が正しく設定されていることを確認してください
- 一時ファイルは自動的に削除されます
