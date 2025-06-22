import os
import json
import logging
import time
import re
import requests
from datetime import datetime
from typing import List, Dict, Set
from dotenv import load_dotenv
from googleapiclient.discovery import build
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 環境変数の読み込み
load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_CREDENTIALS_JSON = os.getenv('GCS_CREDENTIALS_JSON')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

def extract_email(description: str) -> str:
    """説明文からメールアドレスを抽出"""
    if not description:
        return "取得失敗"
    
    # メールアドレスのパターン
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, description)
    
    if match:
        return match.group(0)
    return "取得失敗"

class YouTubeChannelCollector:
    def __init__(self):
        self.youtube = build('youtube', 'v3', developerKey=API_KEY)
        self.existing_channels = set()
        self.channels_df = None
        
        # GCSクライアントの初期化
        if GCS_CREDENTIALS_JSON:
            try:
                credentials_info = json.loads(GCS_CREDENTIALS_JSON)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info
                )
                self.storage_client = storage.Client(credentials=credentials)
                logger.info("GCS認証情報を環境変数から読み込みました")
            except json.JSONDecodeError as e:
                logger.error(f"GCS認証情報のJSON形式が不正です: {str(e)}")
                self.storage_client = None
            except Exception as e:
                logger.error(f"GCS認証情報の読み込みに失敗しました: {str(e)}")
                self.storage_client = None
        else:
            self.storage_client = None
            logger.warning("GCS認証情報が設定されていません。GCSアップロード機能は使用できません。")
        
        # 既存データの読み込み
        self._load_existing_data()
        
    def _get_latest_csv_from_gcs(self) -> str:
        """GCSから最新のCSVファイルを取得"""
        if not self.storage_client or not GCS_BUCKET_NAME:
            logger.warning("GCS認証情報またはバケット名が設定されていないため、新規データとして開始します。")
            return None
        
        try:
            bucket = self.storage_client.bucket(GCS_BUCKET_NAME)
            blobs = list(bucket.list_blobs(prefix='channels_'))
            
            if not blobs:
                logger.info("GCSに既存のCSVファイルが見つかりません。新規データとして開始します。")
                return None
            
            # 最新のCSVファイルを取得（ファイル名の日時でソート）
            latest_blob = max(blobs, key=lambda x: x.name)
            logger.info(f"GCSから最新のCSVファイルを取得: {latest_blob.name}")
            
            # ローカルにダウンロード
            local_filename = f"temp_{latest_blob.name}"
            latest_blob.download_to_filename(local_filename)
            logger.info(f"CSVファイルをローカルにダウンロード: {local_filename}")
            
            return local_filename
            
        except Exception as e:
            logger.error(f"GCSからのCSVファイル取得に失敗しました: {str(e)}")
            return None
    
    def _load_existing_data(self):
        """既存データを読み込み"""
        csv_filename = self._get_latest_csv_from_gcs()
        
        if csv_filename and os.path.exists(csv_filename):
            try:
                self.channels_df = pd.read_csv(csv_filename, encoding='utf-8')
                self.existing_channels = set(self.channels_df['channel_id'].tolist())
                logger.info(f"既存データを読み込みました。チャンネル数: {len(self.existing_channels)}")
                
                # 一時ファイルを削除
                os.remove(csv_filename)
                logger.info(f"一時ファイルを削除しました: {csv_filename}")
                
            except Exception as e:
                logger.error(f"CSVファイルの読み込みに失敗しました: {str(e)}")
                self.channels_df = pd.DataFrame(columns=[
                    'channel_id', 'title', 'description', 'email', 
                    'subscriber_count', 'view_count', 'video_count', 'fetched_at'
                ])
                self.existing_channels = set()
        else:
            # 新規データとして開始
            self.channels_df = pd.DataFrame(columns=[
                'channel_id', 'title', 'description', 'email', 
                'subscriber_count', 'view_count', 'video_count', 'fetched_at'
            ])
            self.existing_channels = set()
            logger.info("新規データとして開始します。")
    
    def _load_category_ids(self) -> List[Dict]:
        """カテゴリIDの設定を読み込み"""
        with open('config/category_ids.json', 'r') as f:
            return json.load(f)['categories']
    
    def get_popular_videos(self, category_id: str) -> List[str]:
        """人気動画からチャンネルIDを取得"""
        try:
            channel_ids = set()
            next_page_token = None
            daily_limit = 10000  # YouTube Data APIの1日のクォータ制限
            total_quota = 0
            
            while True:
                request = self.youtube.videos().list(
                    part='snippet',
                    chart='mostPopular',
                    regionCode='JP',
                    videoCategoryId=category_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                
                # クォータ消費量の計算（videos.listは1リクエストあたり1クォータ）
                total_quota += 1
                
                for item in response.get('items', []):
                    channel_id = item['snippet']['channelId']
                    if channel_id not in self.existing_channels:
                        channel_ids.add(channel_id)
                
                # 次のページのトークンを取得
                next_page_token = response.get('nextPageToken')
                
                # 次のページがない場合、またはクォータ制限に達した場合は終了
                if not next_page_token or total_quota >= daily_limit:
                    break
                
                # API制限を考慮して少し待機
                time.sleep(1)
            
            logger.info(f"カテゴリID[{category_id}]で{len(channel_ids)}件のチャンネルを取得しました。")
            return list(channel_ids)
        except Exception as e:
            logger.error(f"動画の取得に失敗しました。カテゴリID[{category_id}]: {str(e)}")
            return []
    
    def get_channel_details(self, channel_ids: List[str]) -> List[Dict]:
        """チャンネル詳細情報を取得"""
        if not channel_ids:
            return []
        
        channels = []
        # チャンネルIDを50個ずつのバッチに分割
        batch_size = 50
        for i in range(0, len(channel_ids), batch_size):
            batch = channel_ids[i:i + batch_size]
            try:
                request = self.youtube.channels().list(
                    part='snippet,statistics',
                    id=','.join(batch),
                    maxResults=batch_size
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    description = item['snippet'].get('description', '')
                    channel = {
                        'channel_id': item['id'],
                        'title': item['snippet']['title'],
                        'description': description,
                        'email': extract_email(description),
                        'subscriber_count': int(item['statistics'].get('subscriberCount', 0)),
                        'view_count': int(item['statistics'].get('viewCount', 0)),
                        'video_count': int(item['statistics'].get('videoCount', 0)),
                        'fetched_at': datetime.now()
                    }
                    channels.append(channel)
                
                # API制限を考慮して少し待機
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"チャンネル詳細の取得に失敗しました。バッチ {i//batch_size + 1}: {str(e)}")
                continue
        
        return channels
    
    def update_channels_data(self, new_channels: List[Dict]):
        """チャンネル情報をDataFrameに追加"""
        if not new_channels:
            return
        
        # 新規チャンネルをDataFrameに追加
        new_df = pd.DataFrame(new_channels)
        self.channels_df = pd.concat([self.channels_df, new_df], ignore_index=True)
        
        # 既存チャンネルセットを更新
        for channel in new_channels:
            self.existing_channels.add(channel['channel_id'])
        
        logger.info(f"{len(new_channels)}件の新規チャンネルをデータに追加しました。")
    
    def export_to_csv_and_upload(self):
        """チャンネルデータをCSVにエクスポートし、GCSにアップロード"""
        try:
            # CSVファイル名を生成（現在の日時を含める）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f'channels_{timestamp}.csv'
            
            # CSVファイルを保存
            self.channels_df.to_csv(csv_filename, index=False, encoding='utf-8')
            logger.info(f"CSVファイルを作成しました: {csv_filename}")
            
            # GCSにアップロード
            if self.storage_client and GCS_BUCKET_NAME:
                bucket = self.storage_client.bucket(GCS_BUCKET_NAME)
                blob = bucket.blob(csv_filename)
                blob.upload_from_filename(csv_filename)
                logger.info(f"CSVファイルをGCSにアップロードしました: gs://{GCS_BUCKET_NAME}/{csv_filename}")
                
                # ローカルのCSVファイルを削除
                os.remove(csv_filename)
                logger.info(f"ローカルのCSVファイルを削除しました: {csv_filename}")
            else:
                logger.warning("GCS認証情報またはバケット名が設定されていないため、GCSへのアップロードをスキップしました")
                
        except Exception as e:
            logger.error(f"CSVエクスポートまたはGCSアップロード中にエラーが発生しました: {str(e)}")
            # エラーが発生した場合でも、ローカルのCSVファイルは残しておく
            if os.path.exists(csv_filename):
                logger.info(f"エラーが発生したため、CSVファイルを保持します: {csv_filename}")
    
    def send_slack_notification(self, new_channels: List[Dict]):
        """Slackに新規チャンネル情報を通知"""
        if not SLACK_WEBHOOK_URL:
            logger.warning("Slack Webhook URLが設定されていません。通知をスキップします。")
            return
        
        if not new_channels:
            logger.info("新規チャンネルがないため、Slack通知をスキップします。")
            return
        
        try:
            message = f"🎉 YouTubeチャンネル収集バッチ実行完了！\n\n"
            message += f"📊 **実行結果**\n"
            message += f"• 新規取得チャンネル数: {len(new_channels)}件\n"
            message += f"• 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            message += f"• 総チャンネル数: {len(self.channels_df)}件\n\n"
            
            if new_channels:
                message += f"📋 **新規チャンネル一覧**\n"
                
                for i, channel in enumerate(new_channels[:10], 1):  # 最大10件まで表示
                    message += f"{i}. **{channel['title']}**\n"
                    message += f"   • チャンネルID: `{channel['channel_id']}`\n"
                    message += f"   • メール: {channel['email']}\n"
                    message += f"   • 登録者数: {channel['subscriber_count']:,}\n"
                    message += f"   • 総再生回数: {channel['view_count']:,}\n"
                    message += f"   • 動画数: {channel['video_count']:,}\n\n"
                
                if len(new_channels) > 10:
                    message += f"... 他 {len(new_channels) - 10}件のチャンネルも取得されました。\n\n"
            
            message += f"📁 CSVファイルはGCSにアップロードされました。"
            
            payload = {"text": message}
            response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info("Slack通知を送信しました。")
            else:
                logger.error(f"Slack通知の送信に失敗しました。ステータスコード: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Slack通知の送信中にエラーが発生しました: {str(e)}")
    
    def run(self):
        """メイン処理の実行"""
        logger.info(f"バッチ処理を開始します。既存チャンネル数: {len(self.existing_channels)}")
        
        # カテゴリIDの読み込み
        categories = self._load_category_ids()
        total_new_channels = 0
        all_new_channels = []
        
        # カテゴリごとに処理
        for category in categories:
            logger.info(f"処理中 カテゴリ: {category['name']} (ID: {category['id']})")
            
            # 人気動画からチャンネルIDを取得
            channel_ids = self.get_popular_videos(category['id'])
            
            if channel_ids:
                # チャンネル詳細を取得
                channels: List[Dict] = self.get_channel_details(channel_ids)
                
                # データに追加
                self.update_channels_data(channels)
                all_new_channels.extend(channels)
                
                new_channels_count = len(channels)
                total_new_channels += new_channels_count
                logger.info(f"Fetched {new_channels_count} new channels in category {category['name']}")
            
            # API制限を考慮して少し待機
            time.sleep(1)
        
        logger.info(f"処理が完了しました。合計取得チャンネル数: {total_new_channels}")
        
        # CSVエクスポートとGCSアップロードを実行
        logger.info(f"CSV出力+GCSアップロード処理を開始します。")
        self.export_to_csv_and_upload()
        logger.info(f"CSV出力+GCSアップロード処理が完了しました。")
        
        # Slack通知
        logger.info(f"Slack通知処理を開始します。新規チャンネル数: {len(all_new_channels)}")
        self.send_slack_notification(all_new_channels)
        
        logger.info(f"バッチ処理が完了しました。新規チャンネル: {len(all_new_channels)}件, 総チャンネル数: {len(self.channels_df)}件")

if __name__ == '__main__':
    if not API_KEY:
        raise ValueError("YouTube APIキーが設定されていません。")
    if not GCS_CREDENTIALS_JSON:
        raise ValueError("GCS認証情報が設定されていません。")

    collector = YouTubeChannelCollector()
    collector.run() 