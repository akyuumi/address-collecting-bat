import os
import json
import logging
import time
import re
from datetime import datetime
from typing import List, Dict, Set
from dotenv import load_dotenv
from googleapiclient.discovery import build
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
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

# SQLAlchemyの設定
Base = declarative_base()
engine = create_engine('sqlite:///db/channels.db')
Session = sessionmaker(bind=engine)

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

class Channel(Base):
    __tablename__ = 'channels'
    
    channel_id = Column(String, primary_key=True)
    title = Column(String)
    description = Column(String)
    email = Column(String)
    subscriber_count = Column(Integer)
    view_count = Column(Integer)
    video_count = Column(Integer)
    fetched_at = Column(DateTime)

# テーブルの作成
Base.metadata.create_all(engine)

class YouTubeChannelCollector:
    def __init__(self):
        self.youtube = build('youtube', 'v3', developerKey=API_KEY)
        self.session = Session()
        self.existing_channels = self._load_existing_channels()
        
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
        
    def _load_existing_channels(self) -> Set[str]:
        """既存のチャンネルIDを取得"""
        return {channel.channel_id for channel in self.session.query(Channel.channel_id).all()}
    
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
    
    def save_channels(self, channels: List[Dict]):
        """チャンネル情報をDBに保存"""
        for channel_data in channels:
            channel = Channel(**channel_data)
            self.session.merge(channel)
        self.session.commit()
    
    def export_to_csv_and_upload(self):
        """チャンネルデータをCSVにエクスポートし、GCSにアップロード"""
        try:
            # チャンネルデータを取得
            channels = self.session.query(Channel).all()
            
            # DataFrameに変換
            df = pd.DataFrame([{
                'channel_id': c.channel_id,
                'title': c.title,
                'description': c.description,
                'email': c.email,
                'subscriber_count': c.subscriber_count,
                'view_count': c.view_count,
                'video_count': c.video_count,
                'fetched_at': c.fetched_at
            } for c in channels])
            
            # CSVファイル名を生成（現在の日時を含める）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f'channels_{timestamp}.csv'
            
            # CSVファイルを保存
            df.to_csv(csv_filename, index=False, encoding='utf-8')
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
    
    def run(self):
        """メイン処理の実行"""
        # カテゴリIDの読み込み
        categories = self._load_category_ids()
        total_new_channels = 0
        
        # カテゴリごとに処理
        for category in categories:
            logger.info(f"Processing category: {category['name']} (ID: {category['id']})")
            
            # 人気動画からチャンネルIDを取得
            channel_ids = self.get_popular_videos(category['id'])
            
            if channel_ids:
                # チャンネル詳細を取得
                channels: List[Dict] = self.get_channel_details(channel_ids)
                
                # DBに保存
                self.save_channels(channels)
                
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

if __name__ == '__main__':
    if not API_KEY:
        raise ValueError("YouTube APIキーが設定されていません。")
    if not GCS_CREDENTIALS_JSON:
        raise ValueError("GCS認証情報が設定されていません。")

    collector = YouTubeChannelCollector()
    collector.run() 