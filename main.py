import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Set
from dotenv import load_dotenv
from googleapiclient.discovery import build
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 環境変数の読み込み
load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')

# SQLAlchemyの設定
Base = declarative_base()
engine = create_engine('sqlite:///db/channels.db')
Session = sessionmaker(bind=engine)

class Channel(Base):
    __tablename__ = 'channels'
    
    channel_id = Column(String, primary_key=True)
    title = Column(String)
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
            request = self.youtube.videos().list(
                part='snippet',
                chart='mostPopular',
                regionCode='JP',
                videoCategoryId=category_id,
                maxResults=50
            )
            response = request.execute()
            
            channel_ids = set()
            for item in response.get('items', []):
                channel_id = item['snippet']['channelId']
                if channel_id not in self.existing_channels:
                    channel_ids.add(channel_id)
            
            return list(channel_ids)
        except Exception as e:
            logger.error(f"Error fetching popular videos for category {category_id}: {str(e)}")
            return []
    
    def get_channel_details(self, channel_ids: List[str]) -> List[Dict]:
        """チャンネル詳細情報を取得"""
        if not channel_ids:
            return []
        
        try:
            request = self.youtube.channels().list(
                part='snippet,statistics',
                id=','.join(channel_ids),
                maxResults=50
            )
            response = request.execute()
            
            channels = []
            for item in response.get('items', []):
                channel = {
                    'channel_id': item['id'],
                    'title': item['snippet']['title'],
                    'subscriber_count': int(item['statistics'].get('subscriberCount', 0)),
                    'view_count': int(item['statistics'].get('viewCount', 0)),
                    'video_count': int(item['statistics'].get('videoCount', 0)),
                    'fetched_at': datetime.now()
                }
                channels.append(channel)
            
            return channels
        except Exception as e:
            logger.error(f"Error fetching channel details: {str(e)}")
            return []
    
    def save_channels(self, channels: List[Dict]):
        """チャンネル情報をDBに保存"""
        for channel_data in channels:
            channel = Channel(**channel_data)
            self.session.merge(channel)
        self.session.commit()
    
    def run(self):
        """メイン処理の実行"""
        categories = self._load_category_ids()
        total_new_channels = 0
        
        for category in categories:
            logger.info(f"Processing category: {category['name']} (ID: {category['id']})")
            
            # 人気動画からチャンネルIDを取得
            channel_ids = self.get_popular_videos(category['id'])
            
            if channel_ids:
                # チャンネル詳細を取得
                channels = self.get_channel_details(channel_ids)
                
                # DBに保存
                self.save_channels(channels)
                
                new_channels_count = len(channels)
                total_new_channels += new_channels_count
                logger.info(f"Fetched {new_channels_count} new channels in category {category['name']}")
            
            # API制限を考慮して少し待機
            import time
            time.sleep(1)
        
        logger.info(f"Completed! Total new channels collected: {total_new_channels}")

if __name__ == '__main__':
    collector = YouTubeChannelCollector()
    collector.run() 