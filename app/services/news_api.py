from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor
from newsapi import NewsApiClient
from .BaseClient import BaseExchangeClient
from config.settings import settings
import hashlib

#Нужно переписать чтоб выдавало новости каждый час или каждые пол часа по выбора пользователя

class NewsAPIClient(BaseExchangeClient):
    def __init__(self, api_key: Optional[str] = None):
        super().__init__()
        self.api_key = api_key or self._get_default_key()
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.client = NewsApiClient(api_key=self.api_key)
    def _get_default_key(self) -> str:
        """Get default API key from settings or environment variable"""
        key = settings.NEWS_API_KEY
        if not key:
            raise ValueError("NewsAPI key not found in settings")
        return key

    async def fetch_announcements(self) -> List[Dict[str, Any]]:
        """Fetch news articles and standardize the response"""
        try:
            # Run sync newsapi call in thread
            response = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: self.client.get_everything(
                    q="Trading AND Engineering",
                    from_param=(datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d"),
                    to=datetime.utcnow().strftime("%Y-%m-%d"),
                    language='en',
                    sort_by='publishedAt',
                    page_size=50
                )
            )

            articles = response.get("articles", [])
            return [self.standardize_response(item) for item in articles if self.standardize_response(item)]

        except Exception as e:
            self.logger.error(f"NewsAPI fetch failed: {str(e)}")
            return []

    def standardize_response(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert NewsAPI response to our standard format"""
        try:
            return {
                "id": hashlib.md5(item['url'].encode()).hexdigest(),
                "title": item.get("title", ""),
                "content": item.get("description", "") or item.get("content", ""),
                "publish_time": item.get("publishedAt", ""),
                "url": item.get("url", ""),
                "source": "newsapi",
                "raw_data": {
                    "source": item.get("source", {}).get("name", ""),
                    "author": item.get("author", ""),
                    "urlToImage": item.get("urlToImage", "")
                }
            }
        except Exception as e:
            self.logger.error(f"Failed to standardize NewsAPI item: {str(e)}")
            return None

    async def get_breaking_news(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Optional specialized method"""
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: self.client.get_top_headlines(
                    category='business',
                    language='en',
                    page_size=20
                )
            )
            return [self.standardize_response(item) for item in response.get("articles", [])]
        except Exception as e:
            self.logger.error(f"Breaking news fetch failed: {str(e)}")
            return []