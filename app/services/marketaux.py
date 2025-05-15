import aiohttp
from datetime import datetime
from typing import List, Dict, Any
from .BaseClient import BaseExchangeClient
from config.settings import settings

class MarketauxClient(BaseExchangeClient):
    def __init__(self):
        super().__init__()
        self.api_key = settings.MARKET_API_KEY
        self.base_url = "https://api.marketaux.com/v1/news/all"
        self.session = None

    async def _create_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def fetch_announcements(self) -> List[Dict[str, Any]]:
        await self._create_session()
        params = {
            "api_token": self.api_key,
            "language": "en",
            "limit": 50
        }

        try:
            async with self.session.get(self.base_url, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    self.logger.error(f"Marketaux API error: {response.status} - {error_text}")
                    return []

                data = await response.json()
                return [self.standardize_response(item) for item in data.get("data", [])]

        except Exception as e:
            self.logger.error(f"Error fetching Marketaux news: {e}")
            return []

    def standardize_response(self, item: Dict[str, Any]) -> Dict[str, Any]:
        publish_time = datetime.fromisoformat(item.get("published_at", ""))
        try:
            return {
                "id": item.get("uuid"),
                "title": item.get("title", ""),
                "content": item.get("description", ""),
                "publish_time": publish_time,
                "type": "news",
                "tags": item.get("entities", []),
                "url": item.get("url", ""),
                "source": self.source_name,
                "raw_data": item
            }
        except ValueError or TypeError:
            print('somethings gone wrong')
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()