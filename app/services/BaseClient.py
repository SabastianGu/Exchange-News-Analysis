import abc
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

class BaseExchangeClient(abc.ABC):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.source_name = self.__class__.__name__.replace("Client", "").lower()  # e.g. "bybit", "okx"
    @abc.abstractmethod
    async def fetch_announcements(self) -> List[Dict[str, Any]]:
        pass


    def standardize_response(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Universal standardization for all exchanges"""
        try:
            # Handle both timestamp (ms) and ISO format
            timestamp = item.get("dateTimestamp") or item.get("publish_time")
            if isinstance(timestamp, int):
                publish_time = datetime.fromtimestamp(timestamp / 1000)
            elif isinstance(timestamp, str):
                publish_time = datetime.fromisoformat(timestamp)
            else:
                publish_time = datetime.now()

            return {
                "id": str(item.get("id") or item.get("url", "").split("/")[-2]),
                "title": item.get("title", ""),
                "content": item.get("description") or item.get("content", ""),
                "publish_time": publish_time,
                "type": item.get("type", {}).get("title") if isinstance(item.get("type"), dict) else item.get("type"),
                "tags": item.get("tags", []),
                "url": item.get("url", ""),
                "source": self.source_name,  # Track where the data came from
                "raw_data": item
            }
        except Exception as e:
            logging.warning(f"Standardization failed: {e}\nItem: {item}")
            return None