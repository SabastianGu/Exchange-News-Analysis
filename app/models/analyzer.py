import asyncio
from app.services.news_api import NewsAPIClient
import logging
from app.services.ByBitclient import BybitClient
from app.core.model import AnnouncementClassifier
from app.services.tlg_notifier import Notifier
from app.storage.postgres_manager import AnnouncementStorage
from app.services.marketaux import MarketauxClient
from config.settings import settings
from typing import Dict, List
from utils.json_helpers import ensure_serializable, safe_json_dumps

class AnnouncementAnalyzer:
    def __init__(self, storage: AnnouncementStorage):
        self.bybit = BybitClient()
        self.news_api = NewsAPIClient()
        self.marketaux =  MarketauxClient()
        self.model = AnnouncementClassifier()
        self.notifier = Notifier()
        self.seen_ids = set()
        self.storage = storage
        self.logger = logging.getLogger(__name__)
    
    async def run(self, interval_seconds: int = 20):
        while True:
            print("about to check announcements")
            await self._check_announcements()
            print("Check confirmed, going to sleep")
            await asyncio.sleep(interval_seconds)

    async def _check_announcements(self):
        # Fetch from both exchanges in parallel
        bybit_results, newsapi_results, marketaux_results = await asyncio.gather(
            self.bybit.fetch_announcements(),
            self.news_api.fetch_announcements(),
            self.marketaux.fetch_announcements()
        )

        # Process Bybit announcements
        if bybit_results:
            await self._process_batch("bybit", bybit_results)
        
        if newsapi_results:
            await self._process_batch("newsapi", newsapi_results)
        
        if marketaux_results:
            await self._process_batch("marketaux", marketaux_results)

    async def _process_batch(self, exchange: str, announcements: List[Dict]):
        """Handle a batch of announcements from one exchange"""
        # Check which announcements are new
        new_announcements = await self.storage.bulk_check_new(exchange, announcements)
        
        if not new_announcements:
            return

        # Classify new announcements
        texts = [
            f"{item['announcement']['title']}\n{item['announcement'].get('content', '')}"
            for item in new_announcements
        ]
        batch_response = await self.model.predict_batch(texts)

        # Save and notify
        for item, classification in zip(new_announcements, batch_response.results):
            announcement = item['announcement']

            # Save full announcement data - Use for further model training
            await self.storage.save_announcement(
                exchange=exchange,
                announcement=announcement,
                classification=classification
            )

            # Only notify high-confidence events
            if classification.confidence > 0.50 and classification.label not in ['irrelevant']:
                try:
                    # Get the predicted label (already determined by your model)
                    predicted_label = classification.label
                    
                    message = (f"🚨 New {predicted_label} announcement\n"
                            f"📌 {announcement['title']}\n"
                            f"📊 Content: {announcement.get('content', announcement.get('description', ''))}\n"
                            f"⏰ {announcement['url']}\n")
                    
                    channel = "Trading channel" if predicted_label == "trading" else "Engineering channel"
                    print(f"Sending notification: {message} to {channel}")
                    success = await self.notifier.send(message, channel=channel)
                    if not success:
                        self.logger.warning(f"Failed to send notification for {announcement['title']}")
                    else:
                        print("message sent successfully")
                except Exception as e:
                    self.logger.error(f"Notification processing failed: {e}")

    def _format_alert(self, announcement, classification):
        raw_data = announcement.get('raw_data', '')
        if isinstance(raw_data, dict):
            raw_data = safe_json_dumps(ensure_serializable(raw_data))
        
        return (
            f"🚨 New {classification.label} announcement\n"
            f"📌 {announcement['title']}\n"
            f"📊 Confidence: {classification.confidence:.0%}\n"
            f"⏰ {announcement['publish_time']}\n"
            f"🔗 Raw data: {raw_data}"
        )