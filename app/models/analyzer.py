import asyncio
import logging
import time
from typing import Dict, List

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from app.core.model import AnnouncementClassifier
from app.storage.redis_cache import RedisCache
from app.services.ByBitclient import BybitClient
from app.services.forex_factory import ForexFactoryService
from app.services.marketaux import MarketauxClient
from app.services.news_api import NewsAPIClient
from app.services.tlg_notifier import Notifier
from app.storage.postgres_manager import AnnouncementStorage
from app.utilities.json_helpers import ensure_serializable, safe_json_dumps


class AnnouncementAnalyzer:
    def __init__(self, storage: AnnouncementStorage):
        self.bybit = BybitClient()
        self.news_api = NewsAPIClient()
        self.forex_factory = ForexFactoryService()
        self.marketaux = MarketauxClient()
        self.model = AnnouncementClassifier()
        self.notifier = Notifier()
        self.seen_ids = set()
        self.storage = storage
        self.logger = logging.getLogger(__name__)
        self.last_forex_sent = None
        self.cache = RedisCache()

    async def run(self, interval_seconds: int = 20):
        while True:
            print("about to check announcements")
            await self._check_announcements()
            now = time.time()
            if self.last_forex_sent is None or now - self.last_forex_sent >= 3600:
                print("Sending hourly forex update")
                await self._send_forex_events()
                self.last_forex_sent = now
            print("Check confirmed, going to sleep")
            await asyncio.sleep(interval_seconds)

    async def _send_forex_events(self):
        try:
            message = await self.forex_factory.get_formatted_events()
            if message:
                await self.notifier.send(message, channel='Trading channel')
                print("✅ Forex factory update sent.")
            else:
                print("ℹ️ No Forex events to send.")
        except Exception as e:
            self.logger.error(f"Error sending ForexFactory message: {e}")

    async def _check_announcements(self):
        bybit_results, newsapi_results, marketaux_results = await asyncio.gather(
            self.bybit.fetch_announcements(),
            self.news_api.fetch_announcements(),
            self.marketaux.fetch_announcements()
        )

        if bybit_results:
            await self._process_batch("bybit", bybit_results)

        if newsapi_results:
            await self._process_batch("newsapi", newsapi_results)

        if marketaux_results:
            await self._process_batch("marketaux", marketaux_results)

    async def _process_batch(self, exchange: str, announcements: List[Dict]):
        delay_between_messages = 1.5
        new_announcements = await self.storage.bulk_check_new(exchange, announcements)

        if not new_announcements:
            return

        texts = [
            f"{item['announcement']['title']}\n{item['announcement'].get('content', '')}"
            for item in new_announcements
        ]
        keys = self.cache.make_batch_keys(texts)
        cached = await self.cache.get_many(keys)

        results = []
        to_predict = []
        to_predict_texts = []
        index_map = {}

        for i, cached_result in enumerate(cached):
            if cached_result is not None:
                results.append(cached_result)
            else:
                index_map[len(to_predict)] = i  # map new index to original index
                to_predict.append(keys[i])
                to_predict_texts.append(texts[i])
                results.append(None)  # placeholder

        if to_predict_texts:
            batch_response = await self.model.predict_batch(to_predict_texts)
            model_results = [res.dict() for res in batch_response.results]
            await self.cache.set_many(dict(zip(to_predict, model_results)))
            for new_idx, original_idx in index_map.items():
                results[original_idx] = model_results[new_idx]

        for item, classification_dict in zip(new_announcements, results):
            announcement = item['announcement']
            classification = classification_dict

            await self.storage.save_announcement(
                exchange=exchange,
                announcement=announcement,
                classification=classification,
                db_id=item['storage_id']
            )

            if classification["confidence"] > 0.50 and classification["label"] not in ['irrelevant']:
                try:
                    predicted_label = classification["label"]
                    message = (
                        f"🚨 New {predicted_label} announcement\n"
                        f"📌 {announcement['title']}\n"
                        f"📊 Content: {announcement.get('content', announcement.get('description', ''))}\n"
                        f"⏰ {announcement['url']}\n"
                    )
                    announcement_id = item['storage_id']
                    keyboard = [
                        [
                            InlineKeyboardButton("Engineering", callback_data=f"label|engineering|{announcement_id}"),
                            InlineKeyboardButton("Trading", callback_data=f"label|trading|{announcement_id}")
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    channel = "Trading channel" if predicted_label == "trading" else "Engineering channel"
                    print(f"Sending notification: {message} to {channel}")
                    success = await self.notifier.send(message, channel=channel, reply_markup=reply_markup)
                    if not success:
                        self.logger.warning(f"Failed to send notification for {announcement['title']}")
                    else:
                        print("message sent successfully")
                        await asyncio.sleep(delay_between_messages)
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