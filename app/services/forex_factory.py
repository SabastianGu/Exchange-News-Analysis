import requests
import logging
from datetime import datetime
from config.settings import settings
from typing import Dict, List, Optional
import httpx

logger = logging.getLogger(__name__)

class ForexFactoryService:
    def __init__(self):
        self.base_url = "https://www.jblanked.com/news/api/forex-factory/calendar"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Api-Key {settings.JBLANKED_API_KEY}"
        }

    async def get_today_events(self) -> Optional[List[Dict]]:
        try:
            url = f"{self.base_url}/today/"
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
        except httpx.RequestError as e:
            logger.error(f"Failed to fetch Forex Factory data: {e}")
            return None

    async def get_formatted_events(self) -> Optional[str]:
        """Format today's events for Telegram"""
        events = await self.get_today_events()
        if not events:
            return "No Forex Factory data available today"

        formatted = ["📅 *Today's Forex Factory Calendar*"]
        for event in events[:3]:
            time_str = "All Day"
            if event.get('Date'):
                try:
                    dt = datetime.strptime(event['Date'], "%Y.%m.%d %H:%M:%S")
                    time_str = dt.strftime("%I:%M %p")
                except ValueError as e:
                    logger.warning(f"Failed to parse date: {event['Date']}, error: {e}")
                #Обработать случаи, когда сам сайт не подгружает данные Not Loaded
                formatted.append(
                    f"\n⏰ *{time_str}* - {event.get('Name', 'No title')}\n"
                    f"• *Currency*: {event.get('Currency', 'N/A')}\n"
                    f"• *Actual*: {event.get('Actual', 'N/A')}\n"
                    f"• *Forecast*: {event.get('Forecast', 'N/A')}\n"
                    f"• *Previous*: {event.get('Previous', 'N/A')}\n"
                    f"• *Outcome*: {event.get('Outcome', 'N/A')}\n"
                    f"• *Strength*: {event.get('Strength', 'N/A')}\n"
                    f"• *Quality*: {event.get('Quality', 'N/A')}"
                )
            return "\n".join(formatted)

    async def send_to_telegram(self):
        from .tlg_notifier import Notifier as notifier
        """Send formatted events via Telegram"""
        try:
            message = await self.get_formatted_events()
            if not message:
                return False
                
            return await notifier.send(message)
        except Exception as e:
            logger.error(f"Failed to send Forex Factory data: {e}")
            return False