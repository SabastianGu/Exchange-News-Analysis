from pybit.unified_trading import HTTP
from typing import List, Dict, Any, Optional
import asyncio
from concurrent.futures import ThreadPoolExecutor
from .BaseClient import BaseExchangeClient
from config.settings import settings

class BybitClient(BaseExchangeClient):
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: bool = False
    ):
        super().__init__()
        self.api_key = settings.BYBIT_API
        self.api_secret = settings.BYBIT_SECRET
        self.testnet = testnet
        self.session = self._create_session()
        self.executor = ThreadPoolExecutor(max_workers=1)

    def _create_session(self) -> HTTP:
        """Initialize authenticated HTTP session"""
        return HTTP(
            testnet=self.testnet,
            api_key=self.api_key,
            api_secret=self.api_secret,
        )

    async def fetch_announcements(self) -> List[Dict[str, Any]]:
        """Fetch announcements with proper authentication"""
        try:
            # Run sync pybit call in thread
            response = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: self.session.get_announcement(
                    locale="en-US",
                    limit=50,
                    category="new_crypto,delisting,maintenance"
                )
            )

            if response.get("retCode") != 0:
                error_msg = response.get("retMsg", "Unknown error")
                self.logger.error(f"Bybit API error: {error_msg}")
                return []

            return [
                self.standardize_response(item) 
                for item in response.get("result", {}).get("list", []) 
                if self.standardize_response(item)
            ]

        except Exception as e:
            self.logger.error(f"Bybit fetch failed: {str(e)}")
            return []

    async def get_account_info(self) -> Dict[str, Any]:
        """Example of authenticated endpoint"""
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                lambda: self.session.get_wallet_balance(accountType="UNIFIED")
            )
            return response
        except Exception as e:
            self.logger.error(f"Failed to get account info: {e}")
            return {}