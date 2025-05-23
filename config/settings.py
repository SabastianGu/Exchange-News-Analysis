from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str
    TELEGRAM_TRADING_CHAT_ID: str
    TELEGRAM_ENGINEERING_CHAT_ID: str
    #TELEGRAM_WEBHOOK_PATH: str
    #TELEGRAM_WEBHOOK_BASE_URL: str
    #TELEGRAM_WEBHOOK_SECRET: str
    DATABASE_URL: str
    OKX_API_KEY: str
    OKX_API_SECRET: str
    OKX_PASSPHRASE: str
    BYBIT_SECRET: str
    BYBIT_NAME: str
    BYBIT_API: str
    NEWS_API_KEY: str
    JBLANKED_API_KEY: str
    MARKET_API_KEY: str
    #USE_NGROK: bool = False
    #NGROK_AUTHTOKEN: str
    #NGROK_PORT: 8000

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()