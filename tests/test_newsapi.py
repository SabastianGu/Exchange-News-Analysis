from datetime import datetime, timedelta
from newsapi import NewsApiClient
from config.settings import settings
import os
import json

news_APi_key = settings.NEWS_API_KEY


def extract_live_news(
        q: str = "Trading and Engineering news",
        from_param: str = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d"),
        to: str = datetime.utcnow().strftime("%Y-%m-%d"),
        language: str = 'en',
        sort_by: str = 'popularity',
        page_size: int = 50
    ):
    
    news_api = NewsApiClient(api_key=news_APi_key)
    response = news_api.get_everything(
        q = q,
        from_param= from_param,
        to = to,
        language= language,
        sort_by=sort_by,
        page_size=page_size
        #sources = 'usa'
    )
    
