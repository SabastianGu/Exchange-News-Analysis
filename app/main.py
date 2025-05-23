from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging

from app.models.analyzer import AnnouncementAnalyzer
from app.api.endpoints import router
from app.storage.postgres_manager import AnnouncementStorage
from app.services.tlg_notifier import Notifier

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting application...")

    # Initialize components
    storage = AnnouncementStorage()
    await storage.connect()
    print("✅ Connected to PostgreSQL")

    telegram_bot = Notifier()
    analyzer = AnnouncementAnalyzer(storage)
    analyzer.notifier = telegram_bot

    # Attach state
    app.state.telegram_bot = telegram_bot
    app.state.analyzer = analyzer

    # Start services
    try:
        await telegram_bot.start()
        app.state.analyzer_task = asyncio.create_task(analyzer.run(interval_seconds=300))
        logger.info("✅ Analyzer and Telegram bot started")
        yield
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        raise
    finally:
        logger.info("🛑 Shutting down services...")
        if hasattr(app.state, 'analyzer_task'):
            app.state.analyzer_task.cancel()
            try:
                await app.state.analyzer_task
            except asyncio.CancelledError:
                logger.info("Analyzer task cancelled")

        if hasattr(app.state, 'telegram_bot'):
            await app.state.telegram_bot.stop()
            logger.info("Telegram bot stopped")

        if storage and hasattr(storage, 'pool'):
            await storage.pool.close()
            logger.info("🗄️ Database connection closed")

app = FastAPI(lifespan=lifespan)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {
        "analyzer_running": hasattr(app.state, 'analyzer'),
        "telegram_bot_running": hasattr(app.state, 'telegram_bot')
    }

@app.get("/latest")
async def get_latest_analyzed():
    if not hasattr(app.state, 'analyzer'):
        raise HTTPException(status_code=503, detail="Analyzer not initialized")

    try:
        async with app.state.analyzer.storage.pool.acquire() as conn:
            records = await conn.fetch(
                """
                SELECT title, publish_time, content, classification 
                FROM announcements
                WHERE classification IN ('trading', 'engineering')
                ORDER BY publish_time DESC LIMIT 15
                """
            )

        return [
            {
                "title": r["title"],
                "time": r["publish_time"].isoformat(),
                "type": r["classification"],
                "content": r["content"],
            }
            for r in records
        ]
    except Exception as e:
        logger.error(f"Failed to fetch announcements: {e}")
        raise HTTPException(status_code=500, detail="Database error")

# Include API routers
app.include_router(router, prefix="/api")