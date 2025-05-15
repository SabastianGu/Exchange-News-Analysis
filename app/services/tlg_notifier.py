import logging
from typing import List, Dict
import asyncpg
from telegram import Update, BotCommand, BotCommandScopeDefault
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from app.services.forex_factory import ForexFactoryService
from config.settings import settings

class Notifier:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_pool = None
        self.chat_ids = {
            "Trading channel": settings.TELEGRAM_TRADING_CHAT_ID,
            "Engineering channel": settings.TELEGRAM_ENGINEERING_CHAT_ID
        }
        self.forex_factory = ForexFactoryService()

        self.application = ApplicationBuilder() \
            .token(settings.TELEGRAM_BOT_TOKEN) \
            .build()

        self._register_handlers()

    async def connect_db(self):
        if not self.db_pool:
            self.db_pool = await asyncpg.create_pool(
                dsn=settings.DATABASE_URL,
                min_size=1,
                max_size=5,
                command_timeout=60
            )

    def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("news", self._handle_news))
        self.application.add_handler(CommandHandler("forex", self._handle_forex))
        self.application.add_handler(CallbackQueryHandler(self._handle_button_press))
        print("✅ Telegramm Handlers registered")

    async def send(self, message: str, channel: str = "Trading channel") -> bool:
        try:
            chat_id = self.chat_ids.get(channel)
            if not chat_id:
                self.logger.error(f"Chat ID for '{channel}' not found.")
                return False

            await self.application.bot.send_message(
                chat_id=chat_id,
                text=message,
                disable_web_page_preview=True
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            self.logger.debug(f"Message: {message[:200]}")
            return False

    async def get_latest_news(self, limit: int = 10) -> List[Dict]:
        if not self.db_pool:
            await self.connect_db()

        async with self.db_pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, title, content, url, publish_time, exchange 
                FROM announcements 
                ORDER BY publish_time DESC 
                LIMIT $1
                """,
                limit
            )

    async def forex_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Fetching today's Forex events...")
        message = await self.forex_factory.get_formatted_events()
        await update.message.reply_text(message, parse_mode="Markdown")

    async def _handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        commands = [
            BotCommand("news", "Get latest announcements"),
            BotCommand("forex", "Forex Factory economic data"),
            BotCommand("help", "Show help information")
        ]
        await context.bot.set_my_commands(commands, scope=BotCommandScopeDefault())
        print("Registered commands:", [c.command for c in commands])
        await update.message.reply_text(
            "📈 Financial News Bot\n\n"
            "Available commands:\n"
            "/news - Latest announcements\n"
            "/forex - Economic calendar data"
        )

    async def _handle_news(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            news_items = await self.get_latest_news()
            if not news_items:
                await update.message.reply_text("No recent announcements found.")
                return

            response = ["📰 Latest Announcements:\n"]
            for item in news_items:
                response.append(
                    f"• [{item['exchange'].upper()}] {item['title']}\n"
                    f"  {item['publish_time'].strftime('%Y-%m-%d %H:%M')}\n"
                    f"  {item['url']}\n"
                )
            await update.message.reply_text("\n".join(response))
        except Exception as e:
            self.logger.error(f"Failed to handle /news: {e}")
            await update.message.reply_text("❌ Error fetching news")

    async def _handle_forex(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            success = await self.forex_factory.send_to_telegram()
            if not success:
                await update.message.reply_text("Failed to fetch Forex Factory data.")
        except Exception as e:
            self.logger.error(f"Error in /forex command: {e}")
            await update.message.reply_text("❌ Error processing Forex data")

    async def _handle_button_press(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        if query.data == "refresh_news":
            await self._handle_news(update, context)
        elif query.data == "refresh_forex":
            await self._handle_forex(update, context)
        else:
            await query.edit_message_text("Unknown command.")

    async def start(self):
        print("Tlg bot starting services")
        await self.connect_db()
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        print("Bot successfully started working")

    async def stop(self):
        if self.application.updater.running:
            await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()
        if self.db_pool:
            await self.db_pool.close()