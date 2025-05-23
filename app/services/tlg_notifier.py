import logging
import asyncpg
import asyncio
from typing import List, Dict
from telegram import Update, BotCommand, BotCommandScopeDefault, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from app.services.forex_factory import ForexFactoryService
from app.storage.postgres_manager import AnnouncementStorage
from config.settings import settings

class Notifier:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_pool = None
        self.polling_task = None
        self.chat_ids = {
            "Trading channel": settings.TELEGRAM_TRADING_CHAT_ID,
            "Engineering channel": settings.TELEGRAM_ENGINEERING_CHAT_ID
        }
        self.forex_factory = ForexFactoryService()
        self.application = ApplicationBuilder().token(settings.TELEGRAM_BOT_TOKEN).build()
        self.storage = AnnouncementStorage()
        self._register_handlers()

    async def connect_db(self):
        if not self.db_pool:
            self.db_pool = await asyncpg.create_pool(dsn=settings.DATABASE_URL, min_size=1, max_size=5, command_timeout=60)
        await self.storage.connect()

    def _register_handlers(self):
        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("news", self._handle_news))
        self.application.add_handler(CommandHandler("forex", self._handle_forex))
        self.application.add_handler(CallbackQueryHandler(self._handle_button_press))

    async def send(self, message: str, channel: str = "Trading channel", reply_markup=None) -> bool:
        try:
            chat_id = self.chat_ids.get(channel)
            if not chat_id:
                return False
            await self.application.bot.send_message(chat_id=chat_id, text=message, reply_markup=reply_markup)
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False

    async def get_latest_news(self, limit: int = 10) -> List[Dict]:
        if not self.db_pool:
            await self.connect_db()
        async with self.db_pool.acquire() as conn:
            return await conn.fetch(
                "SELECT id, title, content, url, publish_time, exchange FROM announcements ORDER BY publish_time DESC LIMIT $1", limit
            )

    async def _handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        commands = [
            BotCommand("news", "Get latest announcements"),
            BotCommand("forex", "Forex Factory economic data")
        ]
        await context.bot.set_my_commands(commands, scope=BotCommandScopeDefault())
        await update.message.reply_text("📈 Welcome! Use /news or /forex to begin.")

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
                    f"{item['publish_time'].strftime('%Y-%m-%d %H:%M')}\n"
                    f"{item['url']}\n"
                )
            await update.message.reply_text("\n".join(response))
        except Exception as e:
            self.logger.error(f"Failed to handle /news: {e}")
            await update.message.reply_text("❌ Error fetching news")

    async def _handle_forex(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            message = await self.forex_factory.get_formatted_events()
            await update.message.reply_text(message, parse_mode="Markdown")
        except Exception as e:
            self.logger.error(f"Error in /forex command: {e}")
            await update.message.reply_text("❌ Error processing Forex data")

    async def _handle_button_press(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        try:
            if query.data.startswith("label|"):
                _, new_label, announcement_id = query.data.split("|")
                print(new_label, announcement_id)
                success = await self.storage.update_user_classification(announcement_id, new_label)
                if success:
                    await query.edit_message_text(f"✅ Label updated to: *{new_label}*", parse_mode="Markdown")
                else:
                    await query.edit_message_text("❌ Failed to update label")
        except Exception as e:
            self.logger.error(f"Callback handling error: {e}")
            await query.edit_message_text("❌ Error processing your choice")

    async def start(self):
        await self.connect_db()
        await self.application.initialize()
        await self.application.start()
        self.polling_task = asyncio.create_task(self.application.updater.start_polling())
        await self.polling_task

    async def stop(self):
        if self.polling_task:
            self.polling_task.cancel()
            try:
                await self.polling_task
            except asyncio.CancelledError:
                pass
        if self.application.updater.running:
            await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()
        if self.db_pool:
            await self.db_pool.close()





# Для тестирования вебхука
    #def _start_ngrok(self):
    #    from pyngrok import conf
    #    conf.get_default().auth_token = settings.NGROK_AUTHTOKEN
    #    public_url = ngrok.connect(settings.NGROK_PORT, bind_tls = True)
    #    print(f"🔗 Ngrok tunnel started at: {public_url}")
    #    return public_url

    #async def start(self):
    #    print("📡 Telegram bot is starting (webhook mode)...")
    #    await self.connect_db()
    #    await self.application.initialize()
    #    if settings.USE_NGROK:
    #        self.ngrok_url = self._start_ngrok()
    #    else:
    #        webhook_url = f"{settings.TELEGRAM_WEBHOOK_BASE_URL}{settings.TELEGRAM_WEBHOOK_PATH}"
    #    await self.application.bot.set_webhook(
    #        url=webhook_url,
    #        secret_token=settings.TELEGRAM_WEBHOOK_SECRET
    #    )

    #    await self.application.start()
    #    await self.application.updater.start_webhook(
    #        listen="0.0.0.0",
    #        port=settings.NGROK_PORT if settings.USE_NGROK else 8000,
    #        webhook_path=settings.TELEGRAM_WEBHOOK_PATH,
    #        secret_token=settings.TELEGRAM_WEBHOOK_SECRET,
    #    )
    #    print(f"✅ Webhook started at {webhook_url}")

    #async def stop(self):
    #    if self.polling_task:
    #        self.polling_task.cancel()
    #        try:
    #            await self.polling_task
    #        except asyncio.CancelledError:
    #            print("🛑 Polling task cancelled")

    #    await self.application.stop()
    #    await self.application.shutdown()
    #    if self.db_pool:
    #        await self.db_pool.close()
    #    print("📴 Bot stopped")