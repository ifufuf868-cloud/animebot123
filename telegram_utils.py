import os
import logging
import asyncio
import hashlib
import time
from telethon import TelegramClient, events
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Logging
logger = logging.getLogger("CosmosTV.Telegram")

# Configuration
API_ID = 31056597
API_HASH = "29ed3710db4c49cdcf0988ee52869704"
BOT_TOKEN = "8124849329:AAFjag3t7Ks4oJ8NMUOQQ54sgWH1uuNdbZc"
CHANNEL_ID = -1003677639772
ADMIN_CHAT_ID = 6526385624

# For rate limiting
ERROR_CACHE = {} # {error_hash: last_sent_time}
ERROR_COOLDOWN = 600 # 10 minutes for the same error

client = TelegramClient(
    'cosmos_session', 
    API_ID, 
    API_HASH,
    sequential_updates=True,
    flood_sleep_threshold=60,
    connection_retries=10,
    request_retries=10,
    retry_delay=5
)

@client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    """Respond to /start command"""
    await event.respond('Salom! Cosmos TV botiga xush kelibsiz. Bot faqat admin xabarlari uchun ishlamoqda ✅')

async def start_telegram():
    """Start the Telegram client and handle session issues"""
    try:
        await client.start(bot_token=BOT_TOKEN)
        logger.info("✅ Telegram Bot Client started (Notifications only).")
    except Exception as e:
        logger.error(f"❌ Failed to start Telegram Client: {e}")
        # If session error, try deleting and restarting once
        if "auth_key" in str(e).lower() or "session" in str(e).lower():
            try:
                session_path = Path("cosmos_session.session")
                if session_path.exists():
                    logger.warning("🗑 Deleting faulty session file and retrying...")
                    session_path.unlink()
                    await client.start(bot_token=BOT_TOKEN)
                    logger.info("✅ Re-authentication successful after session reset.")
            except Exception as retry_err:
                logger.error(f"❌ Failed to recover from session error: {retry_err}")

async def send_error_to_admin(error_message: str):
    """Send error notification to Admin with better formatting and rate limiting"""
    try:
        # Simple deduplication/rate limiting
        error_hash = hashlib.md5(error_message.encode()).hexdigest()
        now = time.time()
        
        if error_hash in ERROR_CACHE:
            if now - ERROR_CACHE[error_hash] < ERROR_COOLDOWN:
                # Still in cooldown, skip
                return
        
        ERROR_CACHE[error_hash] = now

        if not client.is_connected():
            await start_telegram()
        
        # Format message with bold header and code block for the error
        formatted_msg = f"❗ **COSMOS TV ERROR**\n\n```\n{error_message}\n```"
        
        await client.send_message(ADMIN_CHAT_ID, formatted_msg)
    except Exception as e:
        logger.error(f"❌ Failed to send error to admin: {e}")

# Removed: upload_video_to_telegram, upload_image_to_telegram, get_video_message, stream_telegram_file
# Cleanup reason: Migration to S3/R2 storage.
