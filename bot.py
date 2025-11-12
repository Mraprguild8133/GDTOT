import os
import time
import uuid
import logging
import asyncio
import functools
import base64
import threading
import requests
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import MessageNotModified

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

# --- 1. CONFIGURATION AND ENVIRONMENT SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
API_ID = os.environ.get("API_ID")
API_HASH = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY")
WASABI_BUCKET = os.environ.get("WASABI_BUCKET")
WASABI_REGION = os.environ.get("WASABI_REGION")

# Render-specific configuration
RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL")  # Render provides this automatically
FLASK_HOST = os.environ.get("FLASK_HOST", "0.0.0.0")
FLASK_PORT = int(os.environ.get("PORT", "10000"))  # Render uses PORT environment variable

# Uptime monitoring (optional but recommended)
HEALTHCHECKS_IO_URL = os.environ.get("https://hc-ping.com/52584294-1372-4b39-8cd6-897a094ee134")
UPTIME_ROBOT_URL = os.environ.get("https://status.domain-monitor.io/6220e956-a8f3-41d4-a93a-b2c03386734f/")

# Check for required variables
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    logger.error("One or more required environment variables are missing. Please set all variables.")
    exit(1)

# Use Render URL if available, otherwise construct local URL
if RENDER_URL:
    BASE_URL = RENDER_URL.rstrip('/')
    logger.info(f"Using Render URL: {BASE_URL}")
else:
    BASE_URL = f"http://{FLASK_HOST}:{FLASK_PORT}"
    logger.info(f"Using local URL: {BASE_URL}")

# Wasabi Endpoint Construction
WASABI_ENDPOINT = f"https://s3.{WASABI_REGION}.wasabisys.com"

# Constants
MB = 1024 ** 2
MAX_FILE_SIZE = 4 * 1024 ** 3  # 4GB
URL_EXPIRY = 604800  # 7 days

# --- 2. FLASK APP FOR MEDIA PLAYER ---
flask_app = Flask(__name__, template_folder="templates")

@flask_app.route("/")
def index():
    return render_template("index.html")

@flask_app.route("/player/<media_type>/<encoded_url>")
def player(media_type, encoded_url):
    # Decode the URL
    try:
        # Add padding if needed
        padding = 4 - (len(encoded_url) % 4)
        if padding != 4:
            encoded_url += '=' * padding
        media_url = base64.urlsafe_b64decode(encoded_url).decode()
        logger.info(f"Serving media: {media_type} - {media_url[:50]}...")
        return render_template("player.html", media_type=media_type, media_url=media_url)
    except Exception as e:
        logger.error(f"Error decoding URL: {str(e)}")
        return f"Error decoding URL: {str(e)}", 400

@flask_app.route("/health")
def health():
    return jsonify({
        "status": "ok", 
        "service": "Wasabi Media Player",
        "timestamp": datetime.now().isoformat(),
        "bot_status": "running"
    })

@flask_app.route("/ping")
def ping():
    """Simple ping endpoint for uptime monitoring"""
    return jsonify({"status": "pong", "timestamp": datetime.now().isoformat()})

def run_flask():
    flask_app.run(host=FLASK_HOST, port=FLASK_PORT, debug=False)

# --- 3. UPTIME MONITORING SYSTEM ---
class UptimeMonitor:
    def __init__(self):
        self.is_running = True
        
    def start_monitoring(self):
        """Start periodic health pings"""
        monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        monitor_thread.start()
        logger.info("Uptime monitoring started")
        
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                # Ping our own health endpoint every 5 minutes
                if RENDER_URL:
                    try:
                        response = requests.get(f"{RENDER_URL}/health", timeout=10)
                        if response.status_code == 200:
                            logger.debug("Self-ping successful")
                    except Exception as e:
                        logger.warning(f"Self-ping failed: {e}")
                
                # Ping external monitoring services
                self._ping_external_services()
                
            except Exception as e:
                logger.error(f"Uptime monitoring error: {e}")
            
            # Sleep for 5 minutes
            time.sleep(300)  # 5 minutes
    
    def _ping_external_services(self):
        """Ping external monitoring services"""
        try:
            # Healthchecks.io
            if HEALTHCHECKS_IO_URL:
                requests.get(HEALTHCHECKS_IO_URL, timeout=10)
                logger.debug("Healthchecks.io ping sent")
            
            # UptimeRobot (via webhook)
            if UPTIME_ROBOT_URL:
                requests.get(UPTIME_ROBOT_URL, timeout=10)
                logger.debug("UptimeRobot ping sent")
                
        except Exception as e:
            logger.warning(f"External ping failed: {e}")
    
    def stop(self):
        self.is_running = False

# Initialize uptime monitor
uptime_monitor = UptimeMonitor()

# --- 4. WASABI (BOTO3) INITIALIZATION ---
try:
    s3_config = Config(
        signature_version='s3v4',
        connect_timeout=60,
        read_timeout=60,
        retries={'max_attempts': 10, 'mode': 'standard'}
    )
    
    transfer_config = TransferConfig(
        multipart_threshold=100 * MB,
        max_concurrency=20,
        multipart_chunksize=25 * MB,
        use_threads=True
    )

    s3_client = boto3.client(
        's3',
        endpoint_url=WASABI_ENDPOINT,
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        region_name=WASABI_REGION,
        config=s3_config
    )
    logger.info(f"Wasabi S3 Client Initialized for region: {WASABI_REGION}")
except Exception as e:
    logger.error(f"Error initializing Boto3 client: {e}")
    exit(1)

# --- 5. PYROGRAM BOT INITIALIZATION ---
app = Client(
    "wasabi_file_bot",
    api_id=int(API_ID),
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)
logger.info("Pyrogram Client Initialized.")

# --- 6. PROGRESS TRACKING & UTILITIES ---
processing_messages = set()

class ProgressTracker:
    def __init__(self, client: Client, message: Message, total: int):
        self.client = client
        self.message = message
        self.total = total
        self._current = 0
        self._start_time = time.time()
        self._last_edit_time = 0.0

    def update(self, chunk: int):
        self._current += chunk
        now = time.time()
        if (now - self._last_edit_time) < 1.0:
            return
        self._last_edit_time = now
        self.client.loop.call_soon_threadsafe(
            asyncio.create_task,
            self._edit_message_progress()
        )

    async def _edit_message_progress(self):
        percentage = min(100.0, (self._current * 100) / self.total)
        elapsed = time.time() - self._start_time
        speed = (self._current / elapsed) / MB if elapsed > 0 else 0.0
        status = f"**🔄 Wasabi Upload Progress**\n━━━━━━━━━━━━━━━━━━━━\n**Uploaded:** `{self._current / MB:.2f} MB` / `{self.total / MB:.2f} MB`\n**Speed:** `{speed:.2f} MB/s`\n**Progress:** `[{'▓' * int(percentage // 10):<10}] {percentage:.1f}%`"
        try:
            await self.message.edit_text(status)
        except MessageNotModified:
            pass
        except Exception as e:
            logger.warning(f"Failed to edit progress message: {e}")

async def pyrogram_progress_callback(current, total, client, message):
    now = time.time()
    if (now - message.data.get('last_edit_time', 0.0)) < 1.0:
        return
    message.data['last_edit_time'] = now
    percentage = min(100.0, (current * 100) / total)
    elapsed = now - message.data.get('start_time', now)
    speed = (current / elapsed) / MB if elapsed > 0 else 0.0
    status = f"**⬇️ Telegram Download Progress**\n━━━━━━━━━━━━━━━━━━━━\n**Downloaded:** `{current / MB:.2f} MB` / `{total / MB:.2f} MB`\n**Speed:** `{speed:.2f} MB/s`\n**Progress:** `[{'▓' * int(percentage // 10):<10}] {percentage:.1f}%`"
    try:
        await client.edit_message_text(message.chat.id, message.id, status)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.warning(f"Failed to edit download progress message: {e}")

def get_media_type(file_name):
    video_extensions = ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v']
    audio_extensions = ['.mp3', '.wav', '.ogg', '.m4a', '.flac', '.aac', '.wma']
    file_ext = os.path.splitext(file_name.lower())[1]
    if file_ext in video_extensions:
        return 'video'
    elif file_ext in audio_extensions:
        return 'audio'
    else:
        return 'document'

# --- 7. BOT HANDLERS ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    welcome_text = (
        "👋 **Welcome to the Immortal Speed Wasabi Uploader Bot!**\n\n"
        "This bot automatically handles large file uploads (up to 4GB+) to Wasabi "
        "Cloud Storage using high-speed multipart transfer capabilities.\n\n"
        "**How to use:**\n"
        "1. Simply send me any file (Document, Video, or Audio).\n"
        "2. The file will be uploaded, and I will provide you with multiple download options.\n\n"
        "**Features:**\n• 🚀 Direct download links\n• 📺 Built-in media player for videos/audio\n• ⚡ High-speed multipart uploads\n• 🔒 Secure 7-day access links\n\n"
        "**Service Status:** 🟢 24/7 Running Capacity Support"
    )
    await message.reply_text(welcome_text)

@app.on_message(filters.command("status") & filters.private)
async def status_command(client: Client, message: Message):
    """Check bot status"""
    status_text = (
        "🤖 **Bot Status**\n\n"
        "**Service:** 🟢 Online\n"
        "**Storage:** Wasabi Cloud\n"
        "**Max File Size:** 4GB\n"
        "**Uptime:** 24/7\n"
        "**Last Check:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n\n"
        "The bot is running smoothly and ready to handle your files!"
    )
    await message.reply_text(status_text)

@app.on_callback_query(filters.regex("^upload_another$"))
async def upload_another_callback(client, callback_query):
    await callback_query.message.edit_text("🔄 **Ready for another upload!**\n\nSend me any file and I'll upload it to Wasabi.")

@app.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_file_upload(client: Client, message: Message):
    message_id = f"{message.chat.id}_{message.id}"
    if message_id in processing_messages:
        return
    processing_messages.add(message_id)
    
    try:
        file_info = message.document or message.video or message.audio
        if file_info.file_size > MAX_FILE_SIZE:
            await message.reply_text("❌ File size exceeds the 4GB bot capacity limit.")
            return
        
        file_name = file_info.file_name or f"file-{uuid.uuid4().hex}"
        file_size = file_info.file_size
        temp_file_path = os.path.join(os.getcwd(), str(uuid.uuid4()))
        wasabi_key = f"{message.from_user.id}/{uuid.uuid4().hex}/{file_name}"
        
        progress_msg = await message.reply_text("🔄 Starting file processing...")
        progress_msg.data = {'start_time': time.time(), 'last_edit_time': 0.0}
        download_path = None

        # Download from Telegram
        try:
            await progress_msg.edit_text(f"**⬇️ Starting Telegram download for** `{file_name}` **({file_size / MB:.2f} MB)...**")
            download_path = await client.download_media(message, file_name=temp_file_path, progress=pyrogram_progress_callback, progress_args=(client, progress_msg))
            logger.info(f"Downloaded file to: {download_path}")
            await progress_msg.edit_text("✅ **Download complete!** Starting Wasabi upload...")
        except Exception as e:
            logger.error(f"Error during Telegram download: {e}")
            await progress_msg.edit_text(f"❌ Download failed: {e}")
            return
        
        # Upload to Wasabi
        try:
            tracker = ProgressTracker(client, progress_msg, file_size)
            await progress_msg.edit_text(f"**⬆️ Starting Immortal Speed Wasabi upload for** `{file_name}` **...**")
            upload_function = functools.partial(s3_client.upload_file, download_path, WASABI_BUCKET, wasabi_key, Callback=tracker.update, Config=transfer_config)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, upload_function)
            await tracker._edit_message_progress()
            await progress_msg.edit_text("🎉 **Wasabi Upload Complete!**\n\nGenerating download options...")
        except Exception as e:
            logger.error(f"Error during Wasabi upload: {e}")
            await progress_msg.edit_text(f"❌ Wasabi Upload Failed: {e}")
            if download_path and os.path.exists(download_path):
                os.remove(download_path)
            return

        # Generate Download Options
        try:
            url = s3_client.generate_presigned_url('get_object', Params={'Bucket': WASABI_BUCKET, 'Key': wasabi_key}, ExpiresIn=URL_EXPIRY)
            expiry_date = datetime.now() + timedelta(seconds=URL_EXPIRY)
            expiry_days = (expiry_date - datetime.now()).days
            media_type = get_media_type(file_name)
            player_url = None
            
            if media_type in ['video', 'audio']:
                encoded_url = base64.urlsafe_b64encode(url.encode()).decode().rstrip('=')
                player_url = f"{BASE_URL}/player/{media_type}/{encoded_url}"
                logger.info(f"Generated player URL: {player_url}")
            
            buttons = []
            if player_url:
                buttons.append([InlineKeyboardButton("🎬 Media Player", url=player_url)])
            buttons.append([InlineKeyboardButton("🚀 Direct Download", url=url)])
            buttons.append([InlineKeyboardButton("🔄 Upload Another", callback_data="upload_another")])
            
            keyboard = InlineKeyboardMarkup(buttons)
            final_message = f"**⚡️ TRANSFER SUCCESSFUL!**\n\n**📁 File:** `{file_name}`\n**📊 Size:** `{file_size / MB:.2f} MB`\n**🎯 Type:** `{media_type.upper()}`\n**⏰ Expires:** `{expiry_days} days`\n\n"
            final_message += "**Choose download method:**\n• 🎬 **Media Player** - Stream in browser\n• 🚀 **Direct Download** - Download file" if player_url else "Click **🚀 Direct Download** to get your file!"
            
            await progress_msg.edit_text(final_message, reply_markup=keyboard)
            logger.info(f"Generated URL for {file_name}")

        except Exception as e:
            logger.error(f"Error generating download options: {e}")
            await progress_msg.edit_text(f"❌ Failed to generate download options: {e}")

        finally:
            if download_path and os.path.exists(download_path):
                os.remove(download_path)
                logger.info(f"Cleaned up local file: {download_path}")
                
    finally:
        processing_messages.discard(message_id)

# --- 8. BOT STARTUP AND SHUTDOWN ---
@app.on_raw_update()
async def keep_alive_handler(client, update, users, chats):
    """Handler that keeps the bot active by processing raw updates"""
    pass

async def startup():
    """Bot startup routine"""
    logger.info("🤖 Starting Wasabi File Upload Bot...")
    uptime_monitor.start_monitoring()
    logger.info("✅ Uptime monitoring started")
    
    # Send startup notification if possible
    try:
        await app.send_message("me", "🚀 Wasabi Upload Bot Started Successfully!\n\n"
                                  f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                                  f"🌐 Server: {BASE_URL}")
    except Exception as e:
        logger.warning(f"Could not send startup message: {e}")

async def shutdown():
    """Bot shutdown routine"""
    logger.info("🛑 Shutting down Wasabi File Upload Bot...")
    uptime_monitor.stop()

# --- 9. MAIN EXECUTION ---
if __name__ == "__main__":
    logger.info("Starting Wasabi File Upload Bot with Media Player...")
    
    # Start Flask server in a separate thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Flask media player started on {BASE_URL}")
    
    # Add startup and shutdown handlers
    app.start()
    
    # Run startup routine
    asyncio.run(startup())
    
    try:
        # Keep the main thread alive
        logger.info("✅ Bot is now running and ready to accept files!")
        app.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal...")
    finally:
        asyncio.run(shutdown())
        app.stop()
        logger.info("Bot stopped successfully")
