import os
import time
import uuid
import logging
import asyncio
import functools
from datetime import datetime, timedelta

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

# Check for required variables
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    logger.error("One or more required environment variables are missing. Please set all variables.")
    exit(1)

# Wasabi Endpoint Construction
WASABI_ENDPOINT = f"https://s3.{WASABI_REGION}.wasabisys.com"

# Constants
MB = 1024 ** 2
MAX_FILE_SIZE = 4 * 1024 ** 3  # 4GB
URL_EXPIRY = 604800  # 7 days

# --- 2. WASABI (BOTO3) INITIALIZATION ---
try:
    s3_config = Config(
        signature_version='s3v4',
        connect_timeout=60,
        read_timeout=60,
        retries={'max_attempts': 10, 'mode': 'standard'}
    )
    
    # CORRECT TransferConfig usage
    transfer_config = TransferConfig(
        multipart_threshold=100 * MB,  # Start multipart upload for files > 100MB
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

# --- 3. PYROGRAM BOT INITIALIZATION ---
app = Client(
    "wasabi_file_bot",
    api_id=int(API_ID),
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)
logger.info("Pyrogram Client Initialized.")

# --- 4. PROGRESS TRACKING & UTILITIES ---
class ProgressTracker:
    """Tracks upload progress for Boto3 and updates the Telegram message."""
    def __init__(self, client: Client, message: Message, total: int):
        self.client = client
        self.message = message
        self.total = total
        self._current = 0
        self._start_time = time.time()
        self._last_edit_time = 0.0

    def update(self, chunk: int):
        """Called synchronously by Boto3 for each chunk transfer"""
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
        """Asynchronously edits the Telegram message."""
        percentage = min(100.0, (self._current * 100) / self.total)
        elapsed = time.time() - self._start_time
        
        speed = (self._current / elapsed) / MB if elapsed > 0 else 0.0
            
        status = f"**🔄 Wasabi Upload Progress (Multi-Part)**\n"
        status += f"━━━━━━━━━━━━━━━━━━━━\n"
        status += f"**Uploaded:** `{self._current / MB:.2f} MB` / `{self.total / MB:.2f} MB`\n"
        status += f"**Speed:** `{speed:.2f} MB/s`\n"
        status += f"**Progress:** `[{'▓' * int(percentage // 10):<10}] {percentage:.1f}%`"
        
        try:
            await self.message.edit_text(status)
        except MessageNotModified:
            pass
        except Exception as e:
            logger.warning(f"Failed to edit progress message: {e}")

async def pyrogram_progress_callback(current, total, client, message):
    """Callback for Pyrogram download progress."""
    now = time.time()
    
    if (now - message.data.get('last_edit_time', 0.0)) < 1.0:
        return

    message.data['last_edit_time'] = now
    
    percentage = min(100.0, (current * 100) / total)
    elapsed = now - message.data.get('start_time', now)
    
    speed = (current / elapsed) / MB if elapsed > 0 else 0.0
        
    status = f"**⬇️ Telegram Download Progress**\n"
    status += f"━━━━━━━━━━━━━━━━━━━━\n"
    status += f"**Downloaded:** `{current / MB:.2f} MB` / `{total / MB:.2f} MB`\n"
    status += f"**Speed:** `{speed:.2f} MB/s`\n"
    status += f"**Progress:** `[{'▓' * int(percentage // 10):<10}] {percentage:.1f}%`"

    try:
        await client.edit_message_text(message.chat.id, message.id, status)
    except MessageNotModified:
        pass
    except Exception as e:
        logger.warning(f"Failed to edit download progress message: {e}")

# --- 5. BOT HANDLERS ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    """Handles the /start command."""
    welcome_text = (
        "👋 **Welcome to the Immortal Speed Wasabi Uploader Bot!**\n\n"
        "This bot automatically handles large file uploads (up to 4GB+) to Wasabi "
        "Cloud Storage using high-speed multipart transfer capabilities.\n\n"
        "**How to use:**\n"
        "1. Simply send me any file (Document, Video, or Audio).\n"
        "2. The file will be uploaded, and I will provide you with a secure, "
        "time-limited (7-day) download link.\n\n"
        "**Service Status:** 🟢 24/7 Running Capacity Support"
    )
    
    await message.reply_text(welcome_text)

@app.on_callback_query(filters.regex("^upload_another$"))
async def upload_another_callback(client, callback_query):
    """Handle upload another button"""
    await callback_query.message.edit_text(
        "🔄 **Ready for another upload!**\n\n"
        "Send me any file (document, video, or audio) and I'll upload it to Wasabi."
    )

@app.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_file_upload(client: Client, message: Message):
    """Handles incoming media/document messages."""
    
    file_info = message.document or message.video or message.audio
    
    if file_info.file_size > MAX_FILE_SIZE:
        return await message.reply_text("❌ File size exceeds the 4GB bot capacity limit.")
    
    file_name = file_info.file_name or f"file-{uuid.uuid4().hex}"
    file_size = file_info.file_size
    temp_file_path = os.path.join(os.getcwd(), str(uuid.uuid4()))
    wasabi_key = f"{message.from_user.id}/{uuid.uuid4().hex}/{file_name}"
    
    progress_msg = await message.reply_text("🔄 Starting file processing...")
    progress_msg.data = {
        'start_time': time.time(),
        'last_edit_time': 0.0
    }

    download_path = None

    # Download from Telegram
    try:
        await progress_msg.edit_text(
            f"**⬇️ Starting Telegram download for** `{file_name}` **({file_size / MB:.2f} MB)...**"
        )
        download_path = await client.download_media(
            message,
            file_name=temp_file_path,
            progress=pyrogram_progress_callback,
            progress_args=(client, progress_msg)
        )
        logger.info(f"Downloaded file to: {download_path}")
        await progress_msg.edit_text("✅ **Download complete!** Starting Wasabi upload...")
        
    except Exception as e:
        logger.error(f"Error during Telegram download: {e}")
        return await progress_msg.edit_text(f"❌ Download failed: {e}")
    
    # Upload to Wasabi
    try:
        tracker = ProgressTracker(client, progress_msg, file_size)

        await progress_msg.edit_text(
            f"**⬆️ Starting Immortal Speed Wasabi upload for** `{file_name}` **...**"
        )
        
        # Create a partial function with all parameters for thread execution
        upload_function = functools.partial(
            s3_client.upload_file,
            download_path,
            WASABI_BUCKET,
            wasabi_key,
            Callback=tracker.update,
            Config=transfer_config
        )
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, upload_function)
        
        await tracker._edit_message_progress()
        await progress_msg.edit_text("🎉 **Wasabi Upload Complete!**\n\nGenerating secure download link...")

    except ClientError as e:
        logger.error(f"Wasabi S3 Client Error: {e}")
        await progress_msg.edit_text(f"❌ Wasabi Upload Failed (S3 Error): {e}")
        # Clean up on error
        if download_path and os.path.exists(download_path):
            os.remove(download_path)
        return
    except Exception as e:
        logger.error(f"Error during Wasabi upload: {e}")
        await progress_msg.edit_text(f"❌ Wasabi Upload Failed: {e}")
        # Clean up on error
        if download_path and os.path.exists(download_path):
            os.remove(download_path)
        return

    # Generate Download Link with Button
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': wasabi_key},
            ExpiresIn=URL_EXPIRY
        )
        
        expiry_date = datetime.now() + timedelta(seconds=URL_EXPIRY)
        expiry_days = (expiry_date - datetime.now()).days
        
        # Create enhanced inline keyboard with multiple options
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Direct Download", url=url)],
            [
                InlineKeyboardButton("📋 Copy URL", callback_data="copy_url"),
                InlineKeyboardButton("🔄 Upload Another", callback_data="upload_another")
            ]
        ])
        
        final_message = (
            f"**⚡️ IMMORTAL SPEED TRANSFER SUCCESSFUL!**\n\n"
            f"**📁 File:** `{file_name}`\n"
            f"**📊 Size:** `{file_size / MB:.2f} MB`\n"
            f"**⏰ Link Expires:** `{expiry_days} days` ({expiry_date.strftime('%Y-%m-%d %H:%M:%S UTC')})\n\n"
            f"Click **🚀 Direct Download** to download your file instantly!"
        )
        
        await progress_msg.edit_text(final_message, reply_markup=keyboard)
        logger.info(f"Generated URL for {file_name}")

    except Exception as e:
        logger.error(f"Error generating presigned URL: {e}")
        await progress_msg.edit_text(f"❌ Failed to generate download link: {e}")

    finally:
        # Clean up local file
        if download_path and os.path.exists(download_path):
            os.remove(download_path)
            logger.info(f"Cleaned up local file: {download_path}")

# Additional callback handler for copy URL
@app.on_callback_query(filters.regex("^copy_url$"))
async def copy_url_callback(client, callback_query):
    """Handle copy URL button"""
    # Extract URL from the message text (you might want to store it differently)
    message_text = callback_query.message.text
    # This is a simple extraction - you might want to store the URL more reliably
    await callback_query.answer("URL copy feature would be implemented here", show_alert=True)

# --- 6. MAIN EXECUTION ---
if __name__ == "__main__":
    logger.info("Starting Wasabi File Upload Bot...")
    app.run()
