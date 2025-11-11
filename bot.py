import os
import time
import uuid
import logging
import asyncio
from datetime import datetime, timedelta

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import MessageNotModified

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# Import configuration
from config import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MB = 1024 ** 2

# --- WASABI (BOTO3) INITIALIZATION ---
try:
    s3_config = Config(
        signature_version='s3v4',
        connect_timeout=60,
        read_timeout=60,
        retries={'max_attempts': 10, 'mode': 'standard'}
    )
    
    transfer_config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=config.MULTIPART_THRESHOLD,
        max_concurrency=20,
        multipart_chunksize=config.MULTIPART_CHUNKSIZE,
        use_threads=True
    )

    s3_client = boto3.client(
        's3',
        endpoint_url=config.WASABI_ENDPOINT,
        aws_access_key_id=config.WASABI_ACCESS_KEY,
        aws_secret_access_key=config.WASABI_SECRET_KEY,
        region_name=config.WASABI_REGION,
        config=s3_config
    )
    logger.info(f"Wasabi S3 Client Initialized for region: {config.WASABI_REGION}")
except Exception as e:
    logger.error(f"Error initializing Boto3 client: {e}")
    exit(1)

# --- PYROGRAM BOT INITIALIZATION ---
app = Client(
    "wasabi_file_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)
logger.info("Pyrogram Client Initialized.")

# --- PROGRESS TRACKING & UTILITIES ---
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

# --- BOT HANDLERS ---
@app.on_message(filters.command("start") & filters.private)
async def start_command(client: Client, message: Message):
    """Handles the /start command."""
    await message.reply_text(
        "👋 **Welcome to the Immortal Speed Wasabi Uploader Bot!**\n\n"
        "This bot automatically handles large file uploads (up to 4GB+) to Wasabi "
        "Cloud Storage using high-speed multipart transfer capabilities.\n\n"
        "**How to use:**\n"
        "1. Simply send me any file (Document, Video, or Audio).\n"
        "2. The file will be uploaded, and I will provide you with a secure, "
        "time-limited (7-day) download link.\n\n"
        "**Service Status:** 🟢 24/7 Running Capacity Support"
    )

@app.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def handle_file_upload(client: Client, message: Message):
    """Handles incoming media/document messages."""
    
    file_info = message.document or message.video or message.audio
    
    if file_info.file_size > config.MAX_FILE_SIZE:
        return await message.reply_text("File size exceeds the 4GB bot capacity limit.")
    
    # Generate unique key to prevent collisions
    file_name = file_info.file_name or f"file-{uuid.uuid4().hex}"
    file_size = file_info.file_size
    temp_file_path = os.path.join(os.getcwd(), str(uuid.uuid4()))
    wasabi_key = f"{message.from_user.id}/{uuid.uuid4().hex}/{file_name}"
    
    progress_msg = await message.reply_text("Starting file processing...")
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
        await progress_msg.edit_text(f"✅ Download complete! Starting Wasabi upload...")
        
    except Exception as e:
        logger.error(f"Error during Telegram download: {e}")
        return await progress_msg.edit_text(f"❌ Download failed: {e}")
    
    # Upload to Wasabi
    try:
        tracker = ProgressTracker(client, progress_msg, file_size)

        await progress_msg.edit_text(
            f"**⬆️ Starting Immortal Speed Wasabi upload for** `{file_name}` **...**"
        )
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            s3_client.upload_file,
            download_path,
            config.WASABI_BUCKET,
            wasabi_key,
            Callback=tracker.update,
            Config=transfer_config
        )
        
        await tracker._edit_message_progress()
        await progress_msg.edit_text(f"🎉 **Wasabi Upload Complete!**\n\nGenerating secure download link...")

    except ClientError as e:
        logger.error(f"Wasabi S3 Client Error: {e}")
        await progress_msg.edit_text(f"❌ Wasabi Upload Failed (S3 Error): {e}")
    except Exception as e:
        logger.error(f"Error during Wasabi upload: {e}")
        await progress_msg.edit_text(f"❌ Wasabi Upload Failed: {e}")

    # Generate Download Link
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': config.WASABI_BUCKET, 'Key': wasabi_key},
            ExpiresIn=config.URL_EXPIRY
        )
        
        expiry_date = datetime.now() + timedelta(seconds=config.URL_EXPIRY)
        
        final_message = (
            f"**⚡️ IMMORTAL SPEED TRANSFER SUCCESSFUL!**\n\n"
            f"**File:** `{file_name}`\n"
            f"**Size:** `{file_size / MB:.2f} MB`\n\n"
            f"🔗 **Download Link (Expires {expiry_date.strftime('%Y-%m-%d %H:%M:%S UTC')}):**\n"
            f"`{url}`\n\n"
            f"_Secure, time-limited, direct download._"
        )
        
        await progress_msg.edit_text(final_message)
        logger.info(f"Generated URL for {file_name}")

    except Exception as e:
        logger.error(f"Error generating presigned URL: {e}")
        await progress_msg.edit_text(f"❌ Failed to generate download link: {e}")

    finally:
        # Clean up local file
        if download_path and os.path.exists(download_path):
            os.remove(download_path)
            logger.info(f"Cleaned up local file: {download_path}")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    logger.info("Starting bot...")
    app.run()
