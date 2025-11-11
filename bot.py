import os
import asyncio
import time
import math
import uuid
import logging
import re
from urllib.parse import urlparse
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import MessageNotModified
import boto3
from botocore.config import Config
from yt_dlp import YoutubeDL
from functools import partial

# Import configuration
from config import (
    API_ID, API_HASH, BOT_TOKEN,
    WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION,
    WASABI_ENDPOINT, MAX_FILE_SIZE, PRESIGNED_URL_EXPIRY,
    TEMP_DIR, YTDL_OPTIONS
)

# Security functions
def sanitize_filename(filename):
    """Sanitize filename to prevent path traversal and special characters"""
    if not filename:
        return f"file_{uuid.uuid4().hex}"
    
    filename = re.sub(r'[^\w\-. ]', '', filename)
    filename = filename.replace('..', '')
    return filename[:255]  # Limit length

def validate_youtube_url(url):
    """Validate YouTube URLs"""
    parsed = urlparse(url)
    allowed_domains = ['youtube.com', 'www.youtube.com', 'youtu.be', 'm.youtube.com']
    if not any(domain in parsed.netloc for domain in allowed_domains):
        raise ValueError("Invalid YouTube URL")
    return True

def safe_object_name(original_name, prefix=""):
    """Generate a safe S3 object name"""
    safe_name = sanitize_filename(original_name)
    unique_id = uuid.uuid4().hex
    return f"{prefix}{unique_id}_{safe_name}"

def human_size(bytes, units=[' bytes',' KB',' MB',' GB',' TB']):
    """Returns a human-readable string representation of bytes."""
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])

# Initialize Pyrogram Client
app = Client(
    "wasabi_file_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Initialize Boto3 Client for Wasabi
s3_client = boto3.client(
    's3',
    endpoint_url=WASABI_ENDPOINT,
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    region_name=WASABI_REGION,
    config=Config(
        signature_version='s3v4',
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        }
    )
)

class UploadProgress:
    """
    Callback class for Boto3 uploads to update a Telegram message periodically.
    This runs synchronously in the Boto3 worker thread.
    """
    def __init__(self, filename, filesize, client, message):
        self._filename = filename
        self._size = filesize
        self._seen_so_far = 0
        self._client = client
        self._message = message
        self._start_time = time.time()
        self._last_edit_time = 0
        # Determine the edit frequency based on file size (more frequent for large files)
        self._edit_frequency = 5 # seconds
        if filesize > 100 * 1024 * 1024: # > 100 MB
            self._edit_frequency = 2

    def __call__(self, bytes_amount):
        """Called by Boto3 periodically."""
        self._seen_so_far += bytes_amount
        current_time = time.time()

        if current_time - self._last_edit_time > self._edit_frequency or self._seen_so_far == self._size:
            self._last_edit_time = current_time

            # Calculate speed and percentage
            elapsed_time = current_time - self._start_time
            if elapsed_time > 0:
                speed = self._seen_so_far / elapsed_time
            else:
                speed = 0

            # Generate the progress bar string
            percentage = (self._seen_so_far / self._size) * 100
            done = math.floor(percentage / 10)
            remaining = 10 - done
            progress_bar = f"[{'■' * done}{'□' * remaining}]"

            status_text = (
                f"**🚀 Uploading to Wasabi...**\n"
                f"File: `{self._filename}`\n"
                f"Progress: **{percentage:.1f}%**\n"
                f"{progress_bar}\n"
                f"Uploaded: **{human_size(self._seen_so_far)}** of **{human_size(self._size)}**\n"
                f"Speed: **{human_size(speed)}/s**\n"
                f"Elapsed: **{int(elapsed_time)}s**"
            )

            # Schedule the message edit back into the main event loop
            future = asyncio.run_coroutine_threadsafe(
                self._client.edit_message_text(
                    chat_id=self._message.chat.id,
                    message_id=self._message.id,
                    text=status_text
                ),
                self._client.loop
            )
            try:
                future.result(timeout=2) 
            except asyncio.TimeoutError:
                logging.warning("Message edit scheduling timed out.")
            except MessageNotModified:
                pass
            except Exception as e:
                logging.error(f"Error during progress update: {e}")

async def upload_file_to_wasabi(client, message, file_path, object_name):
    """
    Handles the asynchronous upload of a file to Wasabi S3.
    Boto3 call is wrapped in asyncio.to_thread to prevent blocking.
    """
    file_size = os.path.getsize(file_path)

    # Send initial status message
    status_message = await message.reply_text(f"Starting upload for `{os.path.basename(file_path)}` ({human_size(file_size)})...", quote=True)

    try:
        # Create the callback instance
        progress_callback = UploadProgress(
            filename=os.path.basename(file_path),
            filesize=file_size,
            client=client,
            message=status_message
        )
        
        # Use functools.partial to run the blocking Boto3 call in a separate thread
        upload_task = partial(
            s3_client.upload_file,
            Filename=file_path,
            Bucket=WASABI_BUCKET,
            Key=object_name,
            Callback=progress_callback,
            ExtraArgs={'ACL': 'public-read'}
        )
        
        # Run the synchronous upload in a separate thread
        await asyncio.to_thread(upload_task)

        # Generate pre-signed URL for secure download
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET, 'Key': object_name},
            ExpiresIn=PRESIGNED_URL_EXPIRY
        )

        final_message_text = (
            f"✅ **Upload Complete!** (Wasabi S3)\n\n"
            f"📁 **File:** `{os.path.basename(file_path)}`\n"
            f"🔗 **Download Link (Expires in {PRESIGNED_URL_EXPIRY // (24 * 60 * 60)} days):**\n"
            f"**[Click to Download]({presigned_url})**\n\n"
            f"*(Capacity: 24/7 Running Bot. Supports up to {MAX_FILE_SIZE // (1024**3)}GB files.)*"
        )
        await status_message.edit_text(final_message_text, disable_web_page_preview=True)
        return True

    except Exception as e:
        error_text = f"❌ **Upload Failed!**\n\nError: `{e}`"
        await status_message.edit_text(error_text)
        logging.error(f"Wasabi upload failed: {e}")
        return False

# --- Telegram Bot Handlers ---

@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    """Handles the /start command."""
    await message.reply_text(
        f"👋 **Welcome to the Wasabi Cloud Bot!**\n\n"
        f"This bot can upload your files and YouTube videos directly to your Wasabi S3 storage.\n\n"
        f"**🚀 Features:**\n"
        f"1. **Send a File/Video** (up to {MAX_FILE_SIZE // (1024**3)}GB) to the bot to upload it.\n"
        f"2. Use `/ytdl [URL]` to download a YouTube video and upload it.\n\n"
        f"**Bucket:** `{WASABI_BUCKET}`\n"
        f"**Region:** `{WASABI_REGION}`",
        quote=True
    )

@app.on_message(filters.command("ytdl") & filters.private)
async def youtube_download_and_upload(client, message):
    """Downloads a YouTube URL using yt-dlp and uploads to Wasabi."""
    if len(message.command) < 2:
        return await message.reply_text("Please provide a YouTube URL. Usage: `/ytdl <url>`")

    url = message.command[1]
    
    try:
        validate_youtube_url(url)
    except ValueError as e:
        return await message.reply_text(f"❌ {str(e)}")

    unique_id = uuid.uuid4().hex
    
    # Enhanced yt-dlp options with error handling
    ydl_opts = YTDL_OPTIONS.copy()
    ydl_opts.update({
        'outtmpl': os.path.join(TEMP_DIR, f"{unique_id}_%(title)s.%(ext)s"),
        'logger': logging.getLogger('yt_dlp'),
    })
    
    status_message = await message.reply_text(f"⏳ Downloading video from: `{url}`...", disable_web_page_preview=True, quote=True)
    
    file_path = None
    try:
        def sync_download():
            with YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(url, download=True)
                return ydl.prepare_filename(info_dict)
                
        file_path = await asyncio.to_thread(sync_download)
        
        if not file_path or not os.path.exists(file_path):
            await status_message.edit_text("❌ Download failed or file path not found.")
            return

        # Start the Wasabi upload with safe object name
        object_name = safe_object_name(os.path.basename(file_path), "youtube/")
        await upload_file_to_wasabi(client, status_message, file_path, object_name)

    except Exception as e:
        error_text = f"❌ Video processing failed.\nError: `{str(e)[:200]}`"
        await status_message.edit_text(error_text)
        logging.error(f"YouTube download failed: {e}")
        
    finally:
        # Enhanced cleanup
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to clean up file {file_path}: {e}")

@app.on_message((filters.document | filters.video | filters.audio) & filters.private)
async def handle_file_upload(client, message):
    """Handles direct file or video uploads from the user."""
    
    if message.document:
        file = message.document
        file_name = file.file_name or "document"
    elif message.video:
        file = message.video
        file_name = file.file_name or "video.mp4"
    elif message.audio:
        file = message.audio
        file_name = file.file_name or "audio"
    else:
        return

    # Check file size
    if file.file_size > MAX_FILE_SIZE:
        return await message.reply_text(f"❌ File size exceeds the {MAX_FILE_SIZE // (1024**3)}GB limit.")

    # Sanitize filename
    safe_file_name = sanitize_filename(file_name)
    
    download_status_message = await message.reply_text(
        f"⏳ Starting download of `{safe_file_name}` from Telegram servers...", 
        quote=True
    )
    
    file_path = None
    try:
        # Download the file locally with safe name
        file_path = await client.download_media(
            message=file,
            file_name=os.path.join(TEMP_DIR, safe_file_name)
        )
        
        if not file_path:
            await download_status_message.edit_text("❌ Download failed.")
            return
            
        await download_status_message.edit_text(f"✅ Download complete. Starting Wasabi upload for `{safe_file_name}`...")

        # Start the Wasabi upload with safe object name
        object_name = safe_object_name(safe_file_name, "telegram_uploads/")
        await upload_file_to_wasabi(client, download_status_message, file_path, object_name)

    except Exception as e:
        error_text = f"❌ File processing failed.\nError: `{str(e)[:200]}`"
        await download_status_message.edit_text(error_text)
        logging.error(f"Telegram download/upload failed: {e}")
        
    finally:
        # Enhanced cleanup
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to clean up file {file_path}: {e}")

# --- Bot Start ---
if __name__ == "__main__":
    logging.info("Bot is starting...")
    app.run()
    logging.info("Bot stopped.")
