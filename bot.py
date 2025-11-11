import os
import asyncio
import time
import math
import logging
from pyrogram import Client, filters
from pyrogram.errors import MessageNotModified
from pyrogram.types import Message
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Import your config
from config import config

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
# Use values from your config module
WASABI_ENDPOINT_URL = f"https://s3.{config.WASABI_REGION}.wasabisys.com"

API_ID = config.API_ID
API_HASH = config.API_HASH
BOT_TOKEN = config.BOT_TOKEN
WASABI_ACCESS_KEY = config.WASABI_ACCESS_KEY
WASABI_SECRET_KEY = config.WASABI_SECRET_KEY
WASABI_BUCKET = config.WASABI_BUCKET
WASABI_REGION = config.WASABI_REGION

# Validate essential configuration
if not all([API_ID, API_HASH, BOT_TOKEN, WASABI_ACCESS_KEY, WASABI_SECRET_KEY, WASABI_BUCKET, WASABI_REGION]):
    logger.error("FATAL ERROR: One or more required configuration variables are missing.")
    exit(1)

# --- INITIALIZATION ---
app = Client(
    "wasabi_bot",
    api_id=int(API_ID),
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# Boto3 Configuration
s3_config = Config(
    signature_version='s3v4',
    region_name=WASABI_REGION,
    retries={'max_attempts': 3, 'mode': 'standard'}
)

# Thread pool for blocking operations
executor = ThreadPoolExecutor(max_workers=5)
loop = asyncio.get_event_loop()

# --- UTILITIES ---
def sizeof_fmt(num, suffix='B'):
    """Format file size into human-readable string."""
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def create_progress_callback(client, message, file_name, file_size, start_time):
    """
    Creates a closure that serves as the progress callback function for boto3.
    """
    last_update_time = time.time()
    uploaded_bytes = 0
    
    async def progress_callback(bytes_transferred):
        nonlocal last_update_time, uploaded_bytes
        uploaded_bytes += bytes_transferred
        
        current_time = time.time()
        
        # Only update the message every 1 second to avoid hitting API limits
        if current_time - last_update_time >= 1:
            try:
                # Calculate metrics
                time_elapsed = current_time - start_time
                speed = uploaded_bytes / time_elapsed if time_elapsed > 0 else 0
                percentage = (uploaded_bytes / file_size) * 100
                
                # Format strings
                speed_str = sizeof_fmt(speed)
                uploaded_str = sizeof_fmt(uploaded_bytes)
                total_str = sizeof_fmt(file_size)
                
                # Progress bar visualization
                progress_bar_length = 10
                filled_length = int(progress_bar_length * percentage // 100)
                progress_bar = '█' * filled_length + '░' * (progress_bar_length - filled_length)

                # Construct the update text
                new_text = (
                    f"**⬆️ Uploading: {file_name}**\n\n"
                    f"`{progress_bar}` `{percentage:.1f}%`\n"
                    f"**Progress:** {uploaded_str} / {total_str}\n"
                    f"**Speed:** {speed_str}/s\n"
                    f"**Time Elapsed:** {int(time_elapsed)}s"
                )
                
                await client.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=message.id,
                    text=new_text
                )
                last_update_time = current_time
            except MessageNotModified:
                pass
            except Exception as e:
                logger.error(f"Error during progress update: {e}")

    def sync_callback(bytes_transferred):
        asyncio.run_coroutine_threadsafe(progress_callback(bytes_transferred), loop)

    return sync_callback

def initialize_s3():
    """Initializes and returns the Wasabi S3 client with error handling."""
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=WASABI_ACCESS_KEY,
            aws_secret_access_key=WASABI_SECRET_KEY,
            endpoint_url=WASABI_ENDPOINT_URL,
            config=s3_config
        )
        
        # Test connection by listing buckets
        s3.list_buckets()
        logger.info("Successfully connected to Wasabi S3")
        return s3
        
    except ClientError as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during S3 initialization: {e}")
        return None

# Initialize S3 client
s3_client = initialize_s3()
if s3_client is None:
    logger.error("Cannot proceed without a valid S3 client.")
    exit(1)

# --- TELEGRAM BOT HANDLERS ---
@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    """Handles the /start command."""
    await message.reply_text(
        "👋 Welcome! I am a high-speed Telegram-Wasabi file uploader bot.\n\n"
        "**Commands:**\n"
        "• `/upload` - Reply to a file to upload it to Wasabi\n"
        "• `/link <filename>` - Get a download link for a file\n"
        "• `/help` - Show this help message\n\n"
        f"**Configuration:**\n"
        f"• **Bucket:** `{WASABI_BUCKET}`\n"
        f"• **Region:** `{WASABI_REGION}`"
    )

@app.on_message(filters.command("help") & filters.private)
async def help_handler(client: Client, message: Message):
    """Handles the /help command."""
    await start_handler(client, message)

@app.on_message(filters.command("upload") & filters.private & filters.reply)
async def upload_handler(client: Client, message: Message):
    """Handles the /upload command by replying to a file."""
    
    # Check if the replied message contains a document or media
    reply = message.reply_to_message
    file_info = reply.document or reply.video or reply.audio or reply.photo
    
    if not file_info:
        return await message.reply_text("❌ Please reply to a file, video, audio, or photo to upload it.")

    # Get file name and size
    if reply.document:
        file_name = getattr(file_info, 'file_name', f"document_{file_info.file_unique_id}")
    elif reply.video:
        file_name = getattr(file_info, 'file_name', f"video_{file_info.file_unique_id}.mp4")
    elif reply.audio:
        file_name = getattr(file_info, 'file_name', f"audio_{file_info.file_unique_id}.mp3")
    elif reply.photo:
        file_name = f"photo_{file_info.file_unique_id}.jpg"
    else:
        file_name = f"file_{file_info.file_unique_id}"

    file_size = file_info.file_size
    
    if file_size > 2 * 1024 * 1024 * 1024:  # 2GB limit
        return await message.reply_text("❌ File size exceeds 2GB limit.")

    status_message = await message.reply_text(f"🔄 Starting upload of `{file_name}`...")
    
    temp_path = None
    wasabi_key = f"uploads/{int(time.time())}_{file_name}"  # Add timestamp for uniqueness
    
    try:
        # Download the file from Telegram
        await status_message.edit_text(f"📥 Downloading `{file_name}` from Telegram...")
        start_download = time.time()
        
        temp_path = await client.download_media(
            reply, 
            file_name=file_name
        )
        
        if not temp_path or not os.path.exists(temp_path):
            raise Exception("Failed to download file from Telegram")

        download_time = time.time() - start_download
        file_size = os.path.getsize(temp_path)  # Get actual file size
        
        await status_message.edit_text(
            f"✅ Downloaded ({sizeof_fmt(file_size)}) in {download_time:.1f}s\n"
            f"🚀 Starting upload to Wasabi..."
        )

        # Upload to Wasabi with progress tracking
        start_upload = time.time()
        callback_func = create_progress_callback(client, status_message, file_name, file_size, start_upload)

        await loop.run_in_executor(
            executor,
            lambda: s3_client.upload_file(
                Filename=temp_path,
                Bucket=WASABI_BUCKET,
                Key=wasabi_key,
                Callback=callback_func,
                ExtraArgs={'ACL': 'private'}  # Make files private by default
            )
        )
        
        upload_time = time.time() - start_upload
        final_speed = file_size / upload_time if upload_time > 0 else file_size

        # Final success message
        await status_message.edit_text(
            f"✨ **Upload Complete!** ✨\n\n"
            f"**File:** `{file_name}`\n"
            f"**Size:** {sizeof_fmt(file_size)}\n"
            f"**Wasabi Path:** `{wasabi_key}`\n"
            f"**Upload Time:** {upload_time:.1f}s\n"
            f"**Avg Speed:** {sizeof_fmt(final_speed)}/s\n\n"
            f"Use `/link {wasabi_key}` to generate a download URL."
        )

    except asyncio.TimeoutError:
        await status_message.edit_text("❌ Operation timed out. Please try again.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        logger.error(f"S3 ClientError: {error_code} - {error_msg}")
        await status_message.edit_text(f"❌ Wasabi Error ({error_code}): {error_msg}")
    except Exception as e:
        logger.exception(f"Unexpected error during upload: {e}")
        await status_message.edit_text(f"❌ Upload failed: {str(e)}")
    finally:
        # Clean up local file
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                logger.info(f"Cleaned up temporary file: {temp_path}")
            except Exception as e:
                logger.error(f"Failed to clean up temp file: {e}")

@app.on_message(filters.command("link") & filters.private)
async def link_handler(client: Client, message: Message):
    """Handles the /link command to generate a presigned URL."""
    
    try:
        if len(message.command) < 2:
            return await message.reply_text(
                "Usage: `/link <wasabi_file_key>`\n\n"
                "Example: `/link uploads/1234567890_myfile.pdf`"
            )
        
        wasabi_key = " ".join(message.command[1:])
        status_message = await message.reply_text(f"🔗 Generating download link for `{wasabi_key}`...")

        # Check if the object exists
        try:
            await loop.run_in_executor(
                executor,
                lambda: s3_client.head_object(Bucket=WASABI_BUCKET, Key=wasabi_key)
            )
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return await status_message.edit_text(f"❌ File not found: `{wasabi_key}`")
            elif e.response['Error']['Code'] == '403':
                return await status_message.edit_text(f"❌ Access denied to file: `{wasabi_key}`")
            raise

        # Generate presigned URL (valid for 10 minutes)
        presigned_url = await loop.run_in_executor(
            executor,
            lambda: s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': WASABI_BUCKET, 'Key': wasabi_key},  # FIXED: WASABI_BUCKET (not WASASABI_BUCKET)
                ExpiresIn=600  # 10 minutes
            )
        )

        await status_message.edit_text(
            f"🔗 **Download Link Generated**\n\n"
            f"**File:** `{wasabi_key}`\n"
            f"**Link:** [Click to Download]({presigned_url})\n\n"
            f"*(Link expires in 10 minutes)*",
            disable_web_page_preview=True
        )

    except Exception as e:
        logger.exception(f"Error during link generation: {e}")
        await message.reply_text(f"❌ Failed to generate link: {str(e)}")

@app.on_message(filters.command("status") & filters.private)
async def status_handler(client: Client, message: Message):
    """Check bot and Wasabi status."""
    try:
        # Test S3 connection
        await loop.run_in_executor(
            executor,
            lambda: s3_client.list_buckets()
        )
        
        await message.reply_text(
            "✅ **Bot Status: Operational**\n\n"
            f"• **Wasabi Bucket:** `{WASABI_BUCKET}`\n"
            f"• **Wasabi Region:** `{WASABI_REGION}`\n"
            f"• **S3 Connection:** ✅ Connected\n"
            f"• **Bot Uptime:** Running smoothly"
        )
    except Exception as e:
        await message.reply_text(
            "❌ **Bot Status: Degraded**\n\n"
            f"• **S3 Connection:** ❌ Failed\n"
            f"• **Error:** {str(e)}"
        )

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    logger.info("Wasabi Telegram Bot starting...")
    
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal...")
    except Exception as e:
        logger.exception("Bot crashed with error:")
    finally:
        logger.info("Shutting down bot...")
        executor.shutdown(wait=True)
        logger.info("Bot shutdown complete.")
