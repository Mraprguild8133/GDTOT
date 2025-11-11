import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import BinaryIO, Dict, Any
import uuid

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import ParseMode
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from config import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WasabiStorage:
    """High-performance Wasabi storage handler"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=f"https://{config.WASABI_ENDPOINT}",
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            region_name=config.WASABI_REGION
        )
        
        # High-speed transfer configuration
        self.transfer_config = TransferConfig(
            multipart_threshold=config.CHUNK_SIZE,
            max_concurrency=config.CONCURRENT_TRANSFERS,
            multipart_chunksize=config.CHUNK_SIZE,
            use_threads=True
        )
    
    async def upload_file(self, file_path: str, object_name: str) -> bool:
        """Upload file to Wasabi with high speed"""
        try:
            self.s3_client.upload_file(
                file_path,
                config.WASABI_BUCKET,
                object_name,
                Config=self.transfer_config
            )
            return True
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False
    
    async def download_file(self, object_name: str, file_path: str) -> bool:
        """Download file from Wasabi with high speed"""
        try:
            self.s3_client.download_file(
                config.WASABI_BUCKET,
                object_name,
                file_path,
                Config=self.transfer_config
            )
            return True
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False
    
    def generate_presigned_url(self, object_name: str) -> str:
        """Generate presigned download URL"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': config.WASABI_BUCKET,
                    'Key': object_name
                },
                ExpiresIn=config.DOWNLOAD_LINK_EXPIRE
            )
            return url
        except ClientError as e:
            logger.error(f"URL generation failed: {e}")
            return ""

    def list_files(self, prefix: str = "") -> list:
        """List files in Wasabi bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=config.WASABI_BUCKET,
                Prefix=prefix
            )
            return response.get('Contents', [])
        except ClientError as e:
            logger.error(f"List files failed: {e}")
            return []

# Initialize storage and bot
storage = WasabiStorage()
app = Client("wasabi_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN)

class ProgressTracker:
    """Track upload/download progress"""
    
    def __init__(self, message: Message, operation: str):
        self.message = message
        self.operation = operation
        self.start_time = datetime.now()
    
    async def update_progress(self, current: int, total: int):
        """Update progress in real-time"""
        try:
            percentage = (current / total) * 100
            speed = current / (datetime.now() - self.start_time).total_seconds()
            
            progress_bar = self._create_progress_bar(percentage)
            speed_text = self._format_speed(speed)
            
            text = (
                f"**{self.operation.upper()} IN PROGRESS** ⚡\n\n"
                f"▸ {progress_bar} {percentage:.1f}%\n"
                f"▸ 📦 {self._format_size(current)} / {self._format_size(total)}\n"
                f"▸ 🚀 {speed_text}/s\n"
                f"▸ ⏰ {self._format_time_remaining(current, total)}"
            )
            
            await self.message.edit_text(text)
        except Exception as e:
            logger.error(f"Progress update failed: {e}")
    
    def _create_progress_bar(self, percentage: float) -> str:
        """Create visual progress bar"""
        bars = "█" * int(percentage / 10)
        spaces = "░" * (10 - len(bars))
        return f"[{bars}{spaces}]"
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"
    
    def _format_speed(self, speed_bytes: float) -> str:
        """Format transfer speed"""
        return self._format_size(speed_bytes)
    
    def _format_time_remaining(self, current: int, total: int) -> str:
        """Calculate and format remaining time"""
        if current == 0:
            return "Calculating..."
        
        elapsed = (datetime.now() - self.start_time).total_seconds()
        remaining = (total - current) * elapsed / current
        
        if remaining > 3600:
            return f"{remaining/3600:.1f}h"
        elif remaining > 60:
            return f"{remaining/60:.1f}m"
        else:
            return f"{remaining:.0f}s"

# Bot Command Handlers
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Start command handler"""
    welcome_text = """
    🚀 **WASABI STORAGE BOT** ⚡

    **Features:**
    ✅ 4GB File Support
    ✅ High-Speed Upload/Download
    ✅ 24/7 Service
    ✅ Instant Download Links
    ✅ Wasabi Cloud Storage

    **Commands:**
    /upload - Upload file to Wasabi
    /download - Download file from Wasabi
    /list - List stored files
    /help - Show this help

    **Supported:** All file types
    **Max Size:** 4GB per file
    """
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Upload File", callback_data="upload")],
        [InlineKeyboardButton("📥 Download File", callback_data="download")],
        [InlineKeyboardButton("📋 File List", callback_data="list_files")]
    ])
    
    await message.reply_text(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command("upload") & filters.document | filters.video | filters.audio | filters.photo)
async def upload_file(client, message: Message):
    """Handle file upload to Wasabi"""
    try:
        # Check file size
        if message.document:
            file_size = message.document.file_size
        elif message.video:
            file_size = message.video.file_size
        elif message.audio:
            file_size = message.audio.file_size
        else:
            file_size = 0
        
        if file_size > config.MAX_FILE_SIZE:
            await message.reply_text("❌ File size exceeds 4GB limit!")
            return
        
        status_msg = await message.reply_text("🔄 Starting upload...")
        
        # Download file
        download_path = f"temp_{message.id}_{uuid.uuid4().hex}"
        progress = ProgressTracker(status_msg, "DOWNLOAD")
        
        await message.download(
            file_name=download_path,
            progress=progress.update_progress
        )
        
        # Generate unique object name
        original_name = message.document.file_name if message.document else "file"
        object_name = f"{uuid.uuid4().hex}_{original_name}"
        
        # Upload to Wasabi
        await status_msg.edit_text("☁️ Uploading to Wasabi...")
        progress.operation = "UPLOAD"
        
        success = await storage.upload_file(download_path, object_name)
        
        if success:
            # Generate download link
            download_url = storage.generate_presigned_url(object_name)
            
            # Clean up temp file
            try:
                os.remove(download_path)
            except:
                pass
            
            response_text = f"""
✅ **UPLOAD SUCCESSFUL** 🚀

**File:** `{original_name}`
**Size:** {progress._format_size(file_size)}
**Storage:** Wasabi Cloud

**Download Link:**
`{download_url}`

**Link expires in:** 24 hours
"""
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Download Link", url=download_url)],
                [InlineKeyboardButton("🔄 Upload Another", callback_data="upload")]
            ])
            
            await status_msg.edit_text(response_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        else:
            await status_msg.edit_text("❌ Upload failed! Please try again.")
            
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await message.reply_text("❌ Upload failed due to an error!")

@app.on_message(filters.command("download"))
async def download_file(client, message: Message):
    """Handle file download from Wasabi"""
    try:
        if len(message.command) < 2:
            await message.reply_text("Usage: /download <filename>")
            return
        
        filename = message.command[1]
        status_msg = await message.reply_text("🔍 Searching for file...")
        
        # Check if file exists
        files = storage.list_files(filename)
        if not files:
            await status_msg.edit_text("❌ File not found!")
            return
        
        # Generate download link
        download_url = storage.generate_presigned_url(filename)
        
        if download_url:
            response_text = f"""
📥 **DOWNLOAD READY** ⚡

**File:** `{filename}`
**Size:** {storage._format_size(files[0]['Size'])}
**Expires:** 24 hours

**Download Link:**
`{download_url}`
"""
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📥 Download Now", url=download_url)],
                [InlineKeyboardButton("📋 File List", callback_data="list_files")]
            ])
            
            await status_msg.edit_text(response_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        else:
            await status_msg.edit_text("❌ Failed to generate download link!")
            
    except Exception as e:
        logger.error(f"Download error: {e}")
        await message.reply_text("❌ Download failed due to an error!")

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    """List all files in Wasabi bucket"""
    try:
        status_msg = await message.reply_text("📋 Fetching file list...")
        
        files = storage.list_files()
        if not files:
            await status_msg.edit_text("📭 No files found in storage!")
            return
        
        file_list = "📁 **STORED FILES**\n\n"
        for file in files[:10]:  # Show first 10 files
            size = storage._format_size(file['Size'])
            file_list += f"▸ `{file['Key']}` - {size}\n"
        
        if len(files) > 10:
            file_list += f"\n... and {len(files) - 10} more files"
        
        await status_msg.edit_text(file_list, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"List files error: {e}")
        await message.reply_text("❌ Failed to fetch file list!")

@app.on_callback_query()
async def handle_callbacks(client, callback_query):
    """Handle inline keyboard callbacks"""
    try:
        await callback_query.answer()
        
        if callback_query.data == "upload":
            await callback_query.message.edit_text(
                "📤 **Upload File**\n\nSend me any file (up to 4GB) and I'll upload it to Wasabi storage.",
                parse_mode=ParseMode.MARKDOWN
            )
        elif callback_query.data == "download":
            await callback_query.message.edit_text(
                "📥 **Download File**\n\nUse `/download filename` to get a download link.",
                parse_mode=ParseMode.MARKDOWN
            )
        elif callback_query.data == "list_files":
            await list_files(client, callback_query.message)
            
    except Exception as e:
        logger.error(f"Callback error: {e}")

# Add the missing method to WasabiStorage class
def _format_size(self, size_bytes: int) -> str:
    """Format file size for WasabiStorage class"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

# Add method to WasabiStorage class
WasabiStorage._format_size = _format_size

async def main():
    """Main function to start the bot"""
    try:
        config.validate_config()
        logger.info("🚀 Starting Wasabi Storage Bot...")
        await app.start()
        logger.info("✅ Bot started successfully!")
        await asyncio.Future()  # Run forever
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
    except Exception as e:
        logger.error(f"Bot startup error: {e}")
    finally:
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
