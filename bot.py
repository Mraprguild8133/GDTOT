import os
import asyncio
import logging
from datetime import datetime
from typing import BinaryIO, Dict, Any
import uuid
import time

from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import ParseMode
from pyrogram.errors import RPCError
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from config import config

# Configure logging with more details
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more info
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Pyrogram client with more parameters
app = Client(
    "wasabi_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    sleep_threshold=60,
    workers=100
)

class AsyncWasabiStorage:
    """High-performance async Wasabi storage handler"""
    
    def __init__(self):
        try:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"https://{config.WASABI_ENDPOINT}",
                aws_access_key_id=config.WASABI_ACCESS_KEY,
                aws_secret_access_key=config.WASABI_SECRET_KEY,
                region_name=config.WASABI_REGION
            )
            
            # Test connection
            self.s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
            logger.info("✅ Wasabi connection successful!")
            
        except Exception as e:
            logger.error(f"❌ Wasabi connection failed: {e}")
            raise
        
        # High-speed transfer configuration
        self.transfer_config = TransferConfig(
            multipart_threshold=config.CHUNK_SIZE,
            max_concurrency=config.CONCURRENT_TRANSFERS,
            multipart_chunksize=config.CHUNK_SIZE,
            use_threads=True
        )
    
    async def upload_file(self, file_path: str, object_name: str) -> bool:
        """Upload file to Wasabi with high speed (async wrapper)"""
        try:
            logger.info(f"📤 Starting upload: {object_name}")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, 
                self._upload_file_sync, 
                file_path, 
                object_name
            )
            logger.info(f"✅ Upload successful: {object_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            return False
    
    def _upload_file_sync(self, file_path: str, object_name: str):
        """Synchronous file upload"""
        self.s3_client.upload_file(
            file_path,
            config.WASABI_BUCKET,
            object_name,
            Config=self.transfer_config
        )
    
    async def generate_presigned_url(self, object_name: str) -> str:
        """Generate presigned download URL (async wrapper)"""
        try:
            loop = asyncio.get_event_loop()
            url = await loop.run_in_executor(
                None,
                self._generate_presigned_url_sync,
                object_name
            )
            return url
        except Exception as e:
            logger.error(f"❌ URL generation failed: {e}")
            return ""
    
    def _generate_presigned_url_sync(self, object_name: str) -> str:
        """Synchronous URL generation"""
        url = self.s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': config.WASABI_BUCKET,
                'Key': object_name
            },
            ExpiresIn=config.DOWNLOAD_LINK_EXPIRE
        )
        return url
    
    async def list_files(self, prefix: str = "") -> list:
        """List files in Wasabi bucket (async wrapper)"""
        try:
            loop = asyncio.get_event_loop()
            files = await loop.run_in_executor(
                None,
                self._list_files_sync,
                prefix
            )
            return files
        except Exception as e:
            logger.error(f"❌ List files failed: {e}")
            return []
    
    def _list_files_sync(self, prefix: str = "") -> list:
        """Synchronous file listing"""
        response = self.s3_client.list_objects_v2(
            Bucket=config.WASABI_BUCKET,
            Prefix=prefix
        )
        return response.get('Contents', [])
    
    async def file_exists(self, object_name: str) -> bool:
        """Check if file exists in Wasabi"""
        try:
            loop = asyncio.get_event_loop()
            exists = await loop.run_in_executor(
                None,
                self._file_exists_sync,
                object_name
            )
            return exists
        except Exception as e:
            logger.error(f"❌ File check failed: {e}")
            return False
    
    def _file_exists_sync(self, object_name: str) -> bool:
        """Synchronous file existence check"""
        try:
            self.s3_client.head_object(
                Bucket=config.WASABI_BUCKET,
                Key=object_name
            )
            return True
        except ClientError:
            return False

    def _format_size(self, size_bytes: int) -> str:
        """Format file size for display"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"

# Initialize storage
try:
    storage = AsyncWasabiStorage()
    logger.info("✅ Wasabi storage initialized successfully!")
except Exception as e:
    logger.error(f"❌ Wasabi storage initialization failed: {e}")
    storage = None

class ProgressTracker:
    """Track upload/download progress"""
    
    def __init__(self, message: Message, operation: str):
        self.message = message
        self.operation = operation
        self.start_time = datetime.now()
        self.last_update = 0
    
    async def update_progress(self, current: int, total: int):
        """Update progress in real-time with rate limiting"""
        try:
            current_time = time.time()
            # Update max once per second to avoid flooding
            if current_time - self.last_update < 1.0 and current != total:
                return
                
            self.last_update = current_time
            percentage = (current / total) * 100 if total > 0 else 0
            
            progress_bar = self._create_progress_bar(percentage)
            elapsed = (datetime.now() - self.start_time).total_seconds()
            speed = current / elapsed if elapsed > 0 else 0
            
            text = (
                f"**{self.operation.upper()} IN PROGRESS** ⚡\n\n"
                f"▸ {progress_bar} {percentage:.1f}%\n"
                f"▸ 📦 {self._format_size(current)} / {self._format_size(total)}\n"
                f"▸ 🚀 {self._format_size(speed)}/s\n"
                f"▸ ⏰ {self._format_time_remaining(current, total, elapsed)}"
            )
            
            await self.message.edit_text(text)
        except Exception as e:
            logger.debug(f"Progress update skipped: {e}")
    
    def _create_progress_bar(self, percentage: float) -> str:
        """Create visual progress bar"""
        bars = "█" * int(percentage / 10)
        spaces = "░" * (10 - len(bars))
        return f"[{bars}{spaces}]"
    
    def _format_size(self, size_bytes: float) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"
    
    def _format_time_remaining(self, current: int, total: int, elapsed: float) -> str:
        """Calculate and format remaining time"""
        if current == 0 or elapsed == 0:
            return "Calculating..."
        
        remaining = (total - current) * elapsed / current
        
        if remaining > 3600:
            return f"{remaining/3600:.1f}h"
        elif remaining > 60:
            return f"{remaining/60:.1f}m"
        else:
            return f"{remaining:.0f}s"

# ===== BOT COMMAND HANDLERS =====

@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message: Message):
    """Start command handler"""
    logger.info(f"🎯 Start command from user: {message.from_user.id}")
    try:
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
            [InlineKeyboardButton("📤 Upload File", callback_data="upload_guide")],
            [InlineKeyboardButton("📥 Download File", callback_data="download_guide")],
            [InlineKeyboardButton("📋 File List", callback_data="list_files")]
        ])
        
        await message.reply_text(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"✅ Start response sent to user: {message.from_user.id}")
        
    except Exception as e:
        logger.error(f"❌ Start command error: {e}")
        await message.reply_text("❌ Bot error! Please try again.")

@app.on_message(filters.command("help"))
async def help_command(client, message: Message):
    """Help command handler"""
    try:
        help_text = """
🆘 **BOT HELP GUIDE**

**Upload Files:**
1. Send any file (document, video, audio, photo)
2. Bot will automatically process it
3. Wait for upload completion  
4. Get instant download link

**Download Files:**
1. Use `/list` to see available files
2. Use `/download filename` to get link
3. Links expire in 24 hours

**Commands:**
/start - Start bot
/upload - Upload file info
/download - Download file
/list - List files  
/help - This help
/stats - Bot statistics
"""
        await message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Help command error: {e}")

@app.on_message(filters.command("stats"))
async def stats_command(client, message: Message):
    """Bot statistics"""
    try:
        stats_text = """
📊 **BOT STATISTICS**

**Status:** ✅ Online
**Storage:** Wasabi Cloud
**Max File Size:** 4GB
**Uptime:** 24/7
**Speed:** High Performance

**Features:**
• Fast Upload/Download
• Secure Links
• All File Types
• Reliable Service
"""
        await message.reply_text(stats_text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Stats command error: {e}")

@app.on_message(filters.document | filters.video | filters.audio | filters.photo)
async def handle_files(client, message: Message):
    """Automatically handle any file sent to bot"""
    try:
        logger.info(f"📎 File received from user: {message.from_user.id}")
        
        # Get file info
        if message.document:
            file_size = message.document.file_size
            file_name = message.document.file_name or f"document_{message.id}.bin"
            mime_type = message.document.mime_type or "application/octet-stream"
        elif message.video:
            file_size = message.video.file_size
            file_name = f"video_{message.id}.mp4"
            mime_type = "video/mp4"
        elif message.audio:
            file_size = message.audio.file_size
            file_name = f"audio_{message.id}.mp3" 
            mime_type = "audio/mpeg"
        elif message.photo:
            file_size = message.photo.file_size
            file_name = f"photo_{message.id}.jpg"
            mime_type = "image/jpeg"
        else:
            return

        # Check file size
        if file_size > config.MAX_FILE_SIZE:
            await message.reply_text("❌ File size exceeds 4GB limit!")
            return

        status_msg = await message.reply_text("🔄 Starting upload process...")
        
        # Download file
        download_path = f"temp_{message.id}_{uuid.uuid4().hex}"
        progress = ProgressTracker(status_msg, "DOWNLOAD")
        
        await message.download(
            file_name=download_path,
            progress=progress.update_progress
        )
        
        # Upload to Wasabi
        object_name = f"{uuid.uuid4().hex}_{file_name}"
        await status_msg.edit_text("☁️ Uploading to Wasabi Cloud Storage...")
        
        if await storage.upload_file(download_path, object_name):
            download_url = await storage.generate_presigned_url(object_name)
            
            # Cleanup
            try:
                os.remove(download_path)
            except:
                pass
            
            if download_url:
                success_text = f"""
✅ **UPLOAD SUCCESSFUL** 🚀

**File:** `{file_name}`
**Type:** `{mime_type}`
**Size:** {progress._format_size(file_size)}
**Storage:** Wasabi Cloud

**Download Link:**
`{download_url}`

**Link expires in:** 24 hours
"""
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📥 Download Now", url=download_url)],
                    [InlineKeyboardButton("🔄 Upload More", callback_data="upload_guide")]
                ])
                
                await status_msg.edit_text(success_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
            else:
                await status_msg.edit_text("✅ File uploaded! Use /list to see your files.")
        else:
            await status_msg.edit_text("❌ Upload failed! Please try again.")
            
    except RPCError as e:
        logger.error(f"Telegram RPC error: {e}")
        await message.reply_text("❌ Telegram error! Please try again.")
    except Exception as e:
        logger.error(f"File handling error: {e}")
        await message.reply_text("❌ Processing error! Please try again.")

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    """List files in storage"""
    try:
        status_msg = await message.reply_text("📋 Fetching file list...")
        
        files = await storage.list_files()
        if not files:
            await status_msg.edit_text("📭 No files found in storage!")
            return
        
        file_list = "📁 **STORED FILES**\n\n"
        for i, file in enumerate(files[:10], 1):
            size = storage._format_size(file['Size'])
            file_list += f"`{i:2d}.` `{file['Key']}` - {size}\n"
        
        if len(files) > 10:
            file_list += f"\n... and {len(files) - 10} more files"
        
        file_list += f"\n**Total:** {len(files)} files"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Download Files", callback_data="download_guide")],
            [InlineKeyboardButton("📤 Upload Files", callback_data="upload_guide")]
        ])
        
        await status_msg.edit_text(file_list, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"List files error: {e}")
        await message.reply_text("❌ Failed to fetch file list!")

@app.on_message(filters.command("download"))
async def download_file(client, message: Message):
    """Download file by name"""
    try:
        if len(message.command) < 2:
            await message.reply_text("Usage: `/download filename`\n\nUse `/list` to see available files.", parse_mode=ParseMode.MARKDOWN)
            return
        
        filename = ' '.join(message.command[1:])
        status_msg = await message.reply_text(f"🔍 Searching for: `{filename}`")
        
        if await storage.file_exists(filename):
            download_url = await storage.generate_presigned_url(filename)
            
            if download_url:
                response_text = f"""
📥 **DOWNLOAD READY** ⚡

**File:** `{filename}`
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
        else:
            await status_msg.edit_text("❌ File not found! Use `/list` to see available files.", parse_mode=ParseMode.MARKDOWN)
            
    except Exception as e:
        logger.error(f"Download error: {e}")
        await message.reply_text("❌ Download failed!")

@app.on_callback_query()
async def handle_callbacks(client, callback_query):
    """Handle button callbacks"""
    try:
        await callback_query.answer()
        
        if callback_query.data == "upload_guide":
            await callback_query.message.edit_text(
                "📤 **Upload File**\n\nSimply send me any file (up to 4GB) and I'll automatically upload it to Wasabi storage.\n\nSupported: Documents, Videos, Audio, Photos",
                parse_mode=ParseMode.MARKDOWN
            )
        elif callback_query.data == "download_guide":
            await callback_query.message.edit_text(
                "📥 **Download File**\n\nUse `/download filename` to get a download link.\n\nFirst, use `/list` to see all available files.",
                parse_mode=ParseMode.MARKDOWN
            )
        elif callback_query.data == "list_files":
            await list_files(client, callback_query.message)
            
    except Exception as e:
        logger.error(f"Callback error: {e}")

@app.on_message(filters.command("test"))
async def test_command(client, message: Message):
    """Test command to check if bot is responsive"""
    try:
        await message.reply_text("✅ Bot is working! Test response received.")
        logger.info("✅ Test command executed successfully")
    except Exception as e:
        logger.error(f"Test command error: {e}")

# ===== BOT STARTUP =====

async def main():
    """Main function to start the bot"""
    try:
        # Validate configuration
        config.validate_config()
        logger.info("✅ Configuration validated successfully")
        
        # Start the bot
        logger.info("🚀 Starting Wasabi Storage Bot...")
        await app.start()
        
        # Get bot info
        me = await app.get_me()
        logger.info(f"✅ Bot started successfully: @{me.username}")
        logger.info(f"🤖 Bot ID: {me.id}")
        logger.info(f"📦 Max file size: {config.MAX_FILE_SIZE / (1024**3):.1f}GB")
        
        # Send startup message
        try:
            await app.send_message(
                chat_id="me",  # Saved messages
                text=f"🤖 **Bot Started Successfully**\n\n"
                     f"**Name:** {me.first_name}\n"
                     f"**Username:** @{me.username}\n"
                     f"**ID:** {me.id}\n"
                     f"**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                     f"✅ Ready to receive files!",
                parse_mode=ParseMode.MARKDOWN
            )
        except:
            pass
        
        # Keep bot running
        logger.info("🟢 Bot is now running and waiting for messages...")
        await idle()
        
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
    except Exception as e:
        logger.error(f"❌ Bot startup error: {e}")
    finally:
        logger.info("🛑 Stopping bot...")
        await app.stop()
        logger.info("✅ Bot stopped successfully")

if __name__ == "__main__":
    # Create event loop and run
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("🛑 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        loop.close()
