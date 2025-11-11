import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import BinaryIO, Dict, Any
import uuid
import threading

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

class AsyncWasabiStorage:
    """High-performance async Wasabi storage handler"""
    
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
        """Upload file to Wasabi with high speed (async wrapper)"""
        try:
            # Run synchronous S3 operation in thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, 
                self._upload_file_sync, 
                file_path, 
                object_name
            )
            return True
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False
    
    def _upload_file_sync(self, file_path: str, object_name: str):
        """Synchronous file upload"""
        self.s3_client.upload_file(
            file_path,
            config.WASABI_BUCKET,
            object_name,
            Config=self.transfer_config
        )
    
    async def download_file(self, object_name: str, file_path: str) -> bool:
        """Download file from Wasabi with high speed (async wrapper)"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self._download_file_sync,
                object_name,
                file_path
            )
            return True
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False
    
    def _download_file_sync(self, object_name: str, file_path: str):
        """Synchronous file download"""
        self.s3_client.download_file(
            config.WASABI_BUCKET,
            object_name,
            file_path,
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
            logger.error(f"URL generation failed: {e}")
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
            logger.error(f"List files failed: {e}")
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
            logger.error(f"File check failed: {e}")
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

# Initialize storage and bot
storage = AsyncWasabiStorage()
app = Client("wasabi_bot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN)

class ProgressTracker:
    """Track upload/download progress with enhanced visuals"""
    
    def __init__(self, message: Message, operation: str):
        self.message = message
        self.operation = operation
        self.start_time = datetime.now()
        self.last_update = datetime.now()
    
    async def update_progress(self, current: int, total: int):
        """Update progress in real-time with rate limiting"""
        try:
            # Rate limiting to avoid too many edits
            if (datetime.now() - self.last_update).total_seconds() < 1:
                return
                
            self.last_update = datetime.now()
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
        """Create visual progress bar with emojis"""
        bars = int(percentage / 10)
        progress_emojis = ["🟦"] * bars + ["⬜"] * (10 - bars)
        return "".join(progress_emojis)
    
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

@app.on_message(filters.command("upload") & (filters.document | filters.video | filters.audio | filters.photo))
async def upload_file(client, message: Message):
    """Handle file upload to Wasabi"""
    try:
        # Check if message contains a file
        if not any([message.document, message.video, message.audio, message.photo]):
            await message.reply_text("❌ Please send a file to upload!")
            return
        
        # Get file info
        if message.document:
            file_size = message.document.file_size
            file_name = message.document.file_name
        elif message.video:
            file_size = message.video.file_size
            file_name = f"video_{message.id}.mp4"
        elif message.audio:
            file_size = message.audio.file_size
            file_name = f"audio_{message.id}.mp3"
        elif message.photo:
            file_size = message.photo.file_size
            file_name = f"photo_{message.id}.jpg"
        else:
            await message.reply_text("❌ Unsupported file type!")
            return
        
        # Check file size
        if file_size > config.MAX_FILE_SIZE:
            await message.reply_text("❌ File size exceeds 4GB limit!")
            return
        
        status_msg = await message.reply_text("🔄 Starting upload...")
        
        # Download file with progress
        download_path = f"temp_{message.id}_{uuid.uuid4().hex}"
        progress = ProgressTracker(status_msg, "DOWNLOAD")
        
        await message.download(
            file_name=download_path,
            progress=progress.update_progress
        )
        
        # Generate unique object name
        object_name = f"{uuid.uuid4().hex}_{file_name}"
        
        # Upload to Wasabi
        await status_msg.edit_text("☁️ Uploading to Wasabi Storage...")
        
        upload_success = await storage.upload_file(download_path, object_name)
        
        if upload_success:
            # Generate download link
            download_url = await storage.generate_presigned_url(object_name)
            
            # Clean up temp file
            try:
                os.remove(download_path)
            except Exception as e:
                logger.warning(f"Temp file cleanup failed: {e}")
            
            if download_url:
                response_text = f"""
✅ **UPLOAD SUCCESSFUL** 🚀

**File:** `{file_name}`
**Size:** {progress._format_size(file_size)}
**Storage:** Wasabi Cloud
**Object Key:** `{object_name}`

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
                await status_msg.edit_text("✅ File uploaded but failed to generate download link. Use /list to see files.")
        else:
            await status_msg.edit_text("❌ Upload failed! Please try again.")
            # Clean up temp file on failure
            try:
                os.remove(download_path)
            except:
                pass
            
    except Exception as e:
        logger.error(f"Upload error: {e}")
        await message.reply_text("❌ Upload failed due to an error!")

@app.on_message(filters.command("download"))
async def download_file(client, message: Message):
    """Handle file download from Wasabi"""
    try:
        if len(message.command) < 2:
            await message.reply_text("Usage: `/download filename`\n\nUse /list to see available files.", parse_mode=ParseMode.MARKDOWN)
            return
        
        filename = ' '.join(message.command[1:])
        status_msg = await message.reply_text("🔍 Searching for file...")
        
        # Check if file exists
        file_exists = await storage.file_exists(filename)
        if not file_exists:
            await status_msg.edit_text("❌ File not found in storage! Use /list to see available files.")
            return
        
        # Get file details
        files = await storage.list_files(filename)
        file_size = files[0]['Size'] if files else 0
        
        # Generate download link
        download_url = await storage.generate_presigned_url(filename)
        
        if download_url:
            response_text = f"""
📥 **DOWNLOAD READY** ⚡

**File:** `{filename}`
**Size:** {storage._format_size(file_size)}
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
        status_msg = await message.reply_text("📋 Fetching file list from Wasabi...")
        
        files = await storage.list_files()
        if not files:
            await status_msg.edit_text("📭 No files found in storage!")
            return
        
        file_list = "📁 **STORED FILES**\n\n"
        for i, file in enumerate(files[:15], 1):  # Show first 15 files
            size = storage._format_size(file['Size'])
            file_list += f"`{i:2d}.` `{file['Key']}` - {size}\n"
        
        if len(files) > 15:
            file_list += f"\n... and {len(files) - 15} more files"
        
        file_list += f"\n**Total:** {len(files)} files"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Download Files", callback_data="download")],
            [InlineKeyboardButton("📤 Upload Files", callback_data="upload")]
        ])
        
        await status_msg.edit_text(file_list, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        logger.error(f"List files error: {e}")
        await message.reply_text("❌ Failed to fetch file list!")

@app.on_message(filters.command("help"))
async def help_command(client, message: Message):
    """Help command handler"""
    help_text = """
🆘 **BOT HELP GUIDE**

**Upload Files:**
1. Send any file (document, video, audio, photo)
2. Use `/upload` command or reply to file with /upload
3. Wait for upload completion
4. Get instant download link

**Download Files:**
1. Use `/list` to see available files
2. Use `/download filename` to get link
3. Links expire in 24 hours

**Features:**
- Max file size: 4GB
- All file types supported
- High-speed transfers
- 24/7 availability

**Commands:**
/start - Start bot
/upload - Upload file  
/download - Download file
/list - List files
/help - This help message
"""
    await message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

@app.on_callback_query()
async def handle_callbacks(client, callback_query):
    """Handle inline keyboard callbacks"""
    try:
        await callback_query.answer()
        
        if callback_query.data == "upload":
            await callback_query.message.edit_text(
                "📤 **Upload File**\n\nSimply send me any file (up to 4GB) and I'll automatically upload it to Wasabi storage.\n\nSupported: Documents, Videos, Audio, Photos",
                parse_mode=ParseMode.MARKDOWN
            )
        elif callback_query.data == "download":
            await callback_query.message.edit_text(
                "📥 **Download File**\n\nUse `/download filename` to get a download link.\n\nFirst, use `/list` to see all available files.",
                parse_mode=ParseMode.MARKDOWN
            )
        elif callback_query.data == "list_files":
            await list_files(client, callback_query.message)
            
    except Exception as e:
        logger.error(f"Callback error: {e}")

async def main():
    """Main function to start the bot"""
    try:
        config.validate_config()
        logger.info("🚀 Starting High-Speed Wasabi Storage Bot...")
        logger.info(f"📦 Max file size: {config.MAX_FILE_SIZE / (1024**3):.1f}GB")
        logger.info(f"⚡ Chunk size: {config.CHUNK_SIZE / (1024**2):.1f}MB")
        
        await app.start()
        logger.info("✅ Bot started successfully!")
        
        # Get bot info
        me = await app.get_me()
        logger.info(f"🤖 Bot @{me.username} is now running!")
        
        # Keep the bot running
        await asyncio.Future()  # Run forever
        
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
    except Exception as e:
        logger.error(f"❌ Bot startup error: {e}")
    finally:
        await app.stop()
        logger.info("🛑 Bot stopped")

if __name__ == "__main__":
    asyncio.run(main())
