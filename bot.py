import os
import asyncio
import logging
from datetime import datetime
from typing import BinaryIO, Dict, Any
import uuid
import time
import traceback

from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import ParseMode, MessageMediaType
from pyrogram.errors import RPCError, FloodWait
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

from config import config

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Pyrogram client
app = Client(
    "wasabi_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
    sleep_threshold=60,
    workers=50
)

class AsyncWasabiStorage:
    """High-performance async Wasabi storage handler with better error handling"""
    
    def __init__(self):
        try:
            logger.info("🔧 Initializing Wasabi connection...")
            self.s3_client = boto3.client(
                's3',
                endpoint_url=f"https://{config.WASABI_ENDPOINT}",
                aws_access_key_id=config.WASABI_ACCESS_KEY,
                aws_secret_access_key=config.WASABI_SECRET_KEY,
                region_name=config.WASABI_REGION
            )
            
            # Test connection by listing buckets or checking bucket existence
            logger.info("🔧 Testing Wasabi connection...")
            self.s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
            logger.info("✅ Wasabi connection successful!")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"❌ Bucket not found: {config.WASABI_BUCKET}")
            elif error_code == '403':
                logger.error("❌ Access denied - check your Wasabi credentials")
            else:
                logger.error(f"❌ Wasabi connection failed: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Wasabi initialization failed: {e}")
            raise
        
        # High-speed transfer configuration
        self.transfer_config = TransferConfig(
            multipart_threshold=config.CHUNK_SIZE,
            max_concurrency=config.CONCURRENT_TRANSFERS,
            multipart_chunksize=config.CHUNK_SIZE,
            use_threads=True
        )
    
    async def upload_file(self, file_path: str, object_name: str) -> bool:
        """Upload file to Wasabi with detailed error handling"""
        try:
            logger.info(f"📤 Starting upload: {object_name} from {file_path}")
            
            # Check if file exists locally
            if not os.path.exists(file_path):
                logger.error(f"❌ Local file not found: {file_path}")
                return False
            
            file_size = os.path.getsize(file_path)
            logger.info(f"📦 File size: {self._format_size(file_size)}")
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, 
                self._upload_file_sync, 
                file_path, 
                object_name
            )
            logger.info(f"✅ Upload successful: {object_name}")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Wasabi upload error: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def _upload_file_sync(self, file_path: str, object_name: str):
        """Synchronous file upload"""
        try:
            self.s3_client.upload_file(
                file_path,
                config.WASABI_BUCKET,
                object_name,
                Config=self.transfer_config,
                Callback=self._upload_progress_callback(object_name)
            )
        except Exception as e:
            logger.error(f"❌ Sync upload failed: {e}")
            raise
    
    def _upload_progress_callback(self, object_name: str):
        """Progress callback for upload"""
        def callback(bytes_transferred):
            logger.debug(f"📤 Upload progress {object_name}: {self._format_size(bytes_transferred)}")
        return callback
    
    async def generate_presigned_url(self, object_name: str) -> str:
        """Generate presigned download URL"""
        try:
            logger.info(f"🔗 Generating URL for: {object_name}")
            loop = asyncio.get_event_loop()
            url = await loop.run_in_executor(
                None,
                self._generate_presigned_url_sync,
                object_name
            )
            logger.info(f"✅ URL generated successfully")
            return url
        except Exception as e:
            logger.error(f"❌ URL generation failed: {e}")
            return ""
    
    def _generate_presigned_url_sync(self, object_name: str) -> str:
        """Synchronous URL generation"""
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
            logger.error(f"❌ Presigned URL error: {e}")
            return ""
    
    async def list_files(self, prefix: str = "") -> list:
        """List files in Wasabi bucket"""
        try:
            loop = asyncio.get_event_loop()
            files = await loop.run_in_executor(
                None,
                self._list_files_sync,
                prefix
            )
            logger.info(f"📁 Found {len(files)} files")
            return files
        except Exception as e:
            logger.error(f"❌ List files failed: {e}")
            return []
    
    def _list_files_sync(self, prefix: str = "") -> list:
        """Synchronous file listing"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=config.WASABI_BUCKET,
                Prefix=prefix
            )
            return response.get('Contents', [])
        except ClientError as e:
            logger.error(f"❌ List objects error: {e}")
            return []
    
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
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def _format_size(self, size_bytes: int) -> str:
        """Format file size for display"""
        if size_bytes == 0:
            return "0 B"
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
    """Track upload/download progress with better error handling"""
    
    def __init__(self, message: Message, operation: str):
        self.message = message
        self.operation = operation
        self.start_time = datetime.now()
        self.last_update = 0
        self.last_percentage = 0
    
    async def update_progress(self, current: int, total: int):
        """Update progress in real-time with rate limiting"""
        try:
            current_time = time.time()
            
            # Calculate percentage
            percentage = (current / total) * 100 if total > 0 else 0
            
            # Only update if percentage changed significantly or it's complete
            if (current_time - self.last_update < 2.0 and 
                abs(percentage - self.last_percentage) < 5 and 
                current != total):
                return
                
            self.last_update = current_time
            self.last_percentage = percentage
            
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
            
        except FloodWait as e:
            logger.warning(f"⏳ Flood wait: {e.value}s")
            await asyncio.sleep(e.value)
        except RPCError as e:
            logger.error(f"❌ Telegram error in progress: {e}")
        except Exception as e:
            logger.debug(f"Progress update skipped: {e}")
    
    def _create_progress_bar(self, percentage: float) -> str:
        """Create visual progress bar"""
        bars = int(percentage / 10)
        spaces = 10 - bars
        return f"[{'█' * bars}{'░' * spaces}]"
    
    def _format_size(self, size_bytes: float) -> str:
        """Format file size"""
        if size_bytes == 0:
            return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"
    
    def _format_time_remaining(self, current: int, total: int, elapsed: float) -> str:
        """Calculate and format remaining time"""
        if current == 0 or elapsed == 0:
            return "Calculating..."
        
        try:
            remaining = (total - current) * elapsed / current
            
            if remaining > 3600:
                return f"{remaining/3600:.1f}h"
            elif remaining > 60:
                return f"{remaining/60:.1f}m"
            else:
                return f"{remaining:.0f}s"
        except:
            return "Calculating..."

# ===== BOT COMMAND HANDLERS =====

@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message: Message):
    """Start command handler"""
    try:
        logger.info(f"🎯 Start command from user: {message.from_user.id}")
        
        welcome_text = """
🚀 **WASABI STORAGE BOT** ⚡

**Features:**
✅ 4GB File Support
✅ High-Speed Upload/Download  
✅ 24/7 Service
✅ Instant Download Links
✅ Wasabi Cloud Storage

**How to use:**
1. Send any file (document, video, audio, photo)
2. Bot will automatically upload to Wasabi
3. Get instant download link

**Commands:**
/upload - Upload instructions
/download - Download files  
/list - List stored files
/help - Help guide
/test - Test bot response
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Upload File", callback_data="upload_guide")],
            [InlineKeyboardButton("📥 Download File", callback_data="download_guide")],
            [InlineKeyboardButton("🔄 Test Bot", callback_data="test_bot")]
        ])
        
        await message.reply_text(welcome_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"✅ Start response sent to user: {message.from_user.id}")
        
    except Exception as e:
        logger.error(f"❌ Start command error: {e}")
        await message.reply_text("❌ Error in start command!")

@app.on_message(filters.command("test"))
async def test_command(client, message: Message):
    """Test bot responsiveness"""
    try:
        logger.info(f"🧪 Test command from user: {message.from_user.id}")
        
        # Test storage connection
        storage_status = "✅ Connected" if storage else "❌ Disconnected"
        
        test_text = f"""
🧪 **BOT TEST RESULTS**

**Bot Status:** ✅ Online
**Storage:** {storage_status}
**Max File Size:** 4GB
**Response Time:** Instant

**Tests:**
✓ Command processing
✓ Message sending
✓ Storage connection

**Next:** Try sending a file!
"""
        await message.reply_text(test_text, parse_mode=ParseMode.MARKDOWN)
        logger.info("✅ Test command completed")
        
    except Exception as e:
        logger.error(f"❌ Test command error: {e}")
        await message.reply_text("❌ Test failed!")

@app.on_message(filters.media & filters.private)
async def handle_media_files(client, message: Message):
    """Handle all media files (documents, videos, audio, photos)"""
    try:
        user_id = message.from_user.id
        logger.info(f"📎 Media received from user {user_id}")
        
        # Get file information based on media type
        if message.document:
            file_size = message.document.file_size
            file_name = message.document.file_name or f"document_{message.id}.bin"
            mime_type = message.document.mime_type or "application/octet-stream"
            media_type = "document"
        elif message.video:
            file_size = message.video.file_size
            file_name = f"video_{message.id}.mp4"
            mime_type = "video/mp4"
            media_type = "video"
        elif message.audio:
            file_size = message.audio.file_size
            file_name = f"audio_{message.id}.mp3"
            mime_type = "audio/mpeg"
            media_type = "audio"
        elif message.photo:
            file_size = message.photo.file_size
            file_name = f"photo_{message.id}.jpg"
            mime_type = "image/jpeg"
            media_type = "photo"
        else:
            await message.reply_text("❌ Unsupported file type!")
            return

        logger.info(f"📦 File info: {file_name} ({self._format_size(file_size)}) - {media_type}")

        # Check file size
        if file_size > config.MAX_FILE_SIZE:
            await message.reply_text("❌ File size exceeds 4GB limit!")
            return

        # Check storage connection
        if not storage:
            await message.reply_text("❌ Storage service unavailable. Please try later.")
            return

        status_msg = await message.reply_text("🔄 Starting download from Telegram...")
        
        # Download file from Telegram
        download_path = f"temp_{message.id}_{uuid.uuid4().hex}"
        progress = ProgressTracker(status_msg, "DOWNLOAD")
        
        try:
            await message.download(
                file_name=download_path,
                progress=progress.update_progress
            )
            
            # Verify download
            if not os.path.exists(download_path):
                await status_msg.edit_text("❌ Download failed - file not found!")
                return
                
            downloaded_size = os.path.getsize(download_path)
            logger.info(f"✅ Download completed: {self._format_size(downloaded_size)}")
            
        except Exception as e:
            logger.error(f"❌ Download error: {e}")
            await status_msg.edit_text("❌ Download failed! Please try again.")
            return

        # Upload to Wasabi
        await status_msg.edit_text("☁️ Uploading to Wasabi Cloud Storage...")
        object_name = f"user_{user_id}/{uuid.uuid4().hex}_{file_name}"
        
        try:
            upload_success = await storage.upload_file(download_path, object_name)
            
            if upload_success:
                # Generate download link
                download_url = await storage.generate_presigned_url(object_name)
                
                # Cleanup temp file
                try:
                    os.remove(download_path)
                    logger.info("✅ Temp file cleaned up")
                except Exception as e:
                    logger.warning(f"⚠️ Temp cleanup failed: {e}")
                
                if download_url:
                    success_text = f"""
✅ **UPLOAD SUCCESSFUL** 🚀

**File:** `{file_name}`
**Type:** {media_type.title()}
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
                    logger.info(f"✅ Upload completed for user {user_id}")
                else:
                    await status_msg.edit_text("✅ File uploaded! Use /list to see your files.")
            else:
                await status_msg.edit_text("❌ Upload failed! Storage error.")
                # Cleanup on failure
                try:
                    os.remove(download_path)
                except:
                    pass
                
        except Exception as e:
            logger.error(f"❌ Upload process error: {e}")
            await status_msg.edit_text("❌ Upload process failed!")
            # Cleanup on error
            try:
                os.remove(download_path)
            except:
                pass
            
    except RPCError as e:
        logger.error(f"❌ Telegram RPC error: {e}")
        await message.reply_text("❌ Telegram error! Please try again.")
    except Exception as e:
        logger.error(f"❌ Media handling error: {e}")
        logger.error(traceback.format_exc())
        await message.reply_text("❌ Processing error! Please try again.")

# Add format_size method to global scope for use in handlers
def _format_size(size_bytes: int) -> str:
    """Format file size for display"""
    if size_bytes == 0:
        return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

# Add to global scope for use in handlers
globals()['_format_size'] = _format_size

@app.on_message(filters.command("list"))
async def list_files(client, message: Message):
    """List files in storage"""
    try:
        status_msg = await message.reply_text("📋 Fetching file list from Wasabi...")
        
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
        logger.error(f"❌ List files error: {e}")
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
        logger.error(f"❌ Download error: {e}")
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
        elif callback_query.data == "test_bot":
            await test_command(client, callback_query.message)
        elif callback_query.data == "list_files":
            await list_files(client, callback_query.message)
            
    except Exception as e:
        logger.error(f"❌ Callback error: {e}")

# ===== BOT STARTUP =====

async def main():
    """Main function to start the bot"""
    try:
        # Validate configuration
        logger.info("🔧 Validating configuration...")
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
                chat_id="me",
                text=f"🤖 **Bot Started**\n\n"
                     f"**Name:** {me.first_name}\n"
                     f"**Username:** @{me.username}\n"
                     f"**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                     f"✅ Ready for files!",
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.warning(f"⚠️ Could not send startup message: {e}")
        
        # Keep bot running
        logger.info("🟢 Bot is now running and waiting for messages...")
        await idle()
        
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
    except Exception as e:
        logger.error(f"❌ Bot startup error: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("🛑 Stopping bot...")
        await app.stop()
        logger.info("✅ Bot stopped successfully")

if __name__ == "__main__":
    # Create event loop and run
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("🛑 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        logger.error(traceback.format_exc())
    finally:
        loop.close()
