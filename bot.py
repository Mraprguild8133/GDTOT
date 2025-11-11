import os
import asyncio
import aiofiles
from typing import BinaryIO
from datetime import datetime, timedelta
import time

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FilePartMissing, RPCError
from pyrogram.enums import MessageMediaType

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from config import config

class WasabiTelegramBot:
    def __init__(self):
        self.app = Client(
            "wasabi_bot",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=config.BOT_TOKEN
        )
        
        # Initialize Wasabi S3 client with optimized configuration
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=config.WASABI_ENDPOINT,
            region_name=config.WASABI_REGION,
            config=BotoConfig(
                retries={'max_attempts': 3, 'mode': 'standard'},
                max_pool_connections=50,
                s3={'addressing_style': 'virtual'}
            )
        )
        
        # Semaphores for limiting concurrent operations
        self.upload_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_UPLOADS)
        self.download_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_DOWNLOADS)
        
        self.setup_handlers()
    
    def setup_handlers(self):
        """Setup message handlers"""
        
        @self.app.on_message(filters.command("start"))
        async def start_command(client, message: Message):
            await message.reply_text(
                "🤖 **Wasabi Storage Bot**\n\n"
                "I can help you upload files to Wasabi storage and generate download links.\n\n"
                "**Commands:**\n"
                "• Just send me any file (up to 4GB)\n"
                "• /download <filename> - Download file from Wasabi\n"
                "• /list - List your files\n"
                "• /help - Show this help message",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📁 Upload File", callback_data="upload_help")],
                    [InlineKeyboardButton("📥 Download File", callback_data="download_help")]
                ])
            )
        
        @self.app.on_message(filters.command("help"))
        async def help_command(client, message: Message):
            await start_command(client, message)
        
        @self.app.on_message(filters.command("download"))
        async def download_command(client, message: Message):
            if len(message.command) < 2:
                await message.reply_text("❌ Please provide a filename. Usage: `/download filename.txt`")
                return
            
            filename = message.command[1]
            await self.handle_download(message, filename)
        
        @self.app.on_message(filters.command("list"))
        async def list_command(client, message: Message):
            await self.list_files(message)
        
        # Handle document, video, audio, photo files
        @self.app.on_message(
            filters.document | 
            filters.video | 
            filters.audio | 
            filters.photo
        )
        async def handle_file_upload(client, message: Message):
            await self.handle_upload(message)
    
    async def handle_upload(self, message: Message):
        """Handle file upload to Wasabi"""
        try:
            # Get file information
            if message.media:
                file_size = await self.get_file_size(message)
                if file_size > config.MAX_FILE_SIZE:
                    await message.reply_text(f"❌ File size exceeds 4GB limit. Your file: {file_size/1024/1024/1024:.2f}GB")
                    return
                
                # Send initial message
                status_msg = await message.reply_text("📤 **Starting upload...**\n⏳ Preparing file...")
                
                # Generate unique filename
                file_id = message.id
                user_id = message.from_user.id
                timestamp = int(time.time())
                file_extension = await self.get_file_extension(message)
                filename = f"telegram_{user_id}_{timestamp}_{file_id}{file_extension}"
                
                # Download and upload file
                download_path = await self.download_telegram_file(message, status_msg)
                if download_path:
                    await self.upload_to_wasabi(download_path, filename, status_msg, message)
                    
                    # Cleanup
                    try:
                        os.remove(download_path)
                    except:
                        pass
                else:
                    await status_msg.edit_text("❌ Failed to download file from Telegram")
            
            else:
                await message.reply_text("❌ Please send a file to upload")
                
        except Exception as e:
            await message.reply_text(f"❌ Upload failed: {str(e)}")
    
    async def handle_download(self, message: Message, filename: str):
        """Handle file download from Wasabi"""
        try:
            status_msg = await message.reply_text("📥 **Checking file...**")
            
            async with self.download_semaphore:
                # Generate pre-signed URL
                download_url = self.generate_presigned_url(filename)
                
                if download_url:
                    await status_msg.edit_text(
                        f"**📥 Download Ready**\n"
                        f"**File:** `{filename}`\n"
                        f"**Link expires in:** 1 hour\n\n"
                        f"[Click to Download]({download_url})",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📥 Download Now", url=download_url)]
                        ])
                    )
                else:
                    await status_msg.edit_text("❌ File not found or access denied")
                    
        except Exception as e:
            await message.reply_text(f"❌ Download failed: {str(e)}")
    
    async def upload_to_wasabi(self, file_path: str, filename: str, status_msg: Message, original_msg: Message):
        """Upload file to Wasabi with progress tracking"""
        try:
            async with self.upload_semaphore:
                file_size = os.path.getsize(file_path)
                
                # Upload with progress
                await status_msg.edit_text("📤 **Uploading to Wasabi...**\n🔄 Starting...")
                
                def progress_callback(bytes_uploaded):
                    nonlocal file_size
                    percent = (bytes_uploaded / file_size) * 100
                    asyncio.create_task(
                        status_msg.edit_text(
                            f"📤 **Uploading to Wasabi...**\n"
                            f"📊 Progress: **{percent:.1f}%**\n"
                            f"📦 {bytes_uploaded//1024//1024}MB / {file_size//1024//1024}MB"
                        )
                    )
                
                # Use multipart upload for large files
                if file_size > config.CHUNK_SIZE:
                    await self.multipart_upload(file_path, filename, progress_callback)
                else:
                    with open(file_path, 'rb') as file:
                        self.s3_client.upload_fileobj(
                            file,
                            config.WASABI_BUCKET,
                            filename,
                            Callback=progress_callback
                        )
                
                # Generate share link
                share_url = self.generate_presigned_url(filename)
                
                await status_msg.edit_text(
                    f"✅ **Upload Complete!**\n\n"
                    f"**File:** `{filename}`\n"
                    f"**Size:** {file_size//1024//1024} MB\n"
                    f"**Storage:** Wasabi\n\n"
                    f"**Download Link:** (Expires in 1 hour)\n"
                    f"`{share_url}`",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📥 Download Link", url=share_url)],
                        [InlineKeyboardButton("🔄 Upload Another", callback_data="upload_another")]
                    ])
                )
                
        except Exception as e:
            await status_msg.edit_text(f"❌ Upload failed: {str(e)}")
            raise
    
    async def multipart_upload(self, file_path: str, filename: str, progress_callback):
        """Handle multipart upload for large files"""
        try:
            # Create multipart upload
            mpu = self.s3_client.create_multipart_upload(
                Bucket=config.WASABI_BUCKET,
                Key=filename
            )
            upload_id = mpu['UploadId']
            
            parts = []
            part_number = 1
            uploaded_bytes = 0
            file_size = os.path.getsize(file_path)
            
            with open(file_path, 'rb') as file:
                while True:
                    chunk = file.read(config.CHUNK_SIZE)
                    if not chunk:
                        break
                    
                    # Upload part
                    part = self.s3_client.upload_part(
                        Bucket=config.WASABI_BUCKET,
                        Key=filename,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    
                    uploaded_bytes += len(chunk)
                    progress_callback(uploaded_bytes)
                    part_number += 1
            
            # Complete multipart upload
            self.s3_client.complete_multipart_upload(
                Bucket=config.WASABI_BUCKET,
                Key=filename,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
        except Exception as e:
            # Abort upload on failure
            try:
                self.s3_client.abort_multipart_upload(
                    Bucket=config.WASABI_BUCKET,
                    Key=filename,
                    UploadId=upload_id
                )
            except:
                pass
            raise e
    
    def generate_presigned_url(self, filename: str) -> str:
        """Generate pre-signed URL for download"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': config.WASABI_BUCKET,
                    'Key': filename
                },
                ExpiresIn=config.DOWNLOAD_LINK_EXPIRY
            )
            return url
        except ClientError:
            return None
    
    async def list_files(self, message: Message):
        """List user's files in Wasabi"""
        try:
            user_prefix = f"telegram_{message.from_user.id}_"
            
            response = self.s3_client.list_objects_v2(
                Bucket=config.WASABI_BUCKET,
                Prefix=user_prefix
            )
            
            if 'Contents' in response:
                files = response['Contents']
                file_list = []
                
                for file in files[:10]:  # Show first 10 files
                    size_mb = file['Size'] / 1024 / 1024
                    file_list.append(f"• `{file['Key']}` ({size_mb:.1f} MB)")
                
                await message.reply_text(
                    f"📁 **Your Files**\n\n" + "\n".join(file_list) +
                    f"\n\nTotal: {len(files)} files"
                )
            else:
                await message.reply_text("📁 No files found in your storage")
                
        except Exception as e:
            await message.reply_text(f"❌ Failed to list files: {str(e)}")
    
    async def get_file_size(self, message: Message) -> int:
        """Get file size from message"""
        if message.document:
            return message.document.file_size
        elif message.video:
            return message.video.file_size
        elif message.audio:
            return message.audio.file_size
        elif message.photo:
            return message.photo.file_size
        return 0
    
    async def get_file_extension(self, message: Message) -> str:
        """Get file extension"""
        if message.document:
            return os.path.splitext(message.document.file_name or "file.bin")[1]
        elif message.video:
            return ".mp4"
        elif message.audio:
            return ".mp3"
        elif message.photo:
            return ".jpg"
        return ".bin"
    
    async def download_telegram_file(self, message: Message, status_msg: Message) -> str:
        """Download file from Telegram with progress"""
        try:
            download_path = f"temp_{message.id}"
            
            if message.document:
                file_id = message.document.file_id
            elif message.video:
                file_id = message.video.file_id
            elif message.audio:
                file_id = message.audio.file_id
            elif message.photo:
                file_id = message.photo.file_id
            else:
                return None
            
            # Download with progress
            file_size = await self.get_file_size(message)
            downloaded = 0
            
            async for chunk in self.app.stream_media(file_id):
                async with aiofiles.open(download_path, 'ab') as f:
                    await f.write(chunk)
                
                downloaded += len(chunk)
                percent = (downloaded / file_size) * 100
                
                if int(percent) % 10 == 0:  # Update every 10% to avoid spam
                    await status_msg.edit_text(
                        f"📥 **Downloading from Telegram...**\n"
                        f"📊 Progress: **{percent:.1f}%**\n"
                        f"📦 {downloaded//1024//1024}MB / {file_size//1024//1024}MB"
                    )
            
            return download_path
            
        except Exception as e:
            await status_msg.edit_text(f"❌ Download failed: {str(e)}")
            return None
    
    async def run(self):
        """Start the bot"""
        print("🤖 Starting Wasabi Telegram Bot...")
        print("⚡ Optimized for high-speed transfers")
        print("💾 4GB file support enabled")
        print("🌐 24/7 operation ready")
        
        await self.app.start()
        print("✅ Bot started successfully!")
        
        # Get bot info
        me = await self.app.get_me()
        print(f"🤖 Bot: @{me.username}")
        print(f"🆔 ID: {me.id}")
        
        # Keep running
        await asyncio.Future()

# Run the bot
if __name__ == "__main__":
    bot = WasabiTelegramBot()
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        print("🛑 Bot stopped by user")
    except Exception as e:
        print(f"❌ Bot crashed: {e}")
