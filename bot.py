import os
import asyncio
import aiofiles
import time
from typing import Optional
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode

import boto3
import aioboto3
from botocore.config import Config as BotoConfig
import speedtest
import asyncio_throttle

from config import config

# Initialize Telegram Client
app = Client(
    "wasabi_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)

# Configure Wasabi S3 client for optimal performance
boto_config = BotoConfig(
    region_name=config.WASABI_REGION,
    retries={'max_attempts': 3, 'mode': 'standard'},
    max_pool_connections=50,
    connect_timeout=10,
    read_timeout=30
)

# Throttle to prevent rate limiting
throttler = asyncio_throttle.Throttler(rate_limit=10, period=1)

class WasabiManager:
    def __init__(self):
        self.session = aioboto3.Session()
        self.sync_client = boto3.client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=config.WASABI_ENDPOINT,
            config=boto_config
        )
    
    async def upload_file(self, file_path: str, object_name: str, progress_callback=None):
        """Upload file to Wasabi with progress tracking"""
        async with self.session.client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=config.WASABI_ENDPOINT,
            config=boto_config
        ) as s3:
            
            file_size = os.path.getsize(file_path)
            uploaded = 0
            
            async with aiofiles.open(file_path, 'rb') as file:
                # For large files, use multipart upload
                if file_size > config.CHUNK_SIZE:
                    # Create multipart upload
                    mpu = await s3.create_multipart_upload(
                        Bucket=config.WASABI_BUCKET,
                        Key=object_name
                    )
                    
                    parts = []
                    part_number = 1
                    
                    while True:
                        chunk = await file.read(config.CHUNK_SIZE)
                        if not chunk:
                            break
                            
                        # Upload part
                        part = await s3.upload_part(
                            Bucket=config.WASABI_BUCKET,
                            Key=object_name,
                            PartNumber=part_number,
                            UploadId=mpu['UploadId'],
                            Body=chunk
                        )
                        
                        parts.append({
                            'PartNumber': part_number,
                            'ETag': part['ETag']
                        })
                        
                        uploaded += len(chunk)
                        if progress_callback:
                            await progress_callback(uploaded, file_size)
                            
                        part_number += 1
                        await asyncio.sleep(0.1)  # Small delay to prevent overwhelming
                    
                    # Complete multipart upload
                    await s3.complete_multipart_upload(
                        Bucket=config.WASABI_BUCKET,
                        Key=object_name,
                        UploadId=mpu['UploadId'],
                        MultipartUpload={'Parts': parts}
                    )
                else:
                    # Single part upload for smaller files
                    await file.seek(0)
                    await s3.upload_fileobj(
                        file,
                        config.WASABI_BUCKET,
                        object_name,
                        Callback=progress_callback
                    )
            
            return f"https://{config.WASABI_BUCKET}.s3.{config.WASABI_REGION}.wasabisys.com/{object_name}"
    
    async def download_file(self, object_name: str, file_path: str, progress_callback=None):
        """Download file from Wasabi with progress tracking"""
        async with self.session.client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=config.WASABI_ENDPOINT,
            config=boto_config
        ) as s3:
            
            async with aiofiles.open(file_path, 'wb') as file:
                response = await s3.get_object(
                    Bucket=config.WASABI_BUCKET,
                    Key=object_name
                )
                
                downloaded = 0
                async for chunk in response['Body']:
                    await file.write(chunk)
                    downloaded += len(chunk)
                    
                    if progress_callback:
                        await progress_callback(downloaded)
                
                await response['Body'].close()
    
    async def generate_presigned_url(self, object_name: str, expiration=3600):
        """Generate presigned URL for direct download"""
        url = self.sync_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': config.WASABI_BUCKET,
                'Key': object_name
            },
            ExpiresIn=expiration
        )
        return url
    
    async def get_file_info(self, object_name: str):
        """Get file information from Wasabi"""
        try:
            response = self.sync_client.head_object(
                Bucket=config.WASABI_BUCKET,
                Key=object_name
            )
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'etag': response['ETag']
            }
        except Exception:
            return None

# Initialize Wasabi manager
wasabi = WasabiManager()

def human_readable_size(size_bytes):
    """Convert bytes to human readable format"""
    if size_bytes == 0:
        return "0B"
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    import math
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {units[i]}"

async def speed_test():
    """Perform network speed test"""
    st = speedtest.Speedtest()
    st.get_best_server()
    
    download_speed = st.download() / 1_000_000  # Convert to Mbps
    upload_speed = st.upload() / 1_000_000  # Convert to Mbps
    ping = st.results.ping
    
    return download_speed, upload_speed, ping

@app.on_message(filters.command(["start", "help"]))
async def start_handler(client, message: Message):
    """Start command handler"""
    welcome_text = """
🚀 **Wasabi Storage Bot**

I can help you store and manage files on Wasabi cloud storage with high-speed transfers!

**Available Commands:**
📤 `/upload` - Upload files to Wasabi (reply to a file)
📥 `/download` - Download files from Wasabi
🔗 `/link <filename>` - Generate download link
📊 `/status` - Check bot and network status
📁 `/list` - List files in storage

**Features:**
• 4GB file support
• High-speed transfers
• Progress tracking
• Direct download links
• 24/7 availability

Simply send me a file or use the commands above!
    """
    
    await message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)

@app.on_message(filters.command("upload") & (filters.document | filters.video | filters.audio | filters.photo))
async def upload_handler(client, message: Message):
    """Handle file uploads to Wasabi"""
    try:
        if message.document:
            file_size = message.document.file_size
            file_name = message.document.file_name
        elif message.video:
            file_size = message.video.file_size
            file_name = message.video.file_name or f"video_{message.id}.mp4"
        elif message.audio:
            file_size = message.audio.file_size
            file_name = message.audio.file_name or f"audio_{message.id}.mp3"
        else:
            file_size = message.photo.file_size
            file_name = f"photo_{message.id}.jpg"
        
        # Check file size
        if file_size > config.MAX_FILE_SIZE:
            await message.reply_text(f"❌ File too large! Maximum size is 4GB. Your file: {human_readable_size(file_size)}")
            return
        
        # Download progress callback
        async def download_progress(current, total):
            percent = (current / total) * 100
            if int(percent) % 10 == 0:  # Update every 10% to avoid spam
                await message.edit_text(f"📥 Downloading... {percent:.1f}%")
        
        # Upload progress callback
        async def upload_progress(uploaded, total):
            percent = (uploaded / total) * 100
            if int(percent) % 10 == 0:
                await message.edit_text(f"📤 Uploading to Wasabi... {percent:.1f}%")
        
        # Start processing
        status_msg = await message.reply_text("📥 Starting download...")
        
        # Download file from Telegram
        download_path = await message.download(
            file_name=f"downloads/{file_name}",
            progress=download_progress,
            progress_args=(status_msg,)
        )
        
        await status_msg.edit_text("📤 Uploading to Wasabi storage...")
        
        # Generate unique object name
        timestamp = int(time.time())
        object_name = f"{timestamp}_{file_name}"
        
        # Upload to Wasabi
        wasabi_url = await wasabi.upload_file(
            download_path,
            object_name,
            progress_callback=upload_progress
        )
        
        # Generate presigned URL
        download_url = await wasabi.generate_presigned_url(object_name)
        
        # Clean up local file
        os.remove(download_path)
        
        # Send success message
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Download Link", url=download_url)],
            [InlineKeyboardButton("📁 Storage URL", url=wasabi_url)]
        ])
        
        await status_msg.edit_text(
            f"✅ **File Uploaded Successfully!**\n\n"
            f"📁 **Filename:** `{file_name}`\n"
            f"💾 **Size:** {human_readable_size(file_size)}\n"
            f"🆔 **Object ID:** `{object_name}`\n"
            f"⏰ **Expires:** 1 hour\n\n"
            f"Use `/link {object_name}` to regenerate link later.",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        await message.reply_text(f"❌ Upload failed: {str(e)}")

@app.on_message(filters.command("download"))
async def download_handler(client, message: Message):
    """Handle file downloads from Wasabi"""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Please provide filename. Usage: `/download filename`", parse_mode=ParseMode.MARKDOWN)
            return
        
        object_name = message.command[1]
        
        # Check if file exists
        file_info = await wasabi.get_file_info(object_name)
        if not file_info:
            await message.reply_text("❌ File not found in storage!")
            return
        
        # Download progress callback
        async def download_progress(downloaded):
            percent = (downloaded / file_info['size']) * 100
            if int(percent) % 20 == 0:
                await status_msg.edit_text(f"📥 Downloading from Wasabi... {percent:.1f}%")
        
        status_msg = await message.reply_text("📥 Starting download from Wasabi...")
        
        # Download file
        local_path = f"downloads/{object_name}"
        await wasabi.download_file(object_name, local_path, progress_callback=download_progress)
        
        await status_msg.edit_text("📤 Uploading to Telegram...")
        
        # Upload to Telegram
        await message.reply_document(
            local_path,
            caption=f"📁 {object_name}\n💾 {human_readable_size(file_info['size'])}"
        )
        
        # Clean up
        os.remove(local_path)
        await status_msg.delete()
        
    except Exception as e:
        await message.reply_text(f"❌ Download failed: {str(e)}")

@app.on_message(filters.command("link"))
async def link_handler(client, message: Message):
    """Generate download links"""
    try:
        if len(message.command) < 2:
            await message.reply_text("❌ Please provide filename. Usage: `/link filename`", parse_mode=ParseMode.MARKDOWN)
            return
        
        object_name = message.command[1]
        
        # Check if file exists
        file_info = await wasabi.get_file_info(object_name)
        if not file_info:
            await message.reply_text("❌ File not found in storage!")
            return
        
        # Generate presigned URL
        download_url = await wasabi.generate_presigned_url(object_name)
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 Download Now", url=download_url)]
        ])
        
        await message.reply_text(
            f"🔗 **Download Link Generated**\n\n"
            f"📁 **Filename:** `{object_name}`\n"
            f"💾 **Size:** {human_readable_size(file_info['size'])}\n"
            f"⏰ **Expires:** {file_info['last_modified'].strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"🕒 **Link Valid:** 1 hour",
            reply_markup=keyboard,
            parse_mode=ParseMode.MARKDOWN
        )
        
    except Exception as e:
        await message.reply_text(f"❌ Link generation failed: {str(e)}")

@app.on_message(filters.command("status"))
async def status_handler(client, message: Message):
    """Check bot and network status"""
    try:
        status_msg = await message.reply_text("🔄 Checking status...")
        
        # Check Wasabi connection
        wasabi_status = "✅ Connected"
        try:
            wasabi.sync_client.list_buckets()
        except Exception:
            wasabi_status = "❌ Disconnected"
        
        # Perform speed test
        download_speed, upload_speed, ping = await asyncio.get_event_loop().run_in_executor(None, speed_test)
        
        # Get storage info
        try:
            bucket_size = 0
            file_count = 0
            paginator = wasabi.sync_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=config.WASABI_BUCKET):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        bucket_size += obj['Size']
                        file_count += 1
        except Exception:
            bucket_size = 0
            file_count = 0
        
        status_text = f"""
🏥 **Bot Status Report**

🌐 **Network Status:**
📡 Ping: {ping:.2f} ms
⬇️ Download: {download_speed:.2f} Mbps
⬆️ Upload: {upload_speed:.2f} Mbps

💾 **Storage Status:**
{wasabi_status}
📊 Total Files: {file_count}
💽 Storage Used: {human_readable_size(bucket_size)}

🤖 **Bot Info:**
🕒 Uptime: 24/7
📦 Max File: 4GB
⚡ High Speed: Enabled

**Ready to serve!** 🚀
        """
        
        await status_msg.edit_text(status_text, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        await message.reply_text(f"❌ Status check failed: {str(e)}")

@app.on_message(filters.command("list"))
async def list_handler(client, message: Message):
    """List files in storage"""
    try:
        status_msg = await message.reply_text("📁 Fetching file list...")
        
        files = []
        paginator = wasabi.sync_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=config.WASABI_BUCKET):
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append({
                        'name': obj['Key'],
                        'size': obj['Size'],
                        'modified': obj['LastModified']
                    })
        
        if not files:
            await status_msg.edit_text("📭 No files in storage!")
            return
        
        # Format file list (show first 20 files)
        file_list = "📁 **Files in Storage:**\n\n"
        for i, file in enumerate(files[:20], 1):
            file_list += f"{i}. `{file['name']}`\n"
            file_list += f"   📏 {human_readable_size(file['size'])} | "
            file_list += f"🕒 {file['modified'].strftime('%m/%d %H:%M')}\n\n"
        
        if len(files) > 20:
            file_list += f"... and {len(files) - 20} more files"
        
        await status_msg.edit_text(file_list, parse_mode=ParseMode.MARKDOWN)
        
    except Exception as e:
        await message.reply_text(f"❌ Failed to list files: {str(e)}")

async def main():
    """Main function"""
    # Create downloads directory
    os.makedirs("downloads", exist_ok=True)
    
    print("🚀 Starting Wasabi Telegram Bot...")
    print("✅ Bot is running 24/7 with high-speed Wasabi storage!")
    print("📊 Features: 4GB file support, progress tracking, direct links")
    
    await app.start()
    await asyncio.Event().wait()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
