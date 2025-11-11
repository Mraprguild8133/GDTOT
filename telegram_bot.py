from telethon import TelegramClient, events
from telethon.tl.types import Document, DocumentAttributeFilename
import asyncio
import os
from config import config
from wasabi_client import WasabiClient
from utils import utils

class TelegramWasabiBot:
    def __init__(self):
        self.client = TelegramClient(
            'wasabi_bot_session',
            config.API_ID,
            config.API_HASH
        ).start(bot_token=config.BOT_TOKEN)
        
        self.wasabi = WasabiClient()
        self.upload_tasks = set()
        self.download_tasks = set()
        
        self.setup_handlers()
    
    def setup_handlers(self):
        """Setup Telegram event handlers"""
        
        @self.client.on(events.NewMessage(pattern='/start'))
        async def start_handler(event):
            """Handle /start command"""
            welcome_text = """
🚀 **Wasabi Storage Bot - High Speed File Transfer**

**Features:**
• 📤 Upload files to Wasabi storage (up to 4GB)
• 📥 Download files with generated links
• ⚡ High-speed direct streaming - No temporary files
• 🔗 Temporary download links (1 hour expiry)

**Commands:**
• Just send any file to upload and get a download link
• Use /download <file_id> to get download link
• Use /help for assistance

**24/7 Operational - High Capacity Support**
            """
            await event.reply(welcome_text)
        
        @self.client.on(events.NewMessage(pattern='/help'))
        async def help_handler(event):
            """Handle /help command"""
            help_text = """
🆘 **Bot Help Guide**

**How to Upload:**
1. Send any file (document, video, audio, etc.)
2. Bot will directly stream to Wasabi (no temp files)
3. You'll receive a download link

**How to Download:**
• Use the provided download link
• Links expire in 1 hour for security

**File Limits:**
• Maximum file size: 4GB
• Supported formats: All file types

**Need Help?**
Contact support if you encounter any issues.
            """
            await event.reply(help_text)
        
        @self.client.on(events.NewMessage(pattern='/download'))
        async def download_handler(event):
            """Handle download command"""
            try:
                args = event.message.text.split()
                if len(args) < 2:
                    await event.reply("❌ Please provide a file ID. Usage: `/download <file_id>`")
                    return
                
                file_id = args[1]
                download_url = self.wasabi.generate_download_url(file_id)
                
                if download_url:
                    # Get file info
                    file_info = await self.wasabi.get_file_info(file_id)
                    size_info = utils.format_size(file_info['size']) if file_info else "Unknown"
                    
                    response_text = f"""
📥 **Download Ready!**

**File ID:** `{file_id}`
**Size:** {size_info}
**Link Expiry:** 1 hour

🔗 [Click to Download]({download_url})

⚠️ **Important:** This link will expire in 1 hour.
                    """
                    await event.reply(response_text, link_preview=False)
                else:
                    await event.reply("❌ File not found or error generating download link.")
                    
            except Exception as e:
                await event.reply(f"❌ Error: {str(e)}")
        
        @self.client.on(events.NewMessage)
        async def file_handler(event):
            """Handle file uploads with direct streaming"""
            if event.message.media and not event.message.text.startswith('/'):
                # Create task for upload to avoid blocking
                task = asyncio.create_task(self.handle_direct_upload(event))
                self.upload_tasks.add(task)
                task.add_done_callback(self.upload_tasks.discard)
    
    async def handle_direct_upload(self, event):
        """Handle direct file upload to Wasabi without temporary files"""
        message = await event.reply("📥 **Processing file...**")
        
        try:
            # Get file information
            file_size = event.message.media.document.size
            original_name = self.get_file_name(event)
            
            # Check file size
            if utils.is_file_too_large(file_size):
                await message.edit(f"❌ File too large! Maximum size is {utils.format_size(config.MAX_FILE_SIZE)}")
                return
            
            await message.edit(f"🚀 **Direct streaming to Wasabi...**\nSize: {utils.format_size(file_size)}")
            
            # Generate unique file name
            wasabi_file_name = utils.generate_file_name(original_name)
            
            # Download from Telegram and upload to Wasabi in stream
            file_content = await self.client.download_media(
                event.message.media,
                file=bytes  # Download as bytes to avoid temp files
            )
            
            if file_content:
                # Create stream from bytes
                file_stream = io.BytesIO(file_content)
                
                # Upload to Wasabi directly from stream
                wasabi_object_name = await self.wasabi.upload_from_stream(
                    file_stream, 
                    file_size, 
                    wasabi_file_name
                )
                
                if wasabi_object_name:
                    # Generate download URL
                    download_url = self.wasabi.generate_download_url(wasabi_object_name)
                    
                    success_text = f"""
✅ **Upload Successful!**

📁 **File:** {original_name}
💾 **Size:** {utils.format_size(file_size)}
🆔 **File ID:** `{wasabi_object_name}`
🔗 **Download Link:** [Click Here]({download_url})

⚠️ **Link expires in 1 hour**
💡 Use `/download {wasabi_object_name}` to regenerate link
                    """
                    
                    await message.edit(success_text, link_preview=False)
                else:
                    await message.edit("❌ Failed to upload file to Wasabi storage.")
            else:
                await message.edit("❌ Failed to download file from Telegram.")
            
        except Exception as e:
            error_msg = f"❌ Upload failed: {str(e)}"
            await message.edit(error_msg)
            print(f"Upload error: {e}")
    
    def get_file_name(self, event):
        """Extract file name from Telegram message"""
        if hasattr(event.message.media, 'document'):
            for attr in event.message.media.document.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    return attr.file_name
        return "unknown_file"
    
    async def run(self):
        """Start the bot"""
        print("🚀 Wasabi Telegram Bot Started!")
        print("📊 Configuration:")
        print(f"   • Max File Size: {utils.format_size(config.MAX_FILE_SIZE)}")
        print(f"   • Wasabi Bucket: {config.WASABI_BUCKET}")
        print(f"   • Region: {config.WASABI_REGION}")
        print("🔗 Bot is now running 24/7...")
        print("⚡ Direct streaming mode: No temporary files")
        
        await self.client.run_until_disconnected()
