import logging
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext, CallbackQueryHandler
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json

from config import config

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class GDTOTBot:
    def __init__(self):
        self.session = requests.Session()
        self.setup_headers()

    def setup_headers(self):
        """Setup headers for GDTOT requests"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/x-www-form-urlencoded',
        })

    async def start(self, update: Update, context: CallbackContext) -> None:
        """Send welcome message when command /start is issued."""
        user = update.effective_user
        welcome_text = f"""
👋 Hello {user.first_name}!

🤖 Welcome to GDTOT Bot!

I can help you with:
📤 Upload files to GDTOT
📥 Download files from GDTOT
🔗 Generate direct download links

Simply send me a GDTOT URL or use the commands below:

Commands:
/upload - Upload files to GDTOT
/download - Download from GDTOT URL
/help - Show this help message

Send a GDTOT URL to get started!
        """
        await update.message.reply_text(welcome_text)

    async def help_command(self, update: Update, context: CallbackContext) -> None:
        """Send help message"""
        help_text = """
📖 **GDTOT Bot Help**

**Upload Files:**
1. Use /upload command
2. Send the file you want to upload
3. Bot will provide GDTOT link

**Download Files:**
1. Send any GDTOT URL
2. Bot will generate direct download link

**Supported URLs:**
- https://new28.gdtot.dad/file/*
- GDTOT file links

**Features:**
✅ Direct download links
✅ File information
✅ Fast processing
        """
        await update.message.reply_text(help_text)

    def validate_gdtot_url(self, url: str) -> bool:
        """Validate if the URL is a proper GDTOT URL"""
        try:
            parsed = urlparse(url)
            return 'gdtot' in parsed.netloc and 'file' in parsed.path
        except:
            return False

    async def handle_gdtot_url(self, update: Update, context: CallbackContext) -> None:
        """Handle GDTOT URLs and generate download links"""
        url = update.message.text.strip()
        
        if not self.validate_gdtot_url(url):
            await update.message.reply_text("❌ Invalid GDTOT URL. Please provide a valid GDTOT file URL.")
            return

        try:
            # Send processing message
            processing_msg = await update.message.reply_text("🔄 Processing your GDTOT link...")
            
            # Extract file information and generate download link
            result = await self.process_gdtot_link(url)
            
            if result['success']:
                # Create inline keyboard with download button
                keyboard = [
                    [InlineKeyboardButton("📥 Download Now", url=result['download_url'])]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                response_text = f"""
✅ **File Ready for Download!**

📁 **File Name:** `{result['filename']}`
📊 **File Size:** {result['size']}
🔗 **Direct Link:** [Click to Download]({result['download_url']})

⚠️ **Note:** Download links are temporary. Download soon!
                """
                
                await processing_msg.delete()
                await update.message.reply_text(
                    response_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown'
                )
            else:
                await processing_msg.edit_text(f"❌ Error: {result['error']}")
                
        except Exception as e:
            logger.error(f"Error processing GDTOT URL: {e}")
            await update.message.reply_text("❌ An error occurred while processing the URL. Please try again.")

    async def process_gdtot_link(self, url: str) -> dict:
        """Process GDTOT link and generate download URL"""
        try:
            # First, get the page content
            response = self.session.get(url, timeout=config.REQUEST_TIMEOUT)
            
            if response.status_code != 200:
                return {'success': False, 'error': 'Failed to access the URL'}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract file information
            filename = "Unknown"
            size = "Unknown"
            
            # Try to find filename and size in the page
            title_tag = soup.find('title')
            if title_tag:
                filename = title_tag.text.replace(' - GDTOT', '').strip()
            
            # Look for size information
            size_elements = soup.find_all(['span', 'div'], string=lambda text: text and any(x in text.lower() for x in ['mb', 'gb', 'size']))
            if size_elements:
                size = size_elements[0].text
            
            # Generate download URL
            download_url = await self.generate_download_url(url)
            
            if download_url:
                return {
                    'success': True,
                    'filename': filename,
                    'size': size,
                    'download_url': download_url
                }
            else:
                return {'success': False, 'error': 'Could not generate download link'}
                
        except Exception as e:
            logger.error(f"Error in process_gdtot_link: {e}")
            return {'success': False, 'error': str(e)}

    async def generate_download_url(self, url: str) -> str:
        """Generate direct download URL using GDTOT API"""
        try:
            # Prepare API payload
            payload = {
                'email': config.GDTOT_EMAIL,
                'api_auth': config.GDTOT_API_AUTH,
                'url': url
            }
            
            # Make API request to generate download link
            api_url = f"{config.GDTOT_BASE_URL}{config.GDTOT_API_ENDPOINTS['generate_download']}"
            response = self.session.post(
                api_url, 
                data=payload, 
                timeout=config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('download_url', '')
            else:
                logger.error(f"API returned status code: {response.status_code}")
                return ""
                
        except Exception as e:
            logger.error(f"Error generating download URL: {e}")
            return ""

    async def upload_command(self, update: Update, context: CallbackContext) -> None:
        """Handle file upload to GDTOT"""
        await update.message.reply_text(
            "📤 Please send the file you want to upload to GDTOT.\n\n"
            f"⚠️ Maximum file size: {config.MAX_FILE_SIZE // (1024*1024)}MB"
        )

    async def handle_document(self, update: Update, context: CallbackContext) -> None:
        """Handle document uploads"""
        try:
            document = update.message.document
            file_id = document.file_id
            file_name = document.file_name
            
            # Check file size
            if document.file_size > config.MAX_FILE_SIZE:
                await update.message.reply_text(
                    f"❌ File too large! Maximum size is {config.MAX_FILE_SIZE // (1024*1024)}MB"
                )
                return

            # Send processing message
            processing_msg = await update.message.reply_text(f"📤 Uploading {file_name} to GDTOT...")
            
            # Get file from Telegram
            file = await context.bot.get_file(file_id)
            
            # Download file content
            file_content = await self.download_telegram_file(file.file_path)
            
            # Upload to GDTOT
            gdtot_url = await self.upload_to_gdtot(file_content, file_name)
            
            if gdtot_url:
                await processing_msg.edit_text(
                    f"✅ File uploaded successfully!\n\n"
                    f"📁 File: {file_name}\n"
                    f"🔗 GDTOT URL: {gdtot_url}\n\n"
                    f"Share this link with others!"
                )
            else:
                await processing_msg.edit_text("❌ Failed to upload file to GDTOT. Please try again.")
                
        except Exception as e:
            logger.error(f"Error handling document: {e}")
            await update.message.reply_text("❌ An error occurred while uploading the file.")

    async def download_telegram_file(self, file_path: str) -> bytes:
        """Download file from Telegram"""
        response = requests.get(file_path, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.content

    async def upload_to_gdtot(self, file_content: bytes, filename: str) -> str:
        """Upload file to GDTOT and return the URL"""
        try:
            upload_url = f"{config.GDTOT_BASE_URL}{config.GDTOT_API_ENDPOINTS['upload']}"
            files = {'file': (filename, file_content)}
            data = {
                'email': config.GDTOT_EMAIL,
                'api_auth': config.GDTOT_API_AUTH
            }
            
            response = self.session.post(
                upload_url, 
                files=files, 
                data=data,
                timeout=config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('file_url', '')
            else:
                logger.error(f"Upload failed with status: {response.status_code}")
                return ""
                
        except Exception as e:
            logger.error(f"Error uploading to GDTOT: {e}")
            return ""

    async def file_info_command(self, update: Update, context: CallbackContext) -> None:
        """Get file information from GDTOT URL"""
        if not context.args:
            await update.message.reply_text("❌ Please provide a GDTOT URL. Usage: /fileinfo <gdtot_url>")
            return
        
        url = context.args[0]
        if not self.validate_gdtot_url(url):
            await update.message.reply_text("❌ Invalid GDTOT URL.")
            return

        try:
            processing_msg = await update.message.reply_text("🔍 Getting file information...")
            
            result = await self.get_file_info(url)
            
            if result['success']:
                info_text = f"""
📁 **File Information**

**Name:** {result['filename']}
**Size:** {result['size']}
**Type:** {result.get('file_type', 'Unknown')}
**Upload Date:** {result.get('upload_date', 'Unknown')}
**Downloads:** {result.get('downloads', 'Unknown')}

**Description:** {result.get('description', 'No description available')}
                """
                await processing_msg.edit_text(info_text)
            else:
                await processing_msg.edit_text(f"❌ Error: {result['error']}")
                
        except Exception as e:
            logger.error(f"Error getting file info: {e}")
            await update.message.reply_text("❌ Failed to get file information.")

    async def get_file_info(self, url: str) -> dict:
        """Get detailed file information from GDTOT"""
        try:
            response = self.session.get(url, timeout=config.REQUEST_TIMEOUT)
            
            if response.status_code != 200:
                return {'success': False, 'error': 'Failed to access the URL'}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract file information from the page
            # This will need to be adapted based on the actual page structure
            filename = "Unknown"
            size = "Unknown"
            
            title_tag = soup.find('title')
            if title_tag:
                filename = title_tag.text.replace(' - GDTOT', '').strip()
            
            # Add more parsing logic here based on GDTOT's page structure
            
            return {
                'success': True,
                'filename': filename,
                'size': size,
                'file_type': 'Unknown',
                'upload_date': 'Unknown',
                'downloads': 'Unknown',
                'description': 'No description available'
            }
            
        except Exception as e:
            logger.error(f"Error in get_file_info: {e}")
            return {'success': False, 'error': str(e)}

    async def error_handler(self, update: Update, context: CallbackContext) -> None:
        """Log errors"""
        logger.error(f"Update {update} caused error {context.error}")

def main():
    """Start the bot."""
    # Check if required configuration is present
    if not config.BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found in environment variables!")
        return
    
    if not config.GDTOT_EMAIL or not config.GDTOT_API_AUTH:
        logger.error("❌ GDTOT credentials not found in environment variables!")
        return

    # Create the Application
    application = Application.builder().token(config.BOT_TOKEN).build()
    
    # Initialize GDTOT bot
    gdtot_bot = GDTOTBot()

    # Add handlers
    application.add_handler(CommandHandler("start", gdtot_bot.start))
    application.add_handler(CommandHandler("help", gdtot_bot.help_command))
    application.add_handler(CommandHandler("upload", gdtot_bot.upload_command))
    application.add_handler(CommandHandler("download", gdtot_bot.handle_gdtot_url))
    application.add_handler(CommandHandler("fileinfo", gdtot_bot.file_info_command))
    
    # Handle GDTOT URLs
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        gdtot_bot.handle_gdtot_url
    ))
    
    # Handle file uploads
    application.add_handler(MessageHandler(
        filters.DOCUMENT, 
        gdtot_bot.handle_document
    ))
    
    # Error handler
    application.add_error_handler(gdtot_bot.error_handler)

    # Start the Bot
    print("🤖 GDTOT Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()
