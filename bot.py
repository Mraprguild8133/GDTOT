import logging
import requests
import re
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
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
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        })

    async def start(self, update: Update, context: CallbackContext) -> None:
        """Send welcome message when command /start is issued."""
        user = update.effective_user
        welcome_text = f"""
👋 Hello {user.first_name}!

🤖 Welcome to GDTOT Bot!

I can help you with:
📥 Download files from GDTOT links
🔗 Generate direct download links

Simply send me a GDTOT URL to get started!

Commands:
/start - Start the bot
/help - Show help message
        """
        await update.message.reply_text(welcome_text)

    async def help_command(self, update: Update, context: CallbackContext) -> None:
        """Send help message"""
        help_text = """
📖 **GDTOT Bot Help**

**How to use:**
1. Send any GDTOT file URL
2. I'll generate a direct download link for you

**Supported URLs:**
- https://new28.gdtot.dad/file/*
- Any GDTOT file links

**Features:**
✅ Direct download links
✅ File information
✅ Fast processing

**Note:** This bot only supports downloading from existing GDTOT links.
        """
        await update.message.reply_text(help_text)

    def validate_gdtot_url(self, url: str) -> bool:
        """Validate if the URL is a proper GDTOT URL"""
        try:
            parsed = urlparse(url)
            return 'gdtot' in parsed.netloc
        except:
            return False

    def extract_crypt_value(self, html_content: str) -> str:
        """Extract crypt value from GDTOT page"""
        try:
            # Look for crypt value in JavaScript
            crypt_pattern = r"crypt\s*=\s*['\"]([^'\"]+)['\"]"
            match = re.search(crypt_pattern, html_content)
            if match:
                return match.group(1)
            
            # Alternative pattern
            alt_pattern = r"window\.location\.href\s*=\s*[^']+'([^']+)'"
            match = re.search(alt_pattern, html_content)
            if match:
                return match.group(1)
            
            return None
        except Exception as e:
            logger.error(f"Error extracting crypt value: {e}")
            return None

    async def process_gdtot_link(self, url: str) -> dict:
        """Process GDTOT link and generate download URL using the actual workflow"""
        try:
            # Step 1: Get the main page
            logger.info(f"Processing GDTOT URL: {url}")
            response = self.session.get(url, timeout=config.REQUEST_TIMEOUT)
            
            if response.status_code != 200:
                return {'success': False, 'error': f'Failed to access the URL. Status: {response.status_code}'}
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract file information
            filename = "Unknown"
            size = "Unknown"
            
            # Try to find filename
            title_tag = soup.find('title')
            if title_tag:
                filename = title_tag.text.replace(' - GDTOT', '').strip()
            
            # Try to find file size
            size_elements = soup.find_all(['span', 'div'], string=lambda text: text and any(x in text.lower() for x in ['mb', 'gb', 'size']))
            for element in size_elements:
                if any(x in element.text.lower() for x in ['mb', 'gb']):
                    size = element.text.strip()
                    break
            
            # Step 2: Extract crypt value
            crypt_value = self.extract_crypt_value(response.text)
            
            if not crypt_value:
                return {'success': False, 'error': 'Could not extract required information from the page'}
            
            logger.info(f"Extracted crypt value: {crypt_value}")
            
            # Step 3: Generate download link using the crypt value
            download_url = await self.generate_download_url(crypt_value)
            
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

    async def generate_download_url(self, crypt_value: str) -> str:
        """Generate direct download URL using crypt value"""
        try:
            # The actual GDTOT download generation endpoint
            api_url = "https://new28.gdtot.dad/dld.php"
            
            payload = {
                'crypt': crypt_value
            }
            
            # Add credentials if available
            if config.GDTOT_EMAIL and config.GDTOT_API_AUTH:
                payload['email'] = config.GDTOT_EMAIL
                payload['api_auth'] = config.GDTOT_API_AUTH
            
            logger.info(f"Making request to download API with payload: {payload}")
            
            response = self.session.post(
                api_url, 
                data=payload, 
                timeout=config.REQUEST_TIMEOUT,
                allow_redirects=False
            )
            
            logger.info(f"Download API response status: {response.status_code}")
            logger.info(f"Download API response headers: {response.headers}")
            
            if response.status_code in [200, 302]:
                # Check if we got a direct download link
                if 'location' in response.headers:
                    download_url = response.headers['location']
                    logger.info(f"Got download URL from headers: {download_url}")
                    return download_url
                else:
                    # Try to extract from response body
                    soup = BeautifulSoup(response.content, 'html.parser')
                    link = soup.find('a', href=True)
                    if link and 'download' in link['href']:
                        return link['href']
                    
                    # Look for direct URL in JavaScript
                    url_pattern = r"window\.location\.href\s*=\s*['\"]([^'\"]+)['\"]"
                    match = re.search(url_pattern, response.text)
                    if match:
                        return match.group(1)
            
            return ""
                
        except Exception as e:
            logger.error(f"Error generating download URL: {e}")
            return ""

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
🔗 **Direct Link:** [Click Here]({result['download_url']})

⚠️ **Note:** Download links are temporary. Download soon!
                """
                
                await processing_msg.delete()
                await update.message.reply_text(
                    response_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
            else:
                error_msg = f"❌ Error: {result['error']}"
                if "credentials" in result['error'].lower():
                    error_msg += "\n\n💡 Make sure your GDTOT credentials are correct in the configuration."
                await processing_msg.edit_text(error_msg)
                
        except Exception as e:
            logger.error(f"Error processing GDTOT URL: {e}")
            await update.message.reply_text("❌ An error occurred while processing the URL. Please try again.")

    async def handle_document(self, update: Update, context: CallbackContext) -> None:
        """Handle document uploads - Inform user that upload is not supported"""
        await update.message.reply_text(
            "❌ File upload is currently not supported in this version.\n\n"
            "I can only help you download from existing GDTOT links. "
            "Please send a GDTOT URL to generate a direct download link."
        )

    async def error_handler(self, update: Update, context: CallbackContext) -> None:
        """Log errors"""
        logger.error(f"Update {update} caused error {context.error}")

def main():
    """Start the bot."""
    # Check if required configuration is present
    if not config.BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found in environment variables!")
        return
    
    # Warn if GDTOT credentials are missing (bot will still work but might have limitations)
    if not config.GDTOT_EMAIL or not config.GDTOT_API_AUTH:
        logger.warning("⚠️ GDTOT credentials not found. Some features might be limited.")

    # Create the Application
    application = Application.builder().token(config.BOT_TOKEN).build()
    
    # Initialize GDTOT bot
    gdtot_bot = GDTOTBot()

    # Add handlers
    application.add_handler(CommandHandler("start", gdtot_bot.start))
    application.add_handler(CommandHandler("help", gdtot_bot.help_command))
    
    # Handle GDTOT URLs
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        gdtot_bot.handle_gdtot_url
    ))
    
    # Handle file uploads - inform about limitation
    application.add_handler(MessageHandler(
        filters.ATTACHMENT,
        gdtot_bot.handle_document
    ))
    
    # Error handler
    application.add_error_handler(gdtot_bot.error_handler)

    # Start the Bot
    print("🤖 GDTOT Bot is running...")
    print("⚠️ Note: Upload functionality is disabled. Only download from existing links is supported.")
    application.run_polling()

if __name__ == '__main__':
    main()
