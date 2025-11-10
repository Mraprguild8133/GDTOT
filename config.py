import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram Bot Configuration
    BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    
    # GDTOT Configuration
    GDTOT_EMAIL = os.getenv('GDTOT_EMAIL')
    GDTOT_API_AUTH = os.getenv('GDTOT_API_AUTH')
    GDTOT_BASE_URL = os.getenv('GDTOT_BASE_URL', 'https://new28.gdtot.dad/')
    
    # Bot Settings
    MAX_FILE_SIZE = 2000 * 1024 * 1024  # 2GB in bytes
    REQUEST_TIMEOUT = 30
    
    # API Endpoints
    GDTOT_API_ENDPOINTS = {
        'generate_download': '/api/generate_download',
        'upload': '/api/upload',
        'file_info': '/api/file/info'
    }

# Create config instance
config = Config()
