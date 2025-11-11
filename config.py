import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Telegram Configuration
    API_ID = int(os.getenv('API_ID'))
    API_HASH = os.getenv('API_HASH')
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    
    # Wasabi Configuration
    WASABI_ACCESS_KEY = os.getenv('WASABI_ACCESS_KEY')
    WASABI_SECRET_KEY = os.getenv('WASABI_SECRET_KEY')
    WASABI_BUCKET = os.getenv('WASABI_BUCKET')
    WASABI_REGION = os.getenv('WASABI_REGION', 'us-east-1')
    
    # Bot Configuration
    MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024  # 4GB
    CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunks for better speed
    DOWNLOAD_URL_EXPIRY = 3600  # 1 hour
    
    # Performance Settings
    MAX_CONCURRENT_UPLOADS = 5
    MAX_CONCURRENT_DOWNLOADS = 5

config = Config()
