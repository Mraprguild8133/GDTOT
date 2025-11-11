import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    # Telegram API Configuration (Required)
    API_ID = os.environ.get("API_ID") or "your_api_id_here"
    API_HASH = os.environ.get("API_HASH") or "your_api_hash_here"
    BOT_TOKEN = os.environ.get("BOT_TOKEN") or "your_bot_token_here"
    
    # Wasabi Configuration (Optional - for cloud storage)
    WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY")
    WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY")
    WASABI_BUCKET = os.environ.get("WASABI_BUCKET")
    WASABI_REGION = os.environ.get("WASABI_REGION", "us-east-1")
    
    # Google Drive Configuration (Optional)
    GDRIVE_CREDENTIALS = os.environ.get("GDRIVE_CREDENTIALS")  # Path to credentials.json
    GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")  # Default folder ID
    
    # Dropbox Configuration (Optional)
    DROPBOX_ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN")
    
    # Mega.nz Configuration (Optional)
    MEGA_EMAIL = os.environ.get("MEGA_EMAIL")
    MEGA_PASSWORD = os.environ.get("MEGA_PASSWORD")
    
    # Bot Behavior Configuration
    MAX_DOWNLOAD_SIZE = int(os.environ.get("MAX_DOWNLOAD_SIZE", "2147483648"))  # 2GB default
    MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "3"))
    DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", "3600"))  # 1 hour
    
    # Temporary Storage
    TEMP_DIR = os.environ.get("TEMP_DIR", "temp_downloads")
    
    # YouTube-DL Configuration
    YTDL_FORMAT = os.environ.get("YTDL_FORMAT", "bestvideo+bestaudio/best")
    YTDL_EXTRACT_AUDIO = os.environ.get("YTDL_EXTRACT_AUDIO", "False").lower() == "true"
    
    # Advanced Settings
    ENABLE_TORRENTS = os.environ.get("ENABLE_TORRENTS", "True").lower() == "true"
    ENABLE_YTDL = os.environ.get("ENABLE_YTDL", "True").lower() == "true"
    ENABLE_DIRECT_DOWNLOADS = os.environ.get("ENABLE_DIRECT_DOWNLOADS", "True").lower() == "true"
    
    # Admin Configuration
    ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
    
    # Database (Optional - for tracking downloads)
    DATABASE_URL = os.environ.get("DATABASE_URL")  # PostgreSQL or SQLite
    
    # Redis (Optional - for caching and rate limiting)
    REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    
    # Rate Limiting
    RATE_LIMIT_PER_USER = int(os.environ.get("RATE_LIMIT_PER_USER", "10"))  # Downloads per hour
    RATE_LIMIT_PER_CHAT = int(os.environ.get("RATE_LIMIT_PER_CHAT", "30"))  # Downloads per hour
    
    # Logging
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    
    def validate_config(self):
        """Validate essential configuration."""
        errors = []
        
        # Check required Telegram config
        if not all([self.API_ID, self.API_HASH, self.BOT_TOKEN]):
            errors.append("Telegram API_ID, API_HASH, and BOT_TOKEN are required")
        
        # Check if API_ID is numeric
        try:
            int(self.API_ID)
        except (ValueError, TypeError):
            errors.append("API_ID must be a numeric value")
        
        # Validate Wasabi config if provided
        if any([self.WASABI_ACCESS_KEY, self.WASABI_SECRET_KEY, self.WASABI_BUCKET]):
            if not all([self.WASABI_ACCESS_KEY, self.WASABI_SECRET_KEY, self.WASABI_BUCKET]):
                errors.append("All Wasabi credentials (ACCESS_KEY, SECRET_KEY, BUCKET) must be provided together")
        
        # Validate file size limits
        if self.MAX_DOWNLOAD_SIZE > 10 * 1024 * 1024 * 1024:  # 10GB
            errors.append("MAX_DOWNLOAD_SIZE cannot exceed 10GB for Telegram limitations")
        
        if errors:
            raise ValueError("Configuration errors:\n- " + "\n- ".join(errors))
        
        return True
    
    def get_supported_clouds(self):
        """Get list of enabled cloud storage providers."""
        clouds = {'telegram': True}  # Telegram is always enabled
        
        if all([self.WASABI_ACCESS_KEY, self.WASABI_SECRET_KEY, self.WASABI_BUCKET]):
            clouds['wasabi'] = True
        
        if self.GDRIVE_CREDENTIALS and os.path.exists(self.GDRIVE_CREDENTIALS):
            clouds['gdrive'] = True
        
        if self.DROPBOX_ACCESS_TOKEN:
            clouds['dropbox'] = True
        
        if all([self.MEGA_EMAIL, self.MEGA_PASSWORD]):
            clouds['mega'] = True
        
        return clouds

# Create global config instance
config = Config()

# Validate configuration on import
try:
    config.validate_config()
    print("✅ Configuration validated successfully")
    
    supported_clouds = config.get_supported_clouds()
    print(f"✅ Enabled cloud storage: {', '.join(supported_clouds.keys())}")
    
    print(f"✅ Download features enabled:")
    if config.ENABLE_TORRENTS:
        print("  - Torrent downloads")
    if config.ENABLE_YTDL:
        print("  - YouTube-DL downloads")
    if config.ENABLE_DIRECT_DOWNLOADS:
        print("  - Direct URL downloads")
        
except ValueError as e:
    print(f"❌ Configuration error: {e}")
    exit(1)
