import os
from typing import Optional

class Config:
    """Configuration class for the Wasabi Uploader Bot"""
    
    def __init__(self):
        # Telegram API Configuration
        self.API_ID = int(os.environ.get("API_ID", 0))
        self.API_HASH = os.environ.get("API_HASH", "")
        self.BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
        
        # Wasabi S3 Configuration
        self.WASABI_ACCESS_KEY = os.environ.get("WASABI_ACCESS_KEY", "")
        self.WASABI_SECRET_KEY = os.environ.get("WASABI_SECRET_KEY", "")
        self.WASABI_BUCKET = os.environ.get("WASABI_BUCKET", "")
        self.WASABI_REGION = os.environ.get("WASABI_REGION", "")
        self.WASABI_ENDPOINT = f"https://s3.{self.WASABI_REGION}.wasabisys.com"
        
        # Upload Configuration
        self.MAX_FILE_SIZE = 4 * 1024 ** 3  # 4GB
        self.URL_EXPIRY = 604800  # 7 days in seconds
        self.MULTIPART_THRESHOLD = 100 * 1024 ** 2  # 100MB
        self.MULTIPART_CHUNKSIZE = 25 * 1024 ** 2  # 25MB
        
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate that all required configuration is present"""
        required_vars = {
            "API_ID": self.API_ID,
            "API_HASH": self.API_HASH,
            "BOT_TOKEN": self.BOT_TOKEN,
            "WASABI_ACCESS_KEY": self.WASABI_ACCESS_KEY,
            "WASABI_SECRET_KEY": self.WASABI_SECRET_KEY,
            "WASABI_BUCKET": self.WASABI_BUCKET,
            "WASABI_REGION": self.WASABI_REGION,
        }
        
        missing = [key for key, value in required_vars.items() if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

# Global config instance
config = Config()
