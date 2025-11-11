import os
from typing import Optional

class Config:
    """Configuration class for Wasabi Telegram Bot"""
    
    # Telegram API Configuration
    API_ID: int = int(os.getenv("API_ID", 0))
    API_HASH: str = os.getenv("API_HASH", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    
    # Wasabi Storage Configuration
    WASABI_ACCESS_KEY: str = os.getenv("WASABI_ACCESS_KEY", "")
    WASABI_SECRET_KEY: str = os.getenv("WASABI_SECRET_KEY", "")
    WASABI_BUCKET: str = os.getenv("WASABI_BUCKET", "")
    WASABI_REGION: str = os.getenv("WASABI_REGION", "us-east-1")
    WASABI_ENDPOINT: str = os.getenv("WASABI_ENDPOINT", f"s3.{WASABI_REGION}.wasabisys.com")
    
    # Bot Configuration
    MAX_FILE_SIZE: int = 4 * 1024 * 1024 * 1024  # 4GB
    DOWNLOAD_LINK_EXPIRE: int = 24 * 60 * 60  # 24 hours
    SUPPORTED_FORMATS: list = ['*']  # All formats supported
    
    # Performance Configuration
    CHUNK_SIZE: int = 64 * 1024 * 1024  # 64MB chunks for better performance
    CONCURRENT_TRANSFERS: int = 10
    MAX_RETRIES: int = 3
    
    @classmethod
    def validate_config(cls):
        """Validate all required environment variables"""
        required_vars = {
            'API_ID': cls.API_ID,
            'API_HASH': cls.API_HASH,
            'BOT_TOKEN': cls.BOT_TOKEN,
            'WASABI_ACCESS_KEY': cls.WASABI_ACCESS_KEY,
            'WASABI_SECRET_KEY': cls.WASABI_SECRET_KEY,
            'WASABI_BUCKET': cls.WASABI_BUCKET
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Global config instance
config = Config()
