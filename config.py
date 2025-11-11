import os
from typing import Optional
from pydantic import BaseSettings, validator


class Settings(BaseSettings):
    """Configuration management using Pydantic"""
    
    # Telegram API Configuration
    API_ID: int
    API_HASH: str
    BOT_TOKEN: str
    
    # Wasabi Configuration
    WASABI_ACCESS_KEY: str
    WASABI_SECRET_KEY: str
    WASABI_BUCKET: str
    WASABI_REGION: str = "us-east-1"
    
    # Bot Behavior Configuration
    MAX_FILE_SIZE: int = 4 * 1024 * 1024 * 1024  # 4GB
    DOWNLOAD_LINK_EXPIRY: int = 86400  # 24 hours
    PROGRESS_UPDATE_INTERVAL: int = 2  # seconds
    
    # Upload Optimization
    MULTIPART_THRESHOLD: int = 25 * 1024 * 1024  # 25MB
    MULTIPART_CHUNKSIZE: int = 5 * 1024 * 1024   # 5MB
    MAX_CONCURRENCY: int = 10
    
    @validator('API_ID', pre=True)
    def validate_api_id(cls, v):
        if v == 12345 or not v:
            raise ValueError('API_ID must be set')
        return int(v)
    
    @validator('BOT_TOKEN')
    def validate_bot_token(cls, v):
        if not v or "YOUR_BOT_TOKEN" in v:
            raise ValueError('BOT_TOKEN must be set')
        return v
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global config instance
config = Settings()
