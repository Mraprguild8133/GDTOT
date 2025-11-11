import os
import asyncio
from typing import Optional
import humanize
from config import config

class BotUtils:
    @staticmethod
    def format_size(size_bytes: int) -> str:
        """Format file size to human readable format"""
        return humanize.naturalsize(size_bytes)
    
    @staticmethod
    def is_file_too_large(size_bytes: int) -> bool:
        """Check if file exceeds maximum size"""
        return size_bytes > config.MAX_FILE_SIZE
    
    @staticmethod
    def generate_file_name(original_name: str) -> str:
        """Generate unique file name"""
        import uuid
        import time
        extension = original_name.split('.')[-1] if '.' in original_name else 'bin'
        return f"{uuid.uuid4()}_{int(time.time())}.{extension}"
    
    @staticmethod
    async def stream_file_from_telegram(client, message):
        """Create a stream from Telegram file download"""
        # This will be handled by Telethon's download manager
        # We'll use the file reference directly
        return message

utils = BotUtils()
