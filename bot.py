import os
import time
import asyncio
from typing import Callable, Optional

# Dependencies remain the same
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

# Import the new config
from config import config


# --- Boto3/Wasabi Client Setup ---
s3_config = Config(
    region_name=config.WASABI_REGION,
    s3={'addressing_style': 'virtual'},
    signature_version='s3v4'
)

s3_client = boto3.client(
    's3',
    endpoint_url=f'https://s3.{config.WASABI_REGION}.wasabisys.com',
    aws_access_key_id=config.WASABI_ACCESS_KEY,
    aws_secret_access_key=config.WASABI_SECRET_KEY,
    config=s3_config
)

# Use config values for transfer settings
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=config.MULTIPART_THRESHOLD,
    max_concurrency=config.MAX_CONCURRENCY,
    multipart_chunksize=config.MULTIPART_CHUNKSIZE,
    use_threads=True
)


# --- Pyrogram Client Initialization ---
app = Client(
    "wasabi_cloud_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN
)


# --- Enhanced Wasabi Utility Functions ---

async def upload_file_to_wasabi(
    file_path: str, 
    key: str, 
    progress_callback: Optional[Callable] = None
) -> bool:
    """Uploads file to Wasabi with enhanced error handling"""
    try:
        # Check file size before upload
        file_size = os.path.getsize(file_path)
        if file_size > config.MAX_FILE_SIZE:
            print(f"File too large: {file_size} > {config.MAX_FILE_SIZE}")
            return False

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: s3_client.upload_file(
                file_path,
                config.WASABI_BUCKET,
                key,
                Callback=progress_callback,
                Config=TRANSFER_CONFIG
            )
        )
        return True
    except Exception as e:
        print(f"Wasabi Upload Error for {key}: {e}")
        return False


def get_presigned_url(key: str, expiration: Optional[int] = None) -> Optional[str]:
    """Generates presigned URL with configurable expiration"""
    if expiration is None:
        expiration = config.DOWNLOAD_LINK_EXPIRY
        
    try:
        response = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': config.WASABI_BUCKET,
                'Key': key
            },
            ExpiresIn=expiration
        )
        return response
    except Exception as e:
        print(f"Presigned URL Error for {key}: {e}")
        return None


# --- Enhanced Progress Handler ---

class ProgressHandler:
    def __init__(self, client: Client, msg: Message, total_size: int, start_time: float):
        self.client = client
        self.msg = msg
        self.total_size = total_size
        self.bytes_transferred = 0
        self.start_time = start_time
        self.last_update_time = start_time
        self.lock = asyncio.Lock()
        
    def __call__(self, bytes_transferred):
        self.bytes_transferred += bytes_transferred
        asyncio.run_coroutine_threadsafe(self.update_progress(), self.client.loop)

    async def update_progress(self):
        current_time = time.time()
        
        # Use configurable update interval
        if (current_time - self.last_update_time) < config.PROGRESS_UPDATE_INTERVAL and self.bytes_transferred < self.total_size:
            return

        self.last_update_time = current_time
        
        elapsed = current_time - self.start_time
        speed = self.bytes_transferred / elapsed if elapsed > 0 else 0
        speed_str = self._format_size(speed)
        progress = (self.bytes_transferred / self.total_size) * 100
        
        # Enhanced progress bar
        bar_length = 20
        filled_length = int(bar_length * self.bytes_transferred // self.total_size)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        status = "⏫ Uploading to Wasabi" if self.bytes_transferred < self.total_size else "✅ Finalizing"
        
        text = (
            f"**{status}**\n"
            f"**File:** `{self.msg.document.file_name}`\n"
            f"`[{bar}] {progress:.1f}%`\n"
            f"**Progress:** `{self._format_size(self.bytes_transferred)} / {self._format_size(self.total_size)}`\n"
            f"**Speed:** `{speed_str}/s`\n"
            f"**ETA:** `{self._format_eta(elapsed, progress)}`"
        )
        
        try:
            async with self.lock:
                await self.msg.edit_text(text)
        except FloodWait as e:
            print(f"Flood wait: {e.value}s")
            await asyncio.sleep(e.value)
        except Exception as e:
            print(f"Progress update error: {e}")

    def _format_eta(self, elapsed: float, progress: float) -> str:
        """Calculate and format estimated time remaining"""
        if progress <= 0:
            return "Calculating..."
        total_time = elapsed * (100 / progress)
        remaining = total_time - elapsed
        if remaining <= 0:
            return "Almost done"
        return f"{int(remaining // 60)}m {int(remaining % 60)}s"

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.2f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/1024**2:.2f} MB"
        else:
            return f"{size_bytes/1024**3:.2f} GB"


# --- Updated Command Handlers ---

@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    """Enhanced start command with config info"""
    await message.reply_text(
        "👋 **Wasabi Cloud Uploader Bot**\n\n"
        "**Features:**\n"
        f"• Max file size: {config.MAX_FILE_SIZE // (1024**3)}GB\n"
        f"• Download links: {config.DOWNLOAD_LINK_EXPIRY // 3600}h validity\n"
        "• High-speed concurrent uploads\n\n"
        "**Usage:**\n"
        "• Send any file directly\n"
        "• `/download <key>` - Generate download link\n"
        "• `/upload_url <url>` - Upload from URL\n"
        "• `/info` - Bot status and limits"
    )


@app.on_message(filters.command("info"))
async def info_command(client: Client, message: Message):
    """Show bot configuration and status"""
    # You can add more status information here
    await message.reply_text(
        "🤖 **Bot Information**\n\n"
        f"• **Bucket:** `{config.WASABI_BUCKET}`\n"
        f"• **Region:** `{config.WASABI_REGION}`\n"
        f"• **Max File Size:** `{config.MAX_FILE_SIZE // (1024**3)}GB`\n"
        f"• **Link Expiry:** `{config.DOWNLOAD_LINK_EXPIRY // 3600} hours`\n"
        "• **Status:** ✅ Operational"
    )


# The rest of your handlers (handle_document_upload, generate_download_link_command, etc.)
# remain the same but will automatically use the config values


# --- Health Check Function ---

async def check_wasabi_connection() -> bool:
    """Check if Wasabi bucket is accessible"""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: s3_client.head_bucket(Bucket=config.WASABI_BUCKET)
        )
        return True
    except Exception as e:
        print(f"Wasabi connection check failed: {e}")
        return False


# --- Enhanced Main Runner ---
async def main():
    """Enhanced main function with health checks"""
    print("🚀 Starting Telegram Wasabi Uploader Bot...")
    
    # Check connections before starting
    print("🔍 Checking Wasabi connection...")
    if not await check_wasabi_connection():
        print("❌ Failed to connect to Wasabi. Please check credentials.")
        return
    
    print("✅ All checks passed. Starting bot...")
    await app.start()
    
    # Get bot info
    me = await app.get_me()
    print(f"🤖 Bot @{me.username} is now running!")
    
    await app.idle()
    await app.stop()
    print("🛑 Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
