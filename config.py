# config.py
import os
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Required environment variables
try:
    API_ID = os.environ["API_ID"]
    API_HASH = os.environ["API_HASH"]
    BOT_TOKEN = os.environ["BOT_TOKEN"]
    WASABI_ACCESS_KEY = os.environ["WASABI_ACCESS_KEY"]
    WASABI_SECRET_KEY = os.environ["WASABI_SECRET_KEY"]
    WASABI_BUCKET = os.environ["WASABI_BUCKET"]
    WASABI_REGION = os.environ["WASABI_REGION"]
except KeyError as e:
    logging.error(f"Missing required environment variable: {e}")
    raise SystemExit(1)

# Derived configuration
WASABI_ENDPOINT = f"https://s3.{WASABI_REGION}.wasabisys.com"

# Upload settings
MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024  # 4GB
PRESIGNED_URL_EXPIRY = 7 * 24 * 60 * 60  # 7 days

# Temporary file settings
TEMP_DIR = tempfile.gettempdir()

# YouTube download settings
YTDL_OPTIONS = {
    'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
    'merge_output_format': 'mp4',
    'noplaylist': True,
}
