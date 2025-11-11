import os
import time
import uuid
import logging
import asyncio
from datetime import datetime, timedelta

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import MessageNotModified

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig  # Correct import

# Import configuration
from config import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MB = 1024 ** 2

# --- WASABI (BOTO3) INITIALIZATION ---
try:
    s3_config = Config(
        signature_version='s3v4',
        connect_timeout=60,
        read_timeout=60,
        retries={'max_attempts': 10, 'mode': 'standard'}
    )
    
    # CORRECT TransferConfig import
    transfer_config = TransferConfig(
        multipart_threshold=config.MULTIPART_THRESHOLD,
        max_concurrency=20,
        multipart_chunksize=config.MULTIPART_CHUNKSIZE,
        use_threads=True
    )

    s3_client = boto3.client(
        's3',
        endpoint_url=config.WASABI_ENDPOINT,
        aws_access_key_id=config.WASABI_ACCESS_KEY,
        aws_secret_access_key=config.WASABI_SECRET_KEY,
        region_name=config.WASABI_REGION,
        config=s3_config
    )
    logger.info(f"Wasabi S3 Client Initialized for region: {config.WASABI_REGION}")
except Exception as e:
    logger.error(f"Error initializing Boto3 client: {e}")
    exit(1)
