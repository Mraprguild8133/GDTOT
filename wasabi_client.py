import boto3
import aiobotocore
import asyncio
from botocore.config import Config as BotoConfig
from config import config
import uuid
import time
import io

class WasabiClient:
    def __init__(self):
        self.boto_config = BotoConfig(
            region_name=config.WASABI_REGION,
            signature_version='s3v4',
            retries={'max_attempts': 3, 'mode': 'standard'}
        )
        
        self.sync_client = boto3.client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=f'https://s3.{config.WASABI_REGION}.wasabisys.com',
            config=self.boto_config
        )
    
    async def upload_from_stream(self, file_stream, file_size, object_name=None):
        """Upload file directly from stream to Wasabi - No temporary files"""
        if object_name is None:
            object_name = f"{uuid.uuid4()}_{int(time.time())}"
        
        session = aiobotocore.get_session()
        async with session.create_client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=f'https://s3.{config.WASABI_REGION}.wasabisys.com',
            config=self.boto_config
        ) as client:
            
            try:
                # For large files, use multipart upload
                if file_size > config.CHUNK_SIZE:
                    return await self._multipart_upload(client, file_stream, file_size, object_name)
                else:
                    # For smaller files, direct upload
                    file_data = await file_stream.read()
                    await client.put_object(
                        Bucket=config.WASABI_BUCKET,
                        Key=object_name,
                        Body=file_data
                    )
                    return object_name
                
            except Exception as e:
                print(f"Upload error: {e}")
                raise
    
    async def _multipart_upload(self, client, file_stream, file_size, object_name):
        """Handle multipart upload for large files"""
        # Create multipart upload
        mpu = await client.create_multipart_upload(
            Bucket=config.WASABI_BUCKET,
            Key=object_name
        )
        upload_id = mpu['UploadId']
        
        try:
            parts = []
            part_number = 1
            
            # Upload parts
            while True:
                chunk = await file_stream.read(config.CHUNK_SIZE)
                if not chunk:
                    break
                
                part = await client.upload_part(
                    Bucket=config.WASABI_BUCKET,
                    Key=object_name,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
                
                parts.append({
                    'ETag': part['ETag'],
                    'PartNumber': part_number
                })
                
                part_number += 1
            
            # Complete multipart upload
            await client.complete_multipart_upload(
                Bucket=config.WASABI_BUCKET,
                Key=object_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            return object_name
            
        except Exception as e:
            # Abort upload on error
            await client.abort_multipart_upload(
                Bucket=config.WASABI_BUCKET,
                Key=object_name,
                UploadId=upload_id
            )
            raise
    
    async def download_to_stream(self, object_name):
        """Download file from Wasabi and return as stream"""
        session = aiobotocore.get_session()
        async with session.create_client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=f'https://s3.{config.WASABI_REGION}.wasabisys.com',
            config=self.boto_config
        ) as client:
            
            try:
                response = await client.get_object(
                    Bucket=config.WASABI_BUCKET,
                    Key=object_name
                )
                
                # Return the stream directly
                return response['Body']
                
            except Exception as e:
                print(f"Download error: {e}")
                return None
    
    def generate_download_url(self, object_name, expiry=3600):
        """Generate pre-signed download URL"""
        try:
            url = self.sync_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': config.WASABI_BUCKET,
                    'Key': object_name
                },
                ExpiresIn=expiry
            )
            return url
        except Exception as e:
            print(f"URL generation error: {e}")
            return None
    
    async def delete_file(self, object_name):
        """Delete file from Wasabi"""
        session = aiobotocore.get_session()
        async with session.create_client(
            's3',
            aws_access_key_id=config.WASABI_ACCESS_KEY,
            aws_secret_access_key=config.WASABI_SECRET_KEY,
            endpoint_url=f'https://s3.{config.WASABI_REGION}.wasabisys.com',
            config=self.boto_config
        ) as client:
            
            try:
                await client.delete_object(
                    Bucket=config.WASABI_BUCKET,
                    Key=object_name
                )
                return True
            except Exception as e:
                print(f"Delete error: {e}")
                return False
    
    async def get_file_info(self, object_name):
        """Get file information from Wasabi"""
        try:
            response = self.sync_client.head_object(
                Bucket=config.WASABI_BUCKET,
                Key=object_name
            )
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'content_type': response.get('ContentType', 'unknown')
            }
        except Exception as e:
            print(f"File info error: {e}")
            return None
