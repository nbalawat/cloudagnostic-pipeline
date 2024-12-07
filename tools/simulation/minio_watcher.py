#!/usr/bin/env python3
import os
import json
import time
import logging
import argparse
import asyncio
import aiohttp
from typing import Dict, Set
from minio import Minio
from minio.notificationconfig import NotificationConfig, QueueConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MinioEventHandler:
    def __init__(
        self,
        minio_client: Minio,
        api_url: str,
        patterns: Set[str]
    ):
        self.minio_client = minio_client
        self.api_url = api_url
        self.patterns = patterns
        self.session = None
        self.processing_objects = set()

    async def setup(self):
        """Setup async HTTP session."""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def cleanup(self):
        """Cleanup resources."""
        if self.session is not None:
            await self.session.close()
            self.session = None

    def matches_pattern(self, object_name: str) -> bool:
        """Check if object name matches any of the patterns."""
        return any(
            object_name.endswith(pattern) for pattern in self.patterns
        )

    async def handle_event(self, event: Dict):
        """Handle MinIO event."""
        try:
            records = event.get('Records', [])
            for record in records:
                if record['eventName'].startswith('s3:ObjectCreated:'):
                    await self.process_object(
                        record['s3']['bucket']['name'],
                        record['s3']['object']['key']
                    )
        except Exception as e:
            logger.error(f"Error handling event: {str(e)}")

    async def process_object(self, bucket_name: str, object_name: str):
        """Process new object in MinIO."""
        if not self.matches_pattern(object_name) or object_name.endswith('.metadata.json'):
            return

        if object_name in self.processing_objects:
            return

        self.processing_objects.add(object_name)
        logger.info(f"New object detected: {bucket_name}/{object_name}")

        try:
            # Get object metadata
            metadata = await self.get_object_metadata(bucket_name, object_name)
            if metadata:
                await self.trigger_pipeline(bucket_name, object_name, metadata)
        finally:
            self.processing_objects.remove(object_name)

    async def get_object_metadata(self, bucket_name: str, object_name: str) -> Dict:
        """Get object metadata."""
        try:
            # Try to get the metadata file
            metadata_name = f"{object_name}.metadata.json"
            metadata_obj = self.minio_client.get_object(bucket_name, metadata_name)
            metadata = json.loads(metadata_obj.read().decode('utf-8'))
            
            # Add MinIO-specific information
            metadata.update({
                'bucket_name': bucket_name,
                'object_name': object_name,
                'object_url': f"s3://{bucket_name}/{object_name}"
            })
            
            return metadata
        except Exception as e:
            logger.error(f"Error getting metadata: {str(e)}")
            return None

    async def trigger_pipeline(
        self,
        bucket_name: str,
        object_name: str,
        metadata: Dict
    ):
        """Trigger pipeline via API call."""
        if not self.session:
            await self.setup()

        try:
            payload = {
                'source': {
                    'type': 's3',
                    'bucket': bucket_name,
                    'object': object_name,
                    'metadata': metadata
                },
                'trigger_time': time.time()
            }

            async with self.session.post(
                f"{self.api_url}/api/v1/pipeline/trigger",
                json=payload
            ) as response:
                if response.status == 200:
                    logger.info(
                        f"Successfully triggered pipeline for {bucket_name}/{object_name}"
                    )
                    response_data = await response.json()
                    logger.info(f"Pipeline ID: {response_data.get('pipeline_id')}")
                else:
                    logger.error(
                        f"Failed to trigger pipeline. Status: {response.status}"
                    )
        except Exception as e:
            logger.error(f"Error triggering pipeline: {str(e)}")

class MinioWatcher:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        api_url: str,
        patterns: Set[str],
        secure: bool = False
    ):
        self.minio_client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.event_handler = MinioEventHandler(
            self.minio_client,
            api_url,
            patterns
        )

    async def start(self):
        """Start watching MinIO events."""
        await self.event_handler.setup()
        logger.info("Started watching MinIO events")

        while True:
            try:
                # Poll for events (in production, use proper event notifications)
                await asyncio.sleep(5)
            except KeyboardInterrupt:
                await self.stop()
                break
            except Exception as e:
                logger.error(f"Error in event loop: {str(e)}")
                await asyncio.sleep(5)

    async def stop(self):
        """Stop watching MinIO events."""
        await self.event_handler.cleanup()
        logger.info("Stopped watching MinIO events")

async def main():
    parser = argparse.ArgumentParser(description='Watch MinIO for new objects')
    parser.add_argument('--endpoint', required=True, help='MinIO endpoint')
    parser.add_argument('--access-key', required=True, help='MinIO access key')
    parser.add_argument('--secret-key', required=True, help='MinIO secret key')
    parser.add_argument('--api-url', required=True, help='Pipeline API URL')
    parser.add_argument(
        '--patterns',
        nargs='+',
        default=['.csv'],
        help='File patterns to watch (e.g., .csv .parquet)'
    )
    parser.add_argument('--secure', action='store_true', help='Use HTTPS')
    args = parser.parse_args()

    watcher = MinioWatcher(
        args.endpoint,
        args.access_key,
        args.secret_key,
        args.api_url,
        set(args.patterns),
        args.secure
    )
    
    await watcher.start()

if __name__ == '__main__':
    asyncio.run(main())
