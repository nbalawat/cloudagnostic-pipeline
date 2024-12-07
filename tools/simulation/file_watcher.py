#!/usr/bin/env python3
import os
import time
import json
import logging
import argparse
import asyncio
import aiohttp
import datetime
from pathlib import Path
from typing import Dict, Set
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FileEventHandler(FileSystemEventHandler):
    def __init__(self, api_url: str, patterns: Set[str]):
        self.api_url = api_url
        self.patterns = patterns
        self.processing_files: Set[str] = set()
        self.session = None

    async def setup(self):
        """Setup async HTTP session."""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def cleanup(self):
        """Cleanup resources."""
        if self.session is not None:
            await self.session.close()
            self.session = None

    def matches_pattern(self, filename: str) -> bool:
        """Check if filename matches any of the patterns."""
        return any(
            filename.endswith(pattern) for pattern in self.patterns
        )

    async def trigger_pipeline(self, filepath: str):
        """Trigger pipeline via API call."""
        if not self.session:
            await self.setup()

        file_info = self._get_file_info(filepath)
        if not file_info:
            return

        try:
            async with self.session.post(
                f"{self.api_url}/api/v1/pipeline/trigger",
                json=file_info
            ) as response:
                if response.status == 200:
                    logger.info(f"Successfully triggered pipeline for {filepath}")
                    response_data = await response.json()
                    logger.info(f"Pipeline ID: {response_data.get('pipeline_id')}")
                else:
                    logger.error(
                        f"Failed to trigger pipeline for {filepath}. "
                        f"Status: {response.status}"
                    )
        except Exception as e:
            logger.error(f"Error triggering pipeline: {str(e)}")

    def _get_file_info(self, filepath: str) -> Dict:
        """Get file information including metadata if available."""
        try:
            file_path = Path(filepath)
            metadata_path = file_path.with_suffix('.metadata.json')
            
            file_info = {
                'filepath': str(file_path),
                'filename': file_path.name,
                'file_size': os.path.getsize(filepath),
                'creation_time': datetime.datetime.fromtimestamp(
                    os.path.getctime(filepath)
                ).isoformat(),
                'modification_time': datetime.datetime.fromtimestamp(
                    os.path.getmtime(filepath)
                ).isoformat()
            }

            # Add metadata if available
            if metadata_path.exists():
                with open(metadata_path) as f:
                    metadata = json.load(f)
                file_info['metadata'] = metadata

            return file_info
        except Exception as e:
            logger.error(f"Error getting file info: {str(e)}")
            return None

    def on_created(self, event):
        """Handle file creation event."""
        if event.is_directory:
            return

        filepath = event.src_path
        if not self.matches_pattern(filepath):
            return

        if filepath in self.processing_files:
            return

        self.processing_files.add(filepath)
        logger.info(f"New file detected: {filepath}")

        # Wait for file to be fully written
        time.sleep(1)

        # Trigger pipeline
        asyncio.run(self.trigger_pipeline(filepath))
        self.processing_files.remove(filepath)

class FileWatcher:
    def __init__(self, watch_dir: str, api_url: str, patterns: Set[str]):
        self.watch_dir = watch_dir
        self.event_handler = FileEventHandler(api_url, patterns)
        self.observer = Observer()

    def start(self):
        """Start watching directory."""
        self.observer.schedule(
            self.event_handler,
            self.watch_dir,
            recursive=True
        )
        self.observer.start()
        logger.info(f"Started watching directory: {self.watch_dir}")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop watching directory."""
        self.observer.stop()
        self.observer.join()
        asyncio.run(self.event_handler.cleanup())
        logger.info("Stopped watching directory")

def main():
    parser = argparse.ArgumentParser(description='Watch directory for new files')
    parser.add_argument('--watch-dir', required=True, help='Directory to watch')
    parser.add_argument('--api-url', required=True, help='Pipeline API URL')
    parser.add_argument(
        '--patterns',
        nargs='+',
        default=['.csv'],
        help='File patterns to watch (e.g., .csv .parquet)'
    )
    args = parser.parse_args()

    # Create watch directory if it doesn't exist
    os.makedirs(args.watch_dir, exist_ok=True)

    # Start file watcher
    watcher = FileWatcher(args.watch_dir, args.api_url, set(args.patterns))
    watcher.start()

if __name__ == '__main__':
    main()
