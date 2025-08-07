#!/usr/bin/env python3
"""
Main entry point for the web crawler system.
"""

import asyncio
import argparse
import logging
import signal
import sys
from pathlib import Path
from typing import Optional

# Add src to Python path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from src.utils.config import load_config, Config
from src.crawler.scheduler import CrawlerScheduler


class CrawlerApp:
    """Main application class for the web crawler."""
    
    def __init__(self):
        self.scheduler: Optional[CrawlerScheduler] = None
        self.logger = logging.getLogger(__name__)
        self._shutdown_event = asyncio.Event()
    
    def setup_logging(self, config: Config):
        """Setup logging configuration."""
        # Create logs directory if it doesn't exist
        log_file = Path(config.logging.file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, config.logging.level.upper()),
            format=config.logging.format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Set third-party library log levels
        logging.getLogger('aiohttp').setLevel(logging.WARNING)
        logging.getLogger('cassandra').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        self.logger.info("Logging configured")
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating shutdown...")
            self._shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def run(self, config_path: str, max_pages: Optional[int] = None, 
                  max_duration: Optional[int] = None, dry_run: bool = False):
        """Run the web crawler."""
        try:
            # Load configuration
            config = load_config(config_path)
            self.setup_logging(config)
            self.setup_signal_handlers()
            
            self.logger.info("=== WEB CRAWLER STARTING ===")
            self.logger.info(f"Configuration loaded from: {config_path}")
            self.logger.info(f"Seed URLs: {config.crawler.seed_urls}")
            self.logger.info(f"Max depth: {config.crawler.max_depth}")
            self.logger.info(f"Max concurrent requests: {config.crawler.max_concurrent_requests}")
            self.logger.info(f"Politeness delay: {config.crawler.politeness_delay}s")
            self.logger.info(f"Database type: {config.database.type}")
            
            if dry_run:
                self.logger.info("DRY RUN MODE: No actual crawling will be performed")
                await self._dry_run(config)
                return
            
            # Initialize scheduler
            self.scheduler = CrawlerScheduler(config)
            await self.scheduler.initialize()
            
            # Start crawling with shutdown monitoring
            crawl_task = asyncio.create_task(
                self.scheduler.start_crawling(max_pages, max_duration)
            )
            
            shutdown_task = asyncio.create_task(self._shutdown_event.wait())
            
            # Wait for either crawling to complete or shutdown signal
            done, pending = await asyncio.wait(
                [crawl_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # If shutdown was requested, stop the crawler
            if shutdown_task in done:
                self.logger.info("Shutdown requested, stopping crawler...")
                if self.scheduler:
                    await self.scheduler.stop_crawling()
            
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)
            return 1
        
        finally:
            # Cleanup
            if self.scheduler:
                await self.scheduler.close()
            self.logger.info("=== WEB CRAWLER FINISHED ===")
        
        return 0
    
    async def _dry_run(self, config: Config):
        """Perform a dry run to test configuration and connections."""
        self.logger.info("Testing Redis connection...")
        try:
            import redis.asyncio as redis
            redis_client = redis.Redis(
                host=config.redis.host,
                port=config.redis.port,
                db=config.redis.db,
                password=config.redis.password
            )
            await redis_client.ping()
            await redis_client.close()
            self.logger.info("✓ Redis connection successful")
        except Exception as e:
            self.logger.error(f"✗ Redis connection failed: {e}")
        
        self.logger.info("Testing database configuration...")
        try:
            from src.storage.database import DatabaseManager
            db_manager = DatabaseManager(config.database)
            await db_manager.initialize()
            await db_manager.close()
            self.logger.info("✓ Database initialization successful")
        except Exception as e:
            self.logger.error(f"✗ Database initialization failed: {e}")
        
        self.logger.info("Testing fetcher configuration...")
        try:
            from src.crawler.fetcher import WebFetcher
            async with WebFetcher(
                user_agent=config.crawler.user_agent,
                request_timeout=config.crawler.request_timeout,
                max_concurrent_requests=1,
                respect_robots_txt=config.crawler.respect_robots_txt
            ) as fetcher:
                # Try to fetch a simple test URL
                if config.crawler.seed_urls:
                    test_url = config.crawler.seed_urls[0]
                    result = await fetcher.fetch(test_url)
                    if result.error:
                        self.logger.warning(f"Test fetch failed: {result.error}")
                    else:
                        self.logger.info(f"✓ Test fetch successful: {result.status_code}")
        except Exception as e:
            self.logger.error(f"✗ Fetcher test failed: {e}")
        
        self.logger.info("Dry run completed")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Web Crawler System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                           # Run with default config.yaml
  python main.py --config my_config.yaml  # Run with custom config
  python main.py --max-pages 1000         # Limit to 1000 pages
  python main.py --max-duration 3600      # Run for 1 hour max
  python main.py --dry-run                # Test configuration only
        """
    )
    
    parser.add_argument(
        '--config', 
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '--max-pages',
        type=int,
        help='Maximum number of pages to crawl'
    )
    
    parser.add_argument(
        '--max-duration',
        type=int,
        help='Maximum crawl duration in seconds'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Test configuration without actually crawling'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Web Crawler System 1.0.0'
    )
    
    args = parser.parse_args()
    
    # Check if config file exists
    if not Path(args.config).exists():
        print(f"Error: Configuration file '{args.config}' not found.")
        print("Please create a config.yaml file or specify a different path with --config")
        return 1
    
    # Run the crawler
    app = CrawlerApp()
    try:
        return asyncio.run(app.run(
            config_path=args.config,
            max_pages=args.max_pages,
            max_duration=args.max_duration,
            dry_run=args.dry_run
        ))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        return 1
    except Exception as e:
        print(f"Fatal error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())