"""
Crawler scheduler that coordinates crawling tasks and manages the overall crawl process.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
import redis.asyncio as redis

from .url_frontier import URLFrontier, URLTask, URLPriority
from .fetcher import WebFetcher, FetchResult
from .parser import ContentParser, ParsedContent
from ..storage.database import DatabaseManager
from ..storage.duplicate_detector import DuplicateDetector
from ..utils.config import Config


@dataclass
class CrawlStats:
    """Statistics for crawl operations."""
    start_time: float
    urls_crawled: int = 0
    pages_stored: int = 0
    errors: int = 0
    duplicates_skipped: int = 0
    total_bytes_downloaded: int = 0
    average_response_time: float = 0.0
    urls_in_queue: int = 0
    
    @property
    def elapsed_time(self) -> float:
        return time.time() - self.start_time
    
    @property
    def pages_per_minute(self) -> float:
        elapsed_minutes = self.elapsed_time / 60
        return self.urls_crawled / elapsed_minutes if elapsed_minutes > 0 else 0


class CrawlerScheduler:
    """
    Main scheduler that coordinates all crawler components.
    Manages the crawl process, handles errors, and maintains statistics.
    """
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Components
        self.redis_client: Optional[redis.Redis] = None
        self.url_frontier: Optional[URLFrontier] = None
        self.fetcher: Optional[WebFetcher] = None
        self.parser: Optional[ContentParser] = None
        self.database: Optional[DatabaseManager] = None
        self.duplicate_detector: Optional[DuplicateDetector] = None
        
        # Crawl state
        self.stats = CrawlStats(start_time=time.time())
        self.is_running = False
        self.workers: List[asyncio.Task] = []
        self.max_depth = config.crawler.max_depth
        
        # Rate limiting
        self.domain_delays: Dict[str, float] = {}
        
    async def initialize(self):
        """Initialize all crawler components."""
        try:
            # Initialize Redis connection
            self.redis_client = redis.Redis(
                host=self.config.redis.host,
                port=self.config.redis.port,
                db=self.config.redis.db,
                password=self.config.redis.password,
                decode_responses=False
            )
            
            # Test Redis connection
            await self.redis_client.ping()
            self.logger.info("Redis connection established")
            
            # Initialize components
            self.url_frontier = URLFrontier(
                self.redis_client, 
                self.config.crawler.politeness_delay
            )
            await self.url_frontier.initialize()
            
            self.fetcher = WebFetcher(
                user_agent=self.config.crawler.user_agent,
                request_timeout=self.config.crawler.request_timeout,
                max_concurrent_requests=self.config.crawler.max_concurrent_requests,
                respect_robots_txt=self.config.crawler.respect_robots_txt
            )
            await self.fetcher.start()
            
            self.parser = ContentParser(
                allowed_domains=self.config.crawler.allowed_domains,
                blocked_domains=self.config.crawler.blocked_domains
            )
            
            self.database = DatabaseManager(self.config.database)
            await self.database.initialize()
            
            self.duplicate_detector = DuplicateDetector(self.redis_client)
            await self.duplicate_detector.initialize()
            
            self.logger.info("Crawler scheduler initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize crawler scheduler: {e}")
            raise
    
    async def add_seed_urls(self):
        """Add seed URLs to the frontier."""
        seed_tasks = []
        for url in self.config.crawler.seed_urls:
            task = URLTask(
                url=url,
                depth=0,
                priority=URLPriority.HIGH
            )
            seed_tasks.append(task)
        
        added_count = await self.url_frontier.add_urls(seed_tasks)
        self.logger.info(f"Added {added_count} seed URLs to frontier")
    
    async def start_crawling(self, max_pages: Optional[int] = None, 
                           max_duration: Optional[int] = None):
        """
        Start the crawling process.
        
        Args:
            max_pages: Maximum number of pages to crawl (None for unlimited)
            max_duration: Maximum duration in seconds (None for unlimited)
        """
        if self.is_running:
            self.logger.warning("Crawler is already running")
            return
        
        self.is_running = True
        self.stats = CrawlStats(start_time=time.time())
        
        try:
            # Add seed URLs if frontier is empty
            if await self.url_frontier.is_empty():
                await self.add_seed_urls()
            
            # Start worker tasks
            num_workers = min(
                self.config.crawler.max_concurrent_requests,
                10  # Cap at 10 workers
            )
            
            self.workers = []
            for i in range(num_workers):
                worker = asyncio.create_task(
                    self._worker(f"worker-{i}", max_pages, max_duration)
                )
                self.workers.append(worker)
            
            # Start statistics reporter
            stats_task = asyncio.create_task(self._stats_reporter())
            
            self.logger.info(f"Started crawling with {num_workers} workers")
            
            # Wait for workers to complete
            await asyncio.gather(*self.workers, return_exceptions=True)
            
            # Cancel stats reporter
            stats_task.cancel()
            
            # Final statistics
            await self._log_final_stats()
            
        except Exception as e:
            self.logger.error(f"Error during crawling: {e}")
        finally:
            self.is_running = False
            await self._cleanup_workers()
    
    async def _worker(self, worker_id: str, max_pages: Optional[int] = None, 
                     max_duration: Optional[int] = None):
        """
        Worker coroutine that processes URLs from the frontier.
        """
        self.logger.debug(f"Worker {worker_id} started")
        
        while self.is_running:
            try:
                # Check limits
                if max_pages and self.stats.urls_crawled >= max_pages:
                    self.logger.info(f"Reached max pages limit: {max_pages}")
                    break
                
                if max_duration and self.stats.elapsed_time >= max_duration:
                    self.logger.info(f"Reached max duration: {max_duration} seconds")
                    break
                
                # Get next URL
                url_task = await self.url_frontier.get_next_url()
                if not url_task:
                    # No URLs available, wait and try again
                    await asyncio.sleep(1)
                    continue
                
                # Check depth limit
                if url_task.depth > self.max_depth:
                    self.logger.debug(f"Skipping URL beyond max depth: {url_task.url}")
                    await self.url_frontier.mark_processed(url_task.url)
                    continue
                
                # Process the URL
                await self._process_url(url_task, worker_id)
                
            except asyncio.CancelledError:
                self.logger.debug(f"Worker {worker_id} cancelled")
                break
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {e}")
                self.stats.errors += 1
                await asyncio.sleep(1)  # Brief pause on error
        
        self.logger.debug(f"Worker {worker_id} finished")
    
    async def _process_url(self, url_task: URLTask, worker_id: str):
        """Process a single URL task."""
        start_time = time.time()
        
        try:
            # Fetch the page
            fetch_result = await self.fetcher.fetch(url_task.url)
            self.stats.urls_crawled += 1
            
            # Update response time statistics
            response_time = fetch_result.fetch_time
            self.stats.average_response_time = (
                (self.stats.average_response_time * (self.stats.urls_crawled - 1) + response_time) 
                / self.stats.urls_crawled
            )
            
            if fetch_result.error or not fetch_result.content:
                self.logger.warning(f"Failed to fetch {url_task.url}: {fetch_result.error}")
                await self.url_frontier.mark_failed(url_task, self.config.crawler.retry_attempts)
                self.stats.errors += 1
                return
            
            # Update bytes downloaded
            if fetch_result.content:
                self.stats.total_bytes_downloaded += len(fetch_result.content.encode('utf-8'))
            
            # Parse content
            parsed_content = self.parser.parse(url_task.url, fetch_result.content)
            
            # Check for duplicates
            duplicate_check = await self.duplicate_detector.check_duplicate(parsed_content)
            if self.duplicate_detector.is_duplicate(duplicate_check, strict=False):
                self.logger.debug(f"Skipping duplicate content: {url_task.url}")
                self.stats.duplicates_skipped += 1
                await self.url_frontier.mark_processed(url_task.url)
                return
            
            # Store content
            stored = await self.database.store_content(parsed_content)
            if stored:
                self.stats.pages_stored += 1
                # Add content to duplicate detector
                await self.duplicate_detector.add_content(parsed_content)
                self.logger.debug(f"Stored content: {url_task.url}")
            else:
                self.logger.warning(f"Failed to store content: {url_task.url}")
                self.stats.errors += 1
            
            # Extract and queue new URLs
            await self._queue_new_urls(parsed_content, url_task.depth + 1)
            
            # Mark as processed
            await self.url_frontier.mark_processed(url_task.url)
            
            processing_time = time.time() - start_time
            self.logger.debug(f"Processed {url_task.url} in {processing_time:.2f}s")
            
        except Exception as e:
            self.logger.error(f"Error processing {url_task.url}: {e}")
            await self.url_frontier.mark_failed(url_task, self.config.crawler.retry_attempts)
            self.stats.errors += 1
    
    async def _queue_new_urls(self, parsed_content: ParsedContent, depth: int):
        """Queue new URLs found in parsed content."""
        if depth > self.max_depth:
            return
        
        new_tasks = []
        for link in parsed_content.links:
            task = URLTask(
                url=link,
                depth=depth,
                priority=URLPriority.NORMAL,
                parent_url=parsed_content.url
            )
            new_tasks.append(task)
        
        if new_tasks:
            added_count = await self.url_frontier.add_urls(new_tasks)
            self.logger.debug(f"Queued {added_count} new URLs from {parsed_content.url}")
    
    async def _stats_reporter(self):
        """Periodically log crawl statistics."""
        while self.is_running:
            try:
                await asyncio.sleep(30)  # Report every 30 seconds
                await self._log_current_stats()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in stats reporter: {e}")
    
    async def _log_current_stats(self):
        """Log current crawl statistics."""
        # Update queue size
        frontier_stats = await self.url_frontier.get_stats()
        self.stats.urls_in_queue = frontier_stats['total_queued']
        
        self.logger.info(
            f"Crawl Progress: "
            f"Crawled={self.stats.urls_crawled}, "
            f"Stored={self.stats.pages_stored}, "
            f"Queued={self.stats.urls_in_queue}, "
            f"Errors={self.stats.errors}, "
            f"Duplicates={self.stats.duplicates_skipped}, "
            f"Rate={self.stats.pages_per_minute:.1f} pages/min, "
            f"AvgTime={self.stats.average_response_time:.2f}s"
        )
    
    async def _log_final_stats(self):
        """Log final crawl statistics."""
        frontier_stats = await self.url_frontier.get_stats()
        fetcher_stats = self.fetcher.get_stats()
        duplicate_stats = self.duplicate_detector.get_stats()
        db_stats = await self.database.get_stats()
        
        self.logger.info("=== CRAWL COMPLETED ===")
        self.logger.info(f"Total URLs crawled: {self.stats.urls_crawled}")
        self.logger.info(f"Pages stored: {self.stats.pages_stored}")
        self.logger.info(f"Duplicates skipped: {self.stats.duplicates_skipped}")
        self.logger.info(f"Errors: {self.stats.errors}")
        self.logger.info(f"Total time: {self.stats.elapsed_time:.2f} seconds")
        self.logger.info(f"Average rate: {self.stats.pages_per_minute:.1f} pages/min")
        self.logger.info(f"Data downloaded: {self.stats.total_bytes_downloaded / 1024 / 1024:.1f} MB")
        self.logger.info(f"URLs remaining in queue: {frontier_stats['total_queued']}")
        self.logger.info(f"Domains processed: {frontier_stats['total_domains']}")
        self.logger.info(f"Fetcher stats: {fetcher_stats}")
        self.logger.info(f"Duplicate detection stats: {duplicate_stats}")
        self.logger.info(f"Database stats: {db_stats}")
    
    async def stop_crawling(self):
        """Stop the crawling process gracefully."""
        self.logger.info("Stopping crawler...")
        self.is_running = False
        await self._cleanup_workers()
    
    async def _cleanup_workers(self):
        """Cancel and cleanup worker tasks."""
        if self.workers:
            for worker in self.workers:
                if not worker.done():
                    worker.cancel()
            
            # Wait for workers to finish
            await asyncio.gather(*self.workers, return_exceptions=True)
            self.workers.clear()
    
    async def close(self):
        """Close all connections and cleanup resources."""
        try:
            if self.is_running:
                await self.stop_crawling()
            
            if self.fetcher:
                await self.fetcher.close()
            
            if self.database:
                await self.database.close()
            
            if self.url_frontier:
                await self.url_frontier.cleanup()
            
            if self.redis_client:
                await self.redis_client.close()
            
            self.logger.info("Crawler scheduler closed")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def get_stats(self) -> Dict:
        """Get current crawl statistics."""
        return {
            'urls_crawled': self.stats.urls_crawled,
            'pages_stored': self.stats.pages_stored,
            'errors': self.stats.errors,
            'duplicates_skipped': self.stats.duplicates_skipped,
            'elapsed_time': self.stats.elapsed_time,
            'pages_per_minute': self.stats.pages_per_minute,
            'average_response_time': self.stats.average_response_time,
            'total_bytes_downloaded': self.stats.total_bytes_downloaded,
            'urls_in_queue': self.stats.urls_in_queue,
            'is_running': self.is_running
        }