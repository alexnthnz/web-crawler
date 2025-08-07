"""
URL Frontier implementation for managing URLs to crawl.
Implements politeness policies and priority scheduling.
"""

import asyncio
import logging
import time
from typing import Dict, Set, Optional, List, Tuple
from urllib.parse import urlparse
from dataclasses import dataclass, field
from collections import defaultdict, deque
import redis.asyncio as redis
import json
from enum import Enum


class URLPriority(Enum):
    """URL priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class URLTask:
    """Represents a URL crawling task."""
    url: str
    depth: int
    priority: URLPriority = URLPriority.NORMAL
    parent_url: Optional[str] = None
    discovered_time: float = field(default_factory=time.time)
    retry_count: int = 0
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            'url': self.url,
            'depth': self.depth,
            'priority': self.priority.value,
            'parent_url': self.parent_url,
            'discovered_time': self.discovered_time,
            'retry_count': self.retry_count
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'URLTask':
        """Create URLTask from dictionary."""
        return cls(
            url=data['url'],
            depth=data['depth'],
            priority=URLPriority(data['priority']),
            parent_url=data.get('parent_url'),
            discovered_time=data.get('discovered_time', time.time()),
            retry_count=data.get('retry_count', 0)
        )


class URLFrontier:
    """
    Manages URLs to be crawled with politeness policies.
    Implements per-domain queues and rate limiting.
    """
    
    def __init__(self, redis_client: redis.Redis, politeness_delay: float = 1.0):
        self.redis_client = redis_client
        self.politeness_delay = politeness_delay
        self.logger = logging.getLogger(__name__)
        
        # In-memory structures for fast access
        self.domain_queues: Dict[str, deque] = defaultdict(deque)
        self.domain_last_access: Dict[str, float] = {}
        self.processed_urls: Set[str] = set()
        
        # Redis keys
        self.frontier_key = "crawler:url_frontier"
        self.processed_key = "crawler:processed_urls"
        self.domain_queue_prefix = "crawler:domain:"
        
    async def initialize(self):
        """Initialize the URL frontier and load state from Redis."""
        try:
            # Load processed URLs from Redis
            processed_urls = await self.redis_client.smembers(self.processed_key)
            self.processed_urls = {url.decode('utf-8') for url in processed_urls}
            
            # Load domain queues from Redis
            domains = await self.redis_client.keys(f"{self.domain_queue_prefix}*")
            for domain_key in domains:
                domain = domain_key.decode('utf-8').split(':')[-1]
                queue_data = await self.redis_client.lrange(domain_key, 0, -1)
                for item in queue_data:
                    task_dict = json.loads(item.decode('utf-8'))
                    task = URLTask.from_dict(task_dict)
                    self.domain_queues[domain].append(task)
            
            self.logger.info(f"Initialized URL frontier with {len(self.processed_urls)} processed URLs")
            self.logger.info(f"Loaded queues for {len(self.domain_queues)} domains")
            
        except Exception as e:
            self.logger.error(f"Error initializing URL frontier: {e}")
            raise
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except Exception:
            return "unknown"
    
    async def add_url(self, task: URLTask) -> bool:
        """
        Add a URL to the frontier.
        Returns True if URL was added, False if already processed.
        """
        if task.url in self.processed_urls:
            return False
        
        domain = self._get_domain(task.url)
        
        # Add to in-memory queue
        self.domain_queues[domain].append(task)
        
        # Persist to Redis
        try:
            await self.redis_client.rpush(
                f"{self.domain_queue_prefix}{domain}",
                json.dumps(task.to_dict())
            )
            self.logger.debug(f"Added URL to frontier: {task.url}")
            return True
        except Exception as e:
            self.logger.error(f"Error adding URL to Redis: {e}")
            return False
    
    async def add_urls(self, tasks: List[URLTask]) -> int:
        """Add multiple URLs to the frontier. Returns count of added URLs."""
        added_count = 0
        for task in tasks:
            if await self.add_url(task):
                added_count += 1
        return added_count
    
    async def get_next_url(self) -> Optional[URLTask]:
        """
        Get the next URL to crawl, respecting politeness policies.
        Returns None if no URLs are available or all domains are rate-limited.
        """
        current_time = time.time()
        
        # Sort domains by priority and last access time
        available_domains = []
        for domain, queue in self.domain_queues.items():
            if not queue:
                continue
            
            last_access = self.domain_last_access.get(domain, 0)
            time_since_access = current_time - last_access
            
            if time_since_access >= self.politeness_delay:
                # Get highest priority task from this domain
                highest_priority = max(queue, key=lambda t: t.priority.value)
                available_domains.append((domain, highest_priority, time_since_access))
        
        if not available_domains:
            return None
        
        # Sort by priority, then by time since last access
        available_domains.sort(key=lambda x: (x[1].priority.value, x[2]), reverse=True)
        
        # Get task from the best available domain
        selected_domain = available_domains[0][0]
        queue = self.domain_queues[selected_domain]
        
        # Find and remove the highest priority task
        best_task = None
        best_index = -1
        for i, task in enumerate(queue):
            if best_task is None or task.priority.value > best_task.priority.value:
                best_task = task
                best_index = i
        
        if best_task and best_index >= 0:
            # Remove from in-memory queue
            del queue[best_index]
            
            # Update last access time
            self.domain_last_access[selected_domain] = current_time
            
            # Remove from Redis
            try:
                await self.redis_client.lrem(
                    f"{self.domain_queue_prefix}{selected_domain}",
                    1,
                    json.dumps(best_task.to_dict())
                )
            except Exception as e:
                self.logger.error(f"Error removing URL from Redis: {e}")
            
            self.logger.debug(f"Retrieved URL from frontier: {best_task.url}")
            return best_task
        
        return None
    
    async def mark_processed(self, url: str):
        """Mark a URL as processed."""
        self.processed_urls.add(url)
        try:
            await self.redis_client.sadd(self.processed_key, url)
            self.logger.debug(f"Marked URL as processed: {url}")
        except Exception as e:
            self.logger.error(f"Error marking URL as processed: {e}")
    
    async def mark_failed(self, task: URLTask, max_retries: int = 3):
        """
        Mark a URL as failed and optionally retry.
        Returns True if task was re-queued for retry.
        """
        if task.retry_count < max_retries:
            task.retry_count += 1
            task.priority = URLPriority.LOW  # Lower priority for retries
            await self.add_url(task)
            self.logger.info(f"Retrying URL ({task.retry_count}/{max_retries}): {task.url}")
            return True
        else:
            self.logger.warning(f"URL failed permanently after {max_retries} retries: {task.url}")
            await self.mark_processed(task.url)  # Mark as processed to avoid infinite retries
            return False
    
    async def get_stats(self) -> Dict[str, int]:
        """Get frontier statistics."""
        total_queued = sum(len(queue) for queue in self.domain_queues.values())
        return {
            'total_queued': total_queued,
            'domains_with_urls': len([d for d, q in self.domain_queues.items() if q]),
            'total_processed': len(self.processed_urls),
            'total_domains': len(self.domain_queues)
        }
    
    async def is_empty(self) -> bool:
        """Check if the frontier is empty."""
        return all(len(queue) == 0 for queue in self.domain_queues.values())
    
    async def cleanup(self):
        """Clean up empty domain queues."""
        empty_domains = [domain for domain, queue in self.domain_queues.items() if not queue]
        for domain in empty_domains:
            del self.domain_queues[domain]
            # Clean up Redis keys
            try:
                await self.redis_client.delete(f"{self.domain_queue_prefix}{domain}")
            except Exception as e:
                self.logger.error(f"Error cleaning up Redis key for domain {domain}: {e}")
        
        if empty_domains:
            self.logger.info(f"Cleaned up {len(empty_domains)} empty domain queues")