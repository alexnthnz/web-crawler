"""
Web page fetcher implementation with robots.txt support and rate limiting.
"""

import asyncio
import aiohttp
import logging
import time
from typing import Optional, Dict, Set
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from dataclasses import dataclass
from aiohttp import ClientSession, ClientTimeout, ClientError


@dataclass
class FetchResult:
    """Result of a fetch operation."""
    url: str
    status_code: int
    content: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    error: Optional[str] = None
    fetch_time: float = 0.0
    content_type: Optional[str] = None
    encoding: Optional[str] = None


class RobotsChecker:
    """Manages robots.txt checking for domains."""
    
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self.robots_cache: Dict[str, RobotFileParser] = {}
        self.robots_check_time: Dict[str, float] = {}
        self.cache_ttl = 3600  # 1 hour cache TTL
        self.logger = logging.getLogger(__name__)
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    async def can_fetch(self, url: str, session: ClientSession) -> bool:
        """Check if URL can be fetched according to robots.txt."""
        try:
            domain = self._get_domain(url)
            current_time = time.time()
            
            # Check if we have a cached robots.txt that's still valid
            if (domain in self.robots_cache and 
                domain in self.robots_check_time and
                current_time - self.robots_check_time[domain] < self.cache_ttl):
                rp = self.robots_cache[domain]
                return rp.can_fetch(self.user_agent, url)
            
            # Fetch robots.txt
            robots_url = urljoin(domain, '/robots.txt')
            try:
                async with session.get(robots_url, timeout=10) as response:
                    if response.status == 200:
                        robots_content = await response.text()
                        rp = RobotFileParser()
                        rp.set_url(robots_url)
                        rp.read_robots_txt(robots_content)
                    else:
                        # If robots.txt doesn't exist, allow all
                        rp = RobotFileParser()
                        rp.set_url(robots_url)
                        rp.read_robots_txt("")  # Empty robots.txt allows everything
                
                self.robots_cache[domain] = rp
                self.robots_check_time[domain] = current_time
                
                return rp.can_fetch(self.user_agent, url)
                
            except Exception as e:
                self.logger.warning(f"Could not fetch robots.txt for {domain}: {e}")
                # If we can't fetch robots.txt, allow by default
                return True
                
        except Exception as e:
            self.logger.error(f"Error checking robots.txt for {url}: {e}")
            # If there's an error, allow by default
            return True


class WebFetcher:
    """
    Fetches web pages with rate limiting, robots.txt compliance, and error handling.
    """
    
    def __init__(self, user_agent: str, request_timeout: int = 30, 
                 max_concurrent_requests: int = 10, respect_robots_txt: bool = True):
        self.user_agent = user_agent
        self.request_timeout = request_timeout
        self.max_concurrent_requests = max_concurrent_requests
        self.respect_robots_txt = respect_robots_txt
        
        self.logger = logging.getLogger(__name__)
        self.robots_checker = RobotsChecker(user_agent) if respect_robots_txt else None
        
        # Session management
        self.session: Optional[ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Statistics
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'robots_blocked': 0,
            'total_bytes_downloaded': 0
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def start(self):
        """Initialize the fetcher session."""
        if self.session is None:
            timeout = ClientTimeout(total=self.request_timeout)
            headers = {'User-Agent': self.user_agent}
            
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
                connector=aiohttp.TCPConnector(
                    limit=self.max_concurrent_requests * 2,
                    limit_per_host=10,
                    ttl_dns_cache=300,
                    use_dns_cache=True
                )
            )
            self.logger.info("WebFetcher session started")
    
    async def close(self):
        """Close the fetcher session."""
        if self.session:
            await self.session.close()
            self.session = None
            self.logger.info("WebFetcher session closed")
    
    async def fetch(self, url: str) -> FetchResult:
        """
        Fetch a single URL.
        
        Args:
            url: The URL to fetch
            
        Returns:
            FetchResult object containing the response data or error information
        """
        start_time = time.time()
        
        async with self.semaphore:
            # Check robots.txt if enabled
            if self.respect_robots_txt and self.robots_checker:
                if not await self.robots_checker.can_fetch(url, self.session):
                    self.stats['robots_blocked'] += 1
                    self.logger.info(f"Robots.txt blocks access to: {url}")
                    return FetchResult(
                        url=url,
                        status_code=403,
                        error="Blocked by robots.txt",
                        fetch_time=time.time() - start_time
                    )
            
            try:
                self.stats['total_requests'] += 1
                
                async with self.session.get(url) as response:
                    fetch_time = time.time() - start_time
                    
                    # Get response headers
                    headers = dict(response.headers)
                    content_type = response.headers.get('content-type', '').lower()
                    
                    # Only download text content
                    if not self._is_text_content(content_type):
                        self.logger.debug(f"Skipping non-text content: {url} ({content_type})")
                        return FetchResult(
                            url=url,
                            status_code=response.status,
                            headers=headers,
                            content_type=content_type,
                            error="Non-text content type",
                            fetch_time=fetch_time
                        )
                    
                    # Read content with size limit
                    content = await self._read_content_safely(response)
                    
                    if content:
                        self.stats['total_bytes_downloaded'] += len(content)
                        self.stats['successful_requests'] += 1
                    
                    result = FetchResult(
                        url=url,
                        status_code=response.status,
                        content=content,
                        headers=headers,
                        content_type=content_type,
                        encoding=response.charset,
                        fetch_time=fetch_time
                    )
                    
                    self.logger.debug(f"Fetched {url}: {response.status} ({len(content) if content else 0} bytes)")
                    return result
                    
            except asyncio.TimeoutError:
                self.stats['failed_requests'] += 1
                error_msg = "Request timeout"
                self.logger.warning(f"Timeout fetching {url}")
                
            except ClientError as e:
                self.stats['failed_requests'] += 1
                error_msg = f"Client error: {str(e)}"
                self.logger.warning(f"Client error fetching {url}: {e}")
                
            except Exception as e:
                self.stats['failed_requests'] += 1
                error_msg = f"Unexpected error: {str(e)}"
                self.logger.error(f"Unexpected error fetching {url}: {e}")
            
            return FetchResult(
                url=url,
                status_code=0,
                error=error_msg,
                fetch_time=time.time() - start_time
            )
    
    def _is_text_content(self, content_type: str) -> bool:
        """Check if content type is text-based."""
        text_types = [
            'text/html',
            'text/plain',
            'text/xml',
            'application/xml',
            'application/xhtml+xml',
            'application/json',
            'application/ld+json'
        ]
        
        return any(text_type in content_type for text_type in text_types)
    
    async def _read_content_safely(self, response, max_size: int = 10 * 1024 * 1024) -> Optional[str]:
        """
        Safely read response content with size limit.
        
        Args:
            response: aiohttp response object
            max_size: Maximum content size in bytes (default 10MB)
            
        Returns:
            Content string or None if too large or error
        """
        try:
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > max_size:
                self.logger.warning(f"Content too large ({content_length} bytes): {response.url}")
                return None
            
            # Read content in chunks to respect size limit
            content_bytes = b''
            async for chunk in response.content.iter_chunked(8192):
                content_bytes += chunk
                if len(content_bytes) > max_size:
                    self.logger.warning(f"Content exceeded size limit during reading: {response.url}")
                    return None
            
            # Decode content
            encoding = response.charset or 'utf-8'
            try:
                return content_bytes.decode(encoding)
            except UnicodeDecodeError:
                # Try common encodings
                for fallback_encoding in ['utf-8', 'latin-1', 'cp1252']:
                    try:
                        return content_bytes.decode(fallback_encoding)
                    except UnicodeDecodeError:
                        continue
                
                # If all else fails, decode with errors ignored
                return content_bytes.decode('utf-8', errors='ignore')
                
        except Exception as e:
            self.logger.error(f"Error reading content from {response.url}: {e}")
            return None
    
    async def fetch_multiple(self, urls: list) -> list[FetchResult]:
        """
        Fetch multiple URLs concurrently.
        
        Args:
            urls: List of URLs to fetch
            
        Returns:
            List of FetchResult objects
        """
        tasks = [self.fetch(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that occurred
        fetch_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Exception fetching {urls[i]}: {result}")
                fetch_results.append(FetchResult(
                    url=urls[i],
                    status_code=0,
                    error=str(result),
                    fetch_time=0.0
                ))
            else:
                fetch_results.append(result)
        
        return fetch_results
    
    def get_stats(self) -> Dict[str, int]:
        """Get fetcher statistics."""
        return self.stats.copy()
    
    def reset_stats(self):
        """Reset statistics counters."""
        for key in self.stats:
            self.stats[key] = 0