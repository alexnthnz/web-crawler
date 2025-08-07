"""
Duplicate content detection using various hashing techniques.
"""

import hashlib
import logging
from typing import Set, Optional, Dict, Any
from dataclasses import dataclass
import redis.asyncio as redis
import json
import time
from ..crawler.parser import ParsedContent


@dataclass
class ContentHash:
    """Container for different types of content hashes."""
    url_hash: str
    content_hash: Optional[str] = None
    title_hash: Optional[str] = None
    fuzzy_hash: Optional[str] = None
    timestamp: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            'url_hash': self.url_hash,
            'content_hash': self.content_hash,
            'title_hash': self.title_hash,
            'fuzzy_hash': self.fuzzy_hash,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ContentHash':
        """Create ContentHash from dictionary."""
        return cls(
            url_hash=data['url_hash'],
            content_hash=data.get('content_hash'),
            title_hash=data.get('title_hash'),
            fuzzy_hash=data.get('fuzzy_hash'),
            timestamp=data.get('timestamp', time.time())
        )


class DuplicateDetector:
    """
    Detects duplicate content using multiple hashing strategies.
    
    Uses:
    - URL normalization and hashing
    - Content hashing (exact duplicates)
    - Title hashing (similar pages)
    - Fuzzy hashing (near duplicates)
    """
    
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        
        # Redis keys for different hash types
        self.url_hashes_key = "crawler:duplicates:urls"
        self.content_hashes_key = "crawler:duplicates:content"
        self.title_hashes_key = "crawler:duplicates:titles"
        self.fuzzy_hashes_key = "crawler:duplicates:fuzzy"
        
        # In-memory cache for fast lookups
        self.url_hashes: Set[str] = set()
        self.content_hashes: Set[str] = set()
        self.title_hashes: Set[str] = set()
        self.fuzzy_hashes: Set[str] = set()
        
        # Statistics
        self.stats = {
            'total_checks': 0,
            'url_duplicates': 0,
            'content_duplicates': 0,
            'title_duplicates': 0,
            'fuzzy_duplicates': 0
        }
    
    async def initialize(self):
        """Initialize the duplicate detector by loading existing hashes."""
        try:
            # Load URL hashes
            url_hashes = await self.redis_client.smembers(self.url_hashes_key)
            self.url_hashes = {h.decode('utf-8') for h in url_hashes}
            
            # Load content hashes
            content_hashes = await self.redis_client.smembers(self.content_hashes_key)
            self.content_hashes = {h.decode('utf-8') for h in content_hashes}
            
            # Load title hashes
            title_hashes = await self.redis_client.smembers(self.title_hashes_key)
            self.title_hashes = {h.decode('utf-8') for h in title_hashes}
            
            # Load fuzzy hashes
            fuzzy_hashes = await self.redis_client.smembers(self.fuzzy_hashes_key)
            self.fuzzy_hashes = {h.decode('utf-8') for h in fuzzy_hashes}
            
            self.logger.info(f"Initialized duplicate detector with {len(self.url_hashes)} URL hashes, "
                           f"{len(self.content_hashes)} content hashes, "
                           f"{len(self.title_hashes)} title hashes, "
                           f"{len(self.fuzzy_hashes)} fuzzy hashes")
            
        except Exception as e:
            self.logger.error(f"Error initializing duplicate detector: {e}")
            raise
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL for consistent hashing."""
        # Remove common tracking parameters
        tracking_params = [
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'fbclid', 'gclid', 'ref', 'source', 'campaign'
        ]
        
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        
        try:
            parsed = urlparse(url.lower())
            
            # Remove tracking parameters
            if parsed.query:
                params = parse_qs(parsed.query, keep_blank_values=False)
                filtered_params = {k: v for k, v in params.items() 
                                 if k not in tracking_params}
                new_query = urlencode(sorted(filtered_params.items()), doseq=True)
            else:
                new_query = ''
            
            # Normalize path (remove trailing slash unless it's root)
            path = parsed.path
            if len(path) > 1 and path.endswith('/'):
                path = path[:-1]
            
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                path,
                parsed.params,
                new_query,
                ''  # Remove fragment
            ))
            
            return normalized
            
        except Exception:
            return url.lower()
    
    def _hash_content(self, content: str) -> str:
        """Create hash from content."""
        if not content:
            return ""
        
        # Normalize content for hashing
        normalized = ' '.join(content.lower().split())
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()
    
    def _hash_title(self, title: str) -> str:
        """Create hash from title."""
        if not title:
            return ""
        
        # Normalize title for hashing
        normalized = ' '.join(title.lower().split())
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    def _create_fuzzy_hash(self, content: str, title: str = "") -> str:
        """
        Create fuzzy hash for near-duplicate detection.
        Uses combination of content length, word count, and key phrases.
        """
        if not content:
            return ""
        
        # Extract features for fuzzy matching
        words = content.lower().split()
        word_count = len(words)
        char_count = len(content)
        
        # Get most common words (excluding stop words)
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
            'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
            'before', 'after', 'above', 'below', 'is', 'are', 'was', 'were', 'be',
            'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'could', 'should', 'may', 'might', 'must', 'can', 'this',
            'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they'
        }
        
        significant_words = [w for w in words if len(w) > 3 and w not in stop_words]
        
        # Create feature vector
        features = {
            'word_count_bucket': word_count // 100,  # Bucket word count
            'char_count_bucket': char_count // 1000,  # Bucket character count
            'title_words': len(title.split()) if title else 0,
            'significant_words': len(significant_words),
        }
        
        # Add most frequent significant words (up to 10)
        if significant_words:
            word_freq = {}
            for word in significant_words:
                word_freq[word] = word_freq.get(word, 0) + 1
            
            top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
            features['top_words'] = [word for word, _ in top_words]
        else:
            features['top_words'] = []
        
        # Create hash from features
        feature_str = json.dumps(features, sort_keys=True)
        return hashlib.md5(feature_str.encode('utf-8')).hexdigest()
    
    async def check_duplicate(self, parsed_content: ParsedContent) -> Dict[str, bool]:
        """
        Check if content is duplicate using multiple strategies.
        
        Returns:
            Dictionary with duplicate check results for each strategy
        """
        self.stats['total_checks'] += 1
        
        # Generate hashes
        url_hash = self._hash_content(self._normalize_url(parsed_content.url))
        content_hash = self._hash_content(parsed_content.content or "")
        title_hash = self._hash_title(parsed_content.title or "")
        fuzzy_hash = self._create_fuzzy_hash(
            parsed_content.content or "", 
            parsed_content.title or ""
        )
        
        # Check for duplicates
        results = {
            'url_duplicate': url_hash in self.url_hashes,
            'content_duplicate': content_hash in self.content_hashes if content_hash else False,
            'title_duplicate': title_hash in self.title_hashes if title_hash else False,
            'fuzzy_duplicate': fuzzy_hash in self.fuzzy_hashes if fuzzy_hash else False
        }
        
        # Update statistics
        if results['url_duplicate']:
            self.stats['url_duplicates'] += 1
        if results['content_duplicate']:
            self.stats['content_duplicates'] += 1
        if results['title_duplicate']:
            self.stats['title_duplicates'] += 1
        if results['fuzzy_duplicate']:
            self.stats['fuzzy_duplicates'] += 1
        
        # Log duplicate detection
        duplicate_types = [k for k, v in results.items() if v]
        if duplicate_types:
            self.logger.info(f"Duplicate detected for {parsed_content.url}: {duplicate_types}")
        
        return results
    
    async def add_content(self, parsed_content: ParsedContent):
        """Add content hashes to the duplicate detection system."""
        # Generate hashes
        url_hash = self._hash_content(self._normalize_url(parsed_content.url))
        content_hash = self._hash_content(parsed_content.content or "")
        title_hash = self._hash_title(parsed_content.title or "")
        fuzzy_hash = self._create_fuzzy_hash(
            parsed_content.content or "", 
            parsed_content.title or ""
        )
        
        try:
            # Add to in-memory sets
            self.url_hashes.add(url_hash)
            if content_hash:
                self.content_hashes.add(content_hash)
            if title_hash:
                self.title_hashes.add(title_hash)
            if fuzzy_hash:
                self.fuzzy_hashes.add(fuzzy_hash)
            
            # Add to Redis
            await self.redis_client.sadd(self.url_hashes_key, url_hash)
            if content_hash:
                await self.redis_client.sadd(self.content_hashes_key, content_hash)
            if title_hash:
                await self.redis_client.sadd(self.title_hashes_key, title_hash)
            if fuzzy_hash:
                await self.redis_client.sadd(self.fuzzy_hashes_key, fuzzy_hash)
            
            self.logger.debug(f"Added content hashes for {parsed_content.url}")
            
        except Exception as e:
            self.logger.error(f"Error adding content hashes: {e}")
    
    def is_duplicate(self, duplicate_results: Dict[str, bool], 
                    strict: bool = False) -> bool:
        """
        Determine if content should be considered duplicate.
        
        Args:
            duplicate_results: Results from check_duplicate()
            strict: If True, only consider exact duplicates. If False, include fuzzy duplicates.
            
        Returns:
            True if content should be considered duplicate
        """
        if strict:
            return (duplicate_results['url_duplicate'] or 
                   duplicate_results['content_duplicate'])
        else:
            return (duplicate_results['url_duplicate'] or 
                   duplicate_results['content_duplicate'] or 
                   duplicate_results['fuzzy_duplicate'])
    
    def get_stats(self) -> Dict[str, int]:
        """Get duplicate detection statistics."""
        total_hashes = {
            'total_url_hashes': len(self.url_hashes),
            'total_content_hashes': len(self.content_hashes),
            'total_title_hashes': len(self.title_hashes),
            'total_fuzzy_hashes': len(self.fuzzy_hashes)
        }
        
        return {**self.stats, **total_hashes}
    
    async def cleanup_old_hashes(self, max_age_days: int = 30):
        """Clean up old hashes to prevent memory bloat."""
        # This is a simplified cleanup - in production you might want
        # to store timestamps with hashes and clean based on age
        current_size = len(self.url_hashes)
        max_size = 1000000  # 1M hashes max
        
        if current_size > max_size:
            self.logger.warning(f"Hash cache size ({current_size}) exceeds maximum ({max_size})")
            # In a real implementation, you'd implement LRU or time-based cleanup
            # For now, just log the warning