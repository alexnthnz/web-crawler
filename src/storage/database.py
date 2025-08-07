"""
Database storage layer for crawled content.
Supports both Cassandra and file-based storage.
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional, Dict, List, Any
from dataclasses import asdict
from datetime import datetime

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False

from ..crawler.parser import ParsedContent
from ..utils.config import DatabaseConfig


class DatabaseError(Exception):
    """Custom exception for database operations."""
    pass


class StorageBackend:
    """Abstract base class for storage backends."""
    
    async def initialize(self):
        """Initialize the storage backend."""
        raise NotImplementedError
    
    async def store_content(self, content: ParsedContent) -> bool:
        """Store parsed content."""
        raise NotImplementedError
    
    async def get_content(self, url: str) -> Optional[ParsedContent]:
        """Retrieve content by URL."""
        raise NotImplementedError
    
    async def content_exists(self, url: str) -> bool:
        """Check if content exists for URL."""
        raise NotImplementedError
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        raise NotImplementedError
    
    async def close(self):
        """Close storage connections."""
        raise NotImplementedError


class FileStorageBackend(StorageBackend):
    """File-based storage backend for development and small-scale deployments."""
    
    def __init__(self, data_directory: str):
        self.data_directory = Path(data_directory)
        self.logger = logging.getLogger(__name__)
        self.stats = {
            'total_stored': 0,
            'storage_errors': 0,
            'total_size_bytes': 0
        }
    
    async def initialize(self):
        """Create data directory structure."""
        try:
            self.data_directory.mkdir(parents=True, exist_ok=True)
            
            # Create subdirectories for organization
            (self.data_directory / 'content').mkdir(exist_ok=True)
            (self.data_directory / 'metadata').mkdir(exist_ok=True)
            (self.data_directory / 'index').mkdir(exist_ok=True)
            
            # Load existing statistics
            stats_file = self.data_directory / 'stats.json'
            if stats_file.exists():
                with open(stats_file, 'r') as f:
                    saved_stats = json.load(f)
                    self.stats.update(saved_stats)
            
            self.logger.info(f"File storage initialized at {self.data_directory}")
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize file storage: {e}")
    
    def _get_file_path(self, url: str) -> Path:
        """Generate file path for URL."""
        import hashlib
        url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
        # Use first 2 chars for directory structure
        subdir = url_hash[:2]
        return self.data_directory / 'content' / subdir / f"{url_hash}.json"
    
    async def store_content(self, content: ParsedContent) -> bool:
        """Store content to file."""
        try:
            file_path = self._get_file_path(content.url)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Prepare data for storage
            data = asdict(content)
            data['stored_at'] = datetime.utcnow().isoformat()
            data['storage_version'] = '1.0'
            
            # Write to file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Update statistics
            self.stats['total_stored'] += 1
            self.stats['total_size_bytes'] += file_path.stat().st_size
            
            # Update index
            await self._update_index(content.url, file_path)
            
            self.logger.debug(f"Stored content to {file_path}")
            return True
            
        except Exception as e:
            self.stats['storage_errors'] += 1
            self.logger.error(f"Error storing content for {content.url}: {e}")
            return False
    
    async def get_content(self, url: str) -> Optional[ParsedContent]:
        """Retrieve content from file."""
        try:
            file_path = self._get_file_path(url)
            
            if not file_path.exists():
                return None
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Remove storage metadata
            data.pop('stored_at', None)
            data.pop('storage_version', None)
            
            return ParsedContent(**data)
            
        except Exception as e:
            self.logger.error(f"Error retrieving content for {url}: {e}")
            return None
    
    async def content_exists(self, url: str) -> bool:
        """Check if content file exists."""
        return self._get_file_path(url).exists()
    
    async def _update_index(self, url: str, file_path: Path):
        """Update URL index for faster lookups."""
        index_file = self.data_directory / 'index' / 'url_index.json'
        
        try:
            # Load existing index
            if index_file.exists():
                with open(index_file, 'r') as f:
                    index = json.load(f)
            else:
                index = {}
            
            # Add entry
            index[url] = {
                'file_path': str(file_path.relative_to(self.data_directory)),
                'indexed_at': datetime.utcnow().isoformat()
            }
            
            # Save index
            index_file.parent.mkdir(parents=True, exist_ok=True)
            with open(index_file, 'w') as f:
                json.dump(index, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            self.logger.warning(f"Error updating index: {e}")
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        # Update disk usage
        if self.data_directory.exists():
            total_size = sum(f.stat().st_size for f in self.data_directory.rglob('*') if f.is_file())
            self.stats['total_size_bytes'] = total_size
        
        return self.stats.copy()
    
    async def close(self):
        """Save statistics and cleanup."""
        try:
            stats_file = self.data_directory / 'stats.json'
            with open(stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving statistics: {e}")


class CassandraStorageBackend(StorageBackend):
    """Cassandra storage backend for production deployments."""
    
    def __init__(self, config: Dict[str, Any]):
        if not CASSANDRA_AVAILABLE:
            raise DatabaseError("Cassandra driver not available. Install cassandra-driver package.")
        
        self.config = config
        self.cluster = None
        self.session = None
        self.logger = logging.getLogger(__name__)
        self.stats = {
            'total_stored': 0,
            'storage_errors': 0,
            'query_errors': 0
        }
    
    async def initialize(self):
        """Initialize Cassandra connection and keyspace."""
        try:
            # Setup cluster connection
            hosts = self.config.get('hosts', ['localhost'])
            port = self.config.get('port', 9042)
            
            self.cluster = Cluster(
                hosts,
                port=port,
                load_balancing_policy=DCAwareRoundRobinPolicy()
            )
            
            self.session = self.cluster.connect()
            
            # Create keyspace if it doesn't exist
            keyspace = self.config.get('keyspace', 'crawler_data')
            replication_factor = self.config.get('replication_factor', 1)
            
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{
                    'class': 'SimpleStrategy',
                    'replication_factor': {replication_factor}
                }}
            """)
            
            self.session.set_keyspace(keyspace)
            
            # Create tables
            await self._create_tables()
            
            self.logger.info(f"Cassandra storage initialized with keyspace: {keyspace}")
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize Cassandra: {e}")
    
    async def _create_tables(self):
        """Create required tables."""
        # Main content table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS crawled_content (
                url_hash text PRIMARY KEY,
                url text,
                title text,
                content text,
                meta_description text,
                meta_keywords text,
                language text,
                author text,
                canonical_url text,
                word_count int,
                crawled_at timestamp,
                links list<text>,
                images list<text>,
                headings map<text, list<text>>,
                schema_org_data text
            )
        """)
        
        # URL index table for lookups
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS url_index (
                url text PRIMARY KEY,
                url_hash text,
                crawled_at timestamp,
                status text
            )
        """)
        
        # Statistics table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS crawler_stats (
                stat_type text PRIMARY KEY,
                value counter
            )
        """)
    
    def _get_url_hash(self, url: str) -> str:
        """Generate hash for URL."""
        import hashlib
        return hashlib.sha256(url.encode('utf-8')).hexdigest()
    
    async def store_content(self, content: ParsedContent) -> bool:
        """Store content to Cassandra."""
        try:
            url_hash = self._get_url_hash(content.url)
            
            # Prepare data
            schema_org_json = json.dumps(content.schema_org_data) if content.schema_org_data else None
            
            # Insert content
            self.session.execute("""
                INSERT INTO crawled_content (
                    url_hash, url, title, content, meta_description, meta_keywords,
                    language, author, canonical_url, word_count, crawled_at,
                    links, images, headings, schema_org_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                url_hash,
                content.url,
                content.title,
                content.content,
                content.meta_description,
                content.meta_keywords,
                content.language,
                content.author,
                content.canonical_url,
                content.word_count,
                datetime.utcnow(),
                content.links,
                content.images,
                content.headings,
                schema_org_json
            ))
            
            # Update URL index
            self.session.execute("""
                INSERT INTO url_index (url, url_hash, crawled_at, status)
                VALUES (?, ?, ?, ?)
            """, (content.url, url_hash, datetime.utcnow(), 'stored'))
            
            # Update statistics
            self.session.execute("""
                UPDATE crawler_stats SET value = value + 1 WHERE stat_type = 'total_stored'
            """)
            
            self.stats['total_stored'] += 1
            self.logger.debug(f"Stored content to Cassandra: {content.url}")
            return True
            
        except Exception as e:
            self.stats['storage_errors'] += 1
            self.logger.error(f"Error storing content for {content.url}: {e}")
            return False
    
    async def get_content(self, url: str) -> Optional[ParsedContent]:
        """Retrieve content from Cassandra."""
        try:
            url_hash = self._get_url_hash(url)
            
            result = self.session.execute(
                "SELECT * FROM crawled_content WHERE url_hash = ?",
                (url_hash,)
            )
            
            row = result.one()
            if not row:
                return None
            
            # Parse schema.org data
            schema_org_data = {}
            if row.schema_org_data:
                try:
                    schema_org_data = json.loads(row.schema_org_data)
                except json.JSONDecodeError:
                    pass
            
            return ParsedContent(
                url=row.url,
                title=row.title,
                content=row.content,
                meta_description=row.meta_description,
                meta_keywords=row.meta_keywords,
                language=row.language,
                author=row.author,
                canonical_url=row.canonical_url,
                word_count=row.word_count,
                links=list(row.links) if row.links else [],
                images=list(row.images) if row.images else [],
                headings=dict(row.headings) if row.headings else {},
                schema_org_data=schema_org_data
            )
            
        except Exception as e:
            self.stats['query_errors'] += 1
            self.logger.error(f"Error retrieving content for {url}: {e}")
            return None
    
    async def content_exists(self, url: str) -> bool:
        """Check if content exists in Cassandra."""
        try:
            result = self.session.execute(
                "SELECT url FROM url_index WHERE url = ?",
                (url,)
            )
            return result.one() is not None
            
        except Exception as e:
            self.logger.error(f"Error checking content existence for {url}: {e}")
            return False
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics from Cassandra."""
        try:
            # Get stats from database
            result = self.session.execute("SELECT stat_type, value FROM crawler_stats")
            db_stats = {row.stat_type: row.value for row in result}
            
            # Combine with local stats
            combined_stats = {**self.stats, **db_stats}
            return combined_stats
            
        except Exception as e:
            self.logger.error(f"Error getting stats: {e}")
            return self.stats.copy()
    
    async def close(self):
        """Close Cassandra connections."""
        if self.cluster:
            self.cluster.shutdown()
            self.logger.info("Cassandra connections closed")


class DatabaseManager:
    """Main database manager that handles different storage backends."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.backend: Optional[StorageBackend] = None
        self.logger = logging.getLogger(__name__)
    
    async def initialize(self):
        """Initialize the appropriate storage backend."""
        backend_type = self.config.type.lower()
        
        if backend_type == 'cassandra':
            self.backend = CassandraStorageBackend(self.config.cassandra)
        elif backend_type == 'file':
            self.backend = FileStorageBackend(self.config.file['data_directory'])
        else:
            raise DatabaseError(f"Unknown database type: {backend_type}")
        
        await self.backend.initialize()
        self.logger.info(f"Database manager initialized with {backend_type} backend")
    
    async def store_content(self, content: ParsedContent) -> bool:
        """Store parsed content."""
        if not self.backend:
            raise DatabaseError("Database not initialized")
        return await self.backend.store_content(content)
    
    async def get_content(self, url: str) -> Optional[ParsedContent]:
        """Retrieve content by URL."""
        if not self.backend:
            raise DatabaseError("Database not initialized")
        return await self.backend.get_content(url)
    
    async def content_exists(self, url: str) -> bool:
        """Check if content exists."""
        if not self.backend:
            raise DatabaseError("Database not initialized")
        return await self.backend.content_exists(url)
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        if not self.backend:
            raise DatabaseError("Database not initialized")
        return await self.backend.get_stats()
    
    async def close(self):
        """Close database connections."""
        if self.backend:
            await self.backend.close()
            self.logger.info("Database connections closed")