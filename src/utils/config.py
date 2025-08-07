"""
Configuration management for the web crawler system.
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class CrawlerConfig:
    """Configuration for crawler behavior."""
    seed_urls: List[str]
    max_depth: int
    politeness_delay: float
    max_concurrent_requests: int
    request_timeout: int
    retry_attempts: int
    user_agent: str
    respect_robots_txt: bool
    allowed_domains: List[str]
    blocked_domains: List[str]


@dataclass
class DatabaseConfig:
    """Configuration for database storage."""
    type: str
    cassandra: Dict[str, Any]
    file: Dict[str, Any]


@dataclass
class RedisConfig:
    """Configuration for Redis."""
    host: str
    port: int
    db: int
    password: Optional[str]
    url_frontier_key: str
    processed_urls_key: str


@dataclass
class LoggingConfig:
    """Configuration for logging."""
    level: str
    file: str
    format: str


@dataclass
class MonitoringConfig:
    """Configuration for monitoring."""
    prometheus_port: int
    metrics_enabled: bool


@dataclass
class Config:
    """Main configuration class."""
    crawler: CrawlerConfig
    database: DatabaseConfig
    redis: RedisConfig
    logging: LoggingConfig
    monitoring: MonitoringConfig


class ConfigManager:
    """Manages configuration loading and validation."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = Path(config_path)
        self._config: Optional[Config] = None
    
    def load_config(self) -> Config:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as file:
            config_data = yaml.safe_load(file)
        
        # Parse configuration sections
        crawler_config = CrawlerConfig(**config_data['crawler'])
        database_config = DatabaseConfig(**config_data['database'])
        redis_config = RedisConfig(**config_data['redis'])
        logging_config = LoggingConfig(**config_data['logging'])
        monitoring_config = MonitoringConfig(**config_data['monitoring'])
        
        self._config = Config(
            crawler=crawler_config,
            database=database_config,
            redis=redis_config,
            logging=logging_config,
            monitoring=monitoring_config
        )
        
        self._validate_config()
        return self._config
    
    def _validate_config(self):
        """Validate configuration values."""
        if not self._config:
            raise ValueError("Configuration not loaded")
        
        # Validate seed URLs
        if not self._config.crawler.seed_urls:
            raise ValueError("At least one seed URL must be provided")
        
        # Validate numeric values
        if self._config.crawler.max_depth < 1:
            raise ValueError("max_depth must be at least 1")
        
        if self._config.crawler.politeness_delay < 0:
            raise ValueError("politeness_delay must be non-negative")
        
        if self._config.crawler.max_concurrent_requests < 1:
            raise ValueError("max_concurrent_requests must be at least 1")
        
        # Validate database type
        if self._config.database.type not in ['cassandra', 'file']:
            raise ValueError("Database type must be 'cassandra' or 'file'")
        
        logging.info("Configuration validation passed")
    
    @property
    def config(self) -> Config:
        """Get the loaded configuration."""
        if not self._config:
            raise ValueError("Configuration not loaded. Call load_config() first.")
        return self._config


# Global config manager instance
config_manager = ConfigManager()


def get_config() -> Config:
    """Get the global configuration instance."""
    return config_manager.config


def load_config(config_path: str = "config.yaml") -> Config:
    """Load configuration from file."""
    global config_manager
    config_manager = ConfigManager(config_path)
    return config_manager.load_config()