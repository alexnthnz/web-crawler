"""
Enhanced logging utilities for the web crawler system.
"""

import logging
import logging.handlers
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.utcfromtimestamp(record.created).isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, 'extra_fields'):
            log_entry.update(record.extra_fields)
        
        return json.dumps(log_entry, ensure_ascii=False)


class CrawlerLogAdapter(logging.LoggerAdapter):
    """Logger adapter that adds crawler-specific context."""
    
    def __init__(self, logger: logging.Logger, extra: Optional[Dict[str, Any]] = None):
        super().__init__(logger, extra or {})
    
    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        """Add extra context to log messages."""
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        
        # Add adapter extra fields
        kwargs['extra'].update(self.extra)
        
        return msg, kwargs
    
    def log_url_event(self, level: int, url: str, message: str, **kwargs):
        """Log URL-specific events."""
        extra = kwargs.get('extra', {})
        extra['url'] = url
        extra['event_type'] = 'url_event'
        kwargs['extra'] = extra
        self.log(level, message, **kwargs)
    
    def log_crawler_stat(self, stat_name: str, value: Any, **kwargs):
        """Log crawler statistics."""
        extra = kwargs.get('extra', {})
        extra['stat_name'] = stat_name
        extra['stat_value'] = value
        extra['event_type'] = 'crawler_stat'
        kwargs['extra'] = extra
        self.info(f"Stat: {stat_name} = {value}", **kwargs)


class PerformanceFilter(logging.Filter):
    """Filter to suppress noisy performance-related logs."""
    
    def __init__(self, suppress_modules: Optional[list] = None):
        super().__init__()
        self.suppress_modules = suppress_modules or [
            'aiohttp.access',
            'urllib3.connectionpool',
            'requests.packages.urllib3',
        ]
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Filter out noisy log records."""
        # Suppress logs from noisy modules
        if any(record.name.startswith(module) for module in self.suppress_modules):
            return False
        
        # Suppress very frequent debug messages
        if record.levelno == logging.DEBUG:
            if 'connection pool' in record.getMessage().lower():
                return False
            if 'resetting dropped connection' in record.getMessage().lower():
                return False
        
        return True


def setup_logging(config: Dict[str, Any], 
                 enable_json: bool = False,
                 enable_performance_filtering: bool = True) -> logging.Logger:
    """
    Setup comprehensive logging for the crawler.
    
    Args:
        config: Logging configuration dictionary
        enable_json: Enable JSON formatted logging
        enable_performance_filtering: Enable filtering of noisy logs
        
    Returns:
        Configured root logger
    """
    # Create logs directory
    log_file = Path(config.get('file', 'logs/crawler.log'))
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.get('level', 'INFO').upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Choose formatter
    if enable_json:
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    if enable_performance_filtering:
        console_handler.addFilter(PerformanceFilter())
    
    root_logger.addHandler(console_handler)
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    if enable_performance_filtering:
        file_handler.addFilter(PerformanceFilter())
    
    root_logger.addHandler(file_handler)
    
    # Error file handler
    error_log_file = log_file.parent / 'errors.log'
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=3,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)
    
    # Configure third-party loggers
    third_party_loggers = {
        'aiohttp': logging.WARNING,
        'urllib3': logging.WARNING,
        'requests': logging.WARNING,
        'cassandra': logging.WARNING,
        'redis': logging.WARNING,
        'asyncio': logging.WARNING,
    }
    
    for logger_name, level in third_party_loggers.items():
        logging.getLogger(logger_name).setLevel(level)
    
    # Log startup message
    root_logger.info("Logging system initialized")
    root_logger.info(f"Log file: {log_file}")
    root_logger.info(f"Error log file: {error_log_file}")
    root_logger.info(f"Log level: {config.get('level', 'INFO')}")
    root_logger.info(f"JSON formatting: {enable_json}")
    
    return root_logger


def get_crawler_logger(name: str, **extra_context) -> CrawlerLogAdapter:
    """
    Get a crawler-specific logger with additional context.
    
    Args:
        name: Logger name
        **extra_context: Additional context fields to include in all log messages
        
    Returns:
        CrawlerLogAdapter instance
    """
    logger = logging.getLogger(name)
    return CrawlerLogAdapter(logger, extra_context)


def log_system_info():
    """Log system and environment information."""
    import platform
    import sys
    import psutil
    
    logger = logging.getLogger(__name__)
    
    logger.info("=== SYSTEM INFORMATION ===")
    logger.info(f"Platform: {platform.platform()}")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"CPU cores: {psutil.cpu_count()}")
    logger.info(f"Memory: {psutil.virtual_memory().total / 1024**3:.1f} GB")
    logger.info(f"Architecture: {platform.architecture()}")
    
    # Log key environment variables
    import os
    env_vars = ['PATH', 'PYTHONPATH', 'HOME', 'USER']
    for var in env_vars:
        value = os.environ.get(var, 'Not set')
        logger.debug(f"ENV {var}: {value}")


class LoggingContext:
    """Context manager for adding temporary logging context."""
    
    def __init__(self, logger: CrawlerLogAdapter, **context):
        self.logger = logger
        self.context = context
        self.original_extra = None
    
    def __enter__(self):
        self.original_extra = self.logger.extra.copy()
        self.logger.extra.update(self.context)
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.extra = self.original_extra