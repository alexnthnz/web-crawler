"""
Web crawler core components.
"""

from .url_frontier import URLFrontier, URLTask, URLPriority
from .fetcher import WebFetcher, FetchResult
from .parser import ContentParser, ParsedContent

__all__ = [
    'URLFrontier', 'URLTask', 'URLPriority',
    'WebFetcher', 'FetchResult', 
    'ContentParser', 'ParsedContent'
]