"""
Storage layer for the web crawler system.
"""

from .database import DatabaseManager, DatabaseError
from .duplicate_detector import DuplicateDetector, ContentHash

__all__ = ['DatabaseManager', 'DatabaseError', 'DuplicateDetector', 'ContentHash']