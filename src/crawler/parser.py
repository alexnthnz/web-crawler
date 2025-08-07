"""
Web page parser for extracting content, metadata, and links.
"""

import re
import logging
from typing import List, Dict, Optional, Set
from urllib.parse import urljoin, urlparse, urlunparse
from dataclasses import dataclass
from bs4 import BeautifulSoup, Comment
import json


@dataclass
class ParsedContent:
    """Container for parsed web page content."""
    url: str
    title: Optional[str] = None
    content: Optional[str] = None
    meta_description: Optional[str] = None
    meta_keywords: Optional[str] = None
    links: List[str] = None
    images: List[str] = None
    language: Optional[str] = None
    author: Optional[str] = None
    canonical_url: Optional[str] = None
    schema_org_data: Dict = None
    headings: Dict[str, List[str]] = None
    word_count: int = 0
    
    def __post_init__(self):
        if self.links is None:
            self.links = []
        if self.images is None:
            self.images = []
        if self.schema_org_data is None:
            self.schema_org_data = {}
        if self.headings is None:
            self.headings = {'h1': [], 'h2': [], 'h3': [], 'h4': [], 'h5': [], 'h6': []}


class ContentParser:
    """
    Parses HTML content to extract structured data, links, and metadata.
    """
    
    def __init__(self, allowed_domains: Optional[List[str]] = None, 
                 blocked_domains: Optional[List[str]] = None):
        self.allowed_domains = set(allowed_domains) if allowed_domains else set()
        self.blocked_domains = set(blocked_domains) if blocked_domains else set()
        self.logger = logging.getLogger(__name__)
        
        # Patterns for cleaning content
        self.whitespace_pattern = re.compile(r'\s+')
        self.email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        
    def parse(self, url: str, html_content: str) -> ParsedContent:
        """
        Parse HTML content and extract structured data.
        
        Args:
            url: The URL of the page
            html_content: Raw HTML content
            
        Returns:
            ParsedContent object with extracted data
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Remove script and style elements
            for script in soup(["script", "style", "noscript"]):
                script.decompose()
            
            # Remove comments
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()
            
            parsed_content = ParsedContent(url=url)
            
            # Extract basic metadata
            self._extract_title(soup, parsed_content)
            self._extract_meta_tags(soup, parsed_content)
            self._extract_language(soup, parsed_content)
            self._extract_canonical_url(soup, parsed_content, url)
            
            # Extract structured content
            self._extract_headings(soup, parsed_content)
            self._extract_main_content(soup, parsed_content)
            self._extract_links(soup, parsed_content, url)
            self._extract_images(soup, parsed_content, url)
            self._extract_schema_org(soup, parsed_content)
            
            # Calculate word count
            if parsed_content.content:
                parsed_content.word_count = len(parsed_content.content.split())
            
            self.logger.debug(f"Parsed content from {url}: {parsed_content.word_count} words, "
                            f"{len(parsed_content.links)} links")
            
            return parsed_content
            
        except Exception as e:
            self.logger.error(f"Error parsing content from {url}: {e}")
            return ParsedContent(url=url)
    
    def _extract_title(self, soup: BeautifulSoup, parsed_content: ParsedContent):
        """Extract page title."""
        title_tag = soup.find('title')
        if title_tag:
            parsed_content.title = self._clean_text(title_tag.get_text())
    
    def _extract_meta_tags(self, soup: BeautifulSoup, parsed_content: ParsedContent):
        """Extract meta tag information."""
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'}) or \
                   soup.find('meta', attrs={'property': 'og:description'})
        if meta_desc:
            parsed_content.meta_description = self._clean_text(meta_desc.get('content', ''))
        
        # Meta keywords
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords:
            parsed_content.meta_keywords = self._clean_text(meta_keywords.get('content', ''))
        
        # Author
        meta_author = soup.find('meta', attrs={'name': 'author'}) or \
                     soup.find('meta', attrs={'property': 'article:author'})
        if meta_author:
            parsed_content.author = self._clean_text(meta_author.get('content', ''))
    
    def _extract_language(self, soup: BeautifulSoup, parsed_content: ParsedContent):
        """Extract page language."""
        html_tag = soup.find('html')
        if html_tag:
            parsed_content.language = html_tag.get('lang') or html_tag.get('xml:lang')
    
    def _extract_canonical_url(self, soup: BeautifulSoup, parsed_content: ParsedContent, base_url: str):
        """Extract canonical URL."""
        canonical = soup.find('link', attrs={'rel': 'canonical'})
        if canonical:
            canonical_href = canonical.get('href')
            if canonical_href:
                parsed_content.canonical_url = urljoin(base_url, canonical_href)
    
    def _extract_headings(self, soup: BeautifulSoup, parsed_content: ParsedContent):
        """Extract headings (h1-h6)."""
        for level in range(1, 7):
            tag_name = f'h{level}'
            headings = soup.find_all(tag_name)
            parsed_content.headings[tag_name] = [
                self._clean_text(h.get_text()) for h in headings if h.get_text().strip()
            ]
    
    def _extract_main_content(self, soup: BeautifulSoup, parsed_content: ParsedContent):
        """Extract main text content."""
        # Try to find main content areas
        main_content_selectors = [
            'main',
            'article',
            '[role="main"]',
            '.content',
            '.main-content',
            '#content',
            '#main'
        ]
        
        content_element = None
        for selector in main_content_selectors:
            content_element = soup.select_one(selector)
            if content_element:
                break
        
        # If no main content area found, use body
        if not content_element:
            content_element = soup.find('body')
        
        if not content_element:
            content_element = soup
        
        # Remove navigation, footer, sidebar elements
        for unwanted in content_element.select('nav, footer, aside, .sidebar, .navigation, .menu'):
            unwanted.decompose()
        
        # Extract text content
        text_content = content_element.get_text(separator=' ', strip=True)
        parsed_content.content = self._clean_text(text_content)
    
    def _extract_links(self, soup: BeautifulSoup, parsed_content: ParsedContent, base_url: str):
        """Extract and normalize links."""
        links = set()
        
        for link in soup.find_all('a', href=True):
            href = link['href'].strip()
            if not href or href.startswith('#'):
                continue
            
            # Resolve relative URLs
            absolute_url = urljoin(base_url, href)
            normalized_url = self._normalize_url(absolute_url)
            
            if self._is_valid_url(normalized_url):
                links.add(normalized_url)
        
        parsed_content.links = list(links)
    
    def _extract_images(self, soup: BeautifulSoup, parsed_content: ParsedContent, base_url: str):
        """Extract image URLs."""
        images = set()
        
        for img in soup.find_all('img', src=True):
            src = img['src'].strip()
            if src:
                absolute_url = urljoin(base_url, src)
                normalized_url = self._normalize_url(absolute_url)
                if self._is_valid_url(normalized_url):
                    images.add(normalized_url)
        
        parsed_content.images = list(images)
    
    def _extract_schema_org(self, soup: BeautifulSoup, parsed_content: ParsedContent):
        """Extract Schema.org structured data."""
        schema_data = {}
        
        # JSON-LD structured data
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    schema_type = data.get('@type', 'Unknown')
                    if schema_type not in schema_data:
                        schema_data[schema_type] = []
                    schema_data[schema_type].append(data)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            schema_type = item.get('@type', 'Unknown')
                            if schema_type not in schema_data:
                                schema_data[schema_type] = []
                            schema_data[schema_type].append(item)
            except json.JSONDecodeError:
                continue
        
        # Microdata
        for element in soup.find_all(attrs={'itemtype': True}):
            item_type = element.get('itemtype', '').split('/')[-1]
            if item_type:
                properties = {}
                for prop in element.find_all(attrs={'itemprop': True}):
                    prop_name = prop.get('itemprop')
                    prop_value = prop.get('content') or prop.get_text(strip=True)
                    if prop_name and prop_value:
                        properties[prop_name] = prop_value
                
                if properties:
                    if item_type not in schema_data:
                        schema_data[item_type] = []
                    schema_data[item_type].append(properties)
        
        parsed_content.schema_org_data = schema_data
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL by removing fragments and unnecessary parameters."""
        try:
            parsed = urlparse(url)
            # Remove fragment
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc.lower(),
                parsed.path,
                parsed.params,
                parsed.query,
                ''  # Remove fragment
            ))
            return normalized
        except Exception:
            return url
    
    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid for crawling."""
        try:
            parsed = urlparse(url)
            
            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False
            
            # Only HTTP/HTTPS
            if parsed.scheme not in ['http', 'https']:
                return False
            
            domain = parsed.netloc.lower()
            
            # Check blocked domains
            if self.blocked_domains:
                for blocked_domain in self.blocked_domains:
                    if blocked_domain in domain:
                        return False
            
            # Check allowed domains (if specified)
            if self.allowed_domains:
                allowed = False
                for allowed_domain in self.allowed_domains:
                    if allowed_domain in domain:
                        allowed = True
                        break
                if not allowed:
                    return False
            
            # Avoid common non-content file extensions
            path = parsed.path.lower()
            skip_extensions = [
                '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp',
                '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                '.zip', '.rar', '.tar', '.gz', '.exe', '.dmg', '.iso',
                '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv',
                '.css', '.js', '.ico', '.woff', '.woff2', '.ttf', '.eot'
            ]
            
            if any(path.endswith(ext) for ext in skip_extensions):
                return False
            
            return True
            
        except Exception:
            return False
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = self.whitespace_pattern.sub(' ', text.strip())
        
        # Remove email addresses (privacy)
        text = self.email_pattern.sub('[EMAIL]', text)
        
        return text
    
    def get_outbound_links(self, parsed_content: ParsedContent) -> List[str]:
        """Get list of outbound links from parsed content."""
        return [link for link in parsed_content.links if self._is_valid_url(link)]