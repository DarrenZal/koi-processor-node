"""
Date Extraction Utilities for KOI Content Curator
Extracts publication dates from various content sources with confidence scoring
"""

import re
import json
import hashlib
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any
from dateutil import parser as date_parser
from bs4 import BeautifulSoup
from loguru import logger


class DateExtractor:
    """Extract publication dates from various content formats"""
    
    # Common date patterns in text
    DATE_PATTERNS = [
        # ISO 8601 formats
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[Z\+\-]\d{2}:\d{2}',
        r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
        r'\d{4}-\d{2}-\d{2}',
        
        # Common text formats
        r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
        r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}',
        r'\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}',
        r'\d{1,2}/\d{1,2}/\d{4}',
        r'\d{2}/\d{2}/\d{2}',
        
        # URL patterns
        r'/(\d{4})/(\d{1,2})/(\d{1,2})/',
        r'/(\d{4})-(\d{2})-(\d{2})/',
    ]
    
    # Meta tag names that commonly contain publication dates
    PUBLICATION_META_TAGS = [
        'article:published_time',
        'datePublished',
        'publish_date',
        'publication_date',
        'created',
        'DC.date',
        'DC.date.created',
        'sailthru.date',
        'parsely-pub-date',
        'date',
        'pubdate',
        'publishdate',
        'published_time',
    ]
    
    # Meta tag names for modification dates (lower priority)
    MODIFICATION_META_TAGS = [
        'article:modified_time',
        'dateModified',
        'last-modified',
        'modified',
        'DC.date.modified',
        'updated',
        'lastmod',
    ]
    
    @staticmethod
    def extract_from_html(html_content: str) -> Tuple[Optional[datetime], float]:
        """
        Extract publication date from HTML content
        Returns: (datetime, confidence_score)
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 1. Try meta tags (highest confidence)
            for meta_name in DateExtractor.PUBLICATION_META_TAGS:
                date_str = DateExtractor._extract_meta_date(soup, meta_name)
                if date_str:
                    parsed_date = DateExtractor._parse_date_string(date_str)
                    if parsed_date:
                        return parsed_date, 0.95
            
            # 2. Try JSON-LD structured data (high confidence)
            json_ld_date = DateExtractor._extract_json_ld_date(soup)
            if json_ld_date:
                return json_ld_date, 0.90
            
            # 3. Try time elements (medium-high confidence)
            time_elem = soup.find('time', {'datetime': True})
            if time_elem:
                parsed_date = DateExtractor._parse_date_string(time_elem.get('datetime'))
                if parsed_date:
                    return parsed_date, 0.85
            
            # 4. Try modification dates (medium confidence)
            for meta_name in DateExtractor.MODIFICATION_META_TAGS:
                date_str = DateExtractor._extract_meta_date(soup, meta_name)
                if date_str:
                    parsed_date = DateExtractor._parse_date_string(date_str)
                    if parsed_date:
                        return parsed_date, 0.70
            
            # 5. Try text patterns (lower confidence)
            text_date = DateExtractor._extract_from_text(soup.get_text())
            if text_date:
                return text_date, 0.50
                
        except Exception as e:
            logger.error(f"Error extracting date from HTML: {e}")
        
        return None, 0.0
    
    @staticmethod
    def extract_from_json(json_data: Dict[str, Any]) -> Tuple[Optional[datetime], float]:
        """
        Extract publication date from JSON/API response
        Returns: (datetime, confidence_score)
        """
        # Common JSON field names for dates
        date_fields = [
            'published_at', 'publishedAt', 'published',
            'created_at', 'createdAt', 'created',
            'date', 'pubDate', 'pub_date',
            'timestamp', 'time',
        ]
        
        # Check for date fields
        for field in date_fields:
            if field in json_data:
                parsed_date = DateExtractor._parse_date_string(str(json_data[field]))
                if parsed_date:
                    # Higher confidence for "published" fields
                    confidence = 0.95 if 'publish' in field else 0.85
                    return parsed_date, confidence
        
        # Check nested metadata
        if 'metadata' in json_data:
            return DateExtractor.extract_from_json(json_data['metadata'])
        
        return None, 0.0
    
    @staticmethod
    def extract_from_url(url: str) -> Tuple[Optional[datetime], float]:
        """
        Extract date from URL patterns
        Returns: (datetime, confidence_score)
        """
        # Try common URL date patterns
        patterns = [
            r'/(\d{4})/(\d{1,2})/(\d{1,2})/',  # /2025/09/11/
            r'/(\d{4})-(\d{2})-(\d{2})/',      # /2025-09-11/
            r'/(\d{8})/',                        # /20250911/
            r'[?&]date=(\d{4}-\d{2}-\d{2})',   # ?date=2025-09-11
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                try:
                    if len(match.groups()) == 3:
                        year, month, day = match.groups()
                        date = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
                        return date, 0.75
                    elif len(match.groups()) == 1:
                        date_str = match.group(1)
                        if len(date_str) == 8:  # YYYYMMDD
                            date = datetime.strptime(date_str, '%Y%m%d').replace(tzinfo=timezone.utc)
                            return date, 0.70
                        else:
                            parsed_date = DateExtractor._parse_date_string(date_str)
                            if parsed_date:
                                return parsed_date, 0.70
                except (ValueError, AttributeError):
                    continue
        
        return None, 0.0
    
    @staticmethod
    def extract_from_rss_item(item: Dict[str, Any]) -> Tuple[Optional[datetime], float]:
        """
        Extract date from RSS feed item
        Returns: (datetime, confidence_score)
        """
        # RSS date fields in order of preference
        date_fields = ['pubDate', 'published', 'updated', 'created', 'date']
        
        for field in date_fields:
            if field in item:
                parsed_date = DateExtractor._parse_date_string(str(item[field]))
                if parsed_date:
                    return parsed_date, 0.95
        
        return None, 0.0
    
    @staticmethod
    def extract_from_discourse_post(post_data: Dict[str, Any]) -> Tuple[Optional[datetime], float]:
        """
        Extract date from Discourse forum post
        Returns: (datetime, confidence_score)
        """
        if 'created_at' in post_data:
            parsed_date = DateExtractor._parse_date_string(post_data['created_at'])
            if parsed_date:
                return parsed_date, 0.95
        
        if 'updated_at' in post_data:
            parsed_date = DateExtractor._parse_date_string(post_data['updated_at'])
            if parsed_date:
                return parsed_date, 0.85
                
        return None, 0.0
    
    @staticmethod
    def extract_from_git_commit(commit_data: Dict[str, Any]) -> Tuple[Optional[datetime], float]:
        """
        Extract date from Git commit data
        Returns: (datetime, confidence_score)
        """
        # For code files, use commit date
        if 'date' in commit_data:
            parsed_date = DateExtractor._parse_date_string(commit_data['date'])
            if parsed_date:
                return parsed_date, 0.90
        
        if 'committed_date' in commit_data:
            parsed_date = DateExtractor._parse_date_string(commit_data['committed_date'])
            if parsed_date:
                return parsed_date, 0.90
                
        return None, 0.0
    
    @staticmethod
    def calculate_content_hash(content: str) -> str:
        """Calculate SHA-256 hash of content for deduplication"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    @staticmethod
    def _extract_meta_date(soup: BeautifulSoup, meta_name: str) -> Optional[str]:
        """Extract date from meta tags"""
        # Try property attribute
        meta = soup.find('meta', {'property': meta_name})
        if meta and meta.get('content'):
            return meta.get('content')
        
        # Try name attribute
        meta = soup.find('meta', {'name': meta_name})
        if meta and meta.get('content'):
            return meta.get('content')
        
        # Try itemprop attribute
        meta = soup.find('meta', {'itemprop': meta_name})
        if meta and meta.get('content'):
            return meta.get('content')
            
        return None
    
    @staticmethod
    def _extract_json_ld_date(soup: BeautifulSoup) -> Optional[datetime]:
        """Extract date from JSON-LD structured data"""
        scripts = soup.find_all('script', {'type': 'application/ld+json'})
        for script in scripts:
            try:
                data = json.loads(script.string)
                
                # Handle both single objects and arrays
                items = data if isinstance(data, list) else [data]
                
                for item in items:
                    if isinstance(item, dict):
                        # Check for article/blog posting types
                        if item.get('@type') in ['Article', 'BlogPosting', 'NewsArticle']:
                            for date_field in ['datePublished', 'dateCreated', 'dateModified']:
                                if date_field in item:
                                    parsed_date = DateExtractor._parse_date_string(item[date_field])
                                    if parsed_date:
                                        return parsed_date
            except (json.JSONDecodeError, AttributeError):
                continue
        
        return None
    
    @staticmethod
    def _extract_from_text(text: str) -> Optional[datetime]:
        """Extract date from plain text using patterns"""
        # Look for date patterns near keywords
        keywords = ['published', 'posted', 'created', 'updated', 'date:', 'on']
        
        for keyword in keywords:
            # Find keyword in text (case insensitive)
            pattern = rf'(?i){keyword}[:\s]+([^\n]+)'
            match = re.search(pattern, text)
            if match:
                potential_date = match.group(1)[:50]  # Limit search area
                
                # Try to parse the date
                for date_pattern in DateExtractor.DATE_PATTERNS[:6]:  # Use simpler patterns
                    date_match = re.search(date_pattern, potential_date)
                    if date_match:
                        parsed_date = DateExtractor._parse_date_string(date_match.group(0))
                        if parsed_date:
                            return parsed_date
        
        return None
    
    @staticmethod
    def _parse_date_string(date_str: str) -> Optional[datetime]:
        """Parse a date string into datetime object"""
        if not date_str:
            return None
            
        try:
            # Try dateutil parser (handles many formats)
            parsed = date_parser.parse(date_str)
            
            # Add timezone if missing
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            
            # Sanity check - reject dates too far in future or past
            now = datetime.now(timezone.utc)
            if parsed > now + timedelta(days=365):  # More than 1 year in future
                return None
            if parsed < datetime(2000, 1, 1, tzinfo=timezone.utc):  # Before 2000
                return None
                
            return parsed
            
        except (ValueError, TypeError, AttributeError):
            return None


class ContentDateEnricher:
    """Enrich KOI events with publication dates"""
    
    def __init__(self):
        self.extractor = DateExtractor()
    
    def enrich_event(self, event_data: Dict[str, Any], source_type: str) -> Dict[str, Any]:
        """
        Enrich a KOI event with publication date information
        
        Args:
            event_data: The event data dictionary
            source_type: Type of source (website, twitter, medium, discourse, etc.)
        
        Returns:
            Enriched event data with published_at and confidence fields
        """
        published_at = None
        confidence = 0.0
        
        # Extract based on source type
        if source_type == 'website':
            if 'html' in event_data:
                published_at, confidence = self.extractor.extract_from_html(event_data['html'])
            if not published_at and 'url' in event_data:
                published_at, confidence = self.extractor.extract_from_url(event_data['url'])
                
        elif source_type == 'twitter':
            if 'created_at' in event_data:
                published_at = self.extractor._parse_date_string(event_data['created_at'])
                confidence = 0.95
                
        elif source_type == 'medium':
            if 'published_date' in event_data:
                published_at = self.extractor._parse_date_string(event_data['published_date'])
                confidence = 0.95
            elif 'metadata' in event_data and 'published_date' in event_data['metadata']:
                published_at = self.extractor._parse_date_string(event_data['metadata']['published_date'])
                confidence = 0.95
                
        elif source_type == 'discourse':
            published_at, confidence = self.extractor.extract_from_discourse_post(event_data)
            
        elif source_type == 'podcast':
            published_at, confidence = self.extractor.extract_from_rss_item(event_data)
            
        elif source_type in ['github', 'gitlab']:
            published_at, confidence = self.extractor.extract_from_git_commit(event_data)
        
        # Calculate content hash
        content_hash = None
        if 'content' in event_data:
            content_hash = self.extractor.calculate_content_hash(event_data['content'])
        elif 'text' in event_data:
            content_hash = self.extractor.calculate_content_hash(event_data['text'])
        
        # Add to event data
        enriched = event_data.copy()
        enriched['published_at'] = published_at.isoformat() if published_at else None
        enriched['published_confidence'] = confidence
        enriched['content_hash'] = content_hash
        
        return enriched


# Convenience function for direct use
def extract_publication_date(content: Any, source_type: str = 'unknown') -> Tuple[Optional[datetime], float]:
    """
    Extract publication date from content
    
    Args:
        content: The content (HTML string, JSON dict, etc.)
        source_type: Type of source for better extraction
    
    Returns:
        Tuple of (datetime or None, confidence score)
    """
    extractor = DateExtractor()
    
    if isinstance(content, str):
        # Try as HTML first
        date, confidence = extractor.extract_from_html(content)
        if date:
            return date, confidence
        
        # Try as URL
        date, confidence = extractor.extract_from_url(content)
        if date:
            return date, confidence
            
    elif isinstance(content, dict):
        # Try as JSON/API response
        date, confidence = extractor.extract_from_json(content)
        if date:
            return date, confidence
    
    return None, 0.0