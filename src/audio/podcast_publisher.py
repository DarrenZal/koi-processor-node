#!/usr/bin/env python3
"""
Podcast Publishing System for Regen Network Weekly Digest (Session 14)

Implements:
- RSS 2.0 feed generation with iTunes extensions
- Google Drive backup storage
- Episode metadata management
- Integration with Pathway to Planetary Regeneration podcast
- Support for NotebookLM-generated audio
"""

import json
import os
import sys
import logging
import hashlib
import mimetypes
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
import xml.etree.ElementTree as ET
from xml.dom import minidom
import requests
from dataclasses import dataclass, asdict
import re

# Google Drive integration
try:
    from google.oauth2.credentials import Credentials
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.errors import HttpError
    GOOGLE_DRIVE_AVAILABLE = True
except ImportError:
    GOOGLE_DRIVE_AVAILABLE = False
    print("Warning: Google Drive API not installed. Install with: pip install google-api-python-client google-auth")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PodcastEpisode:
    """Represents a podcast episode with all metadata"""
    episode_number: int
    title: str
    description: str
    summary: str
    audio_file_path: str
    audio_url: str
    duration: str  # Format: HH:MM:SS
    file_size: int
    publication_date: datetime
    author: str = "Regen Network"
    keywords: List[str] = None
    explicit: bool = False
    season: int = 1
    episode_type: str = "full"  # full, trailer, bonus
    
    def __post_init__(self):
        if self.keywords is None:
            self.keywords = ["regenerative", "ecology", "climate", "blockchain", "carbon credits"]


class PodcastPublisher:
    """
    Handles podcast publishing including RSS feed generation and distribution
    """
    
    def __init__(self, config_path: str = "config/podcast_config.json"):
        """Initialize the podcast publisher"""
        self.config = self._load_config(config_path)
        
        # Podcast metadata
        self.podcast_info = self.config.get("podcast", {
            "title": "Pathway to Planetary Regeneration",
            "subtitle": "Weekly insights from the Regen Network ecosystem",
            "description": "Join us for weekly deep dives into regenerative agriculture, ecological economics, and the latest developments in the Regen Network ecosystem. Each episode synthesizes the week's most important updates, governance decisions, and community achievements.",
            "author": "Regen Network",
            "owner_name": "Regen Network",
            "owner_email": "podcast@regen.network",
            "language": "en-us",
            "category": "Science",
            "subcategory": "Earth Sciences",
            "explicit": "no",
            "image_url": "https://regen.network/podcast/cover.jpg",
            "website": "https://regen.network/podcast",
            "feed_url": "https://regen.network/podcast/feed.xml"
        })
        
        # Storage paths
        self.base_path = Path(self.config.get("storage_path", "./podcast"))
        self.episodes_path = self.base_path / "episodes"
        self.feed_path = self.base_path / "feed"
        self.backup_path = self.base_path / "backup"
        
        # Create directories
        for path in [self.episodes_path, self.feed_path, self.backup_path]:
            path.mkdir(parents=True, exist_ok=True)
        
        # Google Drive setup
        self.drive_service = None
        if GOOGLE_DRIVE_AVAILABLE and self.config.get("google_drive", {}).get("enabled", False):
            self._initialize_google_drive()
        
        # Episode tracking
        self.episodes = []
        self._load_episodes()
    
    def _load_config(self, config_path: str) -> Dict:
        """Load or create default configuration"""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            # Default configuration
            default_config = {
                "podcast": {
                    "title": "Pathway to Planetary Regeneration",
                    "subtitle": "Weekly insights from the Regen Network ecosystem",
                    "description": "Weekly deep dives into regenerative agriculture and Regen Network",
                    "author": "Regen Network",
                    "owner_name": "Regen Network",
                    "owner_email": "podcast@regen.network",
                    "language": "en-us",
                    "category": "Science",
                    "explicit": "no"
                },
                "storage_path": "./podcast",
                "google_drive": {
                    "enabled": False,
                    "folder_id": "",
                    "credentials_path": "credentials.json"
                },
                "hosting": {
                    "base_url": "https://regen.network/podcast/episodes/",
                    "feed_url": "https://regen.network/podcast/feed.xml"
                }
            }
            
            # Save default config
            os.makedirs(os.path.dirname(config_path) or ".", exist_ok=True)
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=2)
            
            return default_config
    
    def _initialize_google_drive(self):
        """Initialize Google Drive API service"""
        try:
            creds_path = self.config["google_drive"]["credentials_path"]
            
            # Try service account first
            if creds_path.endswith('.json'):
                credentials = service_account.Credentials.from_service_account_file(
                    creds_path,
                    scopes=['https://www.googleapis.com/auth/drive.file']
                )
            else:
                # OAuth2 credentials
                credentials = Credentials.from_authorized_user_file(
                    creds_path,
                    scopes=['https://www.googleapis.com/auth/drive.file']
                )
            
            self.drive_service = build('drive', 'v3', credentials=credentials)
            logger.info("Google Drive API initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Drive: {e}")
            self.drive_service = None
    
    def _load_episodes(self):
        """Load existing episodes from metadata file"""
        metadata_file = self.base_path / "episodes.json"
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                episodes_data = json.load(f)
                self.episodes = [PodcastEpisode(**ep) for ep in episodes_data]
                logger.info(f"Loaded {len(self.episodes)} existing episodes")
    
    def _save_episodes(self):
        """Save episodes metadata to file"""
        metadata_file = self.base_path / "episodes.json"
        episodes_data = [asdict(ep) for ep in self.episodes]
        
        # Convert datetime objects to ISO format
        for ep in episodes_data:
            if isinstance(ep['publication_date'], datetime):
                ep['publication_date'] = ep['publication_date'].isoformat()
        
        with open(metadata_file, 'w') as f:
            json.dump(episodes_data, f, indent=2, default=str)
    
    def add_episode(self, 
                   audio_file: str,
                   title: str,
                   description: str,
                   summary: Optional[str] = None,
                   publication_date: Optional[datetime] = None,
                   keywords: Optional[List[str]] = None) -> PodcastEpisode:
        """
        Add a new episode to the podcast
        
        Args:
            audio_file: Path to the audio file
            title: Episode title
            description: Full episode description
            summary: Short summary (optional)
            publication_date: Publication date (defaults to now)
            keywords: List of keywords for the episode
        
        Returns:
            PodcastEpisode object
        """
        # Determine episode number
        episode_number = len(self.episodes) + 1
        
        # Get file info
        audio_path = Path(audio_file)
        if not audio_path.exists():
            raise FileNotFoundError(f"Audio file not found: {audio_file}")
        
        file_size = audio_path.stat().st_size
        
        # Get audio duration
        duration = self._get_audio_duration(audio_file)
        
        # Copy to episodes directory
        episode_filename = f"episode_{episode_number:03d}_{audio_path.stem}.mp3"
        episode_path = self.episodes_path / episode_filename
        
        import shutil
        shutil.copy2(audio_file, episode_path)
        logger.info(f"Copied audio to {episode_path}")
        
        # Generate audio URL
        base_url = self.config.get("hosting", {}).get("base_url", "https://regen.network/podcast/episodes/")
        audio_url = f"{base_url}{episode_filename}"
        
        # Upload to Google Drive if enabled
        if self.drive_service:
            drive_url = self._upload_to_drive(episode_path, episode_filename)
            if drive_url:
                audio_url = drive_url
        
        # Create episode object
        episode = PodcastEpisode(
            episode_number=episode_number,
            title=title,
            description=description,
            summary=summary or description[:500],
            audio_file_path=str(episode_path),
            audio_url=audio_url,
            duration=duration,
            file_size=file_size,
            publication_date=publication_date or datetime.now(timezone.utc),
            keywords=keywords
        )
        
        # Add to episodes list
        self.episodes.append(episode)
        self.episodes.sort(key=lambda x: x.episode_number, reverse=True)
        
        # Save metadata
        self._save_episodes()
        
        logger.info(f"Added episode {episode_number}: {title}")
        return episode
    
    def _get_audio_duration(self, audio_file: str) -> str:
        """Get audio duration in HH:MM:SS format"""
        try:
            # Try using mutagen first
            from mutagen.mp3 import MP3
            audio = MP3(audio_file)
            duration_seconds = int(audio.info.length)
        except:
            # Fallback to ffprobe
            try:
                import subprocess
                result = subprocess.run(
                    ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                     '-of', 'default=noprint_wrappers=1:nokey=1', audio_file],
                    capture_output=True, text=True
                )
                duration_seconds = int(float(result.stdout.strip()))
            except:
                # Default duration if detection fails
                duration_seconds = 1200  # 20 minutes
        
        # Convert to HH:MM:SS
        hours = duration_seconds // 3600
        minutes = (duration_seconds % 3600) // 60
        seconds = duration_seconds % 60
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    def _upload_to_drive(self, file_path: Path, filename: str) -> Optional[str]:
        """Upload file to Google Drive and return public URL"""
        if not self.drive_service:
            return None
        
        try:
            folder_id = self.config["google_drive"]["folder_id"]
            
            # File metadata
            file_metadata = {
                'name': filename,
                'parents': [folder_id] if folder_id else []
            }
            
            # Upload file
            media = MediaFileUpload(
                str(file_path),
                mimetype='audio/mpeg',
                resumable=True
            )
            
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,webViewLink,webContentLink'
            ).execute()
            
            # Make file public
            self.drive_service.permissions().create(
                fileId=file['id'],
                body={'type': 'anyone', 'role': 'reader'}
            ).execute()
            
            # Return direct download link
            download_url = file.get('webContentLink')
            logger.info(f"Uploaded to Google Drive: {download_url}")
            return download_url
            
        except Exception as e:
            logger.error(f"Google Drive upload failed: {e}")
            return None
    
    def generate_rss_feed(self) -> str:
        """Generate RSS 2.0 feed with iTunes extensions"""
        
        # Create root RSS element
        rss = ET.Element("rss", {
            "version": "2.0",
            "xmlns:itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
            "xmlns:content": "http://purl.org/rss/1.0/modules/content/",
            "xmlns:atom": "http://www.w3.org/2005/Atom"
        })
        
        # Create channel
        channel = ET.SubElement(rss, "channel")
        
        # Add podcast metadata
        ET.SubElement(channel, "title").text = self.podcast_info["title"]
        ET.SubElement(channel, "description").text = self.podcast_info["description"]
        ET.SubElement(channel, "language").text = self.podcast_info["language"]
        ET.SubElement(channel, "link").text = self.podcast_info.get("website", "https://regen.network")
        
        # iTunes specific tags
        ET.SubElement(channel, "itunes:subtitle").text = self.podcast_info.get("subtitle", "")
        ET.SubElement(channel, "itunes:author").text = self.podcast_info["author"]
        ET.SubElement(channel, "itunes:summary").text = self.podcast_info["description"]
        
        owner = ET.SubElement(channel, "itunes:owner")
        ET.SubElement(owner, "itunes:name").text = self.podcast_info["owner_name"]
        ET.SubElement(owner, "itunes:email").text = self.podcast_info["owner_email"]
        
        ET.SubElement(channel, "itunes:explicit").text = self.podcast_info["explicit"]
        
        # Category
        category = ET.SubElement(channel, "itunes:category", {"text": self.podcast_info["category"]})
        if "subcategory" in self.podcast_info:
            ET.SubElement(category, "itunes:category", {"text": self.podcast_info["subcategory"]})
        
        # Podcast image
        if "image_url" in self.podcast_info:
            image = ET.SubElement(channel, "itunes:image", {"href": self.podcast_info["image_url"]})
            
            image_elem = ET.SubElement(channel, "image")
            ET.SubElement(image_elem, "url").text = self.podcast_info["image_url"]
            ET.SubElement(image_elem, "title").text = self.podcast_info["title"]
            ET.SubElement(image_elem, "link").text = self.podcast_info.get("website", "")
        
        # Self link
        ET.SubElement(channel, "atom:link", {
            "href": self.podcast_info.get("feed_url", ""),
            "rel": "self",
            "type": "application/rss+xml"
        })
        
        # Add episodes
        for episode in self.episodes:
            item = ET.SubElement(channel, "item")
            
            # Basic episode info
            ET.SubElement(item, "title").text = episode.title
            ET.SubElement(item, "description").text = episode.description
            
            # Handle publication date - could be datetime or string
            pub_date = episode.publication_date
            if isinstance(pub_date, str):
                # Already a string, try to parse and reformat
                try:
                    pub_date = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                except:
                    pub_date = datetime.now(timezone.utc)
            
            ET.SubElement(item, "pubDate").text = pub_date.strftime("%a, %d %b %Y %H:%M:%S %z")
            
            # GUID
            guid = ET.SubElement(item, "guid", {"isPermaLink": "false"})
            guid.text = f"regen-weekly-{episode.episode_number:03d}"
            
            # Enclosure (audio file)
            ET.SubElement(item, "enclosure", {
                "url": episode.audio_url,
                "length": str(episode.file_size),
                "type": "audio/mpeg"
            })
            
            # iTunes specific
            ET.SubElement(item, "itunes:author").text = episode.author
            ET.SubElement(item, "itunes:subtitle").text = episode.summary[:255]
            ET.SubElement(item, "itunes:summary").text = episode.summary
            ET.SubElement(item, "itunes:duration").text = episode.duration
            ET.SubElement(item, "itunes:explicit").text = "yes" if episode.explicit else "no"
            ET.SubElement(item, "itunes:episodeType").text = episode.episode_type
            ET.SubElement(item, "itunes:episode").text = str(episode.episode_number)
            ET.SubElement(item, "itunes:season").text = str(episode.season)
            
            # Keywords
            if episode.keywords:
                ET.SubElement(item, "itunes:keywords").text = ", ".join(episode.keywords)
        
        # Convert to string with pretty printing
        xml_str = ET.tostring(rss, encoding='unicode')
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ")
        
        # Remove extra blank lines
        lines = [line for line in pretty_xml.split('\n') if line.strip()]
        return '\n'.join(lines)
    
    def save_rss_feed(self, filename: str = "feed.xml") -> str:
        """Save RSS feed to file"""
        feed_content = self.generate_rss_feed()
        feed_file = self.feed_path / filename
        
        with open(feed_file, 'w', encoding='utf-8') as f:
            f.write(feed_content)
        
        logger.info(f"RSS feed saved to {feed_file}")
        return str(feed_file)
    
    def publish_episode_from_weekly_digest(self, 
                                          audio_file: str,
                                          digest_metadata: Dict[str, Any]) -> PodcastEpisode:
        """
        Publish an episode from weekly digest audio and metadata
        
        Args:
            audio_file: Path to NotebookLM-generated audio
            digest_metadata: Metadata from weekly aggregator
        
        Returns:
            Published episode
        """
        # Extract metadata
        week_ending = digest_metadata.get("week_ending", datetime.now(timezone.utc))
        if isinstance(week_ending, str):
            # Handle ISO format strings, including those with timezone info
            week_ending = datetime.fromisoformat(week_ending.replace('Z', '+00:00'))
        elif not isinstance(week_ending, datetime):
            week_ending = datetime.now(timezone.utc)
        
        # Generate title
        week_num = week_ending.isocalendar()[1]
        year = week_ending.year
        title = f"Week {week_num}, {year}: {digest_metadata.get('theme', 'Weekly Regenerative Update')}"
        
        # Build description from digest
        description = self._build_episode_description(digest_metadata)
        
        # Extract keywords from top tags
        keywords = digest_metadata.get("top_tags", [])[:10]
        if not keywords:
            keywords = ["regenerative", "ecology", "governance", "carbon credits", "blockchain"]
        
        # Add episode
        episode = self.add_episode(
            audio_file=audio_file,
            title=title,
            description=description,
            summary=digest_metadata.get("executive_summary", description[:500]),
            publication_date=week_ending,
            keywords=keywords
        )
        
        # Generate and save RSS feed
        self.save_rss_feed()
        
        # Backup to Google Drive if enabled
        if self.drive_service:
            self._backup_feed_to_drive()
        
        return episode
    
    def _build_episode_description(self, digest_metadata: Dict[str, Any]) -> str:
        """Build episode description from digest metadata"""
        sections = []
        
        # Executive summary
        if "executive_summary" in digest_metadata:
            sections.append(digest_metadata["executive_summary"])
        
        # Top stories
        if "top_stories" in digest_metadata:
            sections.append("\n**Top Stories This Week:**")
            for story in digest_metadata["top_stories"][:5]:
                sections.append(f"• {story.get('title', story.get('summary', ''))}")
        
        # Key themes
        if "themes" in digest_metadata:
            sections.append("\n**Key Themes:**")
            for theme in digest_metadata["themes"][:5]:
                sections.append(f"• {theme}")
        
        # Statistics
        if "stats" in digest_metadata:
            sections.append("\n**By the Numbers:**")
            stats = digest_metadata["stats"]
            if "total_content" in stats:
                sections.append(f"• {stats['total_content']} pieces of content analyzed")
            if "top_sources" in stats:
                sources = ", ".join(stats["top_sources"][:3])
                sections.append(f"• Top sources: {sources}")
        
        # Call to action
        sections.append("\n**Get Involved:**")
        sections.append("• Join the discussion at forum.regen.network")
        sections.append("• Explore carbon credits at registry.regen.network")
        sections.append("• Learn more at docs.regen.network")
        
        return "\n".join(sections)
    
    def _backup_feed_to_drive(self):
        """Backup RSS feed to Google Drive"""
        if not self.drive_service:
            return
        
        try:
            feed_file = self.feed_path / "feed.xml"
            backup_name = f"feed_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xml"
            
            self._upload_to_drive(feed_file, backup_name)
            logger.info("RSS feed backed up to Google Drive")
            
        except Exception as e:
            logger.error(f"Failed to backup feed: {e}")
    
    def validate_feed(self) -> bool:
        """Validate the RSS feed against standards"""
        try:
            feed_file = self.feed_path / "feed.xml"
            if not feed_file.exists():
                logger.error("Feed file does not exist")
                return False
            
            # Parse the feed
            tree = ET.parse(feed_file)
            root = tree.getroot()
            
            # Check required channel elements
            channel = root.find("channel")
            required = ["title", "description", "link", "language"]
            
            for elem in required:
                if channel.find(elem) is None:
                    logger.error(f"Missing required element: {elem}")
                    return False
            
            # Check episodes
            items = channel.findall("item")
            if not items:
                logger.warning("No episodes in feed")
            
            for item in items:
                # Check required item elements
                item_required = ["title", "description", "enclosure"]
                for elem in item_required:
                    if item.find(elem) is None:
                        logger.error(f"Episode missing required element: {elem}")
                        return False
            
            logger.info("RSS feed validation successful")
            return True
            
        except Exception as e:
            logger.error(f"Feed validation failed: {e}")
            return False


def test_podcast_publisher():
    """Test the podcast publisher with sample data"""
    
    # Initialize publisher
    publisher = PodcastPublisher()
    
    # Create test audio file with minimal valid MP3 data
    test_audio = Path("test_episode.mp3")
    if not test_audio.exists():
        # Create a minimal valid MP3 file (silent audio)
        # This is a tiny valid MP3 file header with silence
        mp3_header = bytes([
            0xFF, 0xFB, 0x90, 0x00,  # MP3 frame header
            0x00, 0x00, 0x00, 0x00,  # Padding
            0x00, 0x00, 0x00, 0x00,  # More padding
            0x00, 0x00, 0x00, 0x00,  # Silent audio data
        ])
        test_audio.write_bytes(mp3_header * 100)  # Repeat to make it slightly larger
    
    # Test metadata from weekly digest
    test_metadata = {
        "week_ending": datetime.now(timezone.utc),
        "theme": "Regenerative Finance Innovations",
        "executive_summary": "This week saw major developments in regenerative finance with new carbon credit methodologies approved and increased on-chain activity.",
        "top_stories": [
            {"title": "New Soil Carbon Methodology Approved"},
            {"title": "KlimaDAO Integrates Regen Credits"},
            {"title": "Governance Proposal 47 Passes"}
        ],
        "themes": ["Carbon Markets", "Governance", "Technology Updates"],
        "top_tags": ["carbon", "governance", "defi", "soil", "climate"],
        "stats": {
            "total_content": 127,
            "top_sources": ["Twitter", "Discord", "Forum"]
        }
    }
    
    # Test adding episode
    print("Testing episode creation...")
    episode = publisher.publish_episode_from_weekly_digest(
        audio_file=str(test_audio),
        digest_metadata=test_metadata
    )
    
    print(f"Created episode: {episode.title}")
    print(f"Episode number: {episode.episode_number}")
    print(f"Audio URL: {episode.audio_url}")
    
    # Test RSS feed generation
    print("\nGenerating RSS feed...")
    feed_path = publisher.save_rss_feed()
    print(f"Feed saved to: {feed_path}")
    
    # Validate feed
    print("\nValidating RSS feed...")
    is_valid = publisher.validate_feed()
    print(f"Feed valid: {is_valid}")
    
    # Display feed snippet
    with open(feed_path, 'r') as f:
        feed_content = f.read()
        print("\nFeed preview (first 1000 chars):")
        print(feed_content[:1000])
    
    # Clean up test file
    if test_audio.exists() and test_audio.stat().st_size == 0:
        test_audio.unlink()
    
    return publisher


if __name__ == "__main__":
    # Run test
    print("="*50)
    print("PODCAST PUBLISHER TEST")
    print("="*50)
    
    if "--test" in sys.argv:
        publisher = test_podcast_publisher()
        print("\n✅ Podcast publisher test complete!")
    else:
        print("Usage: python podcast_publisher.py --test")
        print("\nThis module provides podcast publishing functionality including:")
        print("- RSS 2.0 feed generation with iTunes extensions")
        print("- Google Drive backup storage")
        print("- Episode metadata management")
        print("- Integration with weekly digest audio")