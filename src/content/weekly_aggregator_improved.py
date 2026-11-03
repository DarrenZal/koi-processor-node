#!/usr/bin/env python3
"""
Improved Weekly Content Aggregator for Regen Network

Key improvements:
1. Groups posts from same thread together
2. Removes sensor IDs from display
3. Fixes duplicate themes
4. Accurate story counts
5. Provides context for thread updates
"""

import json
import os
import sys
import logging
import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
from sklearn.cluster import DBSCAN
from collections import defaultdict, Counter
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ContentItem:
    """Represents a piece of content from the knowledge base"""
    id: str
    content: str
    title: str
    source: str
    source_type: str  # Clean source type (e.g., "discourse", "github")
    url: Optional[str]
    thread_url: Optional[str]  # Base thread URL for grouping
    publication_date: datetime
    confidence: float
    tags: List[str]
    embedding: Optional[np.ndarray] = None
    cluster_id: Optional[int] = None
    relevance_score: float = 0.0

@dataclass
class ThreadStory:
    """Represents a grouped story from a thread"""
    thread_url: str
    thread_title: str
    posts: List[ContentItem]
    latest_date: datetime
    context: str
    source_type: str

@dataclass
class WeeklyDigest:
    """Represents the weekly digest output"""
    week_start: datetime
    week_end: datetime
    story_count: int  # Actual number of stories (not raw items)
    clusters: List[Dict[str, Any]]
    top_stories: List[ThreadStory]
    brief: str
    citations: List[Dict[str, str]]
    stats: Dict[str, Any]

class ImprovedWeeklyAggregator:
    def __init__(self, config_path: str = "config/weekly_aggregator.json"):
        """Initialize the weekly aggregator"""
        self.config = self._load_config(config_path)
        self.db_conn = None
        self.bge_url = self.config.get("bge_server_url", "http://localhost:8090")
        self.koi_url = self.config.get("koi_coordinator_url", "http://localhost:8000")

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            # Default configuration
            return {
                "database": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "koi_knowledge",
                    "user": "postgres",
                    "password": "postgres"
                },
                "bge_server_url": "http://localhost:8090",
                "koi_coordinator_url": "http://localhost:8000",
                "content": {
                    "min_confidence": 0.6,
                    "max_items": 100,
                    "clustering_eps": 0.3,
                    "min_cluster_size": 3,
                    "brief_word_count": 1000
                },
                "sources": {
                    "prioritize": ["governance", "ecocredits", "discourse", "twitter"],
                    "exclude": []
                }
            }

    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_conn = psycopg2.connect(**self.config["database"])
            logger.info("Connected to database")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def extract_source_type(self, source_sensor: str) -> str:
        """Extract clean source type from sensor ID"""
        # Remove sensor ID suffix (e.g., "discourse-sensor-1758213905.340692" -> "discourse")
        if '-sensor-' in source_sensor:
            return source_sensor.split('-sensor-')[0]
        elif '_sensor_' in source_sensor:
            return source_sensor.split('_sensor_')[0]
        return source_sensor

    def extract_thread_url(self, url: str) -> Optional[str]:
        """Extract base thread URL for grouping"""
        if not url:
            return None

        # For Discourse URLs (e.g., /t/topic-name/123/4 -> /t/topic-name/123)
        if '/t/' in url:
            match = re.match(r'(.*?/t/[^/]+/\d+)', url)
            if match:
                return match.group(1)

        # For GitHub issues/PRs
        if 'github.com' in url and ('/issues/' in url or '/pull/' in url):
            match = re.match(r'(.*?/(issues|pull)/\d+)', url)
            if match:
                return match.group(1)

        return url

    def collect_weekly_content(self, days_back: int = 7) -> List[ContentItem]:
        """Collect content from the past week"""
        if not self.db_conn:
            self.connect_db()

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        query = """
        SELECT
            rid as id,
            content,
            metadata->>'title' as title,
            source_sensor as source,
            metadata->>'url' as url,
            published_at as publication_date,
            published_confidence as confidence,
            metadata->>'tags' as tags
        FROM koi_memories
        WHERE superseded_at IS NULL
            AND event_type != 'FORGET'
            -- Exclude all heartbeat content
            AND content::text NOT LIKE '%%sensor_heartbeat%%'
            AND content::text NOT LIKE '%%heartbeat%%'
            AND rid NOT LIKE '%%heartbeat%%'
            -- Exclude system/operational messages
            AND content::text NOT LIKE '%%Sensor initialized%%'
            AND content::text NOT LIKE '%%Monitoring active%%'
            AND content::text NOT LIKE '%%Starting sensor%%'
            AND content::text NOT LIKE '%%KOI system%%'
            -- ONLY content actually PUBLISHED in the specified window
            AND published_at > %s AND published_at <= %s
            AND published_confidence >= %s
        ORDER BY published_at DESC
        LIMIT %s
        """

        params = (
            start_date,
            end_date,
            self.config["content"]["min_confidence"],
            self.config["content"]["max_items"]
        )

        items = []
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

            for row in rows:
                # Parse content
                content_text = ""
                if row['content']:
                    try:
                        content_json = json.loads(row['content'])
                        if isinstance(content_json, dict):
                            content_text = content_json.get('content', '') or content_json.get('text', '') or str(content_json)
                        else:
                            content_text = str(content_json)
                    except:
                        content_text = str(row['content'])

                # Parse tags
                tags = []
                if row['tags']:
                    try:
                        tags = json.loads(row['tags']) if isinstance(row['tags'], str) else row['tags']
                    except:
                        tags = []

                title = row['title'] or "Untitled"
                source_type = self.extract_source_type(row['source'] or "unknown")
                thread_url = self.extract_thread_url(row['url'])

                items.append(ContentItem(
                    id=row['id'],
                    content=content_text[:1000],  # Truncate for processing
                    title=title,
                    source=row['source'] or "unknown",
                    source_type=source_type,
                    url=row['url'],
                    thread_url=thread_url,
                    publication_date=row['publication_date'],
                    confidence=row['confidence'],
                    tags=tags
                ))

        logger.info(f"Collected {len(items)} items from past {days_back} days")
        return items

    def group_by_thread(self, items: List[ContentItem]) -> List[ThreadStory]:
        """Group content items by thread"""
        thread_groups = defaultdict(list)

        for item in items:
            # Use thread URL for grouping, or fall back to individual URL
            group_key = item.thread_url or item.url or item.title
            thread_groups[group_key].append(item)

        stories = []
        for thread_key, thread_items in thread_groups.items():
            # Sort items by date (newest first)
            thread_items.sort(key=lambda x: x.publication_date, reverse=True)

            # Determine thread title (from first post or most descriptive)
            thread_title = thread_items[0].title
            if "Re:" in thread_title and len(thread_items) > 1:
                # Try to find original title without "Re:"
                for item in thread_items:
                    if "Re:" not in item.title:
                        thread_title = item.title
                        break

            # Build context from multiple posts
            context = self.build_thread_context(thread_items)

            story = ThreadStory(
                thread_url=thread_key,
                thread_title=thread_title,
                posts=thread_items,
                latest_date=thread_items[0].publication_date,
                context=context,
                source_type=thread_items[0].source_type
            )
            stories.append(story)

        # Sort stories by latest activity
        stories.sort(key=lambda x: x.latest_date, reverse=True)

        logger.info(f"Grouped {len(items)} items into {len(stories)} thread stories")
        return stories

    def build_thread_context(self, posts: List[ContentItem], max_context_length: int = 500) -> str:
        """Build meaningful context from thread posts"""
        if not posts:
            return ""

        context_parts = []
        total_length = 0

        # Include content from most recent posts
        for post in posts[:3]:  # Up to 3 most recent posts
            content = post.content.strip()
            if content:
                # Remove "Re:" responses that just quote
                content = re.sub(r'^Re:\s*', '', content)

                # Take first paragraph or up to 200 chars
                first_para = content.split('\n')[0][:200]

                if first_para:
                    context_parts.append(first_para)
                    total_length += len(first_para)

                    if total_length > max_context_length:
                        break

        if context_parts:
            # Join with indication of multiple posts
            if len(posts) > 1:
                context = f"Thread with {len(posts)} posts: " + " [...] ".join(context_parts)
            else:
                context = context_parts[0]

            if len(context) > max_context_length:
                context = context[:max_context_length] + "..."

            return context

        return posts[0].content[:max_context_length] if posts else ""

    def cluster_stories(self, stories: List[ThreadStory]) -> List[List[ThreadStory]]:
        """Cluster similar stories by theme"""
        if not stories:
            return []

        # For now, group by source type and tags
        theme_groups = defaultdict(list)

        for story in stories:
            # Extract main theme from tags
            all_tags = []
            for post in story.posts:
                all_tags.extend(post.tags)

            if all_tags:
                # Get most common tag as theme
                tag_counts = Counter(all_tags)
                theme = tag_counts.most_common(1)[0][0]
            else:
                # Use source type as theme
                theme = story.source_type

            theme_groups[theme].append(story)

        # Convert to list of clusters
        clusters = list(theme_groups.values())

        # Sort clusters by size
        clusters.sort(key=len, reverse=True)

        return clusters

    def generate_brief(self, digest: WeeklyDigest) -> str:
        """Generate the weekly brief narrative"""
        lines = []

        # Header
        lines.append(f"# Regen Network Weekly Digest")
        lines.append(f"{digest.week_start.strftime('%B %d')} - {digest.week_end.strftime('%B %d, %Y')}\n")

        # Executive Summary
        lines.append("## Executive Summary\n")
        lines.append(f"This week saw {digest.story_count} significant updates across the Regen Network ecosystem. ")

        # Theme summary (with deduplication)
        if digest.clusters:
            seen_themes = set()
            unique_themes = []
            for cluster in digest.clusters[:5]:  # Top 5 clusters
                if cluster['theme'] not in seen_themes:
                    seen_themes.add(cluster['theme'])
                    unique_themes.append(f"{cluster['theme']} ({cluster['size']} stories)")

            if unique_themes:
                lines.append(f"Key themes included: {', '.join(unique_themes)}. ")

        lines.append("\n")

        # Top Stories
        lines.append("## Top Stories\n")
        for i, story in enumerate(digest.top_stories[:5], 1):
            lines.append(f"### {i}. {story.thread_title}\n")

            # Story context
            lines.append(f"{story.context}\n")

            # Metadata
            lines.append(f"*Source: {story.source_type} | ")
            lines.append(f"Updated: {story.latest_date.strftime('%Y-%m-%d')} | ")
            lines.append(f"Posts: {len(story.posts)}*\n")
            lines.append("")

        # Thematic Analysis
        lines.append("## Thematic Analysis\n")
        seen_themes = set()
        for cluster in digest.clusters[:5]:
            theme = cluster['theme']
            if theme not in seen_themes:
                seen_themes.add(theme)
                lines.append(f"### {theme.title()}\n")
                lines.append(f"This theme includes {cluster['size']} stories. ")

                # Sample stories from cluster
                sample_stories = cluster['stories'][:3]
                if sample_stories:
                    lines.append("Key discussions:\n")
                    for story in sample_stories:
                        lines.append(f"- {story['title']} ({story['posts']} posts)\n")
                lines.append("")

        # Statistics
        lines.append("## Weekly Statistics\n")
        if digest.stats:
            lines.append(f"- Total Stories: {digest.stats.get('story_count', 0)}\n")
            lines.append(f"- Total Posts/Updates: {digest.stats.get('total_items', 0)}\n")
            lines.append(f"- Active Sources: {digest.stats.get('unique_sources', 0)}\n")
            lines.append(f"- Most Active: {digest.stats.get('most_active_source', 'N/A')}\n")
            lines.append(f"- Average Confidence: {digest.stats.get('avg_confidence', 0):.2f}\n")

        lines.append("\n## References\n")

        # Additional context
        lines.append("\n## Additional Context\n")
        lines.append("The Regen Network continues to advance its mission of ecological regeneration ")
        lines.append("through coordinated action across technology, governance, and community engagement. ")
        lines.append("This week's developments reflect the growing momentum in regenerative finance ")
        lines.append("and ecological data infrastructure.\n")

        lines.append("\n---\n")
        lines.append("*This digest was automatically generated by the Regen Network KOI system.*\n")

        return "".join(lines)

    def generate_citations(self, stories: List[ThreadStory]) -> List[Dict[str, str]]:
        """Generate citations from stories"""
        citations = []
        seen = set()

        for story in stories:
            # Add citation for the thread
            cite_id = f"{story.thread_url}:{story.thread_title}"
            if cite_id not in seen:
                seen.add(cite_id)

                # Clean up URL (remove sensor artifacts)
                clean_url = story.thread_url
                if clean_url and 'github_sensor_' in clean_url:
                    clean_url = re.sub(r'/github_sensor_[^/]+/', '/', clean_url)

                citations.append({
                    "title": story.thread_title,
                    "source": story.source_type,  # Use clean source type
                    "url": clean_url or "",
                    "date": story.latest_date.strftime('%Y-%m-%d'),
                    "posts": len(story.posts)
                })

        return citations

    def calculate_stats(self, stories: List[ThreadStory]) -> Dict[str, Any]:
        """Calculate statistics for digest"""
        if not stories:
            return {}

        # Collect all posts
        all_posts = []
        for story in stories:
            all_posts.extend(story.posts)

        source_types = Counter(story.source_type for story in stories)
        confidences = [post.confidence for post in all_posts]

        return {
            "story_count": len(stories),
            "total_items": len(all_posts),
            "unique_sources": len(source_types),
            "most_active_source": source_types.most_common(1)[0][0] if source_types else "N/A",
            "source_distribution": dict(source_types),
            "avg_confidence": np.mean(confidences) if confidences else 0,
            "min_confidence": min(confidences) if confidences else 0,
            "max_confidence": max(confidences) if confidences else 0
        }

    def generate_digest(self, days_back: int = 7) -> WeeklyDigest:
        """Generate the complete weekly digest"""
        logger.info(f"Generating improved weekly digest for past {days_back} days")

        # Collect content
        items = self.collect_weekly_content(days_back)

        if not items:
            logger.warning("No content found for digest")
            return None

        # Group by thread
        stories = self.group_by_thread(items)

        # Cluster stories by theme
        story_clusters = self.cluster_stories(stories)

        # Prepare cluster data with deduplication
        cluster_data = []
        seen_themes = set()
        theme_counter = 1

        for cluster in story_clusters:
            if cluster:
                # Extract theme from first story's tags or use source type
                all_tags = []
                for story in cluster:
                    for post in story.posts:
                        all_tags.extend(post.tags)

                if all_tags:
                    tag_counts = Counter(all_tags)
                    base_theme = tag_counts.most_common(1)[0][0].title()
                else:
                    base_theme = cluster[0].source_type.title()

                # Make theme unique if duplicate
                theme = base_theme
                if theme in seen_themes:
                    theme = f"{base_theme} {theme_counter}"
                    theme_counter += 1
                seen_themes.add(theme)

                cluster_data.append({
                    "theme": theme,
                    "size": len(cluster),
                    "stories": [
                        {
                            "title": story.thread_title,
                            "source": story.source_type,
                            "posts": len(story.posts),
                            "latest": story.latest_date.isoformat()
                        }
                        for story in cluster[:5]  # Top 5 stories from each cluster
                    ]
                })

        # Sort clusters by size
        cluster_data.sort(key=lambda x: x['size'], reverse=True)

        # Create digest
        digest = WeeklyDigest(
            week_start=datetime.now() - timedelta(days=days_back),
            week_end=datetime.now(),
            story_count=len(stories),
            clusters=cluster_data,
            top_stories=stories[:10],  # Top 10 stories
            brief="",  # Will be generated
            citations=[],  # Will be generated
            stats=self.calculate_stats(stories)
        )

        # Generate brief and citations
        digest.brief = self.generate_brief(digest)
        digest.citations = self.generate_citations(stories)

        logger.info(f"Generated digest with {len(stories)} stories from {len(items)} posts")
        return digest

    def export_to_json(self, digest: WeeklyDigest, output_path: str) -> None:
        """Export digest to JSON format"""
        # Convert to serializable format
        export_data = {
            "week_start": digest.week_start.isoformat(),
            "week_end": digest.week_end.isoformat(),
            "story_count": digest.story_count,
            "clusters": digest.clusters,
            "top_stories": [
                {
                    "title": story.thread_title,
                    "context": story.context,
                    "source": story.source_type,
                    "url": story.thread_url,
                    "latest_date": story.latest_date.isoformat(),
                    "post_count": len(story.posts)
                }
                for story in digest.top_stories
            ],
            "brief": digest.brief,
            "citations": digest.citations,
            "stats": digest.stats
        }

        with open(output_path, 'w') as f:
            json.dump(export_data, f, indent=2)

        logger.info(f"Exported digest to {output_path}")

    def export_to_markdown(self, digest: WeeklyDigest, output_path: str) -> None:
        """Export digest to Markdown format"""
        with open(output_path, 'w') as f:
            f.write(digest.brief)

            # Add citations section
            f.write("\n## Citations\n")
            for i, cite in enumerate(digest.citations, 1):
                f.write(f"{i}. [{cite['title']}]({cite['url']}) - {cite['source']} ({cite['date']})\n")

        logger.info(f"Exported digest to {output_path}")

def main():
    """Main execution function"""
    aggregator = ImprovedWeeklyAggregator()

    # Generate digest
    digest = aggregator.generate_digest(days_back=7)

    if digest:
        # Create output directory
        output_dir = "/opt/projects/koi-processor/output/weekly"
        os.makedirs(output_dir, exist_ok=True)

        # Generate filename with date
        date_str = datetime.now().strftime("%Y-%m-%d")

        # Export to multiple formats
        aggregator.export_to_json(digest, f"{output_dir}/weekly_digest_{date_str}_improved.json")
        aggregator.export_to_markdown(digest, f"{output_dir}/weekly_digest_{date_str}_improved.md")

        print(f"Weekly digest generated successfully")
        print(f"Stories: {digest.story_count}")
        print(f"Total posts: {digest.stats.get('total_items', 0)}")
    else:
        print("Failed to generate weekly digest")
        sys.exit(1)

if __name__ == "__main__":
    main()