#!/usr/bin/env python3
"""
Enhanced Daily Content Curator with LLM Integration
Uses RAG approach to intelligently process ALL content from past 24 hours
"""

import asyncio
import asyncpg
import httpx
import json
import yaml
import re
import uuid
import random
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from loguru import logger
import os
import hashlib
import openai
from openai import AsyncOpenAI

# Configure logging
logger.add("logs/daily_curator_llm.log", rotation="10 MB", retention="7 days")


class DailyCuratorLLM:
    """
    Enhanced Daily Content Curator that uses LLMs to intelligently process content
    """

    def __init__(self, config_path: str = "config/curator_config.yaml"):
        """Initialize the Daily Curator with LLM support"""
        self.config = self._load_config(config_path)
        self.db_url = self.config.get('database_url', os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza'))

        # LLM Configuration
        self.openai_client = AsyncOpenAI(
            api_key=os.getenv('OPENAI_API_KEY'),
            base_url=os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')
        )
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4-turbo-preview')

        # Content parameters
        self.max_thread_posts = 5
        self.min_thread_posts = 3
        self.tweet_max_length = 280

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        else:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return {}

    async def create_unified_sources(self, all_content: List[Dict], posts: List[Dict]) -> List[Dict]:
        """
        Create a unified sources section with actual URLs from all content used
        """
        unified = []
        seen_urls = set()
        seen_topics = set()  # Track forum topics to avoid duplicates

        # Collect unique sources from content
        for item in all_content:
            # Get URL from metadata
            metadata = item.get('metadata', {})
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = {}

            # First check content for URL (discourse sensor puts it there)
            content_data = item.get('content', {})

            # Parse content if it's a JSON string
            if isinstance(content_data, str):
                try:
                    content_data = json.loads(content_data)
                except:
                    pass

            if isinstance(content_data, dict):
                url = content_data.get('url')
            else:
                url = None

            # Then check metadata fields
            if not url:
                # First check for parent_url (for forum child posts)
                url = metadata.get('parent_url') or metadata.get('post_url') or metadata.get('topic_url') or metadata.get('url') or metadata.get('source_url') or metadata.get('link')

                # If still no URL, try parent_rid provenance lookup
                if not url and metadata.get('parent_rid'):
                    parent_rid = metadata.get('parent_rid')
                    try:
                        import requests
                        response = requests.get(f"http://localhost:8002/api/koi/graph/provenance/{parent_rid}", timeout=2)
                        if response.status_code == 200:
                            provenance_data = response.json()
                            if provenance_data.get('document') and provenance_data['document'].get('metadata'):
                                parent_metadata = provenance_data['document']['metadata']
                                if isinstance(parent_metadata, str):
                                    try:
                                        parent_metadata = json.loads(parent_metadata)
                                    except:
                                        parent_metadata = {}
                                if isinstance(parent_metadata, dict):
                                    url = parent_metadata.get('url', '')
                    except:
                        pass  # Silently fail for provenance lookup

            # As a last resort, construct URL from RID for known patterns
            if not url and item.get('rid'):
                rid = item['rid']
                # For forum posts, construct from topic ID
                if 'forum' in rid.lower() or 'discourse' in rid.lower():
                    import re
                    topic_match = re.search(r'_(\d+)_post', rid)
                    if topic_match:
                        topic_id = topic_match.group(1)
                        url = f'https://forum.regen.network/t/topic/{topic_id}'
                        # Deduplicate by topic
                        if topic_id in seen_topics:
                            continue
                        seen_topics.add(topic_id)
                # For GitHub, use repo name
                elif 'github' in rid.lower():
                    # Just use a generic GitHub URL for now
                    url = 'https://github.com/regen-network'

            if url and url not in seen_urls:
                seen_urls.add(url)

                # Determine source type
                source_type = 'unknown'
                if 'forum.regen.network' in url:
                    source_type = 'forum'
                elif 'github.com' in url:
                    source_type = 'github'
                elif 'regentokenomics.org' in url:
                    source_type = 'governance'
                elif 'discord' in url.lower():
                    source_type = 'discord'
                elif 'twitter' in url.lower() or 'x.com' in url:
                    source_type = 'twitter'

                # Extract title based on source type
                title = metadata.get('title') or metadata.get('topic_title')

                # Try to get title from content if not in metadata
                if not title and isinstance(content_data, dict):
                    title = content_data.get('title')

                # Special handling for GitHub sources
                if source_type == 'github' and url and url != 'https://github.com/regen-network':
                    # Extract repo name from URL
                    parts = url.replace('https://github.com/', '').split('/')
                    if len(parts) >= 2:
                        title = f"{parts[1]} repository update"
                    else:
                        title = "GitHub repository update"
                elif source_type == 'forum':
                    # Use topic title if available
                    if not title:
                        title = metadata.get('topic_title', "Forum discussion")
                elif not title:
                    # Generic fallback
                    if isinstance(content_data, dict):
                        text = content_data.get('text', '')
                        if isinstance(text, str) and text:
                            # Take first line or 50 chars
                            first_line = text.split('\n')[0].strip()
                            if first_line and not first_line.startswith('{'):
                                title = first_line[:50] + '...' if len(first_line) > 50 else first_line
                            else:
                                title = f"{source_type.title()} content"
                        else:
                            title = f"{source_type.title()} content"
                    else:
                        title = f"{source_type.title()} content"

                # Clean up title
                if isinstance(title, str) and len(title) > 100:
                    title = title[:97] + '...'

                # Handle published_at properly
                published_at = item.get('published_at')
                if hasattr(published_at, 'isoformat'):
                    published_at = published_at.isoformat()
                elif not isinstance(published_at, str):
                    published_at = str(published_at) if published_at else None

                unified.append({
                    'type': source_type,
                    'title': title,
                    'url': self.clean_url(url),
                    'published_at': published_at,
                    'sensor': item.get('source_sensor', 'unknown')
                })

        # Add comprehensive ledger sources if stats were included
        has_stats_post = any(post.get('type') in ['stats', 'highlight'] for post in posts)
        if has_stats_post:
            # Main blockchain explorer
            unified.append({
                'type': 'ledger',
                'title': 'Regen Network Blockchain Explorer',
                'url': 'https://www.mintscan.io/regen',
                'sensor': 'direct_api'
            })

            # Check for specific ledger sources from posts
            for post in posts:
                if post.get('type') in ['stats', 'highlight']:
                    for source in post.get('sources', []):
                        if source.get('type') == 'ledger':
                            # Add recent blocks if available
                            if source.get('block_links'):
                                for block in source['block_links'][:2]:  # Top 2 blocks
                                    unified.append({
                                        'type': 'block',
                                        'title': f"Block #{block.get('height', '')}",
                                        'url': block.get('url', ''),
                                        'sensor': 'direct_api'
                                    })

                            # Add marketplace link if there's activity
                            if source.get('marketplace_url'):
                                unified.append({
                                    'type': 'marketplace',
                                    'title': 'Marketplace Activity',
                                    'url': source['marketplace_url'],
                                    'sensor': 'direct_api'
                                })

                            # Add validators link
                            if source.get('validators_url'):
                                unified.append({
                                    'type': 'validators',
                                    'title': 'Active Validators',
                                    'url': source['validators_url'],
                                    'sensor': 'direct_api'
                                })
                            break

        # Sort by type priority
        type_priority = {'forum': 0, 'governance': 1, 'ledger': 2, 'github': 3, 'discord': 4, 'twitter': 5}
        unified.sort(key=lambda x: type_priority.get(x['type'], 99))

        return unified

    def clean_url(self, url: str) -> str:
        """Clean malformed GitHub URLs and other URL issues"""
        if not url:
            return url

        # Fix GitHub sensor URLs that include temporary directory paths
        if 'github.com' in url and 'github_sensor_' in url:
            # Remove the github_sensor_XXXXX directory from the path
            url = re.sub(r'/github_sensor_[^/]+/', '/', url)

        return url

    async def get_forum_thread_context(self, conn, recent_post_rid: str) -> List[Dict[str, Any]]:
        """
        Get additional context from forum threads
        If we have a recent post, get other posts from the same thread
        """
        # Extract thread ID from the RID (e.g., "forum.regen.network_529_post_1")
        if '_post_' not in recent_post_rid:
            return []

        thread_part = recent_post_rid.split('_post_')[0]  # Gets "forum.regen.network_529"

        # Get all posts from this thread (up to last 10 posts)
        query = """
            SELECT
                id, rid, source_sensor, event_type,
                content, metadata,
                published_at, published_confidence,
                created_at
            FROM koi_memories
            WHERE rid LIKE %s
              AND superseded_at IS NULL
              -- Exclude private content (#28: curator SQL privacy audit)
              AND (is_private = FALSE OR is_private IS NULL)
              AND event_type != 'FORGET'
              AND rid NOT LIKE '%%heartbeat%%'
            ORDER BY published_at ASC
            LIMIT 10
        """

        thread_pattern = f"{thread_part}_post_%"
        rows = await conn.fetch(query, thread_pattern)
        return [dict(row) for row in rows]

    async def get_recent_forum_threads(self, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get the most recent forum threads regardless of date
        Used as fallback when there's no daily activity
        Returns list of thread summaries with URLs
        """
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                # Simple approach: get most recent forum posts/topics
                query = """
                    SELECT
                        rid,
                        source_sensor,
                        content,
                        metadata,
                        published_at,
                        CASE
                            WHEN rid LIKE '%topic_%' THEN
                                SUBSTRING(rid FROM 'topic_([0-9]+)')
                            ELSE
                                SUBSTRING(rid FROM 'forum\\.regen\\.network_([0-9]+)')
                        END as thread_id
                    FROM koi_memories
                    WHERE source_sensor LIKE '%discourse%'
                      AND (rid LIKE 'regen.forum-post:%' OR rid LIKE 'regen.forum-topic:%')
                      AND superseded_at IS NULL
                      -- Exclude private content (#28: curator SQL privacy audit)
                      AND (is_private = FALSE OR is_private IS NULL)
                      AND event_type != 'FORGET'
                      AND published_at IS NOT NULL
                    ORDER BY published_at DESC
                    LIMIT $1
                """

                rows = await conn.fetch(query, limit * 2)  # Get more to filter later

                threads = []
                seen_threads = set()  # Track which threads we've already added

                for row in rows:
                    thread_id = row['thread_id']

                    # Skip if we've already added this thread
                    if thread_id in seen_threads:
                        continue

                    content = row['content']
                    metadata = row['metadata'] or {}

                    # Parse content if JSON string
                    if isinstance(content, str):
                        try:
                            content = json.loads(content)
                        except:
                            pass

                    # Parse metadata if JSON string
                    if isinstance(metadata, str):
                        try:
                            metadata = json.loads(metadata)
                        except:
                            metadata = {}

                    if isinstance(content, dict):
                        title = content.get('title', '')
                        text = content.get('text', content.get('content', ''))[:200]
                    else:
                        title = ''
                        text = str(content)[:200]

                    # Get thread URL from metadata
                    url = metadata.get('url', '') if isinstance(metadata, dict) else ''
                    if not url and thread_id:
                        # Construct URL from thread ID
                        url = f"https://forum.regen.network/t/{thread_id}"

                    if url and thread_id:  # Only add threads with valid URLs and IDs
                        threads.append({
                            'title': title or f"Recent Forum Discussion",
                            'url': url,
                            'preview': text,
                            'published_at': row['published_at'],
                            'thread_id': thread_id
                        })
                        seen_threads.add(thread_id)

                        # Stop when we have enough unique threads
                        if len(threads) >= limit:
                            break

                # Sort by most recent
                threads.sort(key=lambda x: x['published_at'] if x['published_at'] else '', reverse=True)

                return threads

    async def generate_forum_thread_summary(self, thread: Dict[str, Any]) -> str:
        """
        Generate an engaging summary of a forum thread for the daily post
        """
        try:
            # Extract thread details
            title = thread.get('title', 'Forum Discussion')
            preview = thread.get('preview', '')
            url = thread.get('url', '')

            prompt = f"""Create an engaging tweet about this Regen Network forum discussion.
Thread Title: {title}
Preview: {preview}
URL: {url}

Requirements:
- Start with 💬 emoji
- Briefly summarize the topic in an engaging way
- Include a call to action to join the discussion
- Mention specific value or insight from the thread
- End with the shortened URL
- Use exactly 260-280 characters
- Make it inviting and conversational
- DO NOT just copy the preview text"""

            response = await self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a community engagement expert for Regen Network. Only write about information explicitly provided in the context. Do not fabricate details or assume developments."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=100
            )

            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating forum summary: {e}")
            # Fallback to simple format
            return f"💬 Join the discussion: {title[:100]}... Check out this engaging forum thread and share your thoughts! {url}"

    async def get_all_24h_content(self) -> List[Dict[str, Any]]:
        """
        Get ALL content PUBLISHED in the past 24 hours
        Uses published_at date (when content was actually created)
        NOT when it was ingested into KOI
        """
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                query = """
                    SELECT
                        id, rid, source_sensor, event_type,
                        content, metadata,
                        published_at, published_confidence,
                        created_at
                    FROM koi_memories
                    WHERE superseded_at IS NULL
                      AND event_type != 'FORGET'
                      -- Exclude private content (#28: curator SQL privacy audit)
                      AND (is_private = FALSE OR is_private IS NULL)
                      -- Exclude all heartbeat content
                      AND content::text NOT LIKE '%sensor_heartbeat%'
                      AND content::text NOT LIKE '%heartbeat%'
                      AND rid NOT LIKE '%heartbeat%'
                      -- Exclude system/operational messages
                      AND content::text NOT LIKE '%Sensor initialized%'
                      AND content::text NOT LIKE '%Monitoring active%'
                      AND content::text NOT LIKE '%Starting sensor%'
                      AND content::text NOT LIKE '%KOI system%'
                      -- EXCLUDE GITHUB FILE CHUNKS - only want actual activity for daily posts
                      -- But allow forum/website chunks which are actual content
                      AND NOT (source_sensor LIKE '%github%' AND rid LIKE '%#chunk%')
                      -- DAILY THREADS: Only discourse (forum), GitHub, and ledger sources
                      -- Exclude Notion, website scrapes, and other sources
                      AND (
                          source_sensor LIKE '%discourse%'
                          OR source_sensor LIKE '%github%'
                          OR source_sensor LIKE '%ledger%'
                      )
                      -- ONLY content actually PUBLISHED in last 24 hours
                      AND published_at IS NOT NULL
                      AND published_at >= NOW() - INTERVAL '24 hours'
                      AND published_at <= NOW()
                      -- Reasonable confidence in the published date
                      AND published_confidence >= 0.7
                    ORDER BY published_at DESC
                """

                rows = await conn.fetch(query)
                content_items = [dict(row) for row in rows]

                # For forum posts, get additional thread context
                forum_posts = [item for item in content_items if 'forum' in item.get('source_sensor', '')]
                additional_context = []

                for post in forum_posts:
                    # Get the parent RID if it's a chunk
                    parent_rid = post.get('metadata', {}).get('parent_rid', post.get('rid', ''))

                    # Get thread context for this post
                    thread_context = await self.get_forum_thread_context(conn, parent_rid)

                    # Add context posts that aren't already in our 24h list
                    existing_rids = {item['rid'] for item in content_items}
                    for context_post in thread_context:
                        if context_post['rid'] not in existing_rids:
                            # Mark as context, not primary content
                            context_post['is_thread_context'] = True
                            additional_context.append(context_post)

                # Combine primary content with thread context
                all_content = content_items + additional_context

                logger.info(f"Retrieved {len(content_items)} primary items and {len(additional_context)} context items")
                return all_content

    async def summarize_content_batch(self, content_items: List[Dict]) -> str:
        """
        Use LLM to summarize a batch of content items
        """
        # Prepare content for summarization
        content_texts = []
        for item in content_items:
            content_data = item.get('content', {})

            # Parse content if it's a JSON string
            if isinstance(content_data, str):
                try:
                    content_data = json.loads(content_data)
                except:
                    text = str(content_data)
                    content_data = {}

            if isinstance(content_data, dict):
                text = content_data.get('text', '') or content_data.get('content', '')
            else:
                text = str(content_data)

            if text and len(text.strip()) > 10:
                source = item.get('source_sensor', 'unknown')
                timestamp = item.get('published_at', item.get('created_at', ''))
                content_texts.append(f"[{source}] {text[:500]}")

        if not content_texts:
            return ""

        prompt = f"""Summarize these Regen Network updates from the past 24 hours.
Focus on key themes, important governance proposals, new carbon credits, and community discussions:

{chr(10).join(content_texts[:20])}  # Limit to prevent token overflow

Provide a concise summary highlighting:
1. Most important developments
2. Key governance activities
3. New ecological credits or batches
4. Notable community discussions
5. Technical updates

Keep the summary under 300 words."""

        try:
            response = await self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at summarizing blockchain and environmental finance updates. Only summarize information that is explicitly present in the provided content. Do not invent or assume developments that are not mentioned."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=400
            )

            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Error summarizing content: {e}")
            return ""

    async def generate_tweet_from_content(self, content_items: List[Dict], tweet_type: str, position: int, stats: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Generate a specific tweet using LLM based on all content with source tracking

        Args:
            content_items: All content from past 24h
            tweet_type: Type of tweet (headline, stats, highlight, community, cta)
            position: Position in thread (1-5)
            stats: Optional ledger statistics
        Returns:
            Dict with 'content' and 'sources' keys
        """
        # Track sources for this tweet
        sources = []

        # Separate primary content from thread context
        primary_content = [item for item in content_items if not item.get('is_thread_context', False)]
        thread_context = [item for item in content_items if item.get('is_thread_context', False)]

        # First, get a summary if content is large
        if len(primary_content) > 50:
            summary = await self.summarize_content_batch(primary_content)
            context = f"Summary of {len(primary_content)} updates:\n{summary}"
            # Add source tracking for large batches
            sensor_counts = {}
            for item in content_items:
                sensor = item.get('source_sensor', 'unknown')
                sensor_counts[sensor] = sensor_counts.get(sensor, 0) + 1
            sources.append({
                'type': 'aggregated',
                'description': f"Aggregated from {len(content_items)} items",
                'sensor_breakdown': sensor_counts
            })
        else:
            # Use full content for smaller sets
            content_texts = []
            for item in content_items[:30]:  # Limit to prevent token overflow
                content_data = item.get('content', {})
                metadata = item.get('metadata', {})

                # Parse content if it's a JSON string
                if isinstance(content_data, str):
                    try:
                        content_data = json.loads(content_data)
                    except:
                        text = str(content_data)
                        content_data = {}

                if isinstance(content_data, dict):
                    text = content_data.get('text', '') or str(content_data.get('content', ''))
                else:
                    text = str(content_data)

                if text and len(text.strip()) > 10:
                    content_texts.append(text[:200])
                    # Track individual sources
                    # Handle published_at which may be datetime or string
                    published_at = item.get('published_at', '')
                    if hasattr(published_at, 'isoformat'):
                        # It's a datetime object
                        published_at = published_at.isoformat().split('T')[0]
                    elif isinstance(published_at, str) and published_at:
                        published_at = published_at.split('T')[0]
                    else:
                        published_at = 'unknown'

                    source_info = {
                        'sensor': item.get('source_sensor', 'unknown'),
                        'event_type': item.get('event_type', 'unknown'),
                        'published_at': published_at
                    }

                    # Add URL if available
                    if isinstance(metadata, dict):
                        url = None

                        # Try direct URL first
                        if metadata.get('url'):
                            url = metadata.get('url')
                        elif metadata.get('link'):
                            url = metadata.get('link')

                        # If no URL but has parent_url, use that (for forum posts)
                        elif metadata.get('parent_url'):
                            url = metadata.get('parent_url')

                        # If still no URL, try to get from parent using provenance API
                        elif metadata.get('parent_rid'):
                            parent_rid = metadata.get('parent_rid')
                            try:
                                import requests
                                # Call provenance API to get parent document
                                response = requests.get(f"http://localhost:8002/api/koi/graph/provenance/{parent_rid}", timeout=5)
                                if response.status_code == 200:
                                    provenance_data = response.json()
                                    # Check if parent document has URL
                                    if provenance_data.get('document') and provenance_data['document'].get('metadata'):
                                        parent_metadata = provenance_data['document']['metadata']
                                        if isinstance(parent_metadata, str):
                                            try:
                                                parent_metadata = json.loads(parent_metadata)
                                            except:
                                                parent_metadata = {}
                                        if isinstance(parent_metadata, dict):
                                            url = parent_metadata.get('url', '')
                            except Exception as e:
                                logger.debug(f"Could not fetch provenance for {parent_rid}: {e}")

                        if url:
                            source_info['url'] = self.clean_url(url)

                    sources.append(source_info)

            # Handle empty content gracefully
            if content_texts:
                context = "Recent updates:\n" + "\n".join(content_texts)
            else:
                # No content from last 24h - use ledger-based context
                if stats:
                    context = f"No new GitHub/forum activity in past 24h. Focus on ledger metrics and community building.\nLedger stats: {stats.get('ledger_summary', 'Active network with ongoing transactions')}"
                    # Add ledger as source when no other content
                    if not sources:
                        sources.append({
                            'type': 'ledger',
                            'description': 'Regen Network Blockchain Explorer'
                        })
                else:
                    context = "Quiet day with minimal new activity. Focus on community engagement and ongoing initiatives."

        # Create type-specific prompts
        prompts = {
            'headline': f"""Create an engaging headline tweet for Regen Network's daily thread.
Context: {context}

Requirements:
- Start with 🌱 emoji
- Include #RegenNetwork hashtag
- Highlight the overall theme or mood of the day
- Be welcoming and set the stage for the thread
- Don't get too specific about stats (those come in post 2)
- Use exactly 260-280 characters
- Focus on the community and mission""",

            'stats': f"""Create a statistics tweet highlighting Regen Network's 24h metrics.
Context: {context}

Ledger Statistics:
{stats.get('ledger_summary', '') if stats else 'No ledger data available'}
- Active Proposals: {stats.get('active_proposals', 0) if stats else 0}
- New Credit Batches: {stats.get('new_credits', 0) if stats else 0}
- Marketplace Orders: {stats.get('marketplace_volume', 0) if stats else 0}
- Credit Classes: {stats.get('credit_classes', 0) if stats else 0}

Requirements:
- Start with 📊 emoji
- Include specific numbers from the ledger statistics above
- Mention new credits, proposals, validator count, or volume
- Make numbers compelling with context
- Use exactly 260-280 characters
- Include #ReFi hashtag""",

            'governance': f"""Create a governance-focused tweet about Regen Network proposals and decisions.
Context: {context}

Governance Activity:
- Active Proposals: {stats.get('active_proposals', 0) if stats else 0}
- Total Proposals: {stats.get('total_proposals', 0) if stats else 0}
{f"Active Proposal Titles: {', '.join(stats.get('active_proposal_titles', [])[:2])}" if stats and stats.get('active_proposal_titles') else ''}

Requirements:
- Start with 🗳️ emoji
- Highlight active proposals or recent votes (use the data above)
- Emphasize community participation
- Include specific proposal details if available
- Use exactly 260-280 characters
- Make it actionable""",

            'community': f"""Create a community highlight tweet showcasing discussions and contributions.
Context: {context}

IMPORTANT RULES:
- NEVER invent fake usernames or handles (like @EcoWarrior123)
- ONLY mention specific users if they appear in the context above
- If no specific users are mentioned in context, focus on topics and themes instead
- Do NOT make up community members that don't exist
- DO NOT repeat ledger statistics that were already mentioned in previous posts

Requirements:
- Start with 💬 emoji
- Highlight interesting discussions or contributions from the actual context
- Focus on real topics and themes, not invented users
- Encourage participation without repeating stats
- Use exactly 260-280 characters
- Include #RegenNetwork""",

            'cta': f"""Create a call-to-action tweet to close the daily thread.
Context: {context}

IMPORTANT: Previous posts have already covered ledger stats. DO NOT repeat specific numbers.

Requirements:
- Start with 🌍 emoji
- Include links to regen.network and discord
- Summarize the day's key theme or sentiment (not stats)
- Inspiring message about regenerative finance and community
- Focus on the future and invitation to join
- Use exactly 260-280 characters
- End with #ReFi #ClimateAction"""
        }

        prompt = prompts.get(tweet_type, prompts['headline'])

        try:
            response = await self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": """You are a social media expert for Regen Network, crafting engaging tweets about regenerative finance and ecological credits.
                    CRITICAL RULES:
                    1. Always write tweets that are exactly 260-280 characters
                    2. NEVER invent fake usernames, handles, or specific names that don't appear in the provided context
                    3. Only reference actual data, statistics, and information from the context provided
                    4. If you don't have specific data, use general language instead of making up specifics
                    5. DO NOT fabricate events, deployments, launches, or technical developments that are not explicitly mentioned in the provided data
                    6. Accuracy is paramount - when in doubt, be general rather than specific"""},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=100
            )

            tweet = response.choices[0].message.content.strip()

            # Add ledger source tracking for stats tweets
            if tweet_type in ['stats', 'governance'] and stats:
                # Add ledger source with block explorer links
                ledger_source = {
                    'type': 'ledger',
                    'description': 'Regen Network blockchain data',
                    'sensor': 'ledger_api'
                }

                # Add block explorer links if we have them
                if stats.get('block_explorer_links'):
                    ledger_source['block_links'] = stats['block_explorer_links'][:3]  # Top 3 blocks

                # Add marketplace explorer link if there's marketplace activity
                if stats.get('marketplace_volume', 0) > 0:
                    ledger_source['marketplace_url'] = 'https://mintscan.io/regen/ecosystem'

                # Add validator link if showing validator stats
                if 'validator' in tweet.lower():
                    ledger_source['validators_url'] = 'https://mintscan.io/regen/validators'

                sources.append(ledger_source)

            # For dashboard display, we want the full content
            # Only truncate if tweet is extremely long (likely an error)
            if len(tweet) > 500:
                tweet = tweet[:497] + "..."
            elif len(tweet) < 200:
                # Too short, add context
                tweet += " Join us in building regenerative finance infrastructure. #RegenNetwork"

            # Add ledger as source for stats tweets
            if tweet_type == 'stats' and stats:
                sources.append({
                    'type': 'ledger',
                    'description': 'Regen Network ledger statistics',
                    'sensor': 'ledger_sensor'
                })

            return {
                'content': tweet,
                'sources': sources
            }

        except Exception as e:
            logger.error(f"Error generating tweet: {e}")
            # Return a fallback tweet based on type with empty sources
            fallbacks = {
                'headline': "🌱 Another day building regenerative finance on #RegenNetwork! Check out today's updates on ecological credits, governance, and our growing community. Together we're creating a sustainable future through blockchain innovation. 🌍",
                'stats': "📊 Regen Network continues to grow: Active validators securing the network, credit classes expanding, and marketplace activity ongoing. Join us in building the infrastructure for planetary regeneration. #ReFi",
                'governance': "🗳️ Governance is at the heart of Regen Network. Every voice matters in shaping regenerative finance. Join our community discussions and help guide the future of ecological credits. Your participation makes a difference! #RegenNetwork",
                'community': "💬 Our community continues to drive innovation in regenerative finance. From technical developments to ecological insights, every contribution shapes our collective impact. Join the conversation! #RegenNetwork",
                'cta': "🌍 Ready to be part of the regeneration movement? Learn more at regen.network and join our Discord community. Together, we're proving that finance can heal the planet. #ReFi #ClimateAction"
            }
            return {
                'content': fallbacks.get(tweet_type, fallbacks['headline']),
                'sources': [{'type': 'fallback', 'description': 'Generated fallback due to error'}]
            }

    async def analyze_content_themes(self, content_items: List[Dict]) -> Dict[str, Any]:
        """
        Use LLM to analyze themes and extract key insights from all content
        """
        # Prepare content for analysis
        content_summary = await self.summarize_content_batch(content_items)

        prompt = f"""Analyze these Regen Network updates and extract key themes:

{content_summary}

Provide a JSON response with:
{{
    "main_themes": ["theme1", "theme2", "theme3"],
    "key_governance_items": ["item1", "item2"],
    "new_credits_summary": "summary of new ecological credits",
    "community_highlights": ["highlight1", "highlight2"],
    "trending_topics": ["topic1", "topic2"],
    "recommended_focus": "what should be the main focus of today's thread"
}}"""

        try:
            response = await self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert analyst for blockchain and environmental finance. Always respond with valid JSON. Only extract themes and information that are explicitly present in the provided content. Do not fabricate or assume developments."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"},
                max_tokens=300
            )

            return json.loads(response.choices[0].message.content)

        except Exception as e:
            logger.error(f"Error analyzing themes: {e}")
            return {
                "main_themes": ["regenerative finance", "ecological credits"],
                "key_governance_items": [],
                "trending_topics": []
            }

    async def get_ledger_stats(self) -> Dict[str, Any]:
        """Get blockchain statistics from Regen ledger"""
        try:
            import sys
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from services.regen_ledger import RegenLedgerClient

            ledger_client = RegenLedgerClient()
            summary = await ledger_client.get_ledger_summary()

            # Extract block explorer links if we have block data
            block_explorer_links = []
            if 'recent_blocks' in summary.get('raw_data', {}):
                for block in summary['raw_data']['recent_blocks'][:5]:  # Last 5 blocks
                    if block.get('hash'):
                        # Mintscan is the main block explorer for Regen Network
                        block_explorer_links.append({
                            'type': 'block',
                            'height': block.get('height'),
                            'url': f"https://mintscan.io/regen/blocks/{block.get('height')}",
                            'tx_count': block.get('tx_count', 0)
                        })

            # Format stats for daily thread - include ALL comprehensive data
            stats = {
                'new_credits': summary['statistics'].get('total_credit_batches', 0),
                'active_proposals': summary['statistics'].get('active_proposals', 0),
                'total_proposals': summary['statistics'].get('total_proposals', 0),
                'validator_count': summary['statistics'].get('total_validators', 0),
                'marketplace_volume': summary['statistics'].get('active_sell_orders', 0),
                'credit_classes': summary['statistics'].get('total_credit_classes', 0),
                'baskets': summary['statistics'].get('total_baskets', 0),
                'block_explorer_links': block_explorer_links,
                'total_validators': summary['statistics'].get('total_validators', 0),
                'ibc_channels': summary['statistics'].get('ibc_channels', 0),
                'avg_tx_per_block': summary['statistics'].get('avg_tx_per_block', 0),
                'total_bonded': summary['statistics'].get('total_bonded', '0'),
                'ledger_summary': await ledger_client.format_for_daily_post()
            }

            # Add proposal details if any
            if summary.get('proposals'):
                active = [p for p in summary['proposals'] if p.get('status') == 'VOTING_PERIOD']
                if active:
                    stats['active_proposal_titles'] = [p['title'] for p in active[:2]]

            return stats
        except Exception as e:
            logger.error(f"Error getting ledger stats: {e}")
            return {
                'new_credits': 0,
                'active_proposals': 0,
                'validator_count': 75,
                'marketplace_volume': 0,
                'ledger_summary': ''
            }

    async def generate_daily_thread(self) -> Dict[str, Any]:
        """
        Generate daily thread with adaptive post count (3-5 posts)
        Pattern: headline, stat, 1-2 links, CTA
        Adjusts based on activity level
        """
        logger.info("Generating LLM-powered daily thread...")

        # Get ALL content from past 24 hours
        all_content = await self.get_all_24h_content()
        logger.info(f"Found {len(all_content)} content items from past 24 hours")

        # Get ledger stats
        stats = await self.get_ledger_stats()

        # Analyze themes and extract insights
        themes = await self.analyze_content_themes(all_content)
        logger.info(f"Identified themes: {themes.get('main_themes', [])}")

        # Determine activity level and post count
        # Count unique posts, not chunks (chunks share same base RID)
        primary_content = [item for item in all_content if not item.get('is_thread_context', False)]

        # Count unique base RIDs (excluding chunks)
        unique_posts = set()
        for item in primary_content:
            rid = item.get('rid', '')
            # Remove chunk suffix to get base RID
            base_rid = rid.split('#chunk')[0] if rid else ''
            if base_rid:
                unique_posts.add(base_rid)

        # Also filter by source - ledger-only doesn't count as activity
        non_ledger_sources = set()
        for item in primary_content:
            source = item.get('source_sensor', '')
            if source and 'ledger' not in source.lower():
                non_ledger_sources.add(source)

        # Activity level is based on unique posts from non-ledger sources
        activity_level = len(unique_posts) if non_ledger_sources else 0

        logger.info(f"Found {len(primary_content)} items, {len(unique_posts)} unique posts, {len(non_ledger_sources)} non-ledger sources")

        # Determine post count based on activity
        # Low activity (0-5 items): 3 posts
        # Medium activity (6-15 items): 4 posts
        # High activity (16+ items): 5 posts
        if activity_level <= 5:
            post_count = 3
            activity_category = 'low'
        elif activity_level <= 15:
            post_count = 4
            activity_category = 'medium'
        else:
            post_count = 5
            activity_category = 'high'

        logger.info(f"Activity level: {activity_category} ({activity_level} items) - generating {post_count} posts")

        # Build thread structure
        thread = {
            'thread_id': str(uuid.uuid4()),
            'thread_date': datetime.now(timezone.utc).isoformat(),
            'posts': [],
            'metadata': {
                'content_sources': {
                    'total_content_24h': len(all_content),
                    'primary_content_24h': activity_level,
                    'activity_level': activity_category,
                    'post_count': post_count,
                    'themes': themes.get('main_themes', []),
                    'trending': themes.get('trending_topics', [])
                },
                'stats': stats,
                'llm_analysis': themes
            }
        }

        # Check if we need fallback content for low activity days
        need_fallback = activity_level < 3
        fallback_thread = None

        if need_fallback:
            # Get recent forum threads as fallback
            try:
                recent_threads = await self.get_recent_forum_threads(limit=5)
                if recent_threads:
                    # Randomly select one thread
                    fallback_thread = random.choice(recent_threads)
                    logger.info(f"Using fallback forum thread: {fallback_thread.get('title', 'Unknown')}")
                else:
                    logger.info("No forum threads available for fallback")
            except Exception as e:
                logger.warning(f"Could not fetch forum threads for fallback: {e}")
                fallback_thread = None

        # Generate posts based on activity level

        # Post 1: Always include a headline
        headline_result = await self.generate_tweet_from_content(all_content, 'headline', 1, stats)
        thread['posts'].append({
            'type': 'headline',
            'content': headline_result['content'],
            'sources': headline_result.get('sources', []),
            'metadata': {'position': 1, 'generated_by': 'llm'}
        })

        # Post 2: Always include stats or governance
        if themes.get('key_governance_items') or stats.get('active_proposals', 0) > 0:
            tweet_result = await self.generate_tweet_from_content(all_content, 'governance', 2, stats)
            tweet_type = 'governance'
        else:
            tweet_result = await self.generate_tweet_from_content(all_content, 'stats', 2, stats)
            tweet_type = 'stats'

        thread['posts'].append({
            'type': tweet_type,
            'content': tweet_result['content'],
            'sources': tweet_result.get('sources', []),
            'metadata': {'position': 2, 'generated_by': 'llm'}
        })

        # For low activity (3 posts): headline, stat, forum link/CTA
        # For medium activity (4 posts): headline, stat, link, CTA
        # For high activity (5 posts): headline, stat, link, link, CTA

        # Post 3: For low activity, always try to include a forum thread
        # For medium/high activity, include regular community content
        if post_count == 3:
            # Low activity - try to include forum thread as 3rd post
            if fallback_thread:
                # Generate engaging forum thread summary using LLM
                fallback_content = await self.generate_forum_thread_summary(fallback_thread)

                thread['posts'].append({
                    'type': 'community',
                    'content': fallback_content,
                    'sources': [{
                        'type': 'fallback',
                        'description': f"Recent forum thread (fallback content)",
                        'url': fallback_thread['url'],
                        'published_at': fallback_thread.get('published_at', 'recent')
                    }],
                    'metadata': {'position': 3, 'generated_by': 'llm-fallback'}
                })
            # If no forum thread available, generate CTA directly (skip to final post)

        elif post_count >= 4:
            # Medium/high activity - include community content
            if need_fallback and fallback_thread:
                # Generate engaging forum thread summary using LLM
                fallback_content = await self.generate_forum_thread_summary(fallback_thread)

                thread['posts'].append({
                    'type': 'community',
                    'content': fallback_content,
                    'sources': [{
                        'type': 'fallback',
                        'description': f"Recent forum thread (fallback content)",
                        'url': fallback_thread['url'],
                        'published_at': fallback_thread.get('published_at', 'recent')
                    }],
                    'metadata': {'position': 3, 'generated_by': 'llm-fallback'}
                })
            else:
                # Regular community highlight
                community_result = await self.generate_tweet_from_content(all_content, 'community', 3, stats)
                thread['posts'].append({
                    'type': 'community',
                    'content': community_result['content'],
                    'sources': community_result.get('sources', []),
                    'metadata': {'position': 3, 'generated_by': 'llm'}
                })

        # Post 4: Additional highlight (only for high activity - 5 posts)
        if post_count >= 5:
            if themes.get('new_credits_summary') or stats.get('new_credits', 0) > 0:
                # Focus on credits if there are new ones
                extra_result = await self.generate_tweet_from_content(all_content, 'stats', 4, stats)
            else:
                # Otherwise another community/governance post
                extra_result = await self.generate_tweet_from_content(all_content, 'community', 4, stats)

            thread['posts'].append({
                'type': 'highlight',
                'content': extra_result['content'],
                'sources': extra_result.get('sources', []),
                'metadata': {'position': 4, 'generated_by': 'llm'}
            })

        # Final Post: Call to action (always included as last post)
        final_position = len(thread['posts']) + 1
        cta_result = await self.generate_tweet_from_content(all_content, 'cta', final_position, stats)
        thread['posts'].append({
            'type': 'cta',
            'content': cta_result['content'],
            'sources': cta_result.get('sources', []),
            'metadata': {'position': final_position, 'generated_by': 'llm'}
        })

        logger.info(f"Generated thread with {len(thread['posts'])} posts using LLM")

        # Create unified sources section
        thread['unified_sources'] = await self.create_unified_sources(all_content, thread['posts'])

        # Add a text representation with sources for simple displays
        text_representation = f"ID: {thread.get('thread_id', 'N/A')}\n\n"
        text_representation += f"Daily Thread Posts ({len(thread['posts'])})\n"

        for i, post in enumerate(thread['posts'], 1):
            text_representation += f"\nPost {i}\n"
            text_representation += f"{post['content']}\n"

            # Add sources if available
            if post.get('sources') and len(post['sources']) > 0:
                text_representation += "\nSources:\n"
                for source in post['sources']:
                    if isinstance(source, dict):
                        if source.get('type') == 'aggregated':
                            text_representation += f"  • {source.get('description', 'Aggregated content')}\n"
                            if source.get('sensor_breakdown'):
                                for sensor, count in source['sensor_breakdown'].items():
                                    text_representation += f"    - {sensor}: {count} items\n"
                        elif source.get('type') == 'ledger':
                            text_representation += f"  • {source.get('description', 'Ledger data')}\n"
                        elif source.get('type') == 'fallback':
                            text_representation += f"  • {source.get('description', 'Fallback content')}\n"
                        else:
                            source_text = f"  • {source.get('sensor', 'Unknown')}"
                            if source.get('event_type'):
                                source_text += f" ({source['event_type']})"
                            if source.get('published_at'):
                                source_text += f" - {source['published_at']}"
                            if source.get('url'):
                                source_text += f"\n    URL: {source['url']}"
                            text_representation += source_text + "\n"
                    else:
                        text_representation += f"  • {source}\n"

        thread['text_representation'] = text_representation
        return thread


# Make it compatible with existing system
class DailyCurator(DailyCuratorLLM):
    """Alias for backwards compatibility"""
    pass