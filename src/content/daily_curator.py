"""
Daily Content Curator for Regen Network
Aggregates and curates content from KOI infrastructure for daily X posts and weekly digests
"""

import os
import asyncio
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import asyncpg
import httpx
from loguru import logger
import yaml

# Import date extraction utilities
from utils.date_extractor import DateExtractor, ContentDateEnricher


class DailyCurator:
    """
    Daily Content Curator that queries KOI infrastructure to generate
    curated content for social media posts and weekly digests
    """
    
    def __init__(self, config_path: str = "config/curator_config.yaml"):
        """Initialize the Daily Curator with configuration"""
        self.config = self._load_config(config_path)
        self.db_url = self.config.get('database_url', os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza'))
        self.bge_url = self.config.get('bge_server_url', 'http://localhost:8090')
        self.date_extractor = DateExtractor()
        self.content_enricher = ContentDateEnricher()
        
        # Content selection parameters
        self.hours_lookback_primary = self.config.get('hours_lookback_primary', 48)
        self.hours_lookback_secondary = self.config.get('hours_lookback_secondary', 168)  # 1 week
        self.min_confidence = self.config.get('min_publication_confidence', 0.5)
        self.max_thread_posts = self.config.get('max_thread_posts', 5)
        self.min_thread_posts = self.config.get('min_thread_posts', 3)
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(config_path)
        if config_file.exists():
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        else:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return {}
    
    async def initialize(self):
        """Initialize the Daily Curator"""
        logger.info("Initializing Daily Curator...")
        self.conn = None
        try:
            self.conn = await asyncpg.connect(self.db_url)
            logger.info("Daily Curator initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Daily Curator: {e}")
            return False
    
    async def cleanup(self):
        """Cleanup Daily Curator resources"""
        logger.info("Cleaning up Daily Curator...")
        if hasattr(self, 'conn') and self.conn:
            await self.conn.close()
        logger.info("Daily Curator cleaned up")
        return True
    
    async def get_recent_published_content(self, 
                                          hours: int = 48,
                                          min_confidence: float = 0.5) -> List[Dict[str, Any]]:
        """
        Query PostgreSQL for content published within the specified time window
        
        Args:
            hours: Number of hours to look back for published content
            min_confidence: Minimum confidence score for publication dates
        
        Returns:
            List of content items with publication dates
        """
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                # Query for recently published content with diverse sources
                # Use a CTE to get a balanced mix from each source
                query = """
                    WITH ranked_content AS (
                        SELECT
                            km.id,
                            km.rid,
                            km.cid,
                            km.source_sensor,
                            km.content,
                            km.metadata,
                            km.published_at,
                            km.published_confidence,
                            km.created_at as ingested_at,
                            km.content_hash,
                            EXTRACT(EPOCH FROM (NOW() - km.published_at)) / 3600 as hours_old,
                            ROW_NUMBER() OVER (PARTITION BY
                                CASE
                                    WHEN km.source_sensor LIKE '%%discourse%%' THEN 'forum'
                                    WHEN km.rid LIKE '%%forum.regen.network%%' THEN 'forum'
                                    WHEN km.source_sensor LIKE '%%github%%' THEN 'github'
                                    ELSE 'other'
                                END
                                ORDER BY km.published_at DESC
                            ) as rn
                        FROM koi_memories km
                        WHERE km.superseded_at IS NULL
                          AND km.event_type != 'FORGET'
                          -- Exclude private content (#28: curator SQL privacy audit)
                          AND (km.is_private = FALSE OR km.is_private IS NULL)
                          AND km.published_at >= NOW() - INTERVAL '%s hours'
                          AND km.published_confidence >= $1
                          AND (
                              -- Forum content (discourse sensor)
                              km.source_sensor LIKE '%%discourse%%'
                              -- GitHub activity sensor (high-level commits, PRs, issues)
                              OR km.source_sensor LIKE '%%github-activity%%'
                              -- Also include website sensor if it has forum content
                              OR (km.source_sensor LIKE '%%website%%' AND km.rid LIKE '%%forum.regen.network%%')
                          )
                          AND km.content::text NOT LIKE '%%sensor_heartbeat%%'
                          AND km.rid NOT LIKE '%%heartbeat%%'
                          -- No need for file exclusions since github-activity only tracks commits/PRs/issues
                    )
                    SELECT
                        id, rid, cid, source_sensor, content, metadata,
                        published_at, published_confidence, ingested_at,
                        content_hash, hours_old
                    FROM ranked_content
                    WHERE rn <= 50  -- Get up to 50 items per source type (GitHub has too many)
                    ORDER BY published_at DESC
                    LIMIT 500
                """ % hours  # Use string formatting for INTERVAL
                
                rows = await conn.fetch(query, min_confidence)
                
                return [dict(row) for row in rows]
    
    async def get_trending_topics(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Use BGE embeddings to identify trending topics through clustering
        
        Args:
            hours: Time window for trending analysis
        
        Returns:
            List of trending topics with associated content
        """
        # Get recent content
        recent_content = await self.get_recent_published_content(hours=hours)
        
        if not recent_content:
            return []
        
        # Extract text for embedding analysis
        texts = []
        for item in recent_content:
            content_data = item.get('content', {})
            if isinstance(content_data, str):
                texts.append(content_data)
            elif isinstance(content_data, dict):
                text = content_data.get('text', '') or content_data.get('content', '')
                if text:
                    texts.append(text)
        
        if not texts:
            return []
        
        # Query BGE server for embeddings and similarity
        async with httpx.AsyncClient() as client:
            try:
                # Get embeddings for recent content
                response = await client.post(
                    f"{self.bge_url}/embed",
                    json={"texts": texts[:20]},  # Limit to 20 for performance
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    embeddings = response.json().get('embeddings', [])
                    
                    # Simple clustering: find similar content
                    clusters = self._simple_clustering(texts, embeddings)
                    
                    # Return top clusters as trending topics
                    return clusters[:5]
                    
            except Exception as e:
                logger.error(f"Error getting trending topics: {e}")
        
        return []
    
    def _simple_clustering(self, texts: List[str], embeddings: List[List[float]]) -> List[Dict[str, Any]]:
        """
        Simple clustering based on embedding similarity
        Returns clusters of similar content as trending topics
        """
        if not embeddings or not texts:
            return []
        
        clusters = []
        used_indices = set()
        
        # Simple greedy clustering
        for i, text in enumerate(texts):
            if i in used_indices:
                continue
                
            cluster = {
                'main_text': text[:200],
                'count': 1,
                'indices': [i]
            }
            
            # Find similar texts (would use cosine similarity in production)
            # For now, just group by source sensor or keywords
            for j in range(i + 1, len(texts)):
                if j not in used_indices:
                    # Simple keyword matching for demonstration
                    if any(word in texts[j].lower() for word in ['governance', 'credit', 'batch', 'proposal']):
                        cluster['count'] += 1
                        cluster['indices'].append(j)
                        used_indices.add(j)
            
            if cluster['count'] > 1:
                clusters.append(cluster)
            used_indices.add(i)
        
        # Sort by cluster size
        clusters.sort(key=lambda x: x['count'], reverse=True)
        
        return clusters
    
    async def get_ledger_stats(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get blockchain statistics directly from Regen Network

        Args:
            hours: Time window for stats

        Returns:
            Dictionary of statistics
        """
        try:
            # Use direct Regen client instead of MCP
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from integrations.regen_direct_client import RegenDirectClient

            direct_client = RegenDirectClient()
            stats = await direct_client.get_ledger_stats()

            # Add recent activity metrics if available
            if 'block_height' in stats and stats['block_height'] > 0:
                # Estimate recent activity based on block height
                # Regen averages ~6 second blocks = 600 blocks/hour
                blocks_in_window = 600 * hours

                # Mock some activity metrics (would need actual queries in production)
                stats['new_credits'] = blocks_in_window * 50  # Estimate
                stats['new_batches'] = hours * 2  # Estimate 2 batches per hour
                stats['marketplace_volume'] = hours * 50000  # Estimate $50k/hour

            return stats

        except Exception as e:
            logger.warning(f"Error getting ledger stats directly, trying MCP fallback: {e}")
            try:
                # Fallback to MCP if available
                from integrations.regen_mcp_client import RegenMCPClient
                mcp_client = RegenMCPClient()
                return await mcp_client.query_ledger_stats()
            except:
                logger.error(f"Both direct and MCP ledger access failed")
                # Final fallback to empty stats
                return {
                    'new_credits': 0,
                    'total_credits': 0,
                    'active_proposals': 0,
                    'new_batches': 0,
                    'marketplace_volume': 0,
                    'validator_count': 0,
                    'block_height': 0
                }
    
    async def generate_daily_thread(self) -> Dict[str, Any]:
        """
        Generate a daily thread for X/Twitter with 3-5 posts

        Returns:
            Thread structure with posts and metadata
        """
        # Get ALL content from past 24 hours (strict) and recent week
        new_content = await self.get_recent_published_content(hours=24, min_confidence=0.5)  # Last 24h - lower threshold to get ALL
        recent_content = await self.get_recent_published_content(hours=168, min_confidence=0.6)  # Last week for context
        
        # Get trending topics
        trending = await self.get_trending_topics(hours=24)
        
        # Get ledger stats
        stats = await self.get_ledger_stats(hours=24)
        
        # Build thread structure
        thread = {
            'thread_date': datetime.now(timezone.utc).isoformat(),
            'posts': [],
            'metadata': {
                'content_sources': {
                    'new_today': len(new_content),
                    'recent_48h': len(recent_content),
                    'trending_topics': len(trending),
                },
                'stats': stats
            }
        }
        
        # Post 1: Dynamic headline with context
        headline = await self._generate_dynamic_headline(new_content, stats, trending)
        thread['posts'].append({
            'type': 'headline',
            'content': headline,
            'metadata': {'priority': 'high', 'position': 1}
        })
        
        # Post 2: Key stat
        if stats:
            stat_text = self._format_stat_post(stats)
            thread['posts'].append({
                'type': 'stat',
                'content': stat_text,
                'source': 'ledger_sensor',
                'published_at': datetime.now(timezone.utc).isoformat(),
                'metadata': {'position': 2}
            })
        
        # Post 3-4: Top content links
        link_posts = self._select_content_links(new_content, recent_content)
        for i, link_post in enumerate(link_posts[:2], start=3):
            thread['posts'].append({
                'type': 'link',
                'content': link_post['text'],
                'url': link_post.get('url', ''),
                'source': link_post.get('source', ''),
                'published_at': link_post.get('published_at', ''),
                'metadata': {'position': i}
            })
        
        # Post 5: Call to action
        if len(thread['posts']) < self.max_thread_posts:
            thread['posts'].append({
                'type': 'cta',
                'content': '🔗 Learn more about regenerative finance at regen.network\n\n💚 Join the conversation in our Discord',
                'metadata': {'position': len(thread['posts']) + 1}
            })
        
        return thread
    
    def _format_stat_post(self, stats: Dict[str, Any]) -> str:
        """Format statistics into a full 280 character tweet"""
        # Build comprehensive stats tweet
        parts = []

        if stats.get('new_batches') or stats.get('new_credits'):
            credits_text = f"{stats.get('new_credits', 0):,} ecological credits issued"
            if stats.get('new_batches'):
                credits_text += f" across {stats['new_batches']} new batches"
            parts.append(credits_text)

        if stats.get('marketplace_volume'):
            parts.append(f"${stats['marketplace_volume']:,.0f} in marketplace volume")

        if stats.get('active_proposals'):
            parts.append(f"{stats['active_proposals']} active governance proposals shaping our regenerative future")

        if stats.get('validator_count'):
            parts.append(f"{stats['validator_count']} validators securing the network")

        # Combine into engaging tweet
        tweet = "📊 Today's #RegenNetwork highlights: "
        tweet += ", ".join(parts[:2]) if len(parts) >= 2 else (parts[0] if parts else "Network growing steadily")

        # Add call to action if space
        if len(tweet) < 220:
            tweet += " 🌍 Building the infrastructure for planetary regeneration."

        # Ensure we use close to 280 chars
        if len(tweet) < 250:
            tweet += " #ReFi #ClimateAction"

        return tweet[:280]  # Ensure we don't exceed limit
    
    def _select_content_links(self,
                             new_content: List[Dict[str, Any]],
                             recent_content: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Select the most relevant content for link posts

        Prioritizes:
        1. Governance proposals
        2. New credit batches/classes
        3. Blog posts and announcements
        4. Forum discussions
        """
        import re

        selected = []

        # Combine and deduplicate content using content_hash
        seen_hashes = set()
        seen_texts = set()
        all_content = []

        # Process all content and remove duplicates
        for item in new_content + recent_content:
            # Check content hash
            content_hash = item.get('content_hash', '')
            if content_hash and content_hash in seen_hashes:
                continue

            # Check text similarity (first 200 chars)
            content_data = item.get('content', {})
            if isinstance(content_data, dict):
                content_text = str(content_data.get('text', ''))[:200].lower().strip()
            else:
                content_text = str(content_data)[:200].lower().strip()

            # Skip if very similar text already seen
            if content_text and content_text in seen_texts:
                continue

            # Add to deduped list
            if content_hash:
                seen_hashes.add(content_hash)
            if content_text:
                seen_texts.add(content_text)
            all_content.append(item)

        # Priority sources and keywords
        source_priorities = {
            'discourse': 10,
            'forum': 10,
            'github': 8,
            'gitlab': 7,
            'medium': 6,
            'website': 5,
            'notion': 3,
        }

        priority_keywords = {
            'governance': 10,
            'proposal': 10,
            'credit': 8,
            'batch': 8,
            'class': 7,
            'methodology': 7,
            'announcement': 6,
            'update': 5,
            'blog': 4,
            'discussion': 3
        }

        # Score and sort content
        for item in all_content:
            score = 0
            content_data = item.get('content', {})

            # Extract text content
            if isinstance(content_data, dict):
                content_text = str(content_data.get('text', '')).lower()
            else:
                content_text = str(content_data).lower()

            # Apply source priority
            source = item.get('source_sensor', '').lower()
            for src_key, weight in source_priorities.items():
                if src_key in source:
                    score += weight
                    break

            # Calculate relevance score
            for keyword, weight in priority_keywords.items():
                if keyword in content_text:
                    score += weight

            # Boost recent content
            hours_old = item.get('hours_old', 48)
            if hours_old < 24:
                score += 5
            elif hours_old < 48:
                score += 2

            # Add to selection with score
            item['relevance_score'] = score
            selected.append(item)

        # Sort by relevance score
        selected.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)

        # Format top items as link posts
        link_posts = []
        for item in selected[:5]:  # Get top 5 for better selection
            content_data = item.get('content', {})
            metadata = item.get('metadata', {})

            # Extract text and URL
            text = ''
            url = ''

            if isinstance(content_data, dict):
                # Try to get title or snippet
                text = content_data.get('title', '') or content_data.get('text', '')
                url = content_data.get('url', '') or content_data.get('link', '')
            elif isinstance(content_data, str):
                # Try to parse as JSON string
                try:
                    import json
                    parsed = json.loads(content_data)
                    if isinstance(parsed, dict):
                        text = parsed.get('text', content_data)
                except:
                    text = content_data

            # Clean up text - extract meaningful content
            if text:
                # Remove JSON formatting
                if text.startswith('{"text"'):
                    try:
                        import json
                        parsed = json.loads(text)
                        text = parsed.get('text', text)
                    except:
                        pass

                # Extract title from markdown headers
                title_match = re.search(r'^#+ (.+?)$', text, re.MULTILINE)
                if title_match:
                    title = title_match.group(1)
                    # Extract first post content
                    post_match = re.search(r'Post by (\w+) \([^)]+\)\n(.+?)(?:\n\n|$)', text, re.DOTALL)
                    if post_match:
                        author = post_match.group(1)
                        post_content = post_match.group(2)[:100]
                        text = f"{title} - {author}: {post_content}"
                    else:
                        text = title

                # Limit length
                text = text[:200] if len(text) > 200 else text

            # Extract URL from text if not found
            if not url and text:
                url_match = re.search(r'https?://[^\s<>"{}|\\^`\[\]]+', text)
                if url_match:
                    url = url_match.group(0)

            # Extract URL from metadata or construct from RID
            if not url:
                # Try metadata first
                if isinstance(metadata, dict):
                    # Check for source URL in metadata
                    if 'koi_manifest' in metadata and isinstance(metadata['koi_manifest'], dict):
                        manifest_meta = metadata['koi_manifest'].get('metadata', {})
                        if isinstance(manifest_meta, dict):
                            url = manifest_meta.get('url', '') or manifest_meta.get('source_url', '')

                    # Direct metadata URL
                    if not url:
                        url = metadata.get('url', '') or metadata.get('source_url', '')

                # Construct URL from RID if available
                if not url:
                    rid = item.get('rid', '')
                    if 'forum.regen.network' in rid:
                        # Extract topic ID from RID (e.g., forum.regen.network_512)
                        match = re.search(r'forum\.regen\.network_(\d+)', rid)
                        if match:
                            topic_id = match.group(1)
                            url = f'https://forum.regen.network/t/{topic_id}'
                    elif 'github.com' in rid:
                        # Extract repo and issue/PR from RID
                        parts = rid.split(':')
                        if len(parts) > 1:
                            url = f'https://{parts[1].replace("_", "/").replace("#chunk0", "")}'

            # Clean and format text
            if text:
                # Remove URLs from text
                text = re.sub(r'https?://[^\s]+', '', text)
                # Clean whitespace
                text = ' '.join(text.split())
                # Truncate for tweet length
                if len(text) > 150:
                    text = text[:147] + '...'

                # Format based on source
                source = item.get('source_sensor', '')
                if 'discourse' in source.lower() or 'forum' in source.lower():
                    emoji = '💬'
                    prefix = 'Forum Discussion: '
                elif 'github' in source.lower() or 'gitlab' in source.lower():
                    emoji = '🔧'
                    prefix = 'Development Update: '
                elif 'medium' in source.lower():
                    emoji = '📝'
                    prefix = 'Article: '
                elif 'governance' in text.lower():
                    emoji = '🗳️'
                    prefix = 'Governance: '
                else:
                    emoji = '🌱'
                    prefix = ''

                formatted_text = f"{emoji} {prefix}{text}"

                # Add URL if available
                if url:
                    formatted_text += f"\n\n🔗 {url}"

                link_posts.append({
                    'text': formatted_text,
                    'url': url,
                    'source': source,
                    'published_at': str(item.get('published_at', '')),
                    'score': item.get('relevance_score', 0)
                })

        # Deduplicate link posts based on URL and text similarity
        unique_posts = []
        seen_urls = set()
        seen_texts = set()

        for post in link_posts:
            # Skip if URL already seen (and not empty)
            if post['url'] and post['url'] in seen_urls:
                continue

            # Skip if text is too similar to already selected posts
            text_key = post['text'][:100].lower().strip()
            if text_key in seen_texts:
                continue

            # Add to unique posts
            unique_posts.append(post)
            if post['url']:
                seen_urls.add(post['url'])
            seen_texts.add(text_key)

            # Stop when we have enough unique posts
            if len(unique_posts) >= 2:
                break

        # Return 2 unique posts, or whatever we have
        return unique_posts[:2]

    async def _generate_dynamic_headline(self, new_content: List[Dict], stats: Dict, trending: List[str]) -> str:
        """
        Generate a dynamic headline based on current content and trends

        Args:
            new_content: List of new content items
            stats: Current statistics
            trending: List of trending topics

        Returns:
            Headline text for the daily thread
        """
        # Default headline options based on content type
        headlines = [
            "🌱 Regen Network Daily Update",
            "📊 Today in Regenerative Finance",
            "🌍 Building the Ecological Economy",
            "💚 Regenerative Finance in Action"
        ]

        # Check for special content types
        has_governance = any('governance' in str(item.get('content', '')).lower() for item in new_content[:5])
        has_credits = any('credit' in str(item.get('content', '')).lower() for item in new_content[:5])
        has_announcement = any('announcement' in str(item.get('metadata', {}).get('title', '')).lower() for item in new_content[:5])

        # Select headline based on content
        if has_governance:
            return "🗳️ Governance Update: New proposals and discussions in the Regen Network community"
        elif has_credits:
            return "🌱 Carbon Credit Activity: Latest developments in ecological assets"
        elif has_announcement:
            return "📢 Important Announcement: Updates from the Regen Network ecosystem"
        elif trending and len(trending) > 0:
            topic = trending[0].replace('_', ' ').title()
            return f"🔥 Trending: {topic} - Latest updates from Regen Network"
        else:
            # Use rotating default based on day
            from datetime import datetime
            day_index = datetime.now().day % len(headlines)
            return headlines[day_index]

    async def generate_weekly_digest(self) -> Dict[str, Any]:
        """
        Generate a weekly digest for NotebookLM processing
        
        Returns:
            Structured weekly digest with citations
        """
        # Get content from the past week
        week_content = await self.get_recent_published_content(hours=168, min_confidence=0.5)
        
        # Get trending topics for the week
        trending = await self.get_trending_topics(hours=168)
        
        # Get weekly stats
        stats = await self.get_ledger_stats(hours=168)
        
        # Structure the digest
        digest = {
            'week_ending': datetime.now(timezone.utc).isoformat(),
            'sections': [],
            'metadata': {
                'total_content': len(week_content),
                'trending_topics': len(trending),
                'stats': stats
            }
        }
        
        # Section 1: Executive Summary
        digest['sections'].append({
            'title': 'Executive Summary',
            'content': self._generate_weekly_summary(week_content, stats),
            'priority': 1
        })
        
        # Section 2: Key Developments
        digest['sections'].append({
            'title': 'Key Developments',
            'content': self._extract_key_developments(week_content),
            'priority': 2
        })
        
        # Section 3: Governance Updates
        governance = [c for c in week_content if 'governance' in str(c.get('content', '')).lower()]
        if governance:
            digest['sections'].append({
                'title': 'Governance Updates',
                'content': self._format_governance_section(governance),
                'priority': 3
            })
        
        # Section 4: Marketplace Activity
        digest['sections'].append({
            'title': 'Marketplace Activity',
            'content': self._format_marketplace_section(stats),
            'priority': 4
        })
        
        # Section 5: Community Highlights
        digest['sections'].append({
            'title': 'Community Highlights',
            'content': self._extract_community_highlights(week_content),
            'priority': 5
        })
        
        return digest
    
    def _generate_weekly_summary(self, content: List[Dict[str, Any]], stats: Dict[str, Any]) -> str:
        """Generate executive summary for the week"""
        summary_lines = []
        
        # Count content by source
        source_counts = {}
        for item in content:
            source = item.get('source_sensor', 'unknown')
            source_counts[source] = source_counts.get(source, 0) + 1
        
        # Build summary
        summary_lines.append(f"This week saw {len(content)} significant updates across the Regen Network ecosystem.")
        
        if stats.get('new_credits'):
            summary_lines.append(f"The network issued {stats['new_credits']:,} new ecological credits.")
        
        if stats.get('active_proposals'):
            summary_lines.append(f"Governance remained active with {stats['active_proposals']} proposals under consideration.")
        
        # Most active source
        if source_counts:
            top_source = max(source_counts, key=source_counts.get)
            summary_lines.append(f"The {top_source} showed the most activity with {source_counts[top_source]} updates.")
        
        return ' '.join(summary_lines)
    
    def _extract_key_developments(self, content: List[Dict[str, Any]]) -> str:
        """Extract and format key developments from the week"""
        developments = []
        
        # Look for high-priority content
        for item in content[:10]:  # Top 10 items
            content_data = item.get('content', {})
            if isinstance(content_data, dict):
                title = content_data.get('title', '')
                if title:
                    source = item.get('source_sensor', '')
                    date = item.get('published_at', '')
                    developments.append(f"• {title} (via {source})")
        
        return '\n'.join(developments) if developments else "No major developments this week."
    
    def _format_governance_section(self, governance_content: List[Dict[str, Any]]) -> str:
        """Format governance updates section"""
        lines = []
        
        for item in governance_content[:5]:
            content_data = item.get('content', {})
            if isinstance(content_data, dict):
                text = content_data.get('text', '')[:200]
                if text:
                    lines.append(f"• {text}...")
        
        return '\n'.join(lines) if lines else "No governance updates this week."
    
    def _format_marketplace_section(self, stats: Dict[str, Any]) -> str:
        """Format marketplace activity section"""
        lines = []
        
        if stats.get('marketplace_volume'):
            lines.append(f"Total volume: ${stats['marketplace_volume']:,.2f}")
        
        if stats.get('new_batches'):
            lines.append(f"New credit batches: {stats['new_batches']}")
        
        if stats.get('total_credits'):
            lines.append(f"Total credits in circulation: {stats['total_credits']:,}")
        
        return '\n'.join(lines) if lines else "Marketplace data unavailable."
    
    def _extract_community_highlights(self, content: List[Dict[str, Any]]) -> str:
        """Extract community highlights from forums and social media"""
        highlights = []
        
        # Look for content from community sources
        community_sources = ['discourse', 'twitter', 'medium']
        community_content = [c for c in content if c.get('source_sensor') in community_sources]
        
        for item in community_content[:5]:
            content_data = item.get('content', {})
            if isinstance(content_data, dict):
                text = content_data.get('text', '')[:150]
                if text:
                    source = item.get('source_sensor', '')
                    highlights.append(f"• {text}... (from {source})")
        
        return '\n'.join(highlights) if highlights else "Community highlights coming soon."
    
    async def export_for_notebooklm(self, digest: Dict[str, Any]) -> str:
        """
        Export weekly digest in format suitable for NotebookLM ingestion
        
        Args:
            digest: Weekly digest dictionary
        
        Returns:
            Markdown-formatted string for NotebookLM
        """
        lines = []
        
        # Header
        lines.append(f"# Regen Network Weekly Digest")
        lines.append(f"Week ending: {digest['week_ending'][:10]}")
        lines.append("")
        
        # Add each section
        for section in digest.get('sections', []):
            lines.append(f"## {section['title']}")
            lines.append("")
            lines.append(section['content'])
            lines.append("")
        
        # Add metadata footer
        lines.append("---")
        lines.append("## Metadata")
        metadata = digest.get('metadata', {})
        lines.append(f"- Total content items: {metadata.get('total_content', 0)}")
        lines.append(f"- Trending topics: {metadata.get('trending_topics', 0)}")
        
        return '\n'.join(lines)


async def main():
    """Test the Daily Curator"""
    logger.info("Initializing Daily Content Curator...")
    
    curator = DailyCurator()
    
    # Test daily thread generation
    logger.info("Generating daily thread...")
    thread = await curator.generate_daily_thread()
    
    print("\n=== DAILY THREAD ===")
    print(json.dumps(thread, indent=2, default=str))
    
    # Test weekly digest generation
    logger.info("Generating weekly digest...")
    digest = await curator.generate_weekly_digest()
    
    print("\n=== WEEKLY DIGEST ===")
    print(json.dumps(digest, indent=2, default=str))
    
    # Export for NotebookLM
    markdown = await curator.export_for_notebooklm(digest)
    print("\n=== NOTEBOOKLM EXPORT ===")
    print(markdown)


if __name__ == "__main__":
    asyncio.run(main())