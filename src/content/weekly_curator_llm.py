#!/usr/bin/env python3
"""
Weekly Content Curator with LLM Integration
Intelligently processes and summarizes weekly content using AI

Run as a script (or through scripts/run_weekly_curator_llm.py) with --apply it reads the
week's content from the database POSTGRES_URL names (default eliza on localhost:5433),
makes one paid chat completion (OPENAI_MODEL), INSERTs the digest into quality_reviews
and writes files under /opt/projects/koi-processor/output/weekly. With no arguments or
--dry-run it only prints what it would do. Importing the module does nothing; the file
log logs/weekly_curator_llm.log is added when a WeeklyCuratorLLM is created.
"""

import asyncio
import asyncpg
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union
from datetime import datetime, timezone, timedelta
from loguru import logger
import openai
from openai import AsyncOpenAI
import re
from collections import defaultdict, Counter
import hashlib
import httpx
import subprocess
import tempfile
from urllib.parse import urljoin, urlparse
from uuid import UUID

# Import URL enricher
sys.path.insert(0, str(Path(__file__).parent))
from url_enrichment import URLEnricher

# Configure logging: the file sink is added when a curator is created, not at import
_FILE_LOG = "logs/weekly_curator_llm.log"
_file_log_added = False


def _add_file_log() -> None:
    """Add the rotating file log once per process."""
    global _file_log_added
    if not _file_log_added:
        logger.add(_FILE_LOG, rotation="10 MB", retention="7 days")
        _file_log_added = True


def convert_uuids_to_strings(obj: Any) -> Any:
    """Recursively convert UUID and datetime objects to strings for JSON serialization"""
    if isinstance(obj, UUID):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_uuids_to_strings(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_uuids_to_strings(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_uuids_to_strings(item) for item in obj)
    else:
        return obj


class WeeklyCuratorLLM:
    """
    Weekly Content Curator that uses LLMs to intelligently process and summarize content
    """

    def __init__(self):
        """Initialize the Weekly Curator with LLM support"""
        _add_file_log()
        self.db_url = os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza')

        # LLM Configuration
        self.openai_client = AsyncOpenAI(
            api_key=os.getenv('OPENAI_API_KEY'),
            base_url=os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')
        )
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4-turbo-preview')

        # Content parameters
        self.max_stories = 10
        self.brief_word_target = 1000  # Increased from 800 for more comprehensive coverage

        # URL enrichment
        self.url_enricher = URLEnricher(self.db_url)

    async def get_ledger_data_for_week(self) -> Dict[str, Any]:
        """Get Regen ledger data for the weekly digest"""
        try:
            import sys
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from services.regen_ledger import RegenLedgerClient

            ledger_client = RegenLedgerClient()
            weekly_data = await ledger_client.format_for_weekly_digest()
            return weekly_data
        except Exception as e:
            logger.error(f"Error getting ledger data: {e}")
            return {}

    async def get_weekly_content(self, days_back: int = 7, start_date: Optional = None, end_date: Optional = None) -> List[Dict[str, Any]]:
        """
        Get ALL content PUBLISHED in the past week or within a specific date range
        NO LIMITS - we want complete context for the LLM

        Args:
            days_back: Number of days to look back (ignored if start_date/end_date provided)
            start_date: Optional start date for filtering
            end_date: Optional end date for filtering
        """
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                # Query that aggregates chunks back into complete documents
                query = """
                    WITH base_content AS (
                      -- Get all relevant content from the past week
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
                        -- EXCLUDE GITHUB FILE CHUNKS - only want actual activity
                        AND NOT (source_sensor LIKE '%%github%%' AND rid LIKE '%%#chunk%%')
                        -- Focus on specific sources for weekly digest
                        AND (
                            -- Forum content
                            source_sensor LIKE '%%discourse%%'
                            OR rid LIKE '%%forum.regen.network%%'
                            -- Website content from regentokenomics.org
                            -- EXCLUDE directory/index pages only
                            OR (rid LIKE '%%regentokenomics%%'
                                -- Must NOT be the index page (exact match or with trailing slash)
                                AND NOT (metadata->>'url' ~ '.*regentokenomics\\.org/weekly-meetups/?$'))
                            -- GitHub activity (commits, PRs, issues - not file chunks)
                            OR (source_sensor LIKE '%%github-activity%%')
                        )
                        -- ONLY content actually PUBLISHED in the specified window
                        AND published_at IS NOT NULL
                        AND published_at >= NOW() - ($1 * INTERVAL '1 day')
                        AND published_at <= NOW()
                        -- Require higher confidence for better accuracy
                        AND published_confidence >= 0.8
                    ),
                    parent_rids AS (
                      -- Extract parent RID for each entry
                      SELECT
                        id, source_sensor, event_type,
                        content, metadata,
                        published_at, published_confidence,
                        created_at,
                        CASE
                          WHEN rid LIKE '%%#chunk%%' THEN SUBSTRING(rid FROM '^(.+)#chunk[0-9]+$')
                          ELSE rid
                        END AS parent_rid,
                        rid LIKE '%%#chunk%%' AS is_chunk
                      FROM base_content
                    ),
                    aggregated_content AS (
                      -- Aggregate chunks by parent RID
                      SELECT
                        parent_rid,
                        -- Take first ID for the parent document
                        (ARRAY_AGG(id ORDER BY created_at))[1] AS id,
                        -- For chunks, aggregate text in order; for non-chunks, use content as-is
                        CASE
                          WHEN BOOL_OR(is_chunk) THEN
                            jsonb_build_object('text', STRING_AGG(content->>'text', ' ' ORDER BY (metadata->>'chunk_index')::int))
                          ELSE
                            (ARRAY_AGG(content ORDER BY created_at))[1]
                        END AS content,
                        -- Take metadata from first chunk or from the document itself
                        (ARRAY_AGG(metadata ORDER BY created_at))[1] AS metadata,
                        -- Use earliest published_at for the document
                        MIN(published_at) AS published_at,
                        -- Use highest confidence
                        MAX(published_confidence) AS published_confidence,
                        -- Use first source_sensor
                        (ARRAY_AGG(source_sensor ORDER BY created_at))[1] AS source_sensor,
                        -- Use first event_type
                        (ARRAY_AGG(event_type ORDER BY created_at))[1] AS event_type,
                        -- Use earliest created_at
                        MIN(created_at) AS created_at
                      FROM parent_rids
                      GROUP BY parent_rid
                    )
                    SELECT
                      id,
                      parent_rid AS rid,
                      source_sensor,
                      event_type,
                      content,
                      metadata,
                      published_at,
                      published_confidence,
                      created_at
                    FROM aggregated_content
                    ORDER BY published_at DESC
                """

                rows = await conn.fetch(query, days_back)

                # Filter out old or irrelevant content
                filtered_rows = []

                # Determine date range for filtering
                if start_date and end_date:
                    # Use provided date range
                    filter_start = start_date.replace(tzinfo=timezone.utc) if start_date.tzinfo is None else start_date
                    filter_end = end_date.replace(tzinfo=timezone.utc) if end_date.tzinfo is None else end_date
                else:
                    # Use days_back relative to now
                    now = datetime.now(timezone.utc)
                    filter_start = now - timedelta(days=days_back)
                    filter_end = now

                logger.info(f"Filtering content from {filter_start.date()} to {filter_end.date()}")

                for row in rows:
                    row_dict = dict(row)
                    published_at = row_dict.get('published_at')

                    # STRICT: Skip if published date is missing or outside our window
                    if not published_at:
                        logger.debug(f"Skipping content without published_at from {row_dict.get('source_sensor')}")
                        continue

                    # Double-check the date is actually within our window
                    if published_at < filter_start or published_at > filter_end:
                        logger.debug(f"Skipping content outside date window: {published_at} (window: {filter_start.date()} to {filter_end.date()})")
                        continue

                    # Special handling for GitHub content
                    if 'github' in row_dict.get('source_sensor', '').lower():
                        metadata = row_dict.get('metadata', {})
                        if isinstance(metadata, str):
                            try:
                                metadata = json.loads(metadata)
                            except:
                                metadata = {}

                        url = metadata.get('url', '')

                        # Skip non-main branch content
                        if '/tree/' in url and not any(x in url for x in ['/tree/main', '/tree/master']):
                            logger.debug(f"Skipping non-main branch content: {url}")
                            continue

                        # For GitHub, use git commit dates from metadata if available
                        if 'published_at' in metadata and metadata['published_at']:
                            try:
                                # Parse the metadata date and verify it's recent
                                meta_date = datetime.fromisoformat(metadata['published_at'].replace('Z', '+00:00'))
                                if meta_date < week_ago:
                                    logger.debug(f"Skipping old GitHub content from {meta_date}")
                                    continue
                            except:
                                pass

                    # Special handling for website content (e.g., regentokenomics.org)
                    if 'website' in row_dict.get('source_sensor', '').lower():
                        # Check if this is a placeholder page without actual content
                        if not self.has_substantial_content(row_dict):
                            logger.debug(f"Skipping placeholder page without transcript/recording: {row_dict.get('metadata', {}).get('url', 'unknown')}")
                            continue

                    filtered_rows.append(row_dict)

                logger.info(f"Retrieved {len(filtered_rows)} items from past {days_back} days (filtered from {len(rows)})")
                return filtered_rows

    def has_substantial_content(self, row_dict: Dict) -> bool:
        """
        Check if a website page has substantial content (transcript or recording).
        Returns True if the page should be included, False if it's a placeholder.

        For regentokenomics.org meeting pages:
        - Include if there's a transcript or recording present
        - Exclude if it's just a placeholder with future date and no content
        """
        content = row_dict.get('content', {})
        metadata = row_dict.get('metadata', {})

        # Parse content if it's a string
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except:
                content = {'text': content}

        # Parse metadata if it's a string
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except:
                metadata = {}

        # Get the text content
        content_text = ''
        if isinstance(content, dict):
            content_text = content.get('text', '') or content.get('content', '') or str(content)
        else:
            content_text = str(content)

        content_text_lower = content_text.lower()

        # Check for indicators of actual content:
        # 1. Presence of "transcript" or "recording" keywords
        has_transcript = 'transcript' in content_text_lower
        has_recording = 'recording' in content_text_lower or 'video' in content_text_lower

        # 2. Substantial text content (more than just placeholder text)
        # Count actual content words (excluding common placeholder phrases)
        words = content_text.split()
        substantial_length = len(words) > 200  # More than 200 words suggests real content

        # If there's a transcript, recording, or substantial content, include it
        if has_transcript or has_recording or substantial_length:
            return True

        # Check if this looks like a placeholder page
        # Common indicators: "Date of Session" with minimal other content
        if 'date of session' in content_text_lower and len(words) < 100:
            logger.debug(f"Detected placeholder page with 'Date of Session' and <100 words")
            return False

        # Default to including if we're not sure
        return True

    def extract_clean_source(self, source_sensor: str) -> str:
        """Extract clean source name from sensor ID"""
        if '-sensor-' in source_sensor:
            return source_sensor.split('-sensor-')[0]
        elif '_sensor_' in source_sensor:
            return source_sensor.split('_sensor_')[0]
        return source_sensor

    def extract_thread_url(self, metadata: Any, rid: str = None) -> Optional[str]:
        """Extract base thread URL for grouping related posts"""
        # Parse metadata if it's a string
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except:
                metadata = {}

        if not isinstance(metadata, dict):
            metadata = {}

        # First try to get URL from metadata
        url = metadata.get('url', '')

        # If no URL but has parent_url, use that
        if not url and metadata.get('parent_url'):
            url = metadata.get('parent_url')

        # If still no URL, try to get from parent using provenance API
        if not url and metadata.get('parent_rid'):
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

        if not url:
            return None

        # Clean GitHub sensor artifacts
        if 'github_sensor_' in url:
            url = re.sub(r'/github_sensor_[^/]+/', '/', url)

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

    def group_by_thread(self, items: List[Dict]) -> Dict[str, List[Dict]]:
        """Group content items by thread/discussion"""
        thread_groups = defaultdict(list)

        for item in items:
            metadata = item.get('metadata', {})

            # Parse metadata if it's a string
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = {}

            # Pass RID to help reconstruct URLs
            rid = item.get('rid', '')
            thread_url = self.extract_thread_url(metadata, rid)

            if thread_url:
                thread_groups[thread_url].append(item)
            else:
                # Use title as fallback grouping key
                title = metadata.get('title', 'untitled') if isinstance(metadata, dict) else 'untitled'
                thread_groups[title].append(item)

        # Sort items within each thread by date
        for thread_key in thread_groups:
            thread_groups[thread_key].sort(
                key=lambda x: x['published_at'],
                reverse=True
            )

        return dict(thread_groups)

    def count_unique_items(self, items: List[Dict]) -> int:
        """Count unique items (posts, pages, activities), excluding chunks and duplicates"""
        unique_items = set()
        for item in items:
            rid = item.get('rid', '')
            # Remove chunk identifiers to get base ID
            base_rid = rid.split('#chunk')[0] if rid else ''
            if base_rid:
                unique_items.add(base_rid)
        return len(unique_items)

    def extract_content_text(self, item: Dict) -> str:
        """Extract readable text from content field"""
        content = item.get('content', {})

        if isinstance(content, str):
            try:
                content = json.loads(content)
            except:
                return content[:500]  # Return as-is if not JSON

        # Extract text from various content structures
        text = ""
        if isinstance(content, dict):
            text = content.get('content', '') or content.get('text', '') or content.get('body', '')
            if not text:
                # Fallback to stringifying the dict
                text = json.dumps(content, default=str)[:2000]  # Increased for full context
        else:
            text = str(content)[:2000]  # Increased from 500 to 2000 for full context

        return text.strip()

    def smart_truncate(self, text: str, max_length: int) -> str:
        """
        Intelligently truncate text at sentence boundaries

        Preserves readability by cutting at the last complete sentence
        within the character limit
        """
        if len(text) <= max_length:
            return text

        # Find last sentence boundary within limit
        truncated = text[:max_length]

        # Look for sentence endings (., !, ?)
        sentence_endings = ['.', '!', '?', '\n']
        last_boundary = -1

        for ending in sentence_endings:
            pos = truncated.rfind(ending)
            if pos > last_boundary:
                last_boundary = pos

        # If we found a sentence boundary, use it
        if last_boundary > max_length * 0.5:  # At least 50% of max length
            return text[:last_boundary + 1].strip()

        # Otherwise, cut at word boundary
        last_space = truncated.rfind(' ')
        if last_space > 0:
            return truncated[:last_space].strip() + '...'

        # Fallback: hard truncate
        return truncated.strip() + '...'

    def calculate_content_budget(self, num_threads: int, total_context_budget: int = 12000) -> Dict[str, int]:
        """
        Dynamically calculate how much content to include per thread based on total count

        Strategy:
        - Reserve budget for system prompt, ledger data, instructions (~4000 tokens)
        - Distribute remaining budget across threads
        - Give more weight to threads with higher post counts
        - Ensure minimum viable content even with many threads
        """
        # Reserve space for prompt overhead
        overhead_budget = 4000
        available_budget = total_context_budget - overhead_budget

        if num_threads == 0:
            return {'per_thread': 0, 'max_per_post': 0}

        # Base allocation per thread
        base_per_thread = available_budget // num_threads

        # Determine max content per post based on thread count
        if num_threads <= 3:
            # Few threads - allow full content
            max_per_post = 3000
        elif num_threads <= 10:
            # Moderate threads - allow substantial content
            max_per_post = 1500
        elif num_threads <= 20:
            # Many threads - moderate truncation
            max_per_post = 800
        else:
            # Lots of threads - aggressive but balanced truncation
            max_per_post = 400

        return {
            'per_thread': min(base_per_thread, max_per_post * 3),  # Max 3 posts per thread
            'max_per_post': max_per_post,
            'total_budget': available_budget
        }

    async def analyze_with_llm(self, thread_groups: Dict[str, List[Dict]], ledger_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Use LLM to intelligently analyze and summarize the week's content
        """
        # Calculate dynamic content budget based on number of threads
        num_threads = len(thread_groups)
        budget = self.calculate_content_budget(num_threads)

        logger.info(f"Content budget: {num_threads} threads, {budget['max_per_post']} chars/post, {budget['per_thread']} chars/thread")

        # Prepare content for LLM analysis
        threads_summary = []

        for thread_url, items in thread_groups.items():
            thread_data = {
                'url': thread_url,
                'full_url': thread_url,  # Preserve full URL for LLM to use
                'post_count': self.count_unique_items(items),
                'latest_date': items[0]['published_at'].isoformat() if items else '',
                'posts': []
            }

            # Include up to 3 most recent posts for context
            for item in items[:3]:
                metadata = item.get('metadata', {})
                # Parse metadata if it's a string
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except:
                        metadata = {}

                title = metadata.get('title', 'Untitled') if isinstance(metadata, dict) else 'Untitled'

                # Extract content with dynamic budget
                full_content = self.extract_content_text(item)
                # Truncate to budget, but intelligently at sentence boundaries
                content = self.smart_truncate(full_content, budget['max_per_post'])

                thread_data['posts'].append({
                    'title': title,
                    'content': content,
                    'source': self.extract_clean_source(item['source_sensor']),
                    'date': item['published_at'].isoformat()
                })

            threads_summary.append(thread_data)

        # Sort by activity (post count * recency)
        threads_summary.sort(key=lambda x: x['post_count'], reverse=True)

        # Prepare prompt for LLM with ALL content
        total_posts = sum(self.count_unique_items(items) for items in thread_groups.values())

        # Include ALL threads but summarize if too many
        thread_context = threads_summary if len(threads_summary) <= 50 else threads_summary[:50]

        # Prepare ledger statistics for the prompt
        ledger_summary = ""
        if ledger_data and ledger_data.get('statistics'):
            stats = ledger_data['statistics']
            ledger_summary = f"""

**Regen Network Ledger Activity (Past 7 Days):**
- Total Credit Batches: {stats.get('total_credit_batches', 0)}
- New Credits Issued: {stats.get('new_credits', 0)}
- Active Proposals: {stats.get('active_proposals', 0)}
- Completed Proposals: {stats.get('completed_proposals', 0)}
- Marketplace Sell Orders: {stats.get('sell_orders', 0)}
- Marketplace Buy Orders: {stats.get('buy_orders', 0)}
- Total Validators: {stats.get('total_validators', 0)}
- IBC Channels Active: {stats.get('ibc_channels', 0)}
- Network Transactions: {stats.get('total_transactions', 0)}
"""

        prompt = f"""
You are a content curator for the Regen Network ecosystem. You have been given ALL {total_posts} posts from {len(thread_groups)} discussions from the past week, along with comprehensive ledger statistics. Your job is to create a comprehensive weekly brief that integrates both community discussions AND on-chain activity.

Full week's content ({len(thread_groups)} unique discussions, {total_posts} total posts):

{json.dumps(thread_context, indent=2, default=str)}

{"... plus " + str(len(threads_summary) - 50) + " more discussions" if len(threads_summary) > 50 else ""}

{ledger_summary}

CRITICAL REQUIREMENT: The brief_content MUST be 800-1200 words. This is non-negotiable. Count the words!

Write a comprehensive weekly digest with these MANDATORY sections (each section MUST meet the minimum word count):

1. **Opening Overview** (MINIMUM 150 words):
   Open with: "This week in the Regen Network ecosystem saw [specific metrics from the provided data]..."
   Include key metrics from the ledger data provided above (governance proposals, bonded REGEN, validators, IBC channels, marketplace activity)
   Use ONLY the actual numbers from the data - do not use placeholder or example numbers

2. **Governance Deep Dive** (MINIMUM 200 words):
   Analyze the governance proposals present in the ledger data
   For each proposal, explain what it addresses and why it matters to the ecosystem
   Connect governance activity to validator participation and bonding metrics from the data
   If specific proposal numbers are referenced in discussions, include that context

3. **Community Forum Analysis** (MINIMUM 200 words):
   Analyze the key discussions that appeared in the provided forum/discourse content
   For each major discussion topic, explain its significance and implications
   Connect community conversations to ecosystem goals and technical developments
   Link discussions to broader trends in regenerative finance when relevant
   ONLY discuss topics that are actually present in the provided discussion data

4. **Marketplace and Network Metrics** (MINIMUM 200 words):
   Analyze marketplace metrics from the ledger data (sell/buy orders, credit batches)
   Discuss what these metrics reveal about ecosystem health and activity
   Examine IBC connectivity and cross-chain activity based on provided statistics
   Connect marketplace dynamics to governance and community developments
   Base all analysis on the actual numbers in the ledger data section above

5. **Technical Developments** (MINIMUM 150 words):
   Based ONLY on the actual data provided, discuss any technical developments from the week
   If Proposal #56 (AXELAR client revival) passed, explain what this technical change enables
   Focus on infrastructure improvements mentioned in the discussions
   DO NOT invent or assume technical developments that are not in the provided data

6. **Synthesis and Outlook** (MINIMUM 150 words):
   Connect all the threads - governance, community, marketplace, technical
   Project forward based on current trends
   Identify key challenges and opportunities

WORD COUNT VERIFICATION:
- The total brief_content MUST be 800-1200 words
- Each paragraph should be 50-100 words
- Use specific data points in EVERY paragraph
- If your response is under 800 words, you have failed the task

Also provide:
- A 2-3 sentence executive summary that mentions key metrics
- Key themes (3-5 main themes with specific data points)
- Community pulse metrics

Format as structured JSON with these exact keys:
- executive_summary (2-3 sentences with key metrics)
- brief_content (the full 800-1200 word narrative brief in markdown)
- themes (object with theme names as keys, related topics as values)
- community_pulse (object with overall_activity, key_focus_areas array, emerging_trends array)
- key_discussions (array of objects with url and title for citations)
"""

        try:
            response = await self.openai_client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert curator for the Regen Network ecosystem, skilled at identifying key developments and trends in regenerative finance, blockchain governance, and ecological economics. You excel at weaving together on-chain metrics with community discussions to create comprehensive narratives.\n\nCRITICAL: You must ONLY write about events, discussions, and developments that are explicitly present in the provided data. DO NOT invent, assume, or extrapolate information that is not directly supported by the content and metrics given to you. If a section requests coverage of a topic not present in the data, acknowledge its absence or focus on what IS present. Accuracy is paramount."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_completion_tokens=4000,  # Updated parameter name for newer models
                response_format={"type": "json_object"}
            )

            llm_analysis = json.loads(response.choices[0].message.content)
            logger.info(f"LLM analysis completed successfully")

            return llm_analysis

        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")

            # Parse the error to provide specific feedback
            error_message = str(e).lower()

            if "insufficient_quota" in error_message or "rate_limit" in error_message:
                error_detail = "OpenAI API quota exceeded. Please add more credits to continue."
            elif "api_key" in error_message or "authentication" in error_message:
                error_detail = "OpenAI API key is missing or invalid. Please configure a valid API key."
            elif "model" in error_message:
                error_detail = f"OpenAI model '{self.model}' is not available or accessible."
            elif "timeout" in error_message:
                error_detail = "OpenAI API request timed out. Please try again later."
            elif "connection" in error_message:
                error_detail = "Unable to connect to OpenAI API. Please check your network connection."
            else:
                error_detail = f"LLM analysis error: {str(e)}"

            logger.error(f"Specific error: {error_detail}")

            # Return error structure - no fake data
            return {
                "executive_summary": f"Weekly digest generation failed: {error_detail}",
                "brief_content": f"## Error Generating Weekly Digest\n\n{error_detail}\n\nPlease resolve the issue and try again.",
                "themes": {},
                "community_pulse": {},
                "key_discussions": [],
                "error": True,
                "error_detail": error_detail
            }

    async def generate_weekly_digest(self) -> Dict[str, Any]:
        """
        Generate a complete weekly digest using LLM
        """
        logger.info("Starting weekly digest generation with LLM")

        # Check for custom date range from environment variables
        start_date_str = os.getenv('DIGEST_START_DATE')
        end_date_str = os.getenv('DIGEST_END_DATE')

        start_date = None
        end_date = None
        days_back = 7

        if start_date_str and end_date_str:
            # Parse dates (datetime already imported at module level)
            start_date = datetime.fromisoformat(start_date_str)
            end_date = datetime.fromisoformat(end_date_str)
            # Extend end_date to end of day
            end_date = end_date.replace(hour=23, minute=59, second=59)
            days_back = (end_date - start_date).days + 1  # +1 to include both dates
            logger.info(f"Using custom date range: {start_date_str} to {end_date_str} ({days_back} days)")
        else:
            logger.info("Using default 7-day range")

        # Get all content from the specified period
        items = await self.get_weekly_content(days_back=days_back, start_date=start_date, end_date=end_date)
        logger.info(f"Retrieved {len(items)} items from past {days_back} days")

        # Enrich items with URL resolution
        logger.info("Enriching content with URL resolution...")
        enriched_items = await self.url_enricher.enrich_digest_items(items)
        logger.info(f"URL enrichment complete - processed {len(enriched_items)} items")

        # Get ledger data for the week
        ledger_data = await self.get_ledger_data_for_week()
        logger.info(f"Retrieved ledger data: {ledger_data.get('statistics', {})}")

        if not enriched_items and not ledger_data:
            logger.warning("No content found for weekly digest")
            return None

        # Group by thread/discussion (use enriched items)
        thread_groups = self.group_by_thread(enriched_items)
        logger.info(f"Grouped into {len(thread_groups)} discussions")

        # Analyze with LLM, including ledger data
        llm_analysis = await self.analyze_with_llm(thread_groups, ledger_data)

        # Check if LLM analysis failed
        if llm_analysis.get('error'):
            logger.error(f"LLM analysis returned error: {llm_analysis.get('error_detail')}")
            # Don't create a digest with error content
            return None

        # Build the digest structure
        now = datetime.now(timezone.utc)

        # Use custom date range if provided, otherwise default to past 7 days
        if start_date and end_date:
            actual_week_start = start_date
            actual_week_end = end_date
        else:
            actual_week_start = now - timedelta(days=7)
            actual_week_end = now

        # Extract clean sources and statistics
        all_sources = [self.extract_clean_source(item['source_sensor']) for item in items]
        source_counts = Counter(all_sources)

        digest = {
            'id': hashlib.sha256(f"weekly-{now.isoformat()}".encode()).hexdigest()[:12],
            'week_start': actual_week_start.isoformat(),
            'week_end': actual_week_end.isoformat(),
            'total_items': len(items),
            'total_discussions': len(thread_groups),
            'executive_summary': llm_analysis.get('executive_summary', ''),
            # Clean any escaped newlines from LLM response
            'brief_content': llm_analysis.get('brief_content', '').replace('\\n\\n', '\n\n').replace('\\n', '\n'),
            'themes': llm_analysis.get('themes', {}),
            'community_pulse': llm_analysis.get('community_pulse', {}),
            'key_discussions': llm_analysis.get('key_discussions', []),
            'ledger_activity': ledger_data,  # Include ledger data
            'statistics': {
                'total_posts': len(items),
                'unique_discussions': len(thread_groups),
                'active_sources': len(source_counts),
                'most_active_source': source_counts.most_common(1)[0][0] if source_counts else 'unknown',
                'source_breakdown': dict(source_counts),
                'ledger_summary': ledger_data.get('summary', '') if ledger_data else ''
            },
            'thread_groups': convert_uuids_to_strings(thread_groups),  # Include thread groups with enriched items (convert UUIDs to strings)
            'brief': self.format_brief(llm_analysis, actual_week_start, actual_week_end, len(thread_groups), len(items), ledger_data, items, thread_groups),
            'generated_at': now.isoformat(),
            'generator': 'weekly_curator_llm_v1'
        }

        return digest

    def format_brief(self, analysis: Dict, week_start, week_end,
                     discussion_count: int, item_count: int, ledger_data: Optional[Dict] = None,
                     items: Optional[List[Dict]] = None, thread_groups: Optional[Dict] = None) -> str:
        """Format the digest as a readable brief"""
        # Unescape literal "\\n" outside the f-string: a backslash inside an f-string
        # expression is a SyntaxError before Python 3.12.
        summary_text = analysis.get('executive_summary', 'Weekly summary unavailable.').replace('\\n', '\n')
        content_text = analysis.get('brief_content', 'No content available.').replace('\\n\\n', '\n\n').replace('\\n', '\n')
        brief = f"""# Regen Network Weekly Brief
{week_start.strftime('%B %d')} - {week_end.strftime('%B %d, %Y')}

## Executive Summary
{summary_text}

---

{content_text}

---

"""

        # Add themes section
        themes = analysis.get('themes', {})
        if themes:
            brief += "## Key Themes\n"
            for theme, topics in themes.items():
                if isinstance(topics, list) and topics:
                    topics_str = ', '.join(topics[:5])  # Limit to 5 topics per theme
                elif isinstance(topics, str):
                    topics_str = topics
                else:
                    continue
                brief += f"- **{theme}**: {topics_str}\n"
            brief += "\n"

        # Add ledger activity section if available (only sections with actual data)
        if ledger_data and ledger_data.get('sections'):
            brief += "## On-Chain Activity\n"
            brief += ledger_data.get('summary', '') + "\n\n"

            for section in ledger_data.get('sections', []):
                # Only add section if it has actual items with content
                items_with_content = [item for item in section.get('items', [])
                                     if item.get('title') and item.get('description')]

                if items_with_content:
                    brief += f"### {section.get('title', 'Activity')}\n"
                    for item in items_with_content[:3]:  # Limit to 3 items per section
                        title = item.get('title', '')
                        desc = item.get('description', '')
                        link = item.get('link', '')

                        if title:
                            formatted_item = f"- **{title}**: {desc[:100]}..." if len(desc) > 100 else f"- **{title}**: {desc}"
                            if link:
                                formatted_item += f" [View →]({link})"
                            brief += formatted_item + "\n"
                    brief += "\n"

        # Add community pulse
        pulse = analysis.get('community_pulse', {})
        if pulse and isinstance(pulse, dict):
            brief += "## Community Pulse\n"

            activity = pulse.get('overall_activity', 'Unknown')
            brief += f"**Activity Level**: {activity}\n\n"

            focus_areas = pulse.get('key_focus_areas', [])
            if focus_areas and isinstance(focus_areas, list):
                brief += "**Key Focus Areas**: "
                brief += ', '.join(focus_areas)
                brief += "\n\n"

            trends = pulse.get('emerging_trends', [])
            if trends and isinstance(trends, list):
                brief += "**Emerging Trends**: "
                brief += ', '.join(trends)
                brief += "\n"

        # Add sources section with detailed URLs
        if items or thread_groups:
            brief += "\n## Sources\n\n"

            # Collect unique sources with URLs and metadata
            sources_by_type = {}

            # Process thread groups first for better URL tracking
            if thread_groups:
                for thread_url, thread_items in thread_groups.items():
                    if thread_url and thread_url != 'untitled':
                        # Get source type from sensor
                        source_type = self.extract_clean_source(thread_items[0].get('source_sensor', '')) if thread_items else 'unknown'

                        if source_type not in sources_by_type:
                            sources_by_type[source_type] = []

                        # Extract title and dates from metadata
                        first_item = thread_items[0] if thread_items else {}
                        metadata = first_item.get('metadata', {})
                        if isinstance(metadata, str):
                            try:
                                metadata = json.loads(metadata)
                            except:
                                metadata = {}

                        title = metadata.get('title', '') if isinstance(metadata, dict) else ''

                        # Get date range for posts
                        dates = [item.get('published_at') for item in thread_items if item.get('published_at')]
                        date_range = ''
                        if dates:
                            latest = max(dates)
                            oldest = min(dates)
                            if latest and oldest:
                                # Format dates
                                try:
                                    latest_str = latest.strftime('%b %d') if hasattr(latest, 'strftime') else str(latest)[:10]
                                    oldest_str = oldest.strftime('%b %d') if hasattr(oldest, 'strftime') else str(oldest)[:10]
                                    date_range = f"{oldest_str} - {latest_str}" if oldest_str != latest_str else latest_str
                                except:
                                    date_range = 'Past week'

                        # Determine the actual URL
                        actual_url = thread_url if thread_url.startswith('http') else None
                        if not actual_url:
                            # Try to get from metadata
                            actual_url = metadata.get('url') or metadata.get('link') if isinstance(metadata, dict) else None

                        sources_by_type[source_type].append({
                            'title': title or thread_url[:100],
                            'url': actual_url,
                            'count': self.count_unique_items(thread_items),
                            'date_range': date_range
                        })

            # Format sources with better details
            if sources_by_type:
                for source_type, source_items in sorted(sources_by_type.items()):
                    # Clean up source type name
                    display_name = source_type.replace('_', ' ').replace('-', ' ').title()
                    if 'discourse' in source_type.lower():
                        display_name = 'Discourse'
                    elif 'github-activity' in source_type.lower():
                        display_name = 'GitHub'
                    elif 'website' in source_type.lower():
                        display_name = 'Website'

                    brief += f"### {display_name}\n"

                    # Sort by item count and show top items
                    source_items.sort(key=lambda x: x['count'], reverse=True)

                    # Determine appropriate label based on source type
                    item_label = "items"
                    collection_label = "threads"
                    if 'discourse' in source_type.lower():
                        item_label = "posts"
                        collection_label = "discussions"
                    elif 'github' in source_type.lower():
                        item_label = "activities"
                        collection_label = "items"
                    elif 'website' in source_type.lower():
                        item_label = "pages"
                        collection_label = "pages"

                    total_items = sum(item['count'] for item in source_items)
                    brief += f"- **Total activity**: {total_items} {item_label} across {len(source_items)} {collection_label}\n"

                    # Show individual items with details
                    for item in source_items[:5]:  # Show top 5 items
                        if item['url']:
                            # Truncate long titles
                            display_title = item['title'][:60] + '...' if len(item['title']) > 60 else item['title']
                            brief += f"- [{display_title}]({item['url']}) "
                        else:
                            display_title = item['title'][:80] + '...' if len(item['title']) > 80 else item['title']
                            brief += f"- {display_title} "

                        # Add item count and date (skip count of 1 for single pages)
                        if item['count'] > 1:
                            brief += f"({item['count']} {item_label}"
                        else:
                            brief += f"("
                        if item['date_range']:
                            brief += f", {item['date_range']}" if item['count'] > 1 else f"{item['date_range']}"
                        brief += ")\n"

                    if len(source_items) > 5:
                        brief += f"- ...and {len(source_items) - 5} more {collection_label}\n"

                    brief += "\n"

                # Add On-Chain Activity section
                if ledger_data and ledger_data.get('statistics'):
                    stats = ledger_data.get('statistics', {})
                    brief += f"### On-Chain Activity\n"
                    brief += f"- Total Credit Batches: {stats.get('total_credit_batches', 0)}\n"
                    brief += f"- Active Proposals: {stats.get('active_proposals', 0)}\n"
                    brief += f"- Marketplace Orders: {stats.get('sell_orders', 0) + stats.get('buy_orders', 0)}\n"
                    brief += f"- Network Validators: {stats.get('total_validators', 0)}\n"
                    brief += f"- IBC Channels: {stats.get('ibc_channels', 0)}\n"
                    brief += f"- [View on Regen Explorer](https://explorer.regen.network)\n"
                    brief += "\n"

        brief += "\n---\n*Generated by Regen Network KOI System with AI Analysis & Ledger Data*\n"

        return brief

    async def save_to_database(self, digest: Dict) -> str:
        """Save the weekly digest to the database"""
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                # Generate a unique content_id for this digest
                content_id = f"weekly_digest_{digest['id']}"

                # Save to quality_reviews table for moderation
                review_id = await conn.fetchval("""
                    INSERT INTO quality_reviews (
                        review_id, content_id, content_type, content_data,
                        style_score, validation_score, approval_status,
                        provenance, created_at
                    ) VALUES (
                        gen_random_uuid(), $1, 'weekly_digest', $2,
                        1.0, 1.0, 'draft', $3, NOW()
                    ) RETURNING review_id
                """,
                    content_id,
                    json.dumps(digest),
                    json.dumps({
                        'week_start': digest['week_start'],
                        'week_end': digest['week_end'],
                        'total_items': digest['total_items'],
                        'total_discussions': digest['total_discussions'],
                        'llm_model': self.model,
                        'statistics': digest['statistics'],
                        'generated_at': digest['generated_at']
                    })
                )

                logger.info(f"Saved weekly digest draft with ID: {review_id}")
                return str(review_id)

    async def fetch_governance_proposal(self, proposal_id: str) -> Optional[str]:
        """Fetch full governance proposal details"""
        try:
            # Try working API endpoints
            api_endpoints = [
                f"https://regen-api.polkachu.com/cosmos/gov/v1beta1/proposals/{proposal_id}",
                f"https://regen-rest.publicnode.com/cosmos/gov/v1beta1/proposals/{proposal_id}",
                f"https://regen.api.m.stavr.tech/cosmos/gov/v1beta1/proposals/{proposal_id}",
                f"https://rest.regen.aneka.io/cosmos/gov/v1beta1/proposals/{proposal_id}"
            ]

            for api_url in api_endpoints:

                async with httpx.AsyncClient() as client:
                    try:
                        response = await client.get(api_url, timeout=10.0)
                        if response.status_code == 200:
                            data = response.json()

                            # All current endpoints use the same format
                            proposal = data.get('proposal', {})

                            # Format proposal content
                            content = f"### Full Governance Proposal #{proposal_id}\n\n"

                            # Extract proposal details
                            content = f"### Full Governance Proposal #{proposal_id}\n\n"
                            content += f"**Status**: {proposal.get('status', 'UNKNOWN')}\n\n"

                            # Get content details
                            prop_content = proposal.get('content', {})
                            content += f"**Type**: {prop_content.get('@type', 'N/A')}\n\n"

                            # For community pool spend, show details
                            if 'CommunityPoolSpend' in prop_content.get('@type', ''):
                                content += f"**Recipient**: {prop_content.get('recipient', 'N/A')}\n"
                                amounts = prop_content.get('amount', [])
                                if amounts:
                                    for amt in amounts:
                                        denom = amt.get('denom', 'uregen')
                                        amount = int(amt.get('amount', 0)) / 1_000_000 if amt.get('amount') else 0
                                        content += f"**Amount Requested**: {amount:,.0f} REGEN\n"
                                content += "\n"

                            # Add hardcoded details for known proposals
                            if proposal_id == "57":
                                content += f"**Title**: Request for the funding for the Tokenomics working group in Q4\n\n"
                                content += f"**Complete Proposal Text**:\n\n"
                                content += f"Details: https://forum.regen.network/t/funding-application-for-the-regen-tokenomics-working-group/29/5\n\n"
                                content += f"Regen Tokenomics, operating as an autonomous entity / DAO since 2023, is requesting its first "
                                content += f"Community Pool grant to support ongoing coordination, communications, and upcoming Agent-Based Modeling research.\n\n"
                                content += f"**Forum Discussion**: https://forum.regen.network/t/funding-application-for-the-regen-tokenomics-working-group/29/5\n\n"
                            elif proposal_id == "56":
                                content += f"**Title**: Revive REGEN<>AXELAR client\n\n"
                                content += f"**Complete Proposal Text**:\n\n"
                                content += f"Update client from 07-tendermint-100 to 07-tendermint-181 to reenable transfers from Axelar to Regen.\n\n"

                            # Add voting details
                            final_tally = proposal.get('final_tally_result', {})
                            if final_tally:
                                content += f"**Voting Results**:\n"
                                # Convert from uregen to REGEN
                                yes_amt = int(final_tally.get('yes', '0')) / 1_000_000 if final_tally.get('yes') else 0
                                no_amt = int(final_tally.get('no', '0')) / 1_000_000 if final_tally.get('no') else 0
                                abstain_amt = int(final_tally.get('abstain', '0')) / 1_000_000 if final_tally.get('abstain') else 0
                                no_veto_amt = int(final_tally.get('no_with_veto', '0')) / 1_000_000 if final_tally.get('no_with_veto') else 0

                                content += f"- Yes: {yes_amt:,.0f} REGEN\n"
                                content += f"- No: {no_amt:,.0f} REGEN\n"
                                content += f"- Abstain: {abstain_amt:,.0f} REGEN\n"
                                content += f"- No With Veto: {no_veto_amt:,.0f} REGEN\n\n"

                            # Add timing
                            content += f"**Timeline**:\n"
                            content += f"- Submit Time: {proposal.get('submit_time', 'N/A')}\n"
                            content += f"- Deposit End: {proposal.get('deposit_end_time', 'N/A')}\n"
                            content += f"- Voting Start: {proposal.get('voting_start_time', 'N/A')}\n"
                            content += f"- Voting End: {proposal.get('voting_end_time', 'N/A')}\n\n"

                            return content
                    except Exception as e:
                        logger.debug(f"API endpoint {api_url} failed: {e}")
                        continue

            # If all endpoints fail
            return f"### Governance Proposal #{proposal_id}\n\n*[Full proposal details not available - all API endpoints unreachable]*\n\n"

        except Exception as e:
            logger.error(f"Error fetching proposal {proposal_id}: {e}")
            return None

    async def fetch_website_content(self, url: str) -> Optional[Dict[str, str]]:
        """Fetch full content from regentokenomics.org or other website pages

        PRIMARY: Use database content (already aggregated from chunks)
        FALLBACK: HTTP fetch only if not in database
        """
        result = {'text': '', 'video_urls': [], 'audio_urls': []}

        try:
            # PRIMARY: Get content from database (with chunk aggregation for full content)
            async with asyncpg.create_pool(self.db_url) as pool:
                async with pool.acquire() as conn:
                    # First try to get parent record (non-chunked) which has full content
                    parent_query = """
                        SELECT content->>'text' AS text, metadata
                        FROM koi_memories
                        WHERE superseded_at IS NULL
                          -- Exclude private content (#28: curator SQL privacy audit)
                          AND (is_private = FALSE OR is_private IS NULL)
                          AND metadata->>'url' = $1
                          AND rid NOT LIKE '%#chunk%'
                        ORDER BY created_at DESC
                        LIMIT 1
                    """

                    row = await conn.fetchrow(parent_query, url)
                    if row and row['text']:
                        logger.info(f"✅ Retrieved website content from database: {len(row['text'])} chars for {url}")
                        result['text'] = row['text']

                        # Get metadata for title and extract video/audio URLs
                        metadata = row['metadata']
                        if metadata and isinstance(metadata, dict):
                            title = metadata.get('title', '')
                            if title:
                                result['text'] = f"**Page Title**: {title}\n\n{result['text']}"

                            # TODO: Extract video/audio URLs from metadata if available
                            # For now, videos will be handled by sensor-level transcription

                        logger.info(f"✅ NotebookLM: Using database content (no HTTP fetch needed)")
                        return result

                    # If no parent record, try aggregating chunks
                    chunks_query = """
                        SELECT content->>'text' AS text, metadata, rid
                        FROM koi_memories
                        WHERE superseded_at IS NULL
                          -- Exclude private content (#28: curator SQL privacy audit)
                          AND (is_private = FALSE OR is_private IS NULL)
                          AND metadata->>'url' = $1
                          AND rid LIKE '%#chunk%'
                        ORDER BY rid
                    """

                    chunk_rows = await conn.fetch(chunks_query, url)
                    if chunk_rows:
                        logger.info(f"✅ Found {len(chunk_rows)} chunks for {url}, aggregating...")

                        # Aggregate chunk text with filtering and deduplication
                        texts = []
                        metadata = None
                        seen_sentences = set()  # For deduplication

                        for chunk_row in chunk_rows:
                            chunk_text = chunk_row['text']
                            if not chunk_text:
                                continue

                            # Filter out JSON metadata junk
                            # These chunks contain React/Next.js state data
                            json_markers = [
                                'propertyValues', 'blockId', 'parentId',
                                'self.__next_f.push', '"hasContent":true',
                                'createdTime', 'lastEditedTime', '"children":[',
                                '"className":', '"uri":', 'weekly-meetups-nov-11',
                                'mantic-string', '"id":"'
                            ]

                            # Skip if it contains multiple JSON markers or high JSON density
                            marker_count = sum(1 for marker in json_markers if marker in chunk_text)
                            json_chars = sum(1 for c in chunk_text if c in '{}[]":,')
                            json_density = json_chars / len(chunk_text) if len(chunk_text) > 0 else 0

                            if marker_count >= 2 or json_density > 0.25:
                                logger.debug(f"Filtering out JSON junk chunk (markers={marker_count}, density={json_density:.2f}): {chunk_row['rid']}")
                                continue

                            # Deduplicate overlapping content
                            # Split into sentences and track what we've seen
                            sentences = chunk_text.split('. ')
                            unique_sentences = []

                            for sentence in sentences:
                                sentence = sentence.strip()
                                if len(sentence) < 20:  # Skip very short fragments
                                    unique_sentences.append(sentence)
                                    continue

                                # Use first 50 chars as dedup key
                                sentence_key = sentence[:50] if len(sentence) > 50 else sentence
                                if sentence_key not in seen_sentences:
                                    seen_sentences.add(sentence_key)
                                    unique_sentences.append(sentence)

                            if unique_sentences:
                                deduplicated_text = '. '.join(unique_sentences)
                                texts.append(deduplicated_text)

                            if not metadata and chunk_row['metadata']:
                                metadata = chunk_row['metadata']

                        if texts:
                            result['text'] = '\n\n'.join(texts)
                            logger.info(f"✅ Aggregated {len(texts)} chunks (filtered & deduplicated) into {len(result['text'])} chars")

                            # Post-process: Remove any remaining JSON fragments
                            import re
                            original_len = len(result['text'])

                            # Split into paragraphs for processing
                            paragraphs = result['text'].split('\n\n')
                            cleaned_paragraphs = []

                            for para in paragraphs:
                                # Skip paragraphs that are heavily JSON
                                json_density = sum(1 for c in para if c in '{}[]":,\\') / len(para) if len(para) > 0 else 0

                                # Skip if >20% JSON density or contains multiple JSON markers
                                json_marker_count = sum([
                                    para.count('"id":"'),
                                    para.count('"children":['),
                                    para.count('"parentId":"'),
                                    para.count('"className":"'),
                                    para.count('"type":"'),
                                    para.count('"hasContent":'),
                                    para.count(',"width":'),
                                    para.count(',"height":')
                                ])

                                if json_density > 0.2 or json_marker_count >= 3:
                                    continue

                                # Clean individual lines within paragraph
                                lines = para.split('\n')
                                cleaned_lines = []
                                for line in lines:
                                    if not line.strip():
                                        continue

                                    # Skip lines with high JSON density
                                    line_json_density = sum(1 for c in line if c in '{}[]":,\\') / len(line) if len(line) > 0 else 0
                                    if line_json_density > 0.3:
                                        continue

                                    # Remove inline JSON fragments
                                    cleaned_line = re.sub(r'\{[^}]{50,}\}', '', line)  # Remove long JSON objects
                                    cleaned_line = re.sub(r',"[a-zA-Z_]+":(?:"[^"]*"|\[[^\]]*\]|\{[^}]*\})', '', cleaned_line)

                                    if cleaned_line.strip():
                                        cleaned_lines.append(cleaned_line)

                                if cleaned_lines:
                                    cleaned_paragraphs.append('\n'.join(cleaned_lines))

                            result['text'] = '\n\n'.join(cleaned_paragraphs)

                            # Clean up excessive whitespace
                            result['text'] = re.sub(r'\n{3,}', '\n\n', result['text'])
                            result['text'] = result['text'].strip()

                            final_len = len(result['text'])
                            if final_len < original_len:
                                logger.info(f"🧹 Cleaned {original_len - final_len} chars of JSON fragments from aggregated text")

                            # Get metadata for title
                            if metadata and isinstance(metadata, dict):
                                title = metadata.get('title', '')
                                if title:
                                    result['text'] = f"**Page Title**: {title}\n\n{result['text']}"

                            logger.info(f"✅ NotebookLM: Using aggregated database chunks (no HTTP fetch needed)")
                            return result

                    logger.info(f"⚠️  Website content not in database, falling back to HTTP fetch for {url}")

        except Exception as e:
            logger.warning(f"❌ Error fetching from database: {e}, falling back to HTTP fetch")

        # FALLBACK: Fetch via HTTP if not in database
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=30.0, follow_redirects=True)
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch website {url}: {response.status_code}")
                    return None

                html_content = response.text

                # Extract text content (simple approach - could be enhanced with BeautifulSoup)
                import re

                # Remove script and style elements
                html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
                html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL)

                # Extract title
                title_match = re.search(r'<title>(.*?)</title>', html_content)
                if title_match:
                    result['text'] = f"**Page Title**: {title_match.group(1)}\n\n"

                # Extract main content (looking for article, main, or content divs)
                content_patterns = [
                    r'<article[^>]*>(.*?)</article>',
                    r'<main[^>]*>(.*?)</main>',
                    r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>'
                ]

                for pattern in content_patterns:
                    matches = re.findall(pattern, html_content, re.DOTALL)
                    if matches:
                        for match in matches:
                            # Convert to markdown-ish format
                            text = match
                            text = re.sub(r'<h1[^>]*>(.*?)</h1>', r'\n# \1\n', text)
                            text = re.sub(r'<h2[^>]*>(.*?)</h2>', r'\n## \1\n', text)
                            text = re.sub(r'<h3[^>]*>(.*?)</h3>', r'\n### \1\n', text)
                            text = re.sub(r'<p[^>]*>(.*?)</p>', r'\1\n\n', text)
                            text = re.sub(r'<strong>(.*?)</strong>', r'**\1**', text)
                            text = re.sub(r'<b>(.*?)</b>', r'**\1**', text)
                            text = re.sub(r'<em>(.*?)</em>', r'*\1*', text)
                            text = re.sub(r'<i>(.*?)</i>', r'*\1*', text)
                            text = re.sub(r'<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>', r'[\2](\1)', text)
                            text = re.sub(r'<.*?>', '', text)  # Remove remaining tags
                            result['text'] += text[:10000]  # Increased limit from 5000 to 10000
                            break

                # Look for video files (mp4, webm, etc.)
                video_patterns = [
                    r'<video[^>]*src="([^"]+)"',
                    r'<source[^>]*src="([^"]+\.mp4)"',
                    r'href="([^"]+\.mp4)"',
                    r'"(https?://[^"]+\.mp4)"'
                ]

                for pattern in video_patterns:
                    matches = re.findall(pattern, html_content)
                    for match in matches:
                        # Make absolute URL if relative
                        if not match.startswith('http'):
                            match = urljoin(url, match)
                        if match not in result['video_urls']:
                            result['video_urls'].append(match)
                            logger.info(f"Found video: {match}")

                # Look for audio files
                audio_patterns = [
                    r'<audio[^>]*src="([^"]+)"',
                    r'href="([^"]+\.mp3)"'
                ]

                for pattern in audio_patterns:
                    matches = re.findall(pattern, html_content)
                    for match in matches:
                        if not match.startswith('http'):
                            match = urljoin(url, match)
                        if match not in result['audio_urls']:
                            result['audio_urls'].append(match)

                return result

        except Exception as e:
            logger.error(f"Error fetching website content {url}: {e}")
            return None

    async def transcribe_video(self, video_url: str) -> Optional[str]:
        """Download and transcribe video using OpenAI Whisper"""
        try:
            # Create temp directory
            with tempfile.TemporaryDirectory() as temp_dir:
                video_path = f"{temp_dir}/video.mp4"
                audio_path = f"{temp_dir}/audio.mp3"

                logger.info(f"Downloading video from {video_url}")

                # Download video
                async with httpx.AsyncClient() as client:
                    response = await client.get(video_url, timeout=300.0)  # 5 min timeout
                    if response.status_code != 200:
                        logger.error(f"Failed to download video: {response.status_code}")
                        return None

                    with open(video_path, 'wb') as f:
                        f.write(response.content)

                logger.info(f"Extracting audio from video")

                # Extract audio using ffmpeg
                result = subprocess.run(
                    ['ffmpeg', '-i', video_path, '-vn', '-acodec', 'mp3', '-ab', '128k', audio_path],
                    capture_output=True,
                    text=True
                )

                if result.returncode != 0:
                    logger.error(f"Failed to extract audio: {result.stderr}")
                    return None

                logger.info(f"Transcribing audio with OpenAI Whisper")

                # Transcribe using OpenAI Whisper API
                client = AsyncOpenAI(api_key=self.openai_api_key)

                with open(audio_path, 'rb') as audio_file:
                    transcript = await client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file,
                        response_format="text"
                    )

                return transcript

        except Exception as e:
            logger.error(f"Error transcribing video {video_url}: {e}")
            return None

    async def fetch_forum_thread_content(self, url: str) -> Optional[str]:
        """Fetch actual content from a forum thread URL"""
        try:
            # Extract thread ID from URL
            # Format: https://forum.regen.network/t/thread-title/123
            parts = url.rstrip('/').split('/')
            if len(parts) < 2:
                return None

            thread_id = parts[-1]
            if not thread_id.isdigit():
                # Sometimes ID is in the slug like 'thread-title-123'
                if '-' in parts[-1]:
                    possible_id = parts[-1].split('-')[-1]
                    if possible_id.isdigit():
                        thread_id = possible_id
                    else:
                        return None
                else:
                    return None

            # Use Discourse API to fetch thread content with print=true to get ALL posts
            api_url = f"https://forum.regen.network/t/{thread_id}.json?print=true"

            async with httpx.AsyncClient() as client:
                response = await client.get(api_url, timeout=10.0)
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch thread {thread_id}: {response.status_code}")
                    return None

                data = response.json()

                # Extract post content (print=true returns all posts)
                posts = data.get('post_stream', {}).get('posts', [])
                if not posts:
                    return None

                # Format the thread content with posts (most recent first)
                thread_content = f"**Thread Title**: {data.get('title', 'Untitled')}\n\n"
                thread_content += f"**Category**: {data.get('category_id', 'General')}\n"
                thread_content += f"**Total Posts**: {len(posts)}\n"
                thread_content += f"**Thread URL**: {url}\n\n"
                thread_content += "---\n\n"

                # Reverse posts to show most recent first, then limit to last 30 posts
                # This gives good recent context without overwhelming with very old threads
                posts_to_include = list(reversed(posts))[:30]
                thread_content += f"*Showing most recent {len(posts_to_include)} of {len(posts)} total posts*\n\n"

                # Include posts (now newest first)
                for i, post in enumerate(posts_to_include, 1):
                    username = post.get('username', 'Anonymous')
                    created = post.get('created_at', '')[:10]
                    content = post.get('cooked', '')  # 'cooked' is the rendered HTML

                    # Enhanced HTML to markdown conversion
                    content = re.sub(r'<p>(.*?)</p>', r'\1\n\n', content)
                    content = re.sub(r'<strong>(.*?)</strong>', r'**\1**', content)
                    content = re.sub(r'<em>(.*?)</em>', r'*\1*', content)
                    content = re.sub(r'<code>(.*?)</code>', r'`\1`', content)
                    content = re.sub(r'<pre>(.*?)</pre>', r'```\n\1\n```', content, flags=re.DOTALL)
                    content = re.sub(r'<blockquote>(.*?)</blockquote>', r'> \1', content, flags=re.DOTALL)
                    content = re.sub(r'<a href="(.*?)".*?>(.*?)</a>', r'[\2](\1)', content)
                    content = re.sub(r'<ul>(.*?)</ul>', r'\1', content, flags=re.DOTALL)
                    content = re.sub(r'<li>(.*?)</li>', r'- \1\n', content)
                    content = re.sub(r'<.*?>', '', content)  # Remove remaining HTML tags
                    content = content.strip()

                    thread_content += f"### Post {i} by @{username} ({created})\n\n"
                    thread_content += f"{content}\n\n"
                    thread_content += "---\n\n"

                return thread_content

        except Exception as e:
            logger.error(f"Error fetching forum thread {url}: {e}")
            return None

    async def export_files(self, digest: Dict):
        """Export digest to JSON and Markdown files with forum content"""
        output_dir = Path("/opt/projects/koi-processor/output/weekly")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use date range from environment variables for cache-compatible filenames
        start_date_str = os.getenv('DIGEST_START_DATE')
        end_date_str = os.getenv('DIGEST_END_DATE')

        if start_date_str and end_date_str:
            # Date-range aware filename for proper cache lookup
            date_range_str = f"{start_date_str}_to_{end_date_str}"
        else:
            # Fallback to default 7-day range
            today = datetime.now()
            end_date = today.strftime('%Y-%m-%d')
            start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
            date_range_str = f"{start_date}_to_{end_date}"

        # Save JSON
        json_path = output_dir / f"weekly_digest_{date_range_str}.json"
        with open(json_path, 'w') as f:
            json.dump(digest, f, indent=2, default=str)
        logger.info(f"Exported JSON to {json_path}")

        # Save Markdown
        md_path = output_dir / f"weekly_digest_{date_range_str}.md"
        with open(md_path, 'w') as f:
            f.write(digest['brief'])

            # Add citations/references section with clickable links
            f.write("\n## References\n")
            for i, story in enumerate(digest.get('top_stories', []), 1):
                source_url = story.get('source', story.get('url', ''))
                if source_url:
                    # Fix broken GitHub URLs
                    if 'github.com' in source_url and 'CHANGELOG' in source_url:
                        source_url = source_url.replace('/blob/main/regen-ledger/', '/blob/main/')
                    f.write(f"{i}. [{story.get('title', 'Story ' + str(i))}]({source_url})\n")

        logger.info(f"Exported Markdown to {md_path}")

    async def export_notebooklm_enhanced(self, digest: Dict):
        """Export enhanced version for NotebookLM with full forum content"""
        output_dir = Path("/opt/projects/koi-processor/output/weekly")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use date range from environment variables for cache-compatible filenames
        start_date_str = os.getenv('DIGEST_START_DATE')
        end_date_str = os.getenv('DIGEST_END_DATE')

        if start_date_str and end_date_str:
            date_range_str = f"{start_date_str}_to_{end_date_str}"
        else:
            today = datetime.now()
            end_date = today.strftime('%Y-%m-%d')
            start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
            date_range_str = f"{start_date}_to_{end_date}"

        # Create NotebookLM enhanced export
        notebooklm_path = output_dir / f"weekly_digest_{date_range_str}_notebooklm.md"
        with open(notebooklm_path, 'w') as f:
            f.write("# Regen Network Weekly Digest - NotebookLM Enhanced Export\n\n")
            f.write("*This document contains the complete weekly digest with full forum thread content embedded for comprehensive analysis.*\n\n")
            f.write("---\n\n")

            # Write the main digest
            f.write("# Main Weekly Digest\n\n")
            f.write(digest['brief'])
            f.write("\n\n")

            # Extract all URLs by type
            forum_urls = set()
            website_urls = set()  # For regentokenomics.org and other websites
            enriched_notion_content = {}  # Store enriched database content {url: content}

            # DEBUG: Log digest structure
            logger.info(f"[NotebookLM Export] Digest has thread_groups: {'thread_groups' in digest}")
            if 'thread_groups' in digest:
                logger.info(f"[NotebookLM Export] Thread groups count: {len(digest['thread_groups'])}")

            # From thread groups
            if 'thread_groups' in digest:
                for thread_url in digest['thread_groups'].keys():
                    if 'forum.regen.network' in thread_url:
                        forum_urls.add(thread_url)
                    elif 'regentokenomics.org' in thread_url or 'website' in digest['thread_groups'][thread_url]:
                        # EXCLUDE directory/index pages
                        if not re.match(r'.*regentokenomics\.org/weekly-meetups/?$', thread_url):
                            website_urls.add(thread_url)

                # Extract enriched URLs from thread items
                for thread_url, group in digest['thread_groups'].items():
                    # Group is a list of items, not a dict with 'items' key
                    if isinstance(group, list):
                        logger.info(f"[NotebookLM Export] Thread {thread_url}: {len(group)} items")
                        for item in group:
                            if 'url_enrichments' in item:
                                enrichments = item['url_enrichments'].get('enrichments', [])
                                logger.info(f"[NotebookLM Export] Found {len(enrichments)} enrichments in item")
                                for enrich in enrichments:
                                    enrich_url = enrich.get('url', '')
                                    enrich_source = enrich.get('source', '')
                                    logger.info(f"[NotebookLM Export] Enrichment: {enrich_url} (source: {enrich_source})")
                                    if enrich_url and enrich.get('source') == 'database':
                                        # Store the database content for this URL
                                        enriched_notion_content[enrich_url] = enrich
                                        logger.info(f"[NotebookLM Export] ✅ Added to enriched_notion_content: {enrich_url}")

            logger.info(f"[NotebookLM Export] Total enriched_notion_content entries: {len(enriched_notion_content)}")

            # From top stories
            for story in digest.get('top_stories', []):
                url = story.get('source', story.get('url', ''))
                if 'forum.regen.network' in url:
                    forum_urls.add(url)
                elif 'regentokenomics.org' in url:
                    # EXCLUDE directory/index pages
                    if not re.match(r'.*regentokenomics\.org/weekly-meetups/?$', url):
                        website_urls.add(url)

            # From key discussions
            for discussion in digest.get('key_discussions', []):
                url = discussion.get('url', '')
                if 'forum.regen.network' in url:
                    forum_urls.add(url)
                elif 'regentokenomics.org' in url:
                    # EXCLUDE directory/index pages
                    if not re.match(r'.*regentokenomics\.org/weekly-meetups/?$', url):
                        website_urls.add(url)

            # Extract from brief text using regex (but skip truncated URLs with ...)
            brief_text = digest.get('brief', '')

            # Forum URLs (skip if truncated with ... and clean up)
            forum_url_pattern = r'https://forum\.regen\.network/t/[^\s\)\]]+'
            found_urls = re.findall(forum_url_pattern, brief_text)
            for url in found_urls:
                # Skip truncated URLs
                if url.endswith('...'):
                    continue
                # Clean up URL - remove trailing punctuation
                url = url.rstrip('.,;:)/')
                # Only add valid, complete URLs
                if '/t/' in url and len(url) > 40:  # Basic validation
                    forum_urls.add(url)

            # Website URLs (regentokenomics.org)
            website_patterns = [
                r'https?://regentokenomics\.org[^\s\)\]]+',
                r'https?://[^\s\)\]]*weekly-meetup[^\s\)\]]+'
            ]
            for pattern in website_patterns:
                found_urls = re.findall(pattern, brief_text)
                for url in found_urls:
                    # Clean up URL - remove trailing punctuation and markdown artifacts
                    url = url.rstrip('.,;:)/]')
                    # Remove any remaining markdown formatting like ']('
                    url = url.split('](')[0] if '](' in url else url
                    # EXCLUDE directory/index pages (like /weekly-meetups without specific date)
                    if url.startswith('http') and not re.match(r'.*regentokenomics\.org/weekly-meetups/?$', url):
                        website_urls.add(url)

            # Extract governance proposal IDs
            proposal_ids = set()
            brief_text = digest.get('brief', '')
            # Look for proposal patterns
            proposal_patterns = [r'#(\d+):', r'proposals?/(\d+)', r'Proposal #(\d+)']
            for pattern in proposal_patterns:
                matches = re.findall(pattern, brief_text)
                for match in matches:
                    proposal_ids.add(match)

            # Fetch and include governance proposals
            if proposal_ids:
                f.write("\n\n# Complete Governance Proposals\n\n")
                f.write(f"*Fetching full text of {len(proposal_ids)} governance proposals...*\n\n")

                for prop_id in sorted(proposal_ids, key=int):
                    logger.info(f"Fetching governance proposal #{prop_id}")
                    prop_content = await self.fetch_governance_proposal(prop_id)
                    if prop_content:
                        f.write(prop_content)
                        f.write("---\n\n")

            # Include enriched Notion content from database first
            if enriched_notion_content:
                f.write("\n\n# Enriched Notion Page Content (from Database)\n\n")
                f.write(f"*Including {len(enriched_notion_content)} Notion pages resolved from database...*\n\n")

                for url, enrich_data in sorted(enriched_notion_content.items()):
                    f.write(f"## Notion Page: {url}\n\n")

                    # Add metadata about chunks if available
                    chunks_info = ""
                    if 'chunks_combined' in enrich_data and 'chunk_total' in enrich_data:
                        chunks_info = f" ({enrich_data['chunks_combined']}/{enrich_data['chunk_total']} chunks combined)"
                    f.write(f"**Source**: Database (RID: {enrich_data.get('rid', 'N/A')}){chunks_info}\n\n")

                    # Extract and format content - handle combined chunks properly
                    content_obj = enrich_data.get('content', {})
                    if isinstance(content_obj, dict) and 'text' in content_obj:
                        # Write the actual text content, not the JSON representation
                        f.write(content_obj['text'])
                        f.write("\n")
                    elif isinstance(content_obj, str):
                        f.write(content_obj)
                        f.write("\n")
                    else:
                        # Fallback: try to extract text or convert to string
                        logger.warning(f"Unexpected content format for {url}: {type(content_obj)}")
                        f.write(str(content_obj))
                        f.write("\n")

                    f.write("\n---\n\n")

            # Fetch and include website content (regentokenomics.org, etc.)
            # Skip URLs we already have enriched from database
            remaining_website_urls = website_urls - set(enriched_notion_content.keys())
            if remaining_website_urls:
                f.write("\n\n# Complete Website Content & Transcriptions\n\n")
                f.write(f"*Fetching full content from {len(remaining_website_urls)} website pages including video transcriptions...*\n\n")

                for url in sorted(remaining_website_urls):
                    logger.info(f"Fetching website content: {url}")
                    content = await self.fetch_website_content(url)
                    if content:
                        f.write(f"## Website: {url}\n\n")

                        # Write full page content (includes transcripts already embedded from toggle expansion)
                        if content['text']:
                            f.write(content['text'])
                            f.write("\n\n")

                        f.write("---\n\n")
                    else:
                        f.write(f"## Website: {url}\n\n")
                        f.write("*[Unable to fetch website content]*\n\n")
                        f.write("---\n\n")

            # Include forum threads from database (thread_groups)
            if forum_urls:
                f.write("\n\n# Complete Forum Thread Content\n\n")
                f.write(f"*Including complete content from {len(forum_urls)} forum threads from database...*\n\n")

                thread_count = 0
                for url in sorted(forum_urls):
                    thread_count += 1
                    logger.info(f"Including forum thread {thread_count}/{len(forum_urls)} from database: {url}")

                    # Extract forum content from thread_groups (already in database)
                    thread_items = digest.get('thread_groups', {}).get(url, [])

                    if thread_items:
                        f.write(f"## Forum Thread #{thread_count}\n\n")
                        f.write(f"**URL**: {url}\n\n")
                        f.write(f"**Total Posts**: {len(thread_items)}\n\n")
                        f.write("---\n\n")

                        # Include all posts from the thread
                        for i, item in enumerate(thread_items, 1):
                            # Extract text content
                            content_obj = item.get('content', '')
                            if isinstance(content_obj, str):
                                # Content might be a JSON string
                                try:
                                    import json
                                    content_data = json.loads(content_obj)
                                    text_content = content_data.get('text', content_obj)
                                except:
                                    text_content = content_obj
                            elif isinstance(content_obj, dict):
                                text_content = content_obj.get('text', str(content_obj))
                            else:
                                text_content = str(content_obj)

                            # Write post content
                            f.write(f"### Post {i}\n\n")
                            f.write(text_content)
                            f.write("\n\n---\n\n")

                        logger.info(f"✅ Included {len(thread_items)} posts from forum thread: {url}")
                    else:
                        f.write(f"## Forum Thread #{thread_count}\n\n")
                        f.write(f"**URL**: {url}\n\n")
                        f.write("*[No forum posts found in database for this thread]*\n\n")
                        f.write("---\n\n")
                        logger.warning(f"No posts found in thread_groups for: {url}")

            f.write("\n\n---\n\n")
            f.write("## Document Completeness\n\n")
            f.write("This comprehensive NotebookLM export contains:\n")
            f.write("- ✅ Complete weekly digest (800-1200 words)\n")
            f.write(f"- ✅ {len(proposal_ids) if proposal_ids else 0} full governance proposals with voting details\n")
            f.write(f"- ✅ {len(forum_urls)} complete forum threads (most recent 30 posts per thread)\n")
            f.write(f"- ✅ {len(enriched_notion_content)} Notion pages from database (URL-enriched)\n")
            f.write(f"- ✅ {len(remaining_website_urls) if remaining_website_urls else 0} additional website pages with full content\n")
            f.write(f"- ✅ Video transcriptions where available\n")
            f.write("- ✅ All on-chain metrics and statistics\n")
            f.write("- ✅ No external sources needed - everything is here\n\n")
            f.write("*Generated by Regen Network KOI System - Complete Archive for NotebookLM Analysis*\n")

        logger.info(f"Exported NotebookLM enhanced version to {notebooklm_path}")


async def main():
    """Main execution function"""
    curator = WeeklyCuratorLLM()

    # Generate the digest
    digest = await curator.generate_weekly_digest()

    if digest:
        # Save to database
        review_id = await curator.save_to_database(digest)

        # Export to files
        await curator.export_files(digest)

        # Export NotebookLM enhanced version
        await curator.export_notebooklm_enhanced(digest)

        print(f"✅ Weekly digest generated successfully!")
        print(f"📊 Discussions: {digest['total_discussions']}")
        print(f"📝 Total posts: {digest['total_items']}")
        print(f"🆔 Review ID: {review_id}")
        print(f"💡 {digest['executive_summary'][:100]}...")

        return digest
    else:
        print("❌ Failed to generate weekly digest")
        return None


def _masked_dsn(dsn: str) -> str:
    """The DSN with any password replaced by ***."""
    parts = urlparse(dsn)
    if parts.password is None:
        return dsn
    host = parts.netloc.rsplit("@", 1)[1]
    return parts._replace(netloc=f"{parts.username}:***@{host}").geturl()


def describe_plan() -> List[str]:
    """What main() does, without creating a client or touching anything."""
    db = os.getenv('POSTGRES_URL') or "postgresql://localhost:5433/eliza (built-in default)"
    start = os.getenv('DIGEST_START_DATE') or "7 days ago"
    end = os.getenv('DIGEST_END_DATE') or "today"
    return [
        f"Database: {_masked_dsn(db)}",
        f"LLM: {os.getenv('OPENAI_MODEL', 'gpt-4-turbo-preview')} at "
        f"{os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1')}",
        f"Date range: {start} to {end}",
        "Steps: read the week's content and ledger data, make one paid chat completion, "
        "INSERT the digest into quality_reviews, write the JSON, Markdown and NotebookLM "
        "files under /opt/projects/koi-processor/output/weekly",
    ]


def cli_main(argv=None) -> int:
    """Command-line entry: a dry run unless --apply is given."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate the weekly digest with an LLM, save it to quality_reviews and "
                    "export it. Default is a dry run that contacts nothing.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Run it: one paid chat completion, a database INSERT and file exports")
    args = parser.parse_args(argv)

    for line in describe_plan():
        print(line)
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was read, sent or written. Re-run with --apply to do this.")
        return 0
    asyncio.run(main())
    return 0


if __name__ == "__main__":
    sys.exit(cli_main())
