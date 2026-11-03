#!/usr/bin/env python3
"""
URL Enrichment for Weekly Digest
Intelligently resolves URLs to database content or scrapes external sources
"""

import re
import asyncio
import asyncpg
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urlparse, parse_qs
from loguru import logger
import httpx
from bs4 import BeautifulSoup


class URLEnricher:
    """
    Enriches weekly digest content by resolving URLs to database content
    or scraping external sources
    """

    def __init__(self, db_url: str):
        self.db_url = db_url
        self.url_cache = {}  # Cache resolved URLs

    async def extract_urls(self, content: str) -> Set[str]:
        """Extract all URLs from text content"""
        # Match http(s) URLs
        url_pattern = r'https?://[^\s\)\]\}\"\'<>]+'
        urls = set(re.findall(url_pattern, content))
        return urls

    async def resolve_notion_url(self, url: str, pool: asyncpg.Pool) -> Optional[Dict[str, Any]]:
        """
        Resolve a Notion URL to database content

        Handles:
        - regentokenomics.org/page-slug → Notion page
        - notion.so/page-id → Notion page
        - maxplay.notion.site/page-id → External Notion page (needs scraping)
        """
        # Check cache first
        if url in self.url_cache:
            return self.url_cache[url]

        parsed = urlparse(url)

        # Extract Notion page ID patterns
        notion_id_patterns = [
            # Standard Notion URL: notion.so/Title-123abc...
            r'/([a-f0-9]{32})',
            # With dashes: notion.so/Title-12-34-56...
            r'/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})',
            # Short form
            r'/([a-f0-9]{8,})',
        ]

        notion_id = None
        for pattern in notion_id_patterns:
            match = re.search(pattern, parsed.path)
            if match:
                notion_id = match.group(1).replace('-', '')
                break

        # Also check the full URL (after the page title) for Notion page IDs
        # Pattern: /Title-PAGEID or /Title-PAGE-ID-WITH-DASHES
        if not notion_id:
            # Try to extract from full URL path: /What-We-Learned-...-2b7a755141ee809f9212cc29590ec719
            match = re.search(r'-([a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})(?:\?|$)', url)
            if match:
                notion_id = match.group(1).replace('-', '')

        if not notion_id:
            # Try regentokenomics.org page slug mapping
            if 'regentokenomics.org' in url:
                # Extract slug from URL
                slug = parsed.path.strip('/').split('/')[-1]
                logger.info(f"Attempting to match regentokenomics.org slug: {slug}")

                # First, try to fetch the page and extract embedded Notion page ID
                notion_id = await self._fetch_regentokenomics_notion_id(url)
                if notion_id:
                    logger.info(f"Extracted Notion ID {notion_id} from regentokenomics.org page")
                    # Continue to normal Notion ID lookup below
                else:
                    # Fallback: search for matching Notion page by title/slug
                    async with pool.acquire() as conn:
                        result = await conn.fetch("""
                            SELECT rid, content, metadata, created_at
                            FROM koi_memories
                            WHERE source_sensor LIKE '%notion%'
                            AND (
                                content::text ILIKE $1
                                OR metadata::text ILIKE $1
                                OR rid ILIKE $1
                            )
                            ORDER BY created_at DESC
                            LIMIT 5
                        """, f'%{slug}%')

                        if result:
                            logger.info(f"Found {len(result)} Notion pages matching '{slug}'")
                            # Return the most recent match
                            row = result[0]
                            content_data = {
                                'rid': row['rid'],
                                'content': row['content'],
                                'metadata': row['metadata'],
                                'source': 'database',
                                'url': url
                            }
                            self.url_cache[url] = content_data
                            return content_data

                    return None

        if not notion_id:
            return None

        # Search database for Notion page by ID - fetch ALL chunks
        async with pool.acquire() as conn:
            # Try matching the Notion ID in RID or metadata
            results = await conn.fetch("""
                SELECT rid, content, metadata, created_at,
                       (metadata::json->>'chunk_index')::int as chunk_index,
                       (metadata::json->>'chunk_total')::int as chunk_total,
                       metadata::json->>'parent_rid' as parent_rid
                FROM koi_memories
                WHERE source_sensor LIKE '%notion%'
                AND (
                    rid ILIKE $1
                    OR metadata::text ILIKE $1
                )
                ORDER BY (metadata::json->>'chunk_index')::int
            """, f'%{notion_id}%')

            if results:
                logger.info(f"Found {len(results)} chunks for Notion page ID: {notion_id}")

                # Combine all chunks
                combined_text = ""
                metadata = results[0]['metadata']
                rid = results[0]['parent_rid'] if results[0]['parent_rid'] else results[0]['rid']

                for row in results:
                    content_obj = row['content']
                    if isinstance(content_obj, dict) and 'text' in content_obj:
                        combined_text += content_obj['text'] + "\n"
                    elif isinstance(content_obj, str):
                        combined_text += content_obj + "\n"

                # Check completeness
                if results[0]['chunk_total'] and len(results) < results[0]['chunk_total']:
                    logger.warning(f"Incomplete chunks: {len(results)}/{results[0]['chunk_total']} for {notion_id}")
                else:
                    logger.info(f"Complete content: {len(results)} chunks combined")

                content_data = {
                    'rid': rid,
                    'content': {'text': combined_text.strip()},
                    'metadata': metadata,
                    'source': 'database',
                    'url': url,
                    'chunks_combined': len(results),
                    'chunk_total': results[0]['chunk_total'] if results[0]['chunk_total'] else len(results)
                }
                self.url_cache[url] = content_data
                return content_data

        return None

    async def _fetch_regentokenomics_notion_id(self, url: str) -> Optional[str]:
        """
        Fetch a regentokenomics.org page and extract the embedded Notion page ID.

        regentokenomics.org pages embed Notion content via iframe or have meta tags
        that reference the underlying Notion page ID.
        """
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
                response = await client.get(url, headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; RegenDigest/1.0)'
                })

                if response.status_code != 200:
                    logger.warning(f"Failed to fetch regentokenomics page {url}: HTTP {response.status_code}")
                    return None

                html = response.text

                # Look for Notion page IDs in various places:
                # 1. Notion iframe embed: src="https://notion.so/..." or data-page-id
                # 2. Meta tags with Notion URLs
                # 3. Links to notion.so pages

                # Pattern for 32-char hex Notion page ID
                notion_id_pattern = r'[a-f0-9]{32}'
                notion_uuid_pattern = r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'

                # Try to find Notion URLs in the page
                soup = BeautifulSoup(html, 'lxml')

                # Check iframes
                for iframe in soup.find_all('iframe'):
                    src = iframe.get('src', '')
                    if 'notion' in src.lower():
                        match = re.search(notion_id_pattern, src)
                        if match:
                            return match.group(0)
                        match = re.search(notion_uuid_pattern, src)
                        if match:
                            return match.group(0).replace('-', '')

                # Check for data attributes with Notion IDs
                for elem in soup.find_all(attrs={'data-page-id': True}):
                    page_id = elem.get('data-page-id', '')
                    if re.match(notion_id_pattern, page_id) or re.match(notion_uuid_pattern, page_id):
                        return page_id.replace('-', '')

                # Check links to notion.so
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '')
                    if 'notion.so' in href or 'notion.site' in href:
                        match = re.search(notion_id_pattern, href)
                        if match:
                            return match.group(0)
                        match = re.search(notion_uuid_pattern, href)
                        if match:
                            return match.group(0).replace('-', '')

                # Check for Notion IDs in script tags (Super.so embeds often use this)
                for script in soup.find_all('script'):
                    script_text = script.string or script.get_text() or ''
                    # Look for patterns like "pageId":"..." (JSON format used by Super.so)
                    match = re.search(r'"pageId"\s*:\s*"([a-f0-9]{32})"', script_text)
                    if match:
                        return match.group(1)
                    # Also try with dashes
                    match = re.search(r'"pageId"\s*:\s*"([a-f0-9-]{36})"', script_text)
                    if match:
                        return match.group(1).replace('-', '')

                # Last resort: search the entire HTML for pageId pattern
                # Try unescaped quotes first
                match = re.search(r'"pageId"\s*:\s*"([a-f0-9]{32})"', html)
                if match:
                    return match.group(1)

                # Try with escaped quotes (common in JSON embedded in HTML/JS)
                match = re.search(r'pageId\\":\\"([a-f0-9]{32})', html)
                if match:
                    return match.group(1)

                logger.warning(f"Could not find Notion page ID in {url}")
                return None

        except Exception as e:
            logger.error(f"Error fetching regentokenomics page {url}: {e}")
            return None

    async def scrape_notion_page(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape external Notion page content

        For pages not in our database (e.g., maxplay.notion.site)
        """
        logger.info(f"Scraping external Notion page: {url}")

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
                response = await client.get(url, headers={
                    'User-Agent': 'Mozilla/5.0 (compatible; RegenDigest/1.0)'
                })

                if response.status_code != 200:
                    logger.error(f"Failed to scrape {url}: HTTP {response.status_code}")
                    return None

                # Parse HTML
                soup = BeautifulSoup(response.text, 'lxml')

                # Extract Notion page content
                # Try multiple selectors for different Notion page types
                content_div = None
                selectors = [
                    ('div', {'class': 'notion-page-content'}),
                    ('div', {'class': lambda x: x and 'notion-page' in x}),
                    ('main', {}),
                    ('article', {}),
                    ('body', {})  # Last resort
                ]

                for tag, attrs in selectors:
                    content_div = soup.find(tag, attrs)
                    if content_div:
                        logger.debug(f"Found content using selector: {tag} {attrs}")
                        break

                if content_div:
                    # Remove script and style elements
                    for script in content_div(['script', 'style', 'nav', 'footer', 'header']):
                        script.decompose()

                    # Extract text content
                    text = content_div.get_text(separator='\n', strip=True)

                    # Extract title - try meta tags first for better accuracy
                    title_text = None
                    og_title = soup.find('meta', property='og:title')
                    if og_title:
                        title_text = og_title.get('content', '')
                    if not title_text:
                        title = soup.find('title')
                        title_text = title.get_text() if title else 'Untitled'

                    # Remove Notion suffix from title
                    title_text = re.sub(r'\s*\|\s*Notion$', '', title_text)

                    scraped_data = {
                        'title': title_text,
                        'content': text[:5000],  # Limit content length
                        'url': url,
                        'source': 'scraped',
                        'word_count': len(text.split())
                    }

                    logger.info(f"Successfully scraped Notion page: {title_text} ({len(text)} chars)")
                    self.url_cache[url] = scraped_data
                    return scraped_data
                else:
                    logger.warning(f"Could not find content div in Notion page: {url}")
                    return None

        except Exception as e:
            logger.error(f"Error scraping Notion page {url}: {e}")
            return None

    async def enrich_content_with_urls(self, content: str, pool: asyncpg.Pool) -> Dict[str, Any]:
        """
        Extract and resolve all URLs in content

        Returns enrichment data with resolved URLs
        """
        urls = await self.extract_urls(content)

        if not urls:
            return {'urls': [], 'enrichments': [], 'enrichment_count': 0}

        enrichments = []

        for url in urls:
            # Skip common non-content URLs
            if any(skip in url.lower() for skip in ['twitter.com/i/', 'youtube.com/watch']):
                continue

            enrichment = None

            # Try to resolve from database first
            if 'notion' in url.lower() or 'regentokenomics.org' in url.lower():
                enrichment = await self.resolve_notion_url(url, pool)

                # If not in database and it's an external Notion page, scrape it
                if not enrichment and 'notion.site' in url.lower():
                    enrichment = await self.scrape_notion_page(url)

            if enrichment:
                enrichments.append(enrichment)

        return {
            'urls': list(urls),
            'enrichments': enrichments,
            'enrichment_count': len(enrichments)
        }

    async def enrich_digest_items(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich all items in a digest with URL resolution

        Adds 'url_enrichments' field to each item
        """
        async with asyncpg.create_pool(self.db_url) as pool:
            enriched_items = []

            for item in items:
                # Extract content text
                content = str(item.get('content', ''))

                # Enrich with URLs
                enrichment = await self.enrich_content_with_urls(content, pool)

                # Add enrichment to item
                enriched_item = {**item, 'url_enrichments': enrichment}
                enriched_items.append(enriched_item)

                if enrichment['enrichment_count'] > 0:
                    logger.info(f"Enriched item with {enrichment['enrichment_count']} resolved URLs")

            return enriched_items


# Test function
async def test_url_enricher():
    """Test the URL enricher"""
    import os

    db_url = os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza')
    enricher = URLEnricher(db_url)

    # Test URLs
    test_content = """
    Check out this page: https://regentokenomics.org/weekly-meetups/nov-25

    And this external Notion page: https://maxplay.notion.site/What-We-Learned-Modeling-a-Regen-Token-Economy-2b7a755141ee809f9212cc29590ec719
    """

    async with asyncpg.create_pool(db_url) as pool:
        result = await enricher.enrich_content_with_urls(test_content, pool)

        print("URLs found:", result['urls'])
        print("Enrichments:", len(result['enrichments']))

        for enrichment in result['enrichments']:
            print(f"\n{enrichment['url']}:")
            print(f"  Source: {enrichment['source']}")
            if 'title' in enrichment:
                print(f"  Title: {enrichment['title']}")


if __name__ == '__main__':
    asyncio.run(test_url_enricher())
