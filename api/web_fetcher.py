"""
Web Fetcher — URL validation, fetching, and content extraction for Octo.

Extracts content extraction patterns from RegenAI's website_sensor.py into
a lightweight module for on-demand URL preview and ingestion.
"""

import os
import re
import hashlib
import logging
import ipaddress
import socket
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
from datetime import datetime, timezone

import asyncio

import aiohttp
from bs4 import BeautifulSoup

# Trafilatura — high-quality content extraction (optional but preferred)
try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError:
    trafilatura = None
    TRAFILATURA_AVAILABLE = False

# Playwright is optional — used as fallback for JS-rendered pages
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    async_playwright = None

# Scrapling is optional — used as Tier 3 for anti-bot bypass (Cloudflare, etc.)
try:
    from scrapling.fetchers import StealthyFetcher
    SCRAPLING_AVAILABLE = True
except ImportError:
    StealthyFetcher = None
    SCRAPLING_AVAILABLE = False

logger = logging.getLogger(__name__)

# Limits
MAX_HTML_BYTES = 5 * 1024 * 1024  # 5 MB
MAX_TEXT_CHARS = 100_000           # 100 KB extracted text
FETCH_TIMEOUT = 30                 # seconds
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Content extraction escalation
MIN_WORD_COUNT = int(os.environ.get("MIN_WORD_COUNT", "100"))  # aiohttp->Playwright threshold
PLAYWRIGHT_WORD_THRESHOLD = 50  # Legacy alias (kept for backward compat)
PLAYWRIGHT_TIMEOUT = 30000      # ms
PLAYWRIGHT_WAIT = 3             # seconds after networkidle
SCRAPLING_TIMEOUT = 60          # seconds — Camoufox challenge solving can be slow

# Rate limits
RATE_LIMIT_PER_USER_HOUR = 5
RATE_LIMIT_GLOBAL_HOUR = 20


# =============================================================================
# URL Validation (SSRF Protection)
# =============================================================================

class URLValidationError(Exception):
    pass


class URLValidator:
    """Validate URLs with SSRF protection."""

    BLOCKED_SCHEMES = {"file", "ftp", "gopher", "data", "javascript"}
    BLOCKED_HOSTS = {"metadata.google.internal", "169.254.169.254", "metadata.aws"}

    def validate(self, url: str) -> str:
        """Validate and normalize a URL. Returns normalized URL or raises."""
        parsed = urlparse(url)

        # Scheme check
        if not parsed.scheme:
            url = f"https://{url}"
            parsed = urlparse(url)

        if parsed.scheme not in ("http", "https"):
            raise URLValidationError(f"Blocked scheme: {parsed.scheme}")

        # Host check
        if not parsed.hostname:
            raise URLValidationError("No hostname in URL")

        hostname = parsed.hostname.lower()
        if hostname in self.BLOCKED_HOSTS:
            raise URLValidationError(f"Blocked host: {hostname}")

        # Block private/reserved IPs
        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_reserved or ip.is_loopback or ip.is_link_local:
                raise URLValidationError(f"Blocked private IP: {hostname}")
        except ValueError:
            # Not an IP literal — resolve and check
            try:
                resolved = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC)
                for family, _, _, _, sockaddr in resolved:
                    addr = sockaddr[0]
                    ip = ipaddress.ip_address(addr)
                    if ip.is_private or ip.is_reserved or ip.is_loopback or ip.is_link_local:
                        raise URLValidationError(
                            f"DNS rebinding: {hostname} resolves to private IP {addr}"
                        )
            except socket.gaierror:
                raise URLValidationError(f"Cannot resolve hostname: {hostname}")

        return url


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class PageMetadata:
    """Extracted page metadata."""
    title: str = ""
    description: str = ""
    keywords: List[str] = field(default_factory=list)
    author: str = ""
    published_date: str = ""
    og_image: str = ""
    site_name: str = ""


@dataclass
class MatchingEntity:
    """An entity from entity_registry found in the page text."""
    name: str
    uri: str
    entity_type: str
    match_context: str = ""  # snippet where the entity was found


@dataclass
class WebPreview:
    """Structured result from URL preview."""
    url: str
    rid: str
    domain: str
    title: str
    description: str
    content_text: str
    content_hash: str
    word_count: int
    metadata: PageMetadata
    matching_entities: List[MatchingEntity] = field(default_factory=list)
    fetch_error: Optional[str] = None
    rendered_with: str = "aiohttp"  # "substack_api", "aiohttp", "requests", "playwright", "scrapling"
    raw_html: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "rid": self.rid,
            "domain": self.domain,
            "title": self.title,
            "description": self.description,
            "content_summary": self.content_text[:2000],
            "content_hash": self.content_hash,
            "word_count": self.word_count,
            "rendered_with": self.rendered_with,
            "metadata": {
                "title": self.metadata.title,
                "description": self.metadata.description,
                "keywords": self.metadata.keywords,
                "author": self.metadata.author,
                "published_date": self.metadata.published_date,
                "og_image": self.metadata.og_image,
                "site_name": self.metadata.site_name,
            },
            "matching_entities": [
                {
                    "name": e.name,
                    "uri": e.uri,
                    "type": e.entity_type,
                    "context": e.match_context,
                }
                for e in self.matching_entities
            ],
            "fetch_error": self.fetch_error,
        }


# =============================================================================
# RID Generation
# =============================================================================

def generate_web_rid(url: str) -> str:
    """Generate a KOI-compatible RID for a web page.

    Format: orn:web.page:{domain}/{sha256(url)[:16]}
    Compatible with RegenAI's WebPageRID scheme.
    """
    parsed = urlparse(url)
    domain = parsed.netloc.replace(".", "_").replace(":", "_")
    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return f"orn:web.page:{domain}/{url_hash}"


# =============================================================================
# Content Extraction (ported from RegenAI website_sensor.py)
# =============================================================================

def extract_page_metadata(soup: BeautifulSoup) -> PageMetadata:
    """Extract structured metadata from HTML head."""
    meta = PageMetadata()

    # Title
    title_tag = soup.find("title")
    if title_tag:
        meta.title = title_tag.get_text().strip()

    # Meta tags
    for tag in soup.find_all("meta"):
        name = (tag.get("name") or tag.get("property") or "").lower()
        content = tag.get("content", "")
        if not content:
            continue

        if name in ("description", "og:description"):
            meta.description = meta.description or content
        elif name == "keywords":
            meta.keywords = [k.strip() for k in content.split(",") if k.strip()]
        elif name in ("author", "article:author"):
            meta.author = content
        elif name in ("article:published_time", "date", "dc.date"):
            meta.published_date = content
        elif name == "og:image":
            meta.og_image = content
        elif name == "og:site_name":
            meta.site_name = content

    return meta


def extract_clean_content(soup: BeautifulSoup) -> str:
    """Extract clean text content from HTML.

    Ported from RegenAI website_sensor.py extract_clean_content().
    Finds main content container, strips nav/scripts, deduplicates text.
    """
    # Find main content container
    content_container = None
    for finder in [
        lambda: soup.find("main"),
        lambda: soup.find("article"),
        lambda: soup.find(class_=lambda x: x and "content" in str(x).lower()),
    ]:
        content_container = finder()
        if content_container:
            break

    if not content_container:
        content_container = soup

    # Strip non-content elements
    for tag in content_container(["script", "style", "nav", "footer", "aside", "header"]):
        tag.decompose()

    # Extract text with deduplication (same pattern as sensor)
    seen_texts = set()
    paragraphs = []

    for element in content_container.find_all(
        ["h1", "h2", "h3", "h4", "h5", "h6", "p", "div", "article", "section"]
    ):
        text = element.get_text(separator=" ", strip=True)
        if text and len(text) > 5:
            text = re.sub(r"\s+", " ", text).strip()
            if text not in seen_texts:
                is_subset = any(text in seen for seen in seen_texts)
                if not is_subset:
                    paragraphs.append(text)
                    seen_texts.add(text)

    # List items
    for element in content_container.find_all("li"):
        text = "".join(str(s) for s in element.stripped_strings)
        if text and len(text) > 10:
            text = re.sub(r"\s+", " ", text).strip()
            if text not in seen_texts:
                paragraphs.append(f"- {text}")
                seen_texts.add(text)

    text_content = "\n".join(paragraphs)

    # Collapse multiple blank lines
    text_content = re.sub(r"\n{3,}", "\n\n", text_content).strip()

    # Prepend title if not already present
    title_tag = soup.find("title")
    if title_tag:
        title = title_tag.get_text().strip()
        if title and title not in text_content[:200]:
            text_content = f"# {title}\n\n{text_content}"

    # Enforce size limit
    if len(text_content) > MAX_TEXT_CHARS:
        text_content = text_content[:MAX_TEXT_CHARS] + "\n\n[content truncated]"

    return text_content


def extract_content_trafilatura(html: str, url: str = "") -> Optional[str]:
    """Extract content using trafilatura (higher quality than BS4 for articles).

    Returns extracted text or None if trafilatura unavailable or extraction fails.
    """
    if not TRAFILATURA_AVAILABLE:
        return None
    try:
        text = trafilatura.extract(
            html,
            url=url,
            include_comments=False,
            include_tables=True,
            favor_recall=True,
            deduplicate=True,
        )
        return text
    except Exception as e:
        logger.warning(f"Trafilatura extraction failed: {e}")
        return None


RECALL_MIN_WORDS = int(os.environ.get("RECALL_MIN_WORDS", "600"))  # below this, escalate to nav-inclusive recall extraction


def _extract_recall_fulltext(html: str) -> str:
    """De-noised full text that KEEPS nav/header/footer — the recall fallback for
    component-based SPA/marketing pages.

    Article-readability extractors (trafilatura / BS4 extract_clean_content) strip
    nav/header/footer/aside, which on SPA sites is exactly where the entity-rich
    program names and land-acknowledgements live. This keeps them: it strips only
    script/style/noscript/template, then dedups repeated lines (SPA menus render
    2-3x — desktop/mobile/folder). Verified 2026-07-06 (spa-extraction-fix workflow)
    to lift gold-entity capture to 11/11 ilinationhood, 10/10 hollyhock, 8/8 CDF,
    matching resiliparse with zero new dependency.
    """
    s = BeautifulSoup(html, "html.parser")
    for t in s(["script", "style", "noscript", "template"]):
        t.decompose()
    text = s.get_text("\n", strip=True)
    seen: set = set()
    out = []
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(line)
    return "\n".join(out)


def extract_best_content(html: str, soup: BeautifulSoup, url: str = "") -> str:
    """Extract main content, with a recall fallback for SPA landing pages.

    Normal path: whichever of trafilatura / BS4-clean yields more (precision-biased
    article-readability). Recall fallback: when that best looks under-extracted —
    thin (< RECALL_MIN_WORDS words) OR the nav-inclusive full text is >=1.3x larger
    (readability pruned >=30% of the visible DOM) — return a de-noised full-text
    extraction that keeps nav/footer where SPA entities live. Validated to stay OFF
    on real article pages (a 900-word article keeps the clean trafilatura path).
    """
    bs4_content = extract_clean_content(soup)
    best = bs4_content
    best_words = len(bs4_content.split())

    traf_content = extract_content_trafilatura(html, url)
    if traf_content:
        traf_words = len(traf_content.split())
        if traf_words > best_words:
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text().strip()
                if title and title not in traf_content[:200]:
                    traf_content = f"# {title}\n\n{traf_content}"
            best = traf_content
            best_words = traf_words

    # Recall fallback for under-extracted SPA / component-based pages.
    recall = _extract_recall_fulltext(html)
    recall_words = len(recall.split())
    if recall_words > 0 and (best_words < RECALL_MIN_WORDS or recall_words >= 1.3 * best_words):
        logger.info(
            f"recall fallback for {url}: {recall_words}w (readability best was {best_words}w)"
        )
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text().strip()
            if title and title.lower() not in recall[:200].lower():
                recall = f"# {title}\n\n{recall}"
        return recall

    return best


# =============================================================================
# Cloudflare Detection
# =============================================================================

def _is_cloudflare_challenge(html: str, status_code: int = 200) -> bool:
    """Detect Cloudflare managed challenge / Turnstile page.

    Checks for CF signals on 403/503 responses, or on any status if the page
    title is the telltale "Just a moment..." (catches Playwright-rendered challenge pages).
    """
    cf_signals = [
        "cf-browser-verification",
        "challenge-platform",
        "cdn-cgi/challenge-platform",
        "_cf_chl_opt",
    ]
    if status_code in (403, 503):
        # On error status, any CF signal is sufficient
        if "Just a moment..." in html or any(s in html for s in cf_signals):
            return True
    # On any status, detect the challenge page by title + at least one structural signal
    if "<title>Just a moment...</title>" in html and any(s in html for s in cf_signals):
        return True
    return False


# =============================================================================
# Substack unified-URL resolver (Tier 0 — direct API, no scraping needed)
# =============================================================================
#
# substack.com/@handle/p-ID is a React SPA shell. The publication's own
# api/v1/posts endpoint returns full body_html for the same content.

_SUBSTACK_UNIFIED_RE = re.compile(
    r"https?://substack\.com/@([a-zA-Z0-9_-]+)/p-(\d+)", re.I
)


def _resolve_substack_unified_sync(url: str) -> Optional[str]:
    """Resolve a substack.com/@handle/p-ID URL to full article HTML via the
    publication's JSON API. Returns an HTML string or None on failure.

    The unified URL is a React SPA — server HTML contains only nav/shell.
    The publication subdomain API returns full body_html (typically 3–8K words).
    """
    m = _SUBSTACK_UNIFIED_RE.match(url)
    if not m:
        return None
    handle, post_id_str = m.group(1).lower(), m.group(2)
    post_id = int(post_id_str)
    pub_base = f"https://{handle}.substack.com"

    try:
        import requests as _req
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }

        # Paginate post list to find slug (posts are newest-first; usually 1-2 pages)
        slug = None
        for offset in range(0, 120, 12):
            r = _req.get(
                f"{pub_base}/api/v1/posts?limit=12&offset={offset}",
                headers=headers, timeout=15,
            )
            if r.status_code != 200:
                break
            posts = r.json()
            if not posts:
                break
            for p in posts:
                if p.get("id") == post_id:
                    slug = p.get("slug")
                    break
            if slug:
                break

        if not slug:
            logger.warning(f"Substack resolver: could not find slug for post {post_id} on {handle}")
            return None

        # Fetch full article from detail API
        r = _req.get(f"{pub_base}/api/v1/posts/{slug}", headers=headers, timeout=15)
        if r.status_code != 200:
            logger.warning(f"Substack resolver: detail API returned {r.status_code} for {slug}")
            return None

        data = r.json()
        body_html = data.get("body_html") or data.get("body") or ""
        if not body_html:
            logger.warning(f"Substack resolver: empty body_html for {slug}")
            return None

        title = data.get("title", "")
        subtitle = data.get("subtitle", "")
        author = (data.get("publishedBylines") or [{}])[0].get("name", "")
        pub_date = (data.get("post_date") or "")[:10]

        logger.info(
            f"Substack resolver: fetched '{title}' ({len(body_html.split())} words) "
            f"via {pub_base}/api/v1/posts/{slug}"
        )

        return (
            f"<!DOCTYPE html><html><head>"
            f"<title>{title}</title>"
            f'<meta name="description" content="{subtitle}">'
            f'<meta name="author" content="{author}">'
            f'<meta name="date" content="{pub_date}">'
            f"</head><body>"
            f"<h1>{title}</h1>"
            f"<article>{body_html}</article>"
            f"</body></html>"
        )
    except Exception as e:
        logger.warning(f"Substack resolver failed for {url}: {e}")
        return None


async def _resolve_substack_unified(url: str) -> Optional[str]:
    """Async wrapper — runs the sync resolver in a thread pool."""
    return await asyncio.get_event_loop().run_in_executor(
        None, _resolve_substack_unified_sync, url
    )


# =============================================================================
# Scrapling / StealthyFetcher (Tier 3 — anti-bot bypass)
# =============================================================================

# Optional residential proxy for improved IP reputation
_PROXY_URL = os.environ.get("SCRAPING_PROXY_URL", "")


async def fetch_html_with_scrapling(url: str) -> Optional[str]:
    """Fetch a page using Scrapling's StealthyFetcher (Camoufox) for anti-bot bypass.

    Used as Tier 3 when Cloudflare or similar anti-bot protection is detected.
    Runs synchronously in a thread pool to avoid blocking the event loop.
    """
    if not SCRAPLING_AVAILABLE:
        logger.warning("Scrapling not installed, cannot bypass anti-bot protection")
        return None

    def _fetch_sync():
        kwargs = {
            "headless": True,
            "network_idle": True,
            "block_images": True,
            "solve_cloudflare": True,
            "timeout": SCRAPLING_TIMEOUT * 1000,  # ms
        }
        if _PROXY_URL:
            kwargs["proxy"] = _PROXY_URL

        try:
            page = StealthyFetcher.fetch(url, **kwargs)
            html = page.html_content
            if html and len(html.strip()) > 100:
                return html
            return None
        except Exception as e:
            logger.warning(f"Scrapling fetch failed for {url}: {e}")
            return None

    try:
        html = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _fetch_sync),
            timeout=SCRAPLING_TIMEOUT,
        )
        if html:
            logger.info(f"Scrapling (Camoufox) rendered {len(html)} chars for {url}")
        return html
    except asyncio.TimeoutError:
        logger.warning(f"Scrapling timed out after {SCRAPLING_TIMEOUT}s for {url}")
        return None
    except Exception as e:
        logger.warning(f"Scrapling executor failed for {url}: {e}")
        return None


# =============================================================================
# Entity Scanning
# =============================================================================

async def scan_for_known_entities(
    text: str, db_pool
) -> List[MatchingEntity]:
    """Search extracted text for entity names already in entity_registry.

    Uses case-insensitive word-boundary matching. Returns matches with
    context snippets showing where each entity appears.
    """
    matches = []
    text_lower = text.lower()

    async with db_pool.acquire() as conn:
        # Get all entities with at least 3 chars to avoid noise. Tombstoned
        # (merged-away) rows are excluded on read here, matching the
        # convention used everywhere else this scans/searches rather than
        # resolves-and-follows (e.g. resolve_entity_get's sibling-lookup
        # query) -- a merge keeps the row + aliases + embedding, so an
        # unfiltered scan would keep matching dead entities and returning
        # tombstoned URIs to callers of this function.
        rows = await conn.fetch("""
            SELECT entity_text, fuseki_uri, entity_type
            FROM entity_registry
            WHERE LENGTH(entity_text) >= 3
              AND merged_into IS NULL
            ORDER BY LENGTH(entity_text) DESC
        """)

    for row in rows:
        name = row["entity_text"]
        name_lower = name.lower()

        # Word-boundary match to avoid partial matches
        pattern = re.compile(r"\b" + re.escape(name_lower) + r"\b", re.IGNORECASE)
        match = pattern.search(text)
        if match:
            # Extract context snippet (50 chars before/after)
            start = max(0, match.start() - 50)
            end = min(len(text), match.end() + 50)
            context = text[start:end].strip()
            if start > 0:
                context = "..." + context
            if end < len(text):
                context = context + "..."

            matches.append(MatchingEntity(
                name=name,
                uri=row["fuseki_uri"],
                entity_type=row["entity_type"] or "Unknown",
                match_context=context,
            ))

    return matches


# =============================================================================
# Rate Limiting
# =============================================================================

async def check_rate_limit(
    db_pool,
    submitted_by: Optional[str] = None,
    *,
    _internal_call: bool = False,
) -> Optional[str]:
    """Check rate limits. Returns error message if exceeded, None if OK.

    _internal_call=True bypasses both the global and per-user caps — used by
    multi-page internal jobs (the agentic crawler) that issue many fetches on
    behalf of a single user request.
    """
    if _internal_call:
        return None
    async with db_pool.acquire() as conn:
        # Global rate limit
        global_count = await conn.fetchval("""
            SELECT COUNT(*) FROM web_submissions
            WHERE created_at > NOW() - INTERVAL '1 hour'
        """)
        if global_count >= RATE_LIMIT_GLOBAL_HOUR:
            return f"Global rate limit exceeded ({RATE_LIMIT_GLOBAL_HOUR}/hour)"

        # Per-user rate limit
        if submitted_by:
            user_count = await conn.fetchval("""
                SELECT COUNT(*) FROM web_submissions
                WHERE submitted_by = $1
                AND created_at > NOW() - INTERVAL '1 hour'
            """, submitted_by)
            if user_count >= RATE_LIMIT_PER_USER_HOUR:
                return f"Per-user rate limit exceeded ({RATE_LIMIT_PER_USER_HOUR}/hour)"

    return None


# =============================================================================
# Playwright Rendering (fallback for JS-heavy pages)
# =============================================================================

async def fetch_html_with_playwright(url: str, proxy: Optional[str] = None) -> Optional[str]:
    """Fetch a page using Playwright for JavaScript rendering.

    Launches a headless Chromium browser, navigates to the URL,
    waits for network idle, then extracts rendered HTML.
    Handles shadow DOM by flattening shadow roots into the document.
    Returns None if Playwright is not available or fails.

    If proxy is provided (HTTP URL), routes the browser through it — used as a
    residential-IP fallback for sites that block datacenter IPs at the WAF.
    """
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright not installed, cannot render JS pages")
        return None

    pw = None
    browser = None
    try:
        pw = await async_playwright().start()
        launch_kwargs = {
            "headless": True,
            "args": ["--no-sandbox", "--disable-setuid-sandbox"],
        }
        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}
        browser = await pw.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()

        # SSRF gate: validate every URL the browser is about to request BEFORE
        # the browser opens a socket. URLValidator blocks loopback, RFC1918,
        # metadata, and link-local hosts; a blocked hop is aborted, not fetched.
        async def _ssrf_route_gate(route, request):
            try:
                URLValidator().validate(request.url)
            except URLValidationError as exc:
                logger.warning(
                    f"playwright_route_blocked: {request.url} ({exc})"
                )
                await route.abort()
                return
            await route.continue_()

        await page.route("**", _ssrf_route_gate)

        # Modern sites (analytics / websockets / chat widgets) frequently never
        # reach "networkidle" — a goto that waits for it then times out and we
        # discard the fully-rendered page (0 content). Navigate on the reliable
        # "domcontentloaded" event, then wait for networkidle only BEST-EFFORT
        # (don't fail if it never comes), then settle. (Fix 2026-07-06: Squarespace/
        # Wix/SPA sites were rendering to 0 words because of the strict networkidle.)
        await page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT)
        try:
            await page.wait_for_load_state("networkidle", timeout=3500)  # SPAs rarely idle; short wait (was 8000, usually fully wasted per page)
        except Exception:
            pass  # never idles — DOM is parsed and JS has had time; proceed
        await asyncio.sleep(PLAYWRIGHT_WAIT)

        # Dismiss cookie consent banners (common patterns)
        for selector in [
            'button:has-text("Accept")', 'button:has-text("Accept All")',
            'button:has-text("Got it")', 'button:has-text("I agree")',
            '[id*="cookie"] button', '[class*="cookie"] button',
            '[class*="consent"] button',
        ]:
            try:
                btn = page.locator(selector).first
                if await btn.is_visible(timeout=500):
                    await btn.click()
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                continue

        # Scroll to bottom to trigger lazy-loaded content
        await page.evaluate("""async () => {
            const delay = ms => new Promise(r => setTimeout(r, ms));
            const height = () => document.body.scrollHeight;
            let prev = 0;
            for (let i = 0; i < 5; i++) {
                window.scrollTo(0, height());
                await delay(800);
                if (height() === prev) break;
                prev = height();
            }
            window.scrollTo(0, 0);
        }""")
        await asyncio.sleep(1)

        # First try: get page.content() (works for normal JS-rendered pages)
        html = await page.content()

        # Check if the page uses shadow DOM (web components) —
        # if body.innerText is empty but shadow roots have content,
        # extract text via JS and build a synthetic HTML document.
        body_text = await page.evaluate("() => document.body.innerText || ''")
        if len(body_text.strip()) < 20:
            shadow_text = await page.evaluate("""() => {
                function getAllText(root) {
                    let text = "";
                    if (root.shadowRoot) {
                        text += getAllText(root.shadowRoot);
                    }
                    for (const child of root.childNodes) {
                        if (child.nodeType === Node.TEXT_NODE) {
                            const t = child.textContent.trim();
                            if (t) text += t + " ";
                        } else if (child.nodeType === Node.ELEMENT_NODE) {
                            text += getAllText(child);
                        }
                    }
                    return text;
                }
                return getAllText(document.body);
            }""")
            if len(shadow_text.strip()) > len(body_text.strip()):
                logger.info(
                    f"Shadow DOM detected, extracted {len(shadow_text)} chars "
                    f"from shadow roots for {url}"
                )
                # Get title from the original HTML
                title_match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE)
                title = title_match.group(1) if title_match else ""
                # Build synthetic HTML with the shadow DOM text
                paragraphs = [
                    f"<p>{line.strip()}</p>"
                    for line in shadow_text.split("\n")
                    if line.strip()
                ]
                html = (
                    f"<html><head><title>{title}</title></head>"
                    f"<body>{''.join(paragraphs)}</body></html>"
                )

        await context.close()
        await browser.close()
        await pw.stop()

        logger.info(f"Playwright rendered {len(html)} chars for {url}")
        return html

    except Exception as e:
        logger.warning(f"Playwright fetch failed for {url}: {e}")
        # Ensure cleanup
        try:
            if browser:
                await browser.close()
            if pw:
                await pw.stop()
        except Exception:
            pass
        return None


# =============================================================================
# Main Fetch + Preview
# =============================================================================

MAX_REDIRECT_HOPS = 5


def _absolute_redirect_target(current_url: str, location: str) -> str:
    """Resolve a Location header to an absolute URL."""
    from urllib.parse import urljoin
    return urljoin(current_url, location)


def _validate_hop(next_url: str) -> Optional[str]:
    """Validate a redirect hop. Returns normalized URL or None on SSRF block."""
    try:
        return URLValidator().validate(next_url)
    except URLValidationError as exc:
        logger.warning(f"redirect_blocked: hop URL failed validation: {next_url} ({exc})")
        return None


def _fetch_html_requests_sync(url: str) -> tuple[Optional[str], int]:
    """Sync fallback using requests library. Better TLS fingerprint than aiohttp.

    Used as Tier 1.5 when aiohttp is blocked by Cloudflare or bot-detection that
    aiohttp's TLS handshake triggers. Runs in a thread pool executor.

    Redirects are NOT followed automatically. We read each 3xx Location, validate
    it via URLValidator before issuing the next request, and bail on any failed
    hop — so a redirector cannot coerce us into connecting to a private IP.
    """
    try:
        import requests as _requests
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Dest": "document",
        }
        current = url
        for _ in range(MAX_REDIRECT_HOPS + 1):
            resp = _requests.get(
                current, headers=headers, timeout=FETCH_TIMEOUT, allow_redirects=False
            )
            if 300 <= resp.status_code < 400:
                loc = resp.headers.get("Location")
                if not loc:
                    return None, resp.status_code
                next_url = _absolute_redirect_target(current, loc)
                validated = _validate_hop(next_url)
                if validated is None:
                    return None, resp.status_code
                current = validated
                continue
            if not any(ct in resp.headers.get("Content-Type", "") for ct in ("text/html", "application/xhtml")):
                return None, resp.status_code
            html = resp.content[:MAX_HTML_BYTES].decode("utf-8", errors="replace")
            return html, resp.status_code
        logger.warning(f"requests fallback: too many redirects for {url}")
        return None, 0
    except Exception as e:
        logger.warning(f"requests fallback failed for {url}: {e}")
        return None, 0


async def _fetch_html_aiohttp(url: str) -> tuple[Optional[str], int]:
    """Fetch raw HTML with aiohttp. Returns (HTML string or None, HTTP status code).

    Redirects are NOT auto-followed. We read each 3xx Location, run it through
    URLValidator, then manually issue the follow-up — so a 302 to a private IP
    can never be silently fetched before we see it.
    """
    try:
        timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT)
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Dest": "document",
        }
        async with aiohttp.ClientSession(timeout=timeout) as session:
            current = url
            for _ in range(MAX_REDIRECT_HOPS + 1):
                async with session.get(
                    current,
                    headers=headers,
                    allow_redirects=False,
                ) as response:
                    if 300 <= response.status < 400:
                        loc = response.headers.get("Location")
                        if not loc:
                            return None, response.status
                        next_url = _absolute_redirect_target(current, loc)
                        validated = _validate_hop(next_url)
                        if validated is None:
                            return None, response.status
                        current = validated
                        continue

                    content_type = response.headers.get("Content-Type", "")
                    if not any(ct in content_type for ct in ("text/html", "application/xhtml")):
                        return None, response.status

                    html_bytes = await response.content.read(MAX_HTML_BYTES)
                    html = html_bytes.decode("utf-8", errors="replace")

                    if response.status != 200:
                        return html, response.status  # Return HTML for CF detection

                    return html, response.status
            logger.warning(f"aiohttp fetch: too many redirects for {url}")
            return None, 0

    except Exception as e:
        logger.warning(f"aiohttp fetch failed for {url}: {e}")
        return None, 0


async def fetch_and_preview(
    url: str, db_pool=None, *, _internal_call: bool = False
) -> WebPreview:
    """Fetch a URL and return a structured preview.

    1. Validate URL (SSRF protection)
    2. HTTP fetch with aiohttp
    3. Parse HTML with BeautifulSoup
    4. Extract clean content and metadata
    5. If content is sparse (< 50 words), retry with Playwright
    6. Scan for known entities (if db_pool provided)
    7. Return WebPreview

    _internal_call=True marks this fetch as part of a multi-page internal job
    (the agentic crawler). The flag is forwarded to helpers that need it (e.g.,
    rate-limit checks in the caller). Redirect validation and URL validation
    always run — _internal_call never relaxes SSRF protection.
    """
    _ = _internal_call  # reserved for future internal-path branches
    validator = URLValidator()
    url = validator.validate(url)

    parsed = urlparse(url)
    domain = parsed.netloc
    rid = generate_web_rid(url)

    def _make_error(msg: str) -> WebPreview:
        return WebPreview(
            url=url, rid=rid, domain=domain,
            title="", description="", content_text="",
            content_hash="", word_count=0,
            metadata=PageMetadata(),
            fetch_error=msg,
        )

    # Tier 0: Platform-specific resolvers (bypass scraping entirely)
    if _SUBSTACK_UNIFIED_RE.match(url):
        substack_html = await _resolve_substack_unified(url)
        if substack_html:
            soup = BeautifulSoup(substack_html, "html.parser")
            metadata = extract_page_metadata(soup)
            content_text = extract_best_content(substack_html, soup, url)
            word_count = len(content_text.split())
            content_hash = hashlib.sha256(content_text.encode("utf-8")).hexdigest()
            return WebPreview(
                url=url, rid=rid, domain=domain,
                title=metadata.title, description=metadata.description,
                content_text=content_text, raw_html=substack_html, content_hash=content_hash,
                word_count=word_count, metadata=metadata,
                rendered_with="substack_api",
            )
        logger.info(f"Substack API resolver failed for {url}, falling through to scraping")

    # Step 1: Try aiohttp (fast, lightweight)
    html, status = await _fetch_html_aiohttp(url)
    rendered_with = "aiohttp"
    cloudflare_detected = False

    if html and _is_cloudflare_challenge(html, status):
        cloudflare_detected = True
        logger.info(f"Cloudflare challenge detected for {url} (status={status})")
        html = None  # Force escalation

    if html is None or status != 200:
        # Tier 1.5: requests fallback — better TLS fingerprint than aiohttp, handles
        # sites that block aiohttp's TLS handshake (Substack, some Cloudflare configs).
        logger.info(f"aiohttp {'blocked by CF' if cloudflare_detected else f'failed (status={status})'} for {url}, trying requests fallback")
        req_html, req_status = await asyncio.get_event_loop().run_in_executor(
            None, _fetch_html_requests_sync, url
        )
        if req_html and req_status == 200 and not _is_cloudflare_challenge(req_html, 200):
            html = req_html
            status = req_status
            rendered_with = "requests"
            cloudflare_detected = False
            logger.info(f"requests fallback succeeded for {url}")

    if html is None or status != 200:
        # Tier 2: Playwright — JS rendering for dynamic content (unless CF detected)
        if not cloudflare_detected and PLAYWRIGHT_AVAILABLE:
            logger.info(f"requests fallback failed for {url} (status={status}), trying Playwright")
            html = await fetch_html_with_playwright(url)
            rendered_with = "playwright"

            # Check if Playwright also got a Cloudflare page
            if html and _is_cloudflare_challenge(html, 200):
                cloudflare_detected = True
                logger.info(f"Playwright also hit Cloudflare challenge for {url}")
                html = None

            # Detect non-CF block (WAF "Access Denied", thin server-403 pages).
            # Retry via residential proxy if configured — many small-host WAFs
            # blocklist datacenter IPs but accept residential traffic.
            elif html and len(html) < 500 and _PROXY_URL:
                logger.info(
                    f"Playwright returned thin content ({len(html)} chars) for {url}; "
                    f"retrying via residential proxy"
                )
                prox_html = await fetch_html_with_playwright(url, proxy=_PROXY_URL)
                if prox_html and len(prox_html) > len(html):
                    html = prox_html
                    rendered_with = "playwright+proxy"
                else:
                    html = None  # Force escalation

        # If still no content (or Cloudflare detected), try Scrapling
        if html is None and SCRAPLING_AVAILABLE:
            logger.info(
                f"Escalating to Scrapling (Camoufox) for {url}"
                + (" [Cloudflare detected]" if cloudflare_detected else "")
            )
            html = await fetch_html_with_scrapling(url)
            rendered_with = "scrapling"

            # Check if Scrapling also returned a Cloudflare challenge page.
            # Use status=200 (strict mode) — requires <title>Just a moment...</title>
            # AND a CF signal. The loose 403-mode false-triggers on CDN URLs
            # (e.g. cdn-cgi/challenge-platform) present in normal pages.
            if html and _is_cloudflare_challenge(html, 200):
                cloudflare_detected = True
                logger.info(f"Scrapling also returned Cloudflare challenge for {url}")
                html = None

        if html is None:
            error_msg = "Failed to fetch URL"
            if cloudflare_detected:
                error_msg = (
                    "Site is behind Cloudflare protection that could not be bypassed. "
                    "A residential proxy (SCRAPING_PROXY_URL) may help."
                )
            return _make_error(error_msg)

    soup = BeautifulSoup(html, "html.parser")
    metadata = extract_page_metadata(soup)
    content_text = extract_best_content(html, soup, url)
    word_count = len(content_text.split())

    # Step 2: If content is sparse, retry with better rendering
    if word_count < MIN_WORD_COUNT and rendered_with in ("aiohttp", "requests"):
        if PLAYWRIGHT_AVAILABLE:
            logger.info(
                f"Sparse content ({word_count} words) from aiohttp, "
                f"retrying {url} with Playwright"
            )
            pw_html = await fetch_html_with_playwright(url)
            if pw_html:
                pw_soup = BeautifulSoup(pw_html, "html.parser")
                pw_metadata = extract_page_metadata(pw_soup)
                pw_content = extract_best_content(pw_html, pw_soup, url)
                pw_word_count = len(pw_content.split())

                if pw_word_count > word_count:
                    logger.info(
                        f"Playwright got {pw_word_count} words vs aiohttp's {word_count}"
                    )
                    html = pw_html  # carry the rendered DOM into raw_html so the
                    # crawler sees JS-rendered <a> links (SPA nav) + full content
                    soup = pw_soup
                    metadata = pw_metadata
                    content_text = pw_content
                    word_count = pw_word_count
                    rendered_with = "playwright"

        # If still sparse after Playwright, try Scrapling
        if word_count < MIN_WORD_COUNT and SCRAPLING_AVAILABLE and rendered_with != "scrapling":
            logger.info(f"Still sparse ({word_count} words), trying Scrapling for {url}")
            scr_html = await fetch_html_with_scrapling(url)
            if scr_html and not _is_cloudflare_challenge(scr_html, 403):
                scr_soup = BeautifulSoup(scr_html, "html.parser")
                scr_content = extract_best_content(scr_html, scr_soup, url)
                scr_word_count = len(scr_content.split())
                if scr_word_count > word_count:
                    logger.info(f"Scrapling got {scr_word_count} words vs {word_count}")
                    html = scr_html  # carry rendered DOM into raw_html (links + content)
                    soup = scr_soup
                    metadata = extract_page_metadata(scr_soup)
                    content_text = scr_content
                    word_count = scr_word_count
                    rendered_with = "scrapling"

    content_hash = hashlib.sha256(content_text.encode("utf-8")).hexdigest()

    # Scan for known entities
    matching_entities = []
    if db_pool and content_text:
        matching_entities = await scan_for_known_entities(content_text, db_pool)

    return WebPreview(
        url=url,
        rid=rid,
        domain=domain,
        title=metadata.title or metadata.site_name or domain,
        description=metadata.description,
        content_text=content_text,
        raw_html=html,
        content_hash=content_hash,
        word_count=word_count,
        metadata=metadata,
        matching_entities=matching_entities,
        rendered_with=rendered_with,
    )
