#!/usr/bin/env python3
"""Bridge: full-content paid Substack posts from Gmail (IMAP) → the
substack-corpus ingestion pipeline.

Why this exists
---------------
`substack_sensor.py` drives off Substack's public JSON API with no auth, so it
*skips* paid posts (audience != everyone) and could only get paywalled previews
anyway. But paid posts arrive **in full** in the operator's inbox (they are a
paid subscriber). This bridge reads those post-emails over IMAP, extracts the
full article text, and feeds them through the SAME path the rest of the corpus
uses — `ingest_substack_corpus.py` — so paid posts land under the identical
`substack-corpus:<feed>:<slug>` RID scheme, 3072-dim OpenAI embeddings, and
author entity-linking, and are then picked up by the existing daily deep-extract
job. It deliberately does NOT go through the generic email sensor (that would
1024-dim BGE-embed them under an email RID and duplicate the corpus).

Auth: a Gmail app password read from a file (default ~/.gmail-app-password,
override with SUBSTACK_GMAIL_PASS_FILE). Headless-safe (no browser, no OAuth), so
it works under a scheduler.

Idempotent: ingest_substack_corpus.py dedups by canonical slug (ON CONFLICT DO
NOTHING), so re-running only adds new posts.

Usage:
    # dry-run: search + parse + report (no DB writes)
    python3 scripts/ingest_substack_from_gmail.py

    # backfill everything ever received from these publications
    python3 scripts/ingest_substack_from_gmail.py --apply

    # daily incremental (server-side SINCE filter — fast)
    python3 scripts/ingest_substack_from_gmail.py --apply --since-days 3
"""

import argparse
import email
import imaplib
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from email import policy
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional

IMAP_HOST = "imap.gmail.com"
IMAP_PASS_FILE = os.path.expanduser(os.environ.get("SUBSTACK_GMAIL_PASS_FILE", "~/.gmail-app-password"))
IMAP_MAILBOX = '"[Gmail]/All Mail"'          # search across all mail, not just INBOX

REPO_ROOT = Path(__file__).resolve().parent.parent            # .../koi-processor
INGEST_SCRIPT = REPO_ROOT / "scripts" / "ingest_substack_corpus.py"
PYTHON = sys.executable
BACKFILL_SINCE = "01-Jan-2020"                # generous floor for full backfill

# WHICH publications + which Gmail inbox is personal config, loaded from
# config/substack_publications.yaml (see substack_config). Only publications
# with an `email_sender` participate in the Gmail bridge. Not hardcoded so forks
# configure their own.
from substack_config import load_publications, gmail_user  # noqa: E402

# "View this post on the web at https://<host>/p/<slug>" — the marker that an
# email is an actual POST (notes-digests and "new notes" emails lack it).
_VIEW_RE = re.compile(r"View this post on the web at\s+(https://[^\s<>]+/p/[^\s<>?]+)", re.I)
# Substack wraps links as "text [ https://substack.com/redirect/... ]" in the
# text/plain part — strip the bracketed redirect noise, keep the anchor text.
_REDIRECT_BRACKET_RE = re.compile(r"\s*\[\s*https://substack\.com/redirect/[^\]]+\]")
_UNSUB_RE = re.compile(r"\n\s*Unsubscribe\s+https://\S+", re.I)
_BOILERPLATE_RE = re.compile(
    r"^.*is a reader-supported publication.*consider becoming a (free or paid|paid) subscriber\.?\s*$",
    re.I | re.M,
)


def _decode_body(msg: email.message.Message) -> str:
    """Prefer the text/plain part; fall back to stripped text/html."""
    plain, html = None, None
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain" and plain is None:
                plain = part.get_content()
            elif ctype == "text/html" and html is None:
                html = part.get_content()
    elif msg.get_content_type() == "text/html":
        html = msg.get_content()
    else:
        plain = msg.get_content()
    if plain:
        return plain
    if html:
        return re.sub(r"<[^>]+>", " ", html)
    return ""


def _clean_article(body: str) -> str:
    """Isolate the article text: drop everything up to the View-this-post line,
    cut the Unsubscribe footer, strip redirect-bracket + boilerplate noise."""
    m = _VIEW_RE.search(body)
    if m:
        body = body[m.end():]
    body = _UNSUB_RE.split(body)[0]
    body = _REDIRECT_BRACKET_RE.sub("", body)
    body = _BOILERPLATE_RE.sub("", body)
    body = body.replace("\r\n", "\n")
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body.strip()


def _parse_msg(msg: email.message.Message, host: str) -> Optional[Dict]:
    raw = _decode_body(msg)
    m = _VIEW_RE.search(raw)
    if not m:
        return None                       # not a post (notes digest, etc.)
    url = m.group(1)
    if host not in url:
        return None                       # cross-posted / wrong publication
    content = _clean_article(raw)
    if len(content) < 400:
        return None                       # preview/stub, not a full post
    subject = str(msg.get("Subject", "")).strip()
    try:
        date_iso = parsedate_to_datetime(msg.get("Date")).isoformat()
    except Exception:
        date_iso = None
    return {
        "url": url.rstrip("?/"),
        "title": subject,
        "subtitle": "",
        "date": date_iso,
        "full_content": content,
    }


def _imap_connect(user: str) -> imaplib.IMAP4_SSL:
    with open(IMAP_PASS_FILE) as f:
        pw = f.read().strip()
    M = imaplib.IMAP4_SSL(IMAP_HOST)
    M.login(user, pw)
    M.select(IMAP_MAILBOX, readonly=True)
    return M


def harvest(M: imaplib.IMAP4_SSL, pub: Dict, since: str) -> List[Dict]:
    typ, data = M.search(None, '(FROM "%s" SINCE %s)' % (pub["sender"], since))
    if typ != "OK":
        return []
    ids = data[0].split()
    by_slug: Dict[str, Dict] = {}
    for mid in ids:
        t, md = M.fetch(mid, "(RFC822)")
        if t != "OK" or not md or not md[0]:
            continue
        try:
            msg = email.message_from_bytes(md[0][1], policy=policy.default)
        except Exception:
            continue
        post = _parse_msg(msg, pub["host"])
        if not post:
            continue
        slug = post["url"].split("/p/")[-1]
        if slug not in by_slug or len(post["full_content"]) > len(by_slug[slug]["full_content"]):
            by_slug[slug] = post
    return list(by_slug.values())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually ingest (default: dry-run)")
    ap.add_argument("--since-days", type=int, default=None,
                    help="only messages newer than N days (fast daily mode); default = full backfill")
    ap.add_argument("--out-dir", default="/tmp/substack-from-gmail")
    ap.add_argument("--only", default=None, help="restrict to one feed_slug")
    args = ap.parse_args()

    if args.since_days is not None:
        since = (date.today() - timedelta(days=args.since_days)).strftime("%d-%b-%Y")
    else:
        since = BACKFILL_SINCE

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    user = gmail_user()
    if not user:
        print("ERROR: no gmail_user in substack publications config", file=sys.stderr)
        return 1
    # Only publications with an email_sender participate in the Gmail bridge.
    pubs = {p["feed_slug"]: p for p in load_publications() if p.get("email_sender")}
    if not pubs:
        print("No publications with an email_sender configured — nothing to do.")
        return 0

    try:
        M = _imap_connect(user)
    except Exception as e:
        print(f"ERROR: IMAP connect/login failed: {e}", file=sys.stderr)
        return 1

    grand = 0
    ingest_failures = 0
    try:
        for feed_slug, pub in pubs.items():
            if args.only and feed_slug != args.only:
                continue
            posts = harvest(M, {**pub, "sender": pub["email_sender"]}, since)
            print(f"[{feed_slug}] {len(posts)} full post(s) from Gmail since {since}")
            if not posts:
                continue
            corpus = {
                "author": pub["author"],
                "substack_url": f"https://{pub['host']}",
                "publication_name": pub["author"],
                "scraped_at": None,
                "total_posts": len(posts),
                "note": "harvested from Gmail (paid full-content post emails, via IMAP)",
                "posts": posts,
            }
            corpus_path = out_dir / f"{feed_slug}_from_gmail.json"
            with open(corpus_path, "w") as f:
                json.dump(corpus, f)
            grand += len(posts)
            if not args.apply:
                print(f"    dry-run: wrote {corpus_path} ({len(posts)} posts). Re-run with --apply.")
                continue
            cmd = [
                PYTHON, str(INGEST_SCRIPT),
                "--corpus-path", str(corpus_path),
                "--feed-slug", feed_slug,
                "--substack-domain", pub["host"],
                "--author", pub["author"],
                "--domain", pub["domain"],
                "--tags", ",".join(pub["tags"]) if isinstance(pub["tags"], list) else str(pub["tags"]),
            ]
            print(f"    ingesting → {feed_slug} ({len(posts)} posts)")
            rc = subprocess.run(cmd, cwd=str(REPO_ROOT)).returncode
            if rc != 0:
                print(f"    WARNING: ingest exited {rc} for {feed_slug}", file=sys.stderr)
                ingest_failures += 1
    finally:
        try:
            M.logout()
        except Exception:
            pass
    if ingest_failures:
        print(f"DONE WITH FAILURES: {grand} post(s) processed across publications, "
              f"{ingest_failures} ingest subprocess failure(s) — see WARNING lines above",
              file=sys.stderr)
        return 1
    print(f"DONE: {grand} post(s) processed across publications")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
