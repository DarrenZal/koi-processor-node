#!/usr/bin/env python3
"""Export Proton/Gmail ICS-derived calendar events to a single .ics file.

Reads koi_memories rows where source_sensor='ics-event' and emits an RFC 5545
calendar at ~/.calendars/proton.ics (default), suitable for read-only
consumption by Full Calendar (Obsidian) via a localhost HTTP feed.

Usage:
    python3 export_proton_ics.py [--out PATH] [--include-cancelled]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

DEFAULT_OUT = Path.home() / ".calendars" / "proton.ics"
DEFAULT_DSN = os.environ.get(
    "PERSONAL_KOI_DSN",
    "postgresql://koi@localhost:5432/personal_koi",
)


def fold(line: str) -> str:
    """RFC 5545 line folding: ≤75 octets per line, continuation begins with space.

    Splits on UTF-8 character boundaries so multi-byte chars aren't broken.
    """
    encoded = line.encode("utf-8")
    if len(encoded) <= 75:
        return line

    chunks: list[bytes] = []
    buf = bytearray()
    limit = 75
    for ch in line:
        ch_bytes = ch.encode("utf-8")
        if len(buf) + len(ch_bytes) > limit:
            chunks.append(bytes(buf))
            buf = bytearray()
            limit = 74  # continuation lines reserve 1 byte for leading space
        buf.extend(ch_bytes)
    if buf:
        chunks.append(bytes(buf))
    return "\r\n ".join(c.decode("utf-8") for c in chunks)


def escape_text(value: str | None) -> str:
    if not value:
        return ""
    return (
        value.replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\n", "\\n")
    )


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_utc(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def sanitize_uid(uid: str | None, rid: str) -> str:
    if uid:
        cleaned = re.sub(r"[\r\n]", "", uid).strip()
        if cleaned:
            return cleaned
    return f"{rid}@personal-koi"


def build_vevent(row: dict) -> str | None:
    metadata = row["metadata"] or {}
    content = row["content"] or {}

    dtstart = parse_iso_utc(metadata.get("dtstart"))
    dtend = parse_iso_utc(metadata.get("dtend"))
    if dtstart is None:
        return None
    if dtend is None:
        dtend = dtstart

    dtstamp = parse_iso_utc(metadata.get("dtstamp")) or datetime.now(timezone.utc)
    summary = content.get("title") or "(untitled event)"
    location = metadata.get("location") or ""
    organizer = metadata.get("organizer") or ""
    attendees = metadata.get("attendees") or []
    description = content.get("text") or ""
    status = (metadata.get("status") or "").upper()

    lines: list[str] = ["BEGIN:VEVENT"]
    lines.append(f"UID:{sanitize_uid(metadata.get('uid'), row['rid'])}")
    lines.append(f"DTSTAMP:{fmt_utc(dtstamp)}")
    lines.append(f"DTSTART:{fmt_utc(dtstart)}")
    lines.append(f"DTEND:{fmt_utc(dtend)}")
    lines.append(f"SUMMARY:{escape_text(summary)}")
    if location:
        lines.append(f"LOCATION:{escape_text(location)}")
    # DESCRIPTION intentionally omitted — Proton/Gmail-derived events have
    # noisy auto-generated descriptions (HTML, redundant attendee dumps) that
    # break some ICS parsers and add no info beyond SUMMARY + LOCATION.
    if organizer:
        lines.append(f"ORGANIZER:mailto:{organizer}")
    for a in attendees:
        if isinstance(a, str) and a:
            lines.append(f"ATTENDEE:mailto:{a}")
    if status in {"CONFIRMED", "TENTATIVE", "CANCELLED"}:
        lines.append(f"STATUS:{status}")
    lines.append("END:VEVENT")
    return "\r\n".join(fold(line) for line in lines)


def fetch_events(dsn: str, include_cancelled: bool) -> list[dict]:
    sql = """
        SELECT rid, content, metadata
        FROM koi_memories
        WHERE source_sensor = 'ics-event'
          AND superseded_at IS NULL
        ORDER BY (metadata->>'dtstart') NULLS LAST
    """
    with psycopg2.connect(dsn) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    if not include_cancelled:
        rows = [
            r
            for r in rows
            if (r["metadata"] or {}).get("status") != "CANCELLED"
        ]
    return rows


def build_calendar(rows: list[dict]) -> str:
    header = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//personal-koi//proton-export//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:Proton (via personal-koi)",
    ]
    body: list[str] = []
    for row in rows:
        vevent = build_vevent(row)
        if vevent:
            body.append(vevent)
    footer = ["END:VCALENDAR", ""]
    return "\r\n".join(header + body + footer)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--include-cancelled", action="store_true")
    args = ap.parse_args()

    rows = fetch_events(args.dsn, args.include_cancelled)
    ics = build_calendar(rows)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.out.with_suffix(args.out.suffix + ".tmp")
    tmp.write_text(ics, encoding="utf-8")
    tmp.replace(args.out)

    print(f"wrote {len(rows)} events to {args.out}", file=sys.stderr)

    # Apply window filter to keep parsers happy on long-running calendars.
    filter_script = Path.home() / ".calendars" / "filter_ics.py"
    if filter_script.exists():
        import subprocess

        filtered = args.out.with_name(args.out.stem + "-filtered.ics")
        r = subprocess.run(
            ["python3", str(filter_script), str(args.out), str(filtered)],
            capture_output=True,
            text=True,
        )
        if r.returncode == 0:
            filtered.replace(args.out)
            if r.stderr:
                print(r.stderr.strip(), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
