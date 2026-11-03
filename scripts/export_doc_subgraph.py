#!/usr/bin/env python3
"""
export_doc_subgraph.py — Per-document knowledge + discourse subgraph exporter.

For each matched document (by source_rid set OR title regex) emits, into an
output directory:
  - <slug>.jsonld   : re-importable JSON-LD subgraph (orn:/fuseki URIs verbatim)
  - <slug>.md       : human-readable companion (frontmatter + tables + move tree)

Three extracts per doc (column names verified against information_schema of a
personal_koi database):

  1. Entities : document_entity_links del
                JOIN entity_registry er ON er.fuseki_uri = del.entity_uri
                WHERE del.document_rid = <rid>
                -> @id=fuseki_uri, name=entity_text, type=entity_type,
                   aliases, mention_count
  2. Facts    : knowledge_facts WHERE source_node_rid = <rid>
                -> s=subject_uri, p=predicate, o=object_uri||object_literal,
                   fact_text, valid_from, valid_to
  3. Discourse: session_discourse_moves
                WHERE source_rid = <rid> AND source_type = 'document'
                -> nodes (@id=id, type=move_type, title, detail, status)
                   edges (move.id -> resolves_move_id, rel='resolves')
                   when resolves_move_id IS NOT NULL

Doc metadata (title/tier/url/date/series) comes from document_ingestion_log
and knowledge_facts.group_id.

Usage:
  python3 export_doc_subgraph.py --out DIR --rid document:abc[,document:def]
  python3 export_doc_subgraph.py --out DIR --title-regex 'opinion dynamics|discourse sheaves'

Connection: POSTGRES_URL env (after sourcing config/personal.env), or --dsn.
"""
import argparse
import json
import os
import re
import sys
from datetime import date, datetime

import psycopg2
import psycopg2.extras

DEFAULT_DSN = "postgresql://koi@localhost:5432/personal_koi"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def slugify(text: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", (text or "").lower()).strip("-")
    return s or "untitled"


def jdefault(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def iso(v):
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)


# --------------------------------------------------------------------------- #
# extracts
# --------------------------------------------------------------------------- #
def fetch_doc_meta(cur, rid):
    cur.execute(
        """
        SELECT document_rid, title, tier, source_url, source_path,
               char_count, last_ingested_at, deep_extracted_at
          FROM document_ingestion_log
         WHERE document_rid = %s
        """,
        (rid,),
    )
    row = cur.fetchone()
    # group_id (acts as corpus/series tag) lives on the facts
    cur.execute(
        "SELECT DISTINCT group_id FROM knowledge_facts "
        "WHERE source_node_rid = %s AND group_id IS NOT NULL LIMIT 1",
        (rid,),
    )
    grp = cur.fetchone()
    group_id = grp["group_id"] if grp else None
    if not row:
        return {"document_rid": rid, "title": rid, "tier": None,
                "source_url": None, "source_path": None, "char_count": None,
                "last_ingested_at": None, "group_id": group_id}
    d = dict(row)
    d["group_id"] = group_id
    return d


def fetch_entities(cur, rid):
    cur.execute(
        """
        SELECT er.fuseki_uri, er.entity_text, er.entity_type,
               er.aliases, del.mention_count
          FROM document_entity_links del
          JOIN entity_registry er ON er.fuseki_uri = del.entity_uri
         WHERE del.document_rid = %s
         ORDER BY del.mention_count DESC NULLS LAST, er.entity_text
        """,
        (rid,),
    )
    out = []
    for r in cur.fetchall():
        out.append({
            "@id": r["fuseki_uri"],
            "name": r["entity_text"],
            "type": r["entity_type"],
            "aliases": list(r["aliases"]) if r["aliases"] else [],
            "mention_count": r["mention_count"],
        })
    return out


def fetch_facts(cur, rid):
    cur.execute(
        """
        SELECT subject_uri, predicate, object_uri, object_literal,
               fact_text, valid_from, valid_to
          FROM knowledge_facts
         WHERE source_node_rid = %s
           AND valid_to IS NULL          -- current-only (drop superseded re-extractions)
         ORDER BY created_at
        """,
        (rid,),
    )
    out = []
    for r in cur.fetchall():
        obj = r["object_uri"] if r["object_uri"] else r["object_literal"]
        out.append({
            "s": r["subject_uri"],
            "p": r["predicate"],
            "o": obj,
            "object_is_uri": bool(r["object_uri"]),
            "fact_text": r["fact_text"],
            "valid_from": iso(r["valid_from"]),
            "valid_to": iso(r["valid_to"]),
        })
    return out


def fetch_discourse(cur, rid):
    cur.execute(
        """
        SELECT id, move_type, title, detail, status, resolves_move_id,
               turn_range_start, turn_range_end, created_at
          FROM session_discourse_moves
         WHERE source_rid = %s AND source_type = 'document'
         ORDER BY created_at
        """,
        (rid,),
    )
    nodes, edges = [], []
    for r in cur.fetchall():
        mid = str(r["id"])
        nodes.append({
            "@id": mid,
            "type": r["move_type"],
            "title": r["title"],
            "detail": r["detail"],
            "status": r["status"],
        })
        if r["resolves_move_id"] is not None:
            edges.append({
                "from": mid,
                "to": str(r["resolves_move_id"]),
                "rel": "resolves",
            })
    return {"nodes": nodes, "edges": edges}


# --------------------------------------------------------------------------- #
# rendering
# --------------------------------------------------------------------------- #
def build_jsonld(meta, entities, facts, discourse):
    return {
        "@context": {
            "orn": "orn:personal-koi.entity:",
            "name": "http://schema.org/name",
            "type": "@type",
        },
        "@id": meta["document_rid"],
        "title": meta["title"],
        "tier": meta["tier"],
        "source_url": meta.get("source_url"),
        "series": meta.get("group_id"),
        "exported_at": datetime.now().isoformat(),
        "counts": {
            "entities": len(entities),
            "facts": len(facts),
            "discourse_nodes": len(discourse["nodes"]),
            "discourse_edges": len(discourse["edges"]),
        },
        "entities": entities,
        "facts": facts,
        "discourse": discourse,
    }


def _md_escape(s):
    if s is None:
        return ""
    return str(s).replace("|", "\\|").replace("\n", " ").strip()


def _yaml_q(s):
    if s is None:
        return '""'
    return '"' + str(s).replace('"', '\\"') + '"'


def build_markdown(meta, entities, facts, discourse, top_n=15):
    title = meta["title"]
    series = meta.get("group_id") or "sheaf-corpus"
    dt = meta.get("last_ingested_at")
    date_str = iso(dt)[:10] if dt else iso(datetime.now())[:10]
    url = meta.get("source_url") or ""

    # description: prefer the first thesis/definition move detail; else generic
    desc = None
    pref = {"thesis": 0, "definition": 1, "claim": 2}
    cand = sorted(
        [n for n in discourse["nodes"] if n.get("detail")],
        key=lambda n: pref.get(n.get("type"), 9),
    )
    if cand:
        desc = cand[0]["detail"]
    if not desc:
        desc = f"Knowledge + discourse subgraph extracted for '{title}'."
    desc = desc.strip()
    if len(desc) > 280:
        desc = desc[:277] + "..."

    # tags: top concept entity names + series
    tags = []
    for e in entities:
        if (e.get("type") or "").lower() == "concept" and e.get("name"):
            tags.append(slugify(e["name"]))
        if len(tags) >= 6:
            break

    lines = []
    lines.append("---")
    lines.append(f"title: {_yaml_q(title)}")
    lines.append(f"description: {_yaml_q(desc)}")
    lines.append(f"date: {date_str}")
    lines.append(f"series: {_yaml_q(series)}")
    lines.append(f"tier: {_yaml_q(meta.get('tier'))}")
    if url:
        lines.append(f"source_url: {_yaml_q(url)}")
    lines.append(f"document_rid: {_yaml_q(meta['document_rid'])}")
    lines.append("tags:")
    for t in (tags or [slugify(series)]):
        lines.append(f"  - {t}")
    lines.append("show: true")
    lines.append("---")
    lines.append("")
    lines.append(f"# {title}")
    lines.append("")
    lines.append(desc)
    lines.append("")
    lines.append(
        f"*{len(entities)} entities · {len(facts)} facts · "
        f"{len(discourse['nodes'])} discourse moves "
        f"({len(discourse['edges'])} resolution edges). "
        f"Source: [{url}]({url})*" if url else
        f"*{len(entities)} entities · {len(facts)} facts · "
        f"{len(discourse['nodes'])} discourse moves "
        f"({len(discourse['edges'])} resolution edges).*"
    )
    lines.append("")

    # Top entities table
    lines.append(f"## Top entities (of {len(entities)})")
    lines.append("")
    lines.append("| Entity | Type | Mentions | Aliases |")
    lines.append("|---|---|---|---|")
    for e in entities[:top_n]:
        aliases = ", ".join(e.get("aliases") or [])
        lines.append(
            f"| {_md_escape(e['name'])} | {_md_escape(e['type'])} | "
            f"{e.get('mention_count') if e.get('mention_count') is not None else ''} | "
            f"{_md_escape(aliases)} |"
        )
    lines.append("")

    # Facts table
    lines.append(f"## Facts (of {len(facts)})")
    lines.append("")
    lines.append("| Subject | Predicate | Object | Statement |")
    lines.append("|---|---|---|---|")

    def short_uri(u):
        if not u:
            return ""
        return u.split(":")[-1] if u.startswith("orn:") else u

    for f in facts[:top_n]:
        lines.append(
            f"| {_md_escape(short_uri(f['s']))} | {_md_escape(f['p'])} | "
            f"{_md_escape(short_uri(f['o']) if f['object_is_uri'] else f['o'])} | "
            f"{_md_escape(f['fact_text'])} |"
        )
    if len(facts) > top_n:
        lines.append(f"| … | | | _{len(facts) - top_n} more facts in the JSON-LD_ |")
    lines.append("")

    # Discourse moves as a nested list (resolution tree)
    lines.append(f"## Discourse moves ({len(discourse['nodes'])})")
    lines.append("")
    by_id = {n["@id"]: n for n in discourse["nodes"]}
    children = {}
    for ed in discourse["edges"]:
        children.setdefault(ed["to"], []).append(ed["from"])
    roots = [n["@id"] for n in discourse["nodes"]
             if not any(e["from"] == n["@id"] for e in discourse["edges"])]

    def render_move(mid, depth):
        n = by_id[mid]
        indent = "  " * depth
        head = n.get("title") or (n.get("detail") or "")[:80]
        lines.append(
            f"{indent}- **[{n['type']}]** ({n['status']}) {_md_escape(head)}"
        )
        for child in children.get(mid, []):
            # child resolves this node
            render_move(child, depth + 1)

    for r in roots:
        render_move(r, 0)
    # any orphan children not reachable from roots (defensive)
    rendered = set()

    lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def resolve_rids(cur, rids, title_regex):
    if rids:
        return list(rids)
    cur.execute(
        "SELECT document_rid, title FROM document_ingestion_log "
        "WHERE title ~* %s ORDER BY title",
        (title_regex,),
    )
    return [r["document_rid"] for r in cur.fetchall()]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True, help="output directory")
    ap.add_argument("--rid", help="comma-separated document_rid(s)")
    ap.add_argument("--title-regex", help="POSIX regex matched against title (~*)")
    ap.add_argument("--dsn", default=os.environ.get("POSTGRES_URL", DEFAULT_DSN))
    ap.add_argument("--top-n", type=int, default=15,
                    help="rows shown in markdown tables (default 15)")
    args = ap.parse_args()

    if not args.rid and not args.title_regex:
        ap.error("provide --rid or --title-regex")

    rids = [r.strip() for r in args.rid.split(",")] if args.rid else None
    os.makedirs(args.out, exist_ok=True)

    conn = psycopg2.connect(args.dsn)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    written = []
    with conn.cursor() as cur:
        targets = resolve_rids(cur, rids, args.title_regex)
        if not targets:
            print("No documents matched.", file=sys.stderr)
            sys.exit(2)
        for rid in targets:
            meta = fetch_doc_meta(cur, rid)
            entities = fetch_entities(cur, rid)
            facts = fetch_facts(cur, rid)
            discourse = fetch_discourse(cur, rid)
            slug = slugify(meta["title"])

            jsonld = build_jsonld(meta, entities, facts, discourse)
            md = build_markdown(meta, entities, facts, discourse, top_n=args.top_n)

            jpath = os.path.join(args.out, f"{slug}.jsonld")
            mpath = os.path.join(args.out, f"{slug}.md")
            with open(jpath, "w") as fh:
                json.dump(jsonld, fh, indent=2, default=jdefault, ensure_ascii=False)
            with open(mpath, "w") as fh:
                fh.write(md)
            written.append((rid, jpath, mpath, len(entities), len(facts),
                            len(discourse["nodes"]), len(discourse["edges"])))
            print(f"[ok] {meta['title']}")
            print(f"     rid={rid}")
            print(f"     entities={len(entities)} facts={len(facts)} "
                  f"moves={len(discourse['nodes'])} edges={len(discourse['edges'])}")
            print(f"     -> {jpath}")
            print(f"     -> {mpath}")
    conn.close()
    return written


if __name__ == "__main__":
    main()
