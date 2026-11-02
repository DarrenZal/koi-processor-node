"""
B9a — Retrieval executor functions.

Each executor wraps a retrieval operation from the monolithic chat_endpoint
and returns list[EvidenceBundle]. These are the building blocks that both
the legacy monolithic path and the planner path dispatch to.

See: api/schemas/query_plan.py for EvidenceBundle, RetrievalOp, SourceType.
See: docs/specs/b9a-query-plan-spec.md for decision matrix.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, List, Optional

import asyncpg

from api.schemas.query_plan import EvidenceBundle, RetrievalOp, SourceType

logger = logging.getLogger(__name__)

# Entity types excluded from /chat entity_lookup to reduce noise.
# These are governance/project-management artifacts that dilute results
# for knowledge questions about ecology, organizations, and mechanisms.
#
# 2026-08-15: Document and Event added. Both are UNREGISTERED types produced by
# finished one-shot backfills, and both are things the entity graph should not be
# answering knowledge questions with:
#
#   Document (240 rows, johar-corpus-intake-v1 + knowledge-add) is Substack post and
#   essay TITLES. 227 of them exact-title-match a row that already exists in
#   koi_memories, so /chat can already reach that content through the document
#   surface; reaching it a second time as an entity just returns a headline where a
#   fact belongs. Zero of the 240 carry a vault_rid — they are graph-only.
#
#   Event (150 rows, extract-session-entities) is ~34 real gatherings, 25 BARE DATE
#   STRINGS like "2026-04-05", and ~28 fragments of plan scaffolding. A bare date
#   embedded in an ANN matches a great many unrelated queries.
#
# 390 of 27,122 embedded entities, so ~1.4% of the candidate pool.
#
# This is a RETRIEVAL decision, not an ontology one, and deliberately not a data
# change. The rows stay: entity_relationships carries ON DELETE CASCADE on both
# subject and object, so deleting the 240 Documents would silently destroy 598
# relationship rows including 371 sourced_from edges that are the sole provenance of
# the Claims citing them. Excluding a type from one ANN is reversible with git
# revert; a cascade is not reversible at all.
#
# None of the three is registered in the vault Ontology, and that is also deliberate
# rather than pending: entity_type is a RESOLUTION KEY, not a semantic label (see the
# type_aliases comment in Ontology/schema-concept.md). Registering a type that shares
# a folder with an existing one is what partitioned GitHub, Signal, Otter and six
# others across duplicate rows.
# Tombstones are still embedded, so without this they compete in the ANN as
# duplicates of the very rows they were merged into. /entities/merge tombstones
# via merged_into rather than deleting, and deliberately leaves embedding_3072
# intact, so every merge ever performed has been adding a decoy here.
#
# Measured 2026-08-16: 202 tombstones in the /chat candidate pool, 167 of them
# sharing a normalized_text with a live, also-embedded survivor. "Polis" alone
# has two dead rows (Concept and Project) both merged into the live "Pol.is", so
# the model is handed one entity three times and can cite the dead fuseki_uri.
# There is no downstream dedup: merged_into appeared ZERO times in this file.
#
# 25 other query sites in api/ already filter it. This one did not, which is why
# the type-exclusion work in 278f527 sat directly on top of a larger contamination
# than the one it removed.
LIVE_FILTER = "AND merged_into IS NULL"
#
# Module level, not a local: _keyword_entity_search is a separate function and the
# first version of this fix left the name unresolvable there. It is the FALLBACK
# path, reached only when vector search has already failed, so a NameError would
# have surfaced only during an already-degraded state and turned a soft failure
# into a hard one.
CHAT_EXCLUDE_TYPES = [
    "Claim", "WorkItem", "Milestone", "Metric", "Risk", "Initiative", "Decision",
    "Document", "Event",
]


# ---------------------------------------------------------------------------
# Live executors
# ---------------------------------------------------------------------------

async def entity_lookup(
    query: str,
    query_embedding: list[float] | None,
    conn: asyncpg.Connection,
    *,
    max_results: int = 5,
    include_node_private: bool = False,
    exclude_types: list[str] | None = None,
    quartz_url_fn: Callable[[str, str], str | None] | None = None,
) -> list[EvidenceBundle]:
    """Semantic + keyword search on entity_registry.

    Wraps the entity search block from chat_endpoint (lines 5196-5273).
    Returns EvidenceBundles with source_type=LOCAL_AUTHORITATIVE.
    """
    privacy_filter = "" if include_node_private else "AND NOT node_private"


    # Build type exclusion filter with dynamic param offsets
    type_filter = ""
    type_params: list[Any] = []
    if exclude_types:
        type_params = list(exclude_types)
        # Placeholder offsets computed per-path below
    rows: list = []

    if query_embedding:
        embedding_str = '[' + ','.join(str(x) for x in query_embedding) + ']'
        # Vector path base params: $1=embedding, $2=max_results
        if type_params:
            placeholders = ", ".join(f"${i}" for i in range(3, 3 + len(type_params)))
            type_filter = f"AND entity_type NOT IN ({placeholders})"
        try:
            # Reads from embedding_3072 (post-2026-04-23 OpenAI 3072-dim migration);
            # halfvec cast required because pgvector full-precision vector ANN caps
            # at 2000 dims. Uses idx_entity_registry_embedding_3072_hnsw.
            rows = await conn.fetch(f"""
                SELECT fuseki_uri, entity_text, entity_type, metadata,
                       1 - (embedding_3072::halfvec(3072)
                            <=> $1::halfvec(3072)) AS similarity
                FROM entity_registry
                WHERE embedding_3072 IS NOT NULL {privacy_filter} {LIVE_FILTER} {type_filter}
                ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                LIMIT $2
            """, embedding_str, max_results, *type_params)
        except (asyncpg.exceptions.UndefinedColumnError,
                asyncpg.exceptions.UndefinedFunctionError,
                asyncpg.exceptions.DataError) as e:
            logger.warning("entity_registry vector search failed, falling back to text: %s", e)
            rows = await _keyword_entity_search(query, conn, max_results, privacy_filter, exclude_types)
    else:
        rows = await _keyword_entity_search(query, conn, max_results, privacy_filter, exclude_types)

    bundles: list[EvidenceBundle] = []
    for row in rows:
        meta = row['metadata'] or {}
        if isinstance(meta, str):
            meta = json.loads(meta)
        description = (meta.get('description') or meta.get('context') or '') if isinstance(meta, dict) else ''
        quartz = quartz_url_fn(row['entity_type'], row['entity_text']) if quartz_url_fn else None
        bundle_meta: dict[str, Any] = {
            "entity_type": row['entity_type'],
            "label": row['entity_text'],
            "fuseki_uri": row['fuseki_uri'],
        }
        if quartz:
            bundle_meta["quartz_url"] = quartz
        bundles.append(EvidenceBundle(
            source_uri=row['fuseki_uri'],
            source_type=SourceType.LOCAL_AUTHORITATIVE,
            retrieval_op=RetrievalOp.ENTITY_LOOKUP,
            confidence=round(float(row['similarity']), 4),
            text=description,
            metadata=bundle_meta,
        ))
    return bundles


async def _keyword_entity_search(
    query: str,
    conn: asyncpg.Connection,
    max_results: int,
    privacy_filter: str,
    exclude_types: list[str] | None = None,
) -> list:
    """Keyword fallback for entity lookup when vector search is unavailable."""
    words = [w for w in query.lower().split() if len(w) >= 3]
    if not words:
        return []
    conditions = " OR ".join(f"normalized_text ILIKE ${i+1}" for i in range(len(words)))
    match_score = " + ".join(
        f"CASE WHEN normalized_text ILIKE ${i+1} THEN 1 ELSE 0 END"
        for i in range(len(words))
    )
    params: list[Any] = [f"%{w}%" for w in words]
    params.append(max_results)
    # Keyword path base params: $1..$N=words, $N+1=max_results
    type_filter = ""
    if exclude_types:
        base_offset = len(words) + 2  # after words ($1..$N) and max_results ($N+1)
        placeholders = ", ".join(f"${base_offset + i}" for i in range(len(exclude_types)))
        type_filter = f"AND entity_type NOT IN ({placeholders})"
        params.extend(exclude_types)
    return await conn.fetch(f"""
        SELECT fuseki_uri, entity_text, entity_type, metadata,
               ({match_score})::float / {len(words)} AS similarity
        FROM entity_registry
        WHERE ({conditions}) {privacy_filter} {LIVE_FILTER} {type_filter}
        ORDER BY ({match_score}) DESC, created_at DESC
        LIMIT ${len(words)+1}
    """, *params)


async def relationship_traverse(
    entity_uris: list[str],
    conn: asyncpg.Connection,
    *,
    max_hops: int = 2,
    max_results: int = 50,
) -> list[EvidenceBundle]:
    """N-hop recursive CTE on entity_relationships.

    Wraps the relationship expansion block from chat_endpoint (lines 5278-5315).
    Returns EvidenceBundles with source_type=LOCAL_AUTHORITATIVE.
    Each bundle's text is the formatted edge string.

    Note: node_private filtering is hardcoded (v0). Phase 4 will add
    include_node_private param when PolicyScope is threaded through.
    """
    if not entity_uris:
        return []

    rel_rows = await conn.fetch("""
        WITH RECURSIVE traverse AS (
            SELECT
                r.subject_uri, r.object_uri, r.predicate,
                1 AS depth
            FROM entity_relationships r
            WHERE r.subject_uri = ANY($1) OR r.object_uri = ANY($1)
            UNION
            SELECT
                r2.subject_uri, r2.object_uri, r2.predicate,
                t.depth + 1
            FROM traverse t
            JOIN entity_relationships r2
                ON r2.subject_uri IN (t.subject_uri, t.object_uri)
                OR r2.object_uri IN (t.subject_uri, t.object_uri)
            WHERE t.depth < $2
        )
        SELECT DISTINCT ON (t.subject_uri, t.predicate, t.object_uri)
            t.subject_uri,
            s.entity_text AS subject_label,
            t.predicate,
            t.object_uri,
            o.entity_text AS object_label,
            t.depth
        FROM traverse t
        LEFT JOIN entity_registry s ON s.fuseki_uri = t.subject_uri
        LEFT JOIN entity_registry o ON o.fuseki_uri = t.object_uri
        WHERE NOT COALESCE(s.node_private, false)
          AND NOT COALESCE(o.node_private, false)
        ORDER BY t.subject_uri, t.predicate, t.object_uri, t.depth
        LIMIT $3
    """, entity_uris, max_hops, max_results)

    bundles: list[EvidenceBundle] = []
    for rr in rel_rows:
        subj = rr['subject_label'] or rr['subject_uri']
        obj = rr['object_label'] or rr['object_uri']
        edge_text = f"{subj} --[{rr['predicate']}]--> {obj}"
        bundles.append(EvidenceBundle(
            source_uri=f"{rr['subject_uri']}:{rr['predicate']}:{rr['object_uri']}",
            source_type=SourceType.LOCAL_AUTHORITATIVE,
            retrieval_op=RetrievalOp.RELATIONSHIP_TRAVERSE,
            confidence=1.0 / rr['depth'],  # closer hops = higher confidence
            text=edge_text,
            metadata={
                "subject_uri": rr['subject_uri'],
                "subject_label": subj,
                "predicate": rr['predicate'],
                "object_uri": rr['object_uri'],
                "object_label": obj,
                "depth": rr['depth'],
            },
        ))
    return bundles


async def text_search(
    query: str,
    query_embedding: list[float] | None,
    conn: asyncpg.Connection,
    *,
    multi_query: bool = False,
    include_code: bool = False,
    top_k: int = 8,
    max_rrf_candidates: int = 20,
    generate_embedding_fn: Callable | None = None,
    expand_queries_fn: Callable | None = None,
    rerank_fn: Callable | None = None,
    fields: list[str] | None = None,
) -> list[EvidenceBundle]:
    """Hybrid BM25+vector RRF fusion + FlashRank reranking.

    Wraps the chunk retrieval block from chat_endpoint (lines 5326-5456).
    Returns EvidenceBundles with source_type=LOCAL_DOCUMENT.

    Option B field scoping: when `fields` is a non-empty list, results are scoped
    to documents that are members of those learning fields (via
    document_field_membership). When None/empty (default), behavior is UNCHANGED —
    retrieval is globally visible across all documents.
    """
    code_filter = "" if include_code else "AND c.content->>'entity_name' IS NULL"

    # Option B: optional field-membership scoping. The main hybrid query binds
    # $1=embedding, $2=query_text, so fields go in $3; the BM25-only fallback binds
    # $1=query_text, so fields go in $2. When fields is empty these are no-ops and
    # no extra parameter is passed (fully backward compatible).
    if fields:
        field_join_main = (
            "JOIN document_field_membership fm "
            "ON fm.document_rid = c.document_rid AND fm.field_id = ANY($3::text[])"
        )
        field_join_bm25 = (
            "JOIN document_field_membership fm "
            "ON fm.document_rid = c.document_rid AND fm.field_id = ANY($2::text[])"
        )
        field_args = [list(fields)]
    else:
        field_join_main = ""
        field_join_bm25 = ""
        field_args = []

    # B8b: determine query variants
    if multi_query and expand_queries_fn:
        query_variants = await expand_queries_fn(query, n=3)
        if len(query_variants) > 1:
            logger.info(f"B8b multi-query: {len(query_variants)} variants")
    else:
        query_variants = [query]

    # Accumulator for cross-query RRF fusion
    all_chunk_rows: Dict[str, dict] = {}

    for q_idx, q_text in enumerate(query_variants):
        # Get embedding for this variant
        if q_idx == 0:
            q_embedding = query_embedding
        elif generate_embedding_fn:
            q_embedding = await generate_embedding_fn(q_text)
        else:
            q_embedding = None

        q_embedding_str = '[' + ','.join(str(x) for x in q_embedding) + ']' if q_embedding else None

        if q_embedding_str:
            try:
                # Reads from embedding_3072 (post-2026-04-23 OpenAI 3072-dim
                # migration); halfvec cast required because pgvector full-precision
                # vector ANN caps at 2000 dims. Uses
                # idx_koi_memory_chunks_embedding_3072_hnsw.
                chunk_rows = await conn.fetch(f"""
                    WITH vector_results AS (
                        SELECT c.id, c.document_rid,
                               m.content->>'title' AS title,
                               LEFT(c.content->>'text', 2000) AS chunk_text,
                               c.content->>'context' AS chunk_context,
                               c.content->>'section_id' AS section_id,
                               c.content->>'section_title' AS section_title,
                               c.content->>'wiki_url' AS wiki_url,
                               ROW_NUMBER() OVER (
                                   ORDER BY c.embedding_3072::halfvec(3072)
                                            <=> $1::halfvec(3072)
                               ) AS vrank
                        FROM koi_memory_chunks c
                        JOIN koi_memories m ON m.rid = c.document_rid
                        {field_join_main}
                        WHERE c.embedding_3072 IS NOT NULL {code_filter}
                        ORDER BY c.embedding_3072::halfvec(3072)
                                 <=> $1::halfvec(3072) LIMIT 40
                    ),
                    bm25_results AS (
                        SELECT c.id, c.document_rid,
                               m.content->>'title' AS title,
                               LEFT(c.content->>'text', 2000) AS chunk_text,
                               c.content->>'context' AS chunk_context,
                               c.content->>'section_id' AS section_id,
                               c.content->>'section_title' AS section_title,
                               c.content->>'wiki_url' AS wiki_url,
                               ROW_NUMBER() OVER (ORDER BY ts_rank_cd(c.tsv, plainto_tsquery('english', $2)) DESC) AS brank
                        FROM koi_memory_chunks c
                        JOIN koi_memories m ON m.rid = c.document_rid
                        {field_join_main}
                        WHERE c.tsv @@ plainto_tsquery('english', $2) {code_filter}
                        ORDER BY ts_rank_cd(c.tsv, plainto_tsquery('english', $2)) DESC LIMIT 40
                    )
                    SELECT COALESCE(v.id, b.id) AS id,
                           COALESCE(v.document_rid, b.document_rid) AS document_rid,
                           COALESCE(v.title, b.title) AS title,
                           LEFT(COALESCE(v.chunk_text, b.chunk_text), 2000) AS chunk_text,
                           COALESCE(v.chunk_context, b.chunk_context) AS chunk_context,
                           COALESCE(v.section_id, b.section_id) AS section_id,
                           COALESCE(v.section_title, b.section_title) AS section_title,
                           COALESCE(v.wiki_url, b.wiki_url) AS wiki_url,
                           COALESCE(1.0/(v.vrank+60), 0) + COALESCE(1.0/(b.brank+60), 0) AS rrf_score
                    FROM vector_results v
                    FULL OUTER JOIN bm25_results b ON v.id = b.id
                    ORDER BY rrf_score DESC
                    LIMIT {max_rrf_candidates}
                """, q_embedding_str, q_text, *field_args)
                for cr in chunk_rows:
                    dedup_key = str(cr['id']) if cr['id'] is not None else cr['document_rid']
                    if dedup_key in all_chunk_rows:
                        all_chunk_rows[dedup_key]['rrf_score'] += cr['rrf_score']
                    else:
                        all_chunk_rows[dedup_key] = dict(cr)
            except (asyncpg.exceptions.UndefinedTableError,
                    asyncpg.exceptions.UndefinedColumnError):
                pass  # koi_memory_chunks not available
            except asyncpg.exceptions.DataError as e:
                logger.warning(f"Vector dimension mismatch (variant {q_idx}), BM25-only fallback: {e}")
                try:
                    chunk_rows = await conn.fetch(f"""
                        WITH bm25_only AS (
                            SELECT c.id, c.document_rid,
                                   m.content->>'title' AS title,
                                   LEFT(c.content->>'text', 2000) AS chunk_text,
                                   c.content->>'context' AS chunk_context,
                                   c.content->>'section_id' AS section_id,
                                   c.content->>'section_title' AS section_title,
                                   c.content->>'wiki_url' AS wiki_url,
                                   ROW_NUMBER() OVER (
                                       ORDER BY ts_rank_cd(c.tsv, plainto_tsquery('english', $1)) DESC
                                   ) AS brank
                            FROM koi_memory_chunks c
                            JOIN koi_memories m ON m.rid = c.document_rid
                            {field_join_bm25}
                            WHERE c.tsv @@ plainto_tsquery('english', $1) {code_filter}
                        )
                        SELECT id, document_rid, title, chunk_text, chunk_context,
                               section_id, section_title, wiki_url,
                               -- Same RRF formula (k=60) as the primary hybrid path
                               -- above, so this fallback's contribution stays on the
                               -- same ~0-0.0164 scale instead of an unbounded raw
                               -- ts_rank_cd value that would otherwise dominate the
                               -- fused ranking whenever this fallback fires.
                               1.0/(brank+60) AS rrf_score
                        FROM bm25_only
                        ORDER BY brank
                        LIMIT {max_rrf_candidates}
                    """, q_text, *field_args)
                    for cr in chunk_rows:
                        dedup_key = str(cr['id']) if cr.get('id') is not None else cr['document_rid']
                        if dedup_key in all_chunk_rows:
                            all_chunk_rows[dedup_key]['rrf_score'] += cr['rrf_score']
                        else:
                            all_chunk_rows[dedup_key] = dict(cr)
                except Exception as bm25_err:
                    logger.warning(f"BM25 fallback also failed (variant {q_idx}): {bm25_err}")

    # Sort fused results, take top candidates
    sorted_chunks = sorted(all_chunk_rows.values(), key=lambda r: r['rrf_score'], reverse=True)[:max_rrf_candidates]

    # Build doc_chunks dicts for reranking (legacy format expected by _rerank_chunks)
    doc_chunks = []
    for cr in sorted_chunks:
        ctx = cr.get('chunk_context') or ""
        doc_chunks.append({
            "id": cr.get('id'),
            "rid": cr['document_rid'],
            "title": cr.get('title') or cr['document_rid'],
            "text": cr.get('chunk_text') or "",
            "context": ctx,
            "score": round(float(cr['rrf_score']), 4),
            "section_id": cr.get('section_id'),
            "section_title": cr.get('section_title'),
            "wiki_url": cr.get('wiki_url'),
        })

    # B7: Rerank top candidates -> top_k
    if doc_chunks and rerank_fn:
        doc_chunks = rerank_fn(query, doc_chunks, top_k=top_k)

    # Convert to EvidenceBundles
    bundles: list[EvidenceBundle] = []
    for d in doc_chunks:
        bundles.append(EvidenceBundle(
            source_uri=d['rid'],
            source_type=SourceType.LOCAL_DOCUMENT,
            retrieval_op=RetrievalOp.TEXT_SEARCH,
            confidence=d.get('rerank_score', d.get('score', 0.0)),
            text=d.get('text', ''),
            metadata={
                "title": d.get('title', ''),
                "context": d.get('context', ''),
                "section_id": d.get('section_id'),
                "section_title": d.get('section_title'),
                "wiki_url": d.get('wiki_url'),
            },
        ))
    return bundles


async def _expand_page_context(
    reranked_chunks: list[dict],
    conn: asyncpg.Connection,
    max_expansions: int = 2,
    max_extra_chunks: int = 3,
) -> list[dict]:
    """Promote page co-occurrence: when 2+ chunks from the same wiki page
    appear in reranked results, fetch adjacent sections for fuller context.
    Only applies to mediawiki-sensor chunks (identified by wiki_url presence
    and source_sensor join)."""
    from collections import Counter

    # Group by document_rid for wiki chunks only
    page_counts: Counter = Counter(
        d['rid'] for d in reranked_chunks
        if d.get('wiki_url')
    )
    co_occurring = [(rid, cnt) for rid, cnt in page_counts.items() if cnt >= 2]
    if not co_occurring:
        return reranked_chunks

    co_occurring.sort(key=lambda x: -x[1])

    existing_ids = {d.get('id') for d in reranked_chunks if d.get('id') is not None}
    extra_chunks: list[dict] = []

    for doc_rid, _ in co_occurring[:max_expansions]:
        # Collect existing chunk IDs for this page to exclude
        page_existing_ids = [
            d['id'] for d in reranked_chunks
            if d.get('rid') == doc_rid and d.get('id') is not None
        ]
        rows = await conn.fetch("""
            SELECT c.id, c.document_rid,
                   m.content->>'title' AS title,
                   LEFT(c.content->>'text', 2000) AS chunk_text,
                   c.content->>'context' AS chunk_context,
                   c.content->>'section_id' AS section_id,
                   c.content->>'section_title' AS section_title,
                   c.content->>'wiki_url' AS wiki_url
            FROM koi_memory_chunks c
            JOIN koi_memories m ON m.rid = c.document_rid
            WHERE c.document_rid = $1
              AND m.source_sensor LIKE 'mediawiki%'
              AND c.id != ALL($2::int[])
            ORDER BY COALESCE((c.content->>'chunk_index')::int, 999999)
            LIMIT $3
        """, doc_rid, page_existing_ids, max_extra_chunks)

        for row in rows:
            if row['id'] in existing_ids:
                continue
            existing_ids.add(row['id'])
            extra_chunks.append({
                "id": row['id'],
                "rid": row['document_rid'],
                "title": row.get('title') or row['document_rid'],
                "text": row.get('chunk_text') or "",
                "context": row.get('chunk_context') or "",
                "score": 0.01,
                "section_id": row.get('section_id'),
                "section_title": row.get('section_title'),
                "wiki_url": row.get('wiki_url'),
                "_page_expanded": True,
            })

    if extra_chunks:
        logger.info(f"Page expansion: {len(extra_chunks)} chunks from {len(co_occurring[:max_expansions])} pages")

    return reranked_chunks + extra_chunks


async def web_source_lookup(
    entity_uris: list[str],
    conn: asyncpg.Connection,
) -> list[EvidenceBundle]:
    """Web submissions linked to matched entities.

    Wraps the web source block from chat_endpoint (lines 5462-5492).
    Returns EvidenceBundles with source_type=LOCAL_WEB.
    """
    if not entity_uris:
        return []

    try:
        ws_rows = await conn.fetch("""
            SELECT DISTINCT ON (ws.url)
                ws.url,
                ws.title,
                ws.description
            FROM web_submissions ws
            JOIN document_entity_links del
                ON del.document_rid = 'web:' || ws.rid::text
            WHERE del.entity_uri = ANY($1)
              AND ws.status IN ('ingested', 'monitoring')
            LIMIT 5
        """, entity_uris)
    except (asyncpg.exceptions.UndefinedTableError,
            asyncpg.exceptions.UndefinedColumnError):
        return []

    bundles: list[EvidenceBundle] = []
    for wr in ws_rows:
        desc = wr['description'] or ""
        bundles.append(EvidenceBundle(
            source_uri=wr['url'],
            source_type=SourceType.LOCAL_WEB,
            retrieval_op=RetrievalOp.ENTITY_LOOKUP,  # web sources are entity-linked
            confidence=0.8,
            text=desc,
            metadata={
                "url": wr['url'],
                "title": wr['title'] or wr['url'],
                "summary": desc[:200],
            },
        ))
    return bundles


# ---------------------------------------------------------------------------
# Stubs (log + return empty)
# ---------------------------------------------------------------------------

async def graph_query(**kwargs) -> list[EvidenceBundle]:
    """B9c stub — Cypher via AGE. Not implemented in B9a."""
    logger.debug("graph_query stub called — returning empty (B9c not implemented)")
    return []


async def structured_sql(
    query: str,
    conn: asyncpg.Connection,
    *,
    template: str = "commitment",
    entity_uris: list[str] | None = None,
    max_results: int = 20,
) -> list[EvidenceBundle]:
    """B9b — Deterministic SQL templates for structured data retrieval.

    Uses allowlisted query templates with parameterized entity_uri filtering.
    Never generates freeform SQL from user queries.

    Templates:
        commitment: commitments + claims tables (filtered by pledger/pool/claimant)
        roadmap: entity_registry roadmap types (Initiative, WorkItem, Milestone, etc.)
    """
    # Cap unfiltered results to avoid noisy global dumps
    effective_max = min(max_results, 5) if not entity_uris else max_results
    uri_param = entity_uris if entity_uris else None

    if template == "commitment":
        return await _structured_sql_commitment(conn, uri_param, effective_max)
    elif template == "roadmap":
        return await _structured_sql_roadmap(conn, uri_param, effective_max)
    else:
        logger.warning(f"structured_sql: unknown template '{template}', returning empty")
        return []


async def _structured_sql_commitment(
    conn: asyncpg.Connection,
    entity_uris: list[str] | None,
    max_results: int,
) -> list[EvidenceBundle]:
    """Commitment + claim template: queries commitments and claims tables."""
    bundles: list[EvidenceBundle] = []

    # Sub-query 1: Commitments
    try:
        commitment_rows = await conn.fetch("""
            SELECT c.commitment_rid, c.title, c.description, c.offer_type,
                   c.state::text AS state, c.quantity, c.unit,
                   cp.name AS pool_name, cp.state::text AS pool_state, cp.bioregion_uri
            FROM commitments c
            LEFT JOIN commitment_pools cp ON cp.id = c.pool_id
            WHERE ($1::text[] IS NULL OR c.pledger_uri = ANY($1)
                   OR cp.steward_uri = ANY($1) OR cp.bioregion_uri = ANY($1))
            ORDER BY c.updated_at DESC
            LIMIT $2
        """, entity_uris, max_results)

        for row in commitment_rows:
            title = row['title'] or 'Untitled commitment'
            offer_type = row['offer_type'] or 'unknown'
            state = row['state'] or 'unknown'
            pool_name = row['pool_name'] or 'no pool'
            desc = (row['description'] or '')[:200]
            text = f"Commitment: {title} ({offer_type}, {state}) in pool {pool_name} — {desc}"
            bundles.append(EvidenceBundle(
                source_uri=row['commitment_rid'],
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.STRUCTURED_SQL,
                confidence=0.9,
                text=text,
                metadata={
                    "label": title,
                    "entity_type": "Commitment",
                    "template": "commitment",
                    "state": state,
                    "pool_name": pool_name,
                },
            ))
    except Exception as e:
        logger.warning(f"structured_sql commitment query failed: {e}")

    # Sub-query 2: Claims
    try:
        claim_rows = await conn.fetch("""
            SELECT cl.claim_rid, cl.claim_type, cl.verification, cl.statement,
                   cl.claimant_uri
            FROM claims cl
            WHERE ($1::text[] IS NULL OR cl.claimant_uri = ANY($1)
                   OR cl.entity_uri = ANY($1))
            ORDER BY cl.created_at DESC
            LIMIT $2
        """, entity_uris, max_results)

        for row in claim_rows:
            claim_type = row['claim_type'] or 'unknown'
            verification = row['verification'] or 'unverified'
            statement = row['statement'] or ''
            text = f"Claim ({claim_type}, {verification}): {statement[:300]}"
            bundles.append(EvidenceBundle(
                source_uri=row['claim_rid'],
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.STRUCTURED_SQL,
                confidence=0.85,
                text=text,
                metadata={
                    "label": statement[:80] if statement else "Untitled claim",
                    "entity_type": "Claim",
                    "template": "commitment",
                    "verification": verification,
                },
            ))
    except Exception as e:
        logger.warning(f"structured_sql claims query failed: {e}")

    return bundles


async def _structured_sql_roadmap(
    conn: asyncpg.Connection,
    entity_uris: list[str] | None,
    max_results: int,
) -> list[EvidenceBundle]:
    """Roadmap template: queries entity_registry for roadmap entity types."""
    bundles: list[EvidenceBundle] = []

    try:
        rows = await conn.fetch("""
            SELECT er.fuseki_uri, er.entity_text, er.entity_type, er.description,
                   er.metadata, er.last_seen_at
            FROM entity_registry er
            WHERE er.entity_type IN ('Initiative', 'WorkItem', 'Milestone', 'Outcome',
                                     'Decision', 'Metric', 'Risk')
              -- same reason as the ANN above: a merged WorkItem is not a work item
              AND er.merged_into IS NULL
              AND (er.node_private IS NULL OR er.node_private = false)
              AND ($1::text[] IS NULL OR er.fuseki_uri = ANY($1))
            ORDER BY er.last_seen_at DESC NULLS LAST
            LIMIT $2
        """, entity_uris, max_results)

        for row in rows:
            entity_text = row['entity_text'] or 'Untitled'
            entity_type = row['entity_type'] or 'WorkItem'
            desc = (row['description'] or '')[:300]
            text = f"{entity_type}: {entity_text} — {desc}"
            bundles.append(EvidenceBundle(
                source_uri=row['fuseki_uri'],
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.STRUCTURED_SQL,
                confidence=0.9,
                text=text,
                metadata={
                    "label": entity_text,
                    "entity_type": entity_type,
                    "template": "roadmap",
                },
            ))
    except Exception as e:
        logger.warning(f"structured_sql roadmap query failed: {e}")

    return bundles


async def peer_query(**kwargs) -> list[EvidenceBundle]:
    """B12 stub — Federation fan-out. Not implemented in B9a."""
    logger.debug("peer_query stub called — returning empty (B12 not implemented)")
    return []


# ---------------------------------------------------------------------------
# Legacy adapter
# ---------------------------------------------------------------------------

def evidence_bundles_to_legacy_format(
    all_bundles: list[EvidenceBundle],
) -> tuple[list[dict], list[str], list[dict], list[dict]]:
    """Partition a flat EvidenceBundle list into the legacy 4-tuple expected
    by the prompt builder in chat_endpoint.

    Returns:
        (sources, relationships_ctx, doc_chunks, web_sources)

    Partitioning rules:
        - retrieval_op=ENTITY_LOOKUP + source_type=LOCAL_AUTHORITATIVE -> sources
        - retrieval_op=RELATIONSHIP_TRAVERSE -> relationships_ctx (formatted strings)
        - retrieval_op=TEXT_SEARCH + source_type=LOCAL_DOCUMENT -> doc_chunks
        - source_type=LOCAL_WEB -> web_sources (also added to sources)
    """
    sources: list[dict] = []
    relationships_ctx: list[str] = []
    doc_chunks: list[dict] = []
    web_sources: list[dict] = []

    for b in all_bundles:
        if b.retrieval_op == RetrievalOp.RELATIONSHIP_TRAVERSE:
            relationships_ctx.append(b.text)

        elif b.source_type == SourceType.LOCAL_WEB:
            web_sources.append({
                "url": b.metadata.get("url", b.source_uri),
                "title": b.metadata.get("title", b.source_uri),
                "summary": b.metadata.get("summary", b.text[:200]),
            })
            sources.append({
                "uri": b.source_uri,
                "label": b.metadata.get("title", b.source_uri),
                "entity_type": "WebSource",
                "score": b.confidence,
                "description": b.text[:200],
            })

        elif b.retrieval_op == RetrievalOp.TEXT_SEARCH and b.source_type == SourceType.LOCAL_DOCUMENT:
            doc_chunks.append({
                "rid": b.source_uri,
                "title": b.metadata.get("title", b.source_uri),
                "text": b.text,
                "context": b.metadata.get("context", ""),
                "score": b.confidence,
                "section_id": b.metadata.get("section_id"),
                "section_title": b.metadata.get("section_title"),
                "wiki_url": b.metadata.get("wiki_url"),
            })
            ctx = b.metadata.get("context", "")
            sources.append({
                "uri": b.source_uri,
                "label": b.metadata.get("title", b.source_uri),
                "entity_type": "Document",
                "score": b.confidence,
                "description": ((ctx + " " if ctx else "") + b.text)[:400],
                "url": b.metadata.get("wiki_url"),
            })

        elif b.retrieval_op == RetrievalOp.STRUCTURED_SQL and b.source_type == SourceType.LOCAL_AUTHORITATIVE:
            sources.append({
                "uri": b.source_uri,
                "label": b.metadata.get("label", ""),
                "entity_type": b.metadata.get("entity_type", ""),
                "score": b.confidence,
                "description": b.text,
            })

        elif b.retrieval_op == RetrievalOp.ENTITY_LOOKUP and b.source_type == SourceType.LOCAL_AUTHORITATIVE:
            source_dict: dict[str, Any] = {
                "uri": b.source_uri,
                "label": b.metadata.get("label", ""),
                "entity_type": b.metadata.get("entity_type", ""),
                "score": b.confidence,
                "description": b.text,
            }
            if b.metadata.get("quartz_url"):
                source_dict["quartz_url"] = b.metadata["quartz_url"]
            sources.append(source_dict)

    return sources, relationships_ctx, doc_chunks, web_sources
