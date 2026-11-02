"""Cross-source discourse link candidates: discovery, review, retrieval.

Stage 1 prototype for gaiaaiagent/koi-processor#60. Nothing in the service
imports this module or mounts its router. Its tables come from
migrations/133_discourse_link_candidates.sql, applied on personal_koi for the
2026-10-19 candidate-only canary. Discovery is off by default, and judgments
are disabled outside scratch databases unless KOI_DISCOURSE_JUDGMENTS=1.

THE GAP THIS ADDRESSES
----------------------
Thorough document ingest extracts discourse moves per source. Their only
relation, resolves_move_id, is resolved by exact title against the same
document's moves, so every live value points inside its own source. Nothing at
ingest time looks at other sources' moves.

WHO MAY SEE WHAT (A1)
---------------------
Every read, report and discovery takes a CallerContext. Its visibility
predicate runs inside the SQL of each query, before ranking, fusion, caps,
counts and manifest entries, so hidden material neither appears nor moves a
visible item's rank or a count. Only the personal context sees sources in no
field and private sources. Field membership grants nothing by itself. Wiki
material of the held Restoration/P2P pilot (mediawiki rids) is excluded from
every context.

WHAT A CLAIM IS BOUND TO (A2)
-----------------------------
A move's representation is REPR_SQL, a hash of its type, title, detail and
status computed in SQL only. The text behind every recorded hash is kept in
discourse_claim_representations. A move's binding says how it is tied to the
source version it was extracted from:
  recorded_extraction       written by the extraction run, with the version it
                            observed around reading its input (association)
  content_addressed_source  the rid is document:<sha256>: source attribution,
                            not extraction provenance
  inferred_timestamp        legacy Substack, inferred from timestamps
  unknown
Only recorded_extraction with association captured_with_input can be admitted.

WHO MAY JUDGE (Q2, D2)
----------------------
Judgments need a minted AdmissionAuthority. The router mints one only for a
caller presenting the privileged local admin credential (basis
privileged-local-credential). That is local-credential authorization, not
proof that Darren personally approved anything. fixture-replay authority works
only in a scratch database. Agents may propose labels, which are
caller-declared and never counted.

CORRECTION (A3)
---------------
Judgments are append-only. Withdrawal and correction are review events.

RUNS
----
A canary run records every row it creates or changes in
discourse_link_run_effects, inside the same savepoint as the change, and stops
at its approved write limits (RunRecorder). Rollback replays only those rows.

TWO-PHASE DISCOVERY (O2)
------------------------
plan_discovery computes vectors and ranks candidates in a READ ONLY
transaction and returns a plan: every endpoint's representation and binding,
the caller context, embedding profile, method, scores, caps and proposed
effects. apply_plan writes that plan in a short transaction after locking and
rechecking everything it names, and refuses on any difference. Ranking is the
same code as discover_candidates; only where the vectors come from differs.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, FrozenSet, Iterable, List, Optional, Sequence, Tuple

import asyncpg

logger = logging.getLogger(__name__)

METHOD = "hybrid-rrf/v0"
RRF_K = 60  # same constant as /knowledge/unified-search
DEFAULT_PER_TARGET_SOURCE_CAP = 2   # experimental discovery budget, not an evidence rule

RELATION_SEMANTICS: Dict[str, frozenset] = {
    "epistemic-relation/v0": frozenset({"supports", "challenges", "qualifies", "responds_to"}),
    "identity/v0": frozenset({"equivalent_to"}),
    "lineage/v0": frozenset({"restates"}),
    "design-alignment/v0": frozenset({"aligns_with"}),
    "topical/v0": frozenset({"related_to"}),
}

DEFAULT_MOVE_TYPES = (
    "thesis", "claim", "premise", "evidence", "counterpoint",
    "open_question", "implication", "definition",
)

LOCAL_CREDENTIAL = "privileged-local-credential"
ADMISSION_BASES = frozenset({LOCAL_CREDENTIAL, "fixture-replay"})
ADMISSIBLE_BINDINGS = frozenset({"recorded_extraction"})       # D1: content addresses are attribution only
SCRATCH_DATABASE_PREFIX = "koi_xsrc_scratch_"
JUDGMENTS_ENV = "KOI_DISCOURSE_JUDGMENTS"
# Restoration/P2P wiki material belongs to the held wiki-alignment pilot.
EXCLUDED_SOURCE_PATTERNS = ("mediawiki:%",)

_TOKEN_RE = re.compile(r"[a-z0-9]+")
_MAX_QUERY_TOKENS = 64

# The exact claim representation. SQL only: a Python twin would drift.
REPR_SQL = ("encode(sha256(convert_to(json_build_array({a}.move_type, {a}.title, "
            "{a}.detail, {a}.status)::text, 'UTF8')), 'hex')")

# Source version: what the source rows' content hash actually is now. Needs
# aliases l = document_ingestion_log and km = koi_memories. The log holds the
# feed:slug for every substack-corpus rid (extract_deep_documents writes
# document_rid.split(":", 1)[-1]), so a non-hex value is never a version.
_KM_HASH = "coalesce(nullif(km.metadata->>'content_hash', ''), km.content_hash)"
_KM_BOUND = "(l.deep_extracted_at IS NOT NULL AND km.updated_at <= l.deep_extracted_at)"
_DOC_RID = "m.source_rid ~ '^document:[0-9a-f]{64}$'"


def _sql(template: str) -> str:
    return template.replace("KMH", _KM_HASH).replace("BOUND", _KM_BOUND).replace("DOCRID", _DOC_RID)


SOURCE_VERSION_SQL = _sql(
    "CASE"
    " WHEN l.content_hash ~ '^[0-9a-f]{64}$' THEN 'sha256:' || l.content_hash"
    " WHEN KMH ~ '^[0-9a-f]{64}$' THEN 'sha256:' || KMH"
    " WHEN KMH ~ '^[0-9a-f]{16}$' THEN 'sha256-prefix16:' || KMH"
    " END")
SOURCE_VERSION_BASIS_SQL = _sql(
    "CASE"
    " WHEN l.content_hash ~ '^[0-9a-f]{64}$' THEN 'document_ingestion_log.content_hash'"
    " WHEN KMH ~ '^[0-9a-f]{16}([0-9a-f]{48})?$' THEN 'koi_memories content hash'"
    " WHEN l.document_rid IS NULL THEN 'unknown: no document_ingestion_log row and no content hash'"
    " ELSE 'unknown: no content hash recorded'"
    " END")
# Binding needs alias b = discourse_move_extraction_bindings as well.
BINDING_KIND_SQL = _sql(
    "CASE"
    " WHEN b.move_id IS NOT NULL THEN 'recorded_extraction'"
    " WHEN DOCRID THEN 'content_addressed_source'"
    " WHEN KMH ~ '^[0-9a-f]{16}([0-9a-f]{48})?$' AND BOUND THEN 'inferred_timestamp'"
    " ELSE 'unknown'"
    " END")
BOUND_VERSION_SQL = _sql(
    "CASE"
    " WHEN b.move_id IS NOT NULL THEN b.source_version_at_extraction"
    " WHEN DOCRID THEN 'sha256:' || substr(m.source_rid, 10)"
    " WHEN KMH ~ '^[0-9a-f]{64}$' AND BOUND THEN 'sha256:' || KMH"
    " WHEN KMH ~ '^[0-9a-f]{16}$' AND BOUND THEN 'sha256-prefix16:' || KMH"
    " END")
BINDING_BASIS_SQL = _sql(
    "CASE"
    " WHEN b.move_id IS NOT NULL THEN 'recorded by ' || b.recorded_by || ', run ' || b.run_id || ', ' || b.association"
    " WHEN DOCRID THEN 'source attribution: source_rid is the sha256 of the source text, not extraction provenance'"
    " WHEN KMH ~ '^[0-9a-f]{16}([0-9a-f]{48})?$' AND BOUND"
    "   THEN 'inferred: koi_memories row unchanged since deep extraction'"
    " WHEN KMH ~ '^[0-9a-f]{16}([0-9a-f]{48})?$' THEN 'unknown: source row changed after deep extraction'"
    " WHEN l.document_rid IS NULL THEN 'unknown: no document_ingestion_log row'"
    " ELSE 'unknown: no content hash recorded'"
    " END")

_ASSOCIATION_BLOCKERS = {
    "changed_while_reading": "source changed while the extraction read its input",
    "source_changed_during_extraction": "source changed during extraction",
    "unestablished": "extraction input/version association not established",
}


class ReviewRefused(ValueError):
    """A proposal, admission, decline, withdrawal or correction the policy does not allow."""


class NotVisible(KeyError):
    """The move does not exist, or the caller's context may not see it."""


class RunLimitExceeded(RuntimeError):
    """A run tried to write more than its approved limits allow."""


class PlanRefused(RuntimeError):
    """A discovery plan cannot be made, or no longer matches what it bound to.
    Nothing of the plan has been written."""


# ---------------------------------------------------------------------------
# Authority (Q2, D2): minted, never constructed from caller strings
# ---------------------------------------------------------------------------

_MINT = object()


@dataclass(frozen=True)
class OperatorPrincipal:
    """A caller that presented the privileged local admin credential. Minted
    only by the router's credential dependency. It is not proof that a
    particular person approved anything. Constructing one directly raises."""
    actor: str
    authenticated_by: str
    _mint: object = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        if self._mint is not _MINT:
            raise ReviewRefused("an OperatorPrincipal is minted only by the local-credential surface")


@dataclass(frozen=True)
class AdmissionAuthority:
    """Who writes a judgment, and on what basis. Recorded attribution: the actor
    string proves neither identity nor independence. Constructing one raises."""
    actor: str
    basis: str
    _mint: object = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        if self._mint is not _MINT:
            raise ReviewRefused("an AdmissionAuthority must be minted, not constructed")


def _mint_operator_principal(actor: str, authenticated_by: str) -> OperatorPrincipal:
    """For the credential-checking surface only. The actor comes from server config."""
    if not actor:
        raise ReviewRefused("operator actor is not configured")
    return OperatorPrincipal(actor=actor, authenticated_by=authenticated_by, _mint=_MINT)


def mint_operator_authority(principal: OperatorPrincipal) -> AdmissionAuthority:
    if not isinstance(principal, OperatorPrincipal):
        raise ReviewRefused("local-credential authority needs a minted OperatorPrincipal")
    return AdmissionAuthority(actor=principal.actor, basis=LOCAL_CREDENTIAL, _mint=_MINT)


def mint_fixture_replay_authority(actor: str) -> AdmissionAuthority:
    """Replays decisions an external fixture already authored. Usable only in a
    scratch database (checked at use); no service route mints it."""
    return AdmissionAuthority(actor=actor, basis="fixture-replay", _mint=_MINT)


async def _database(conn) -> str:
    return await conn.fetchval("SELECT current_database()")


async def _check_authority(conn, authority: Any, *, not_actors: Sequence[Optional[str]] = (),
                           bases: Iterable[str] = ADMISSION_BASES) -> AdmissionAuthority:
    if not isinstance(authority, AdmissionAuthority):
        raise ReviewRefused("this needs a minted AdmissionAuthority")
    if authority.basis not in set(bases):
        raise ReviewRefused(f"authority basis {authority.basis!r} is not allowed here ({sorted(set(bases))})")
    if authority.basis == "fixture-replay" and not (await _database(conn)).startswith(SCRATCH_DATABASE_PREFIX):
        raise ReviewRefused("fixture-replay authority is only usable in a scratch database")
    # Guards against accidental self-admission. It proves nothing about independence.
    if authority.actor in [a for a in not_actors if a]:
        raise ReviewRefused("the discovering or proposing actor cannot review its own candidate")
    return authority


async def _check_judgments_enabled(conn) -> None:
    """Judgments (admit, decline, authored, withdraw, correct, reopen) stay off in
    any real database until KOI_DISCOURSE_JUDGMENTS=1. The canary keeps them off."""
    if os.getenv(JUDGMENTS_ENV) == "1":
        return
    db = await _database(conn)
    if not db.startswith(SCRATCH_DATABASE_PREFIX):
        raise ReviewRefused(f"judgments are disabled in {db}: set {JUDGMENTS_ENV}=1 to enable them")


# ---------------------------------------------------------------------------
# Visibility (A1)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CallerContext:
    view: str                                   # personal | field | public | federated
    fields: FrozenSet[str] = frozenset()        # explicitly granted fields
    sources: FrozenSet[str] = frozenset()       # explicitly granted source rids
    include_unassigned: bool = False            # sources in no field
    include_private: bool = False               # koi_memories.is_private sources
    peer: Optional[str] = None

    def describe(self) -> Dict[str, Any]:
        return {"view": self.view, "fields": sorted(self.fields), "sources": sorted(self.sources),
                "include_unassigned": self.include_unassigned, "include_private": self.include_private,
                "peer": self.peer, "excluded_source_patterns": list(EXCLUDED_SOURCE_PATTERNS)}


def personal_context() -> CallerContext:
    """The operator's own node: every non-excluded source, assigned or not, private or not."""
    return CallerContext(view="personal", include_unassigned=True, include_private=True)


def field_context(fields: Iterable[str], *, sources: Iterable[str] = (),
                  include_unassigned: bool = False, include_private: bool = False) -> CallerContext:
    return CallerContext(view="field", fields=frozenset(fields), sources=frozenset(sources),
                         include_unassigned=include_unassigned, include_private=include_private)


def public_context(*, sources: Iterable[str] = ()) -> CallerContext:
    return CallerContext(view="public", sources=frozenset(sources))


def federated_context(peer: str, *, fields: Iterable[str] = (), sources: Iterable[str] = ()) -> CallerContext:
    return CallerContext(view="federated", peer=peer, fields=frozenset(fields), sources=frozenset(sources))


def _visibility(ctx: CallerContext, alias: str, start: int) -> Tuple[str, List[Any]]:
    """SQL predicate on move alias `alias` with six parameters from $start."""
    a, n = alias, start
    sql = (f"(NOT ({a}.source_rid LIKE ANY(${n}::text[])) AND (${n + 1}::bool"
           f" OR {a}.source_rid = ANY(${n + 2}::text[])"
           f" OR ((${n + 3}::bool OR NOT EXISTS (SELECT 1 FROM koi_memories kp"
           f"        WHERE kp.rid = {a}.source_rid AND kp.is_private))"
           f"     AND (EXISTS (SELECT 1 FROM document_field_membership fm"
           f"            WHERE fm.document_rid = {a}.source_rid AND fm.field_id = ANY(${n + 4}::text[]))"
           f"          OR (${n + 5}::bool AND NOT EXISTS (SELECT 1 FROM document_field_membership fu"
           f"            WHERE fu.document_rid = {a}.source_rid))))))")
    params = [list(EXCLUDED_SOURCE_PATTERNS), ctx.view == "personal", sorted(ctx.sources),
              ctx.include_private, sorted(ctx.fields), ctx.include_unassigned]
    return sql, params


async def visible_ids(conn, ctx: CallerContext, move_ids: Iterable[Any]) -> set:
    ids = sorted({str(m) for m in move_ids})
    if not ids:
        return set()
    vis, vp = _visibility(ctx, "t", 2)
    rows = await conn.fetch(f"SELECT t.id FROM session_discourse_moves t WHERE t.id = ANY($1::uuid[]) AND {vis}",
                            ids, *vp)
    return {str(r["id"]) for r in rows}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def embedding_text(title: str, detail: Optional[str]) -> str:
    text = (title or "").strip()
    if detail and detail.strip():
        text = f"{text}\n{detail.strip()}"
    return text[:2000]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def extraction_input_digest(chunks: Sequence[Tuple[int, str]]) -> str:
    """sha256 of the exact (chunk_index, text) list an extraction read."""
    return _sha256(json.dumps([[int(i), t or ""] for i, t in chunks], ensure_ascii=False, separators=(",", ":")))


def candidate_key(end_a: Sequence[Optional[str]], end_b: Sequence[Optional[str]]) -> str:
    """Hash of the unordered pair of (move id, representation, bound version)."""
    pair = sorted([[str(x) if x is not None else "unknown" for x in end] for end in (end_a, end_b)])
    return _sha256(json.dumps(pair, separators=(",", ":")))


def lexical_or_query(text: str) -> str:
    """A websearch_to_tsquery string that ORs the text's words. Tokens are
    reduced to [a-z0-9]+ so nothing in the source text can act as query syntax."""
    seen: List[str] = []
    for tok in _TOKEN_RE.findall((text or "").lower()):
        if tok not in seen and tok != "or":
            seen.append(tok)
        if len(seen) >= _MAX_QUERY_TOKENS:
            break
    return " or ".join(seen)


def rrf_fuse(lexical: Sequence[Dict[str, Any]], semantic: Sequence[Dict[str, Any]],
             k: int = RRF_K) -> List[Dict[str, Any]]:
    """Reciprocal-rank fusion of two ranked lists of {"id", "score"}. Ties break by id."""
    fused: Dict[str, Dict[str, Any]] = {}
    for leg, rows in (("lexical", lexical), ("semantic", semantic)):
        for rank, row in enumerate(rows, start=1):
            entry = fused.setdefault(str(row["id"]), {
                "id": str(row["id"]), "fused_score": 0.0,
                "lexical_rank": None, "lexical_score": None,
                "semantic_rank": None, "semantic_score": None,
            })
            entry["fused_score"] += 1.0 / (k + rank)
            entry[f"{leg}_rank"] = rank
            entry[f"{leg}_score"] = float(row["score"])
    return sorted(fused.values(), key=lambda e: (-e["fused_score"], e["id"]))


async def _has_table(conn, name: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", name))


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


# ---------------------------------------------------------------------------
# Run effects
# ---------------------------------------------------------------------------

class RunRecorder:
    """Records each row a run creates or changes, in the caller's savepoint, and
    stops the run at its approved write limits."""

    def __init__(self, run_id: str, *, max_embedding_writes: int, max_candidate_inserts: int):
        self.run_id = run_id
        self.max_embedding_writes = max_embedding_writes
        self.max_candidate_inserts = max_candidate_inserts
        self.counts: Counter = Counter()

    async def note(self, conn, effect: str, *, move_id=None, model=None, candidate_id=None,
                   representation_sha256=None, after_state=None, prior_state=None) -> None:
        self.counts[effect] += 1
        embeddings = self.counts["embedding_inserted"] + self.counts["embedding_replaced"]
        if embeddings > self.max_embedding_writes:
            raise RunLimitExceeded(f"{self.run_id}: {embeddings} embedding writes > approved {self.max_embedding_writes}")
        if self.counts["candidate_inserted"] > self.max_candidate_inserts:
            raise RunLimitExceeded(f"{self.run_id}: {self.counts['candidate_inserted']} candidates > "
                                   f"approved {self.max_candidate_inserts}")
        await conn.execute(
            """INSERT INTO discourse_link_run_effects
                 (run_id, effect, move_id, model, candidate_id, representation_sha256, after_state, prior_state)
               VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb)""",
            self.run_id, effect, move_id, model, candidate_id, representation_sha256,
            json.dumps(after_state, default=str) if after_state is not None else None,
            json.dumps(prior_state, default=str) if prior_state is not None else None)


# ---------------------------------------------------------------------------
# Moves, with provenance, binding and admissibility
# ---------------------------------------------------------------------------

_NO_BINDINGS = ("(SELECT NULL::uuid AS move_id, NULL::text AS source_version_at_extraction, "
                "NULL::text AS source_version_at_record, NULL::text AS association, "
                "NULL::text AS extraction_input_sha256, NULL::text AS representation_sha256, "
                "NULL::text AS run_id, NULL::text AS recorded_by)")


def _move_sql(has_bindings: bool) -> str:
    bindings = "discourse_move_extraction_bindings" if has_bindings else _NO_BINDINGS
    return f"""
    SELECT m.id, m.move_type, m.title, m.detail, m.status, m.source_type,
           m.source_rid, m.turn_range_start, m.turn_range_end, m.superseded_at,
           l.document_rid IS NOT NULL AS has_log, l.title AS source_title, l.source_url,
           {SOURCE_VERSION_SQL} AS source_version, {SOURCE_VERSION_BASIS_SQL} AS version_basis,
           {BINDING_KIND_SQL} AS binding_kind, {BOUND_VERSION_SQL} AS bound_version,
           {BINDING_BASIS_SQL} AS binding_basis, b.association AS binding_association,
           b.source_version_at_record,
           b.representation_sha256 AS extracted_representation_sha256,
           b.extraction_input_sha256, b.run_id AS binding_run_id,
           {REPR_SQL.format(a='m')} AS representation_sha256,
           to_tsvector('english', coalesce(m.title,'')||' '||coalesce(m.detail,'')) <> ''::tsvector AS lexical_indexable
      FROM session_discourse_moves m
      LEFT JOIN document_ingestion_log l ON l.document_rid = m.source_rid
      LEFT JOIN koi_memories km ON km.rid = m.source_rid
      LEFT JOIN {bindings} b ON b.move_id = m.id
     WHERE m.id = ANY($1::uuid[])
    """


def _move_view(row) -> Dict[str, Any]:
    ineligible = []
    if row["superseded_at"] is not None:
        ineligible.append("move superseded")
    if row["source_type"] != "document":
        ineligible.append("not a document move")
    if not row["has_log"]:
        ineligible.append("no source log row")
    kind, bound, current_version = row["binding_kind"], row["bound_version"], row["source_version"]
    association = row["binding_association"]
    binding_current = bound is not None and bound == current_version
    text_matches = (row["extracted_representation_sha256"] == row["representation_sha256"]
                    if kind == "recorded_extraction" else None)
    blockers = list(ineligible)
    if kind not in ADMISSIBLE_BINDINGS:
        blockers.append({
            "inferred_timestamp": "binding inferred from timestamps, not recorded",
            "content_addressed_source": "content-addressed source attribution, not recorded extraction provenance",
        }.get(kind, "no extraction binding"))
    elif association != "captured_with_input":
        blockers.append(_ASSOCIATION_BLOCKERS.get(association, "extraction input/version association not established"))
    elif bound is None:
        blockers.append("source had no content hash at extraction")
    elif not binding_current:
        blockers.append("source changed since extraction")
    if text_matches is False:
        blockers.append("claim text differs from its recorded extraction")
    return {
        "id": str(row["id"]),
        "move_type": row["move_type"],
        "title": row["title"],
        "detail": row["detail"],
        "status": row["status"],
        "chunk_range": [row["turn_range_start"], row["turn_range_end"]],
        "representation_sha256": row["representation_sha256"],
        "lexical_indexable": row["lexical_indexable"],
        "eligible": not ineligible,
        "ineligible_reasons": ineligible,
        "source": {
            "rid": row["source_rid"],
            "title": row["source_title"],
            "source_url": row["source_url"],
            "version": current_version,
            "version_status": "known" if current_version else "unknown",
            "version_basis": row["version_basis"],
        },
        "extraction": {
            "binding_kind": kind,
            "binding_basis": row["binding_basis"],
            "association": association,
            "recorded": kind in ADMISSIBLE_BINDINGS,
            "bound_version": bound,
            "version_at_record": row["source_version_at_record"],
            "current": binding_current,
            "text_matches_extraction": text_matches,
            "input_sha256": row["extraction_input_sha256"],
            "run_id": row["binding_run_id"],
        },
        "admissible": not blockers,
        "admission_blockers": blockers,
    }


async def fetch_moves(conn, move_ids: Iterable[Any]) -> Dict[str, Dict[str, Any]]:
    """Moves with provenance. Internal: no visibility check, callers filter with visible_ids."""
    ids = sorted({str(m) for m in move_ids})
    if not ids:
        return {}
    rows = await conn.fetch(_move_sql(await _has_table(conn, "discourse_move_extraction_bindings")), ids)
    return {str(r["id"]): _move_view(r) for r in rows}


def _end_problems(recorded_repr: str, recorded_bound: Optional[str], move: Optional[Dict[str, Any]],
                  *, for_admission: bool) -> List[str]:
    """Why a recorded end no longer matches the current row. Empty means current.
    for_admission adds the binding requirements a new judgment must meet."""
    if move is None:
        return ["move no longer exists"]
    problems = list(move["ineligible_reasons"])
    if move["representation_sha256"] != recorded_repr:
        problems.append("claim text changed")
    ext = move["extraction"]
    if ext["bound_version"] is None:
        problems.append("extraction binding unknown")
    elif recorded_bound is None:
        problems.append("bound version was unknown when recorded")
    elif ext["bound_version"] != recorded_bound:
        problems.append("source version changed")
    elif not ext["current"]:
        problems.append("source changed since extraction")
    if for_admission:
        problems += [b for b in move["admission_blockers"] if b not in problems
                     and b not in move["ineligible_reasons"] and b != "source changed since extraction"]
    return sorted(set(problems))


async def _capture_representations(conn, move_ids: Iterable[Any]) -> List[str]:
    """Store the exact text behind each move's current representation. Returns
    the hashes this call inserted (none if all were already stored)."""
    rows = await conn.fetch(
        f"""INSERT INTO discourse_claim_representations
              (representation_sha256, move_id, move_type, title, detail, status)
            SELECT {REPR_SQL.format(a='m')}, m.id, m.move_type, m.title, m.detail, m.status
              FROM session_discourse_moves m WHERE m.id = ANY($1::uuid[])
            ON CONFLICT (representation_sha256) DO NOTHING
            RETURNING representation_sha256""",
        sorted({str(m) for m in move_ids}))
    return [r["representation_sha256"] for r in rows]


async def _representations(conn, hashes: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """Internal: retained text for hashes the caller already may see, each checked against its hash."""
    hs = sorted({h for h in hashes if h})
    if not hs:
        return {}
    rows = await conn.fetch(
        f"""SELECT r.*, {REPR_SQL.format(a='r')} = r.representation_sha256 AS intact
              FROM discourse_claim_representations r WHERE r.representation_sha256 = ANY($1::text[])""", hs)
    return {r["representation_sha256"]: {"move_type": r["move_type"], "title": r["title"],
                                         "detail": r["detail"], "status": r["status"], "intact": r["intact"]}
            for r in rows}


def _text_of(reps: Dict[str, Dict[str, Any]], h: str) -> Dict[str, Any]:
    r = reps.get(h)
    if r is None:
        return {"representation_sha256": h, "text": None, "text_status": "hash only, text not retained"}
    status = "retained" if r["intact"] else "retained text fails its hash"
    return {"representation_sha256": h, "text_status": status,
            "text": {k: r[k] for k in ("move_type", "title", "detail", "status")}}


# ---------------------------------------------------------------------------
# A2: extraction provenance, captured with the input
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ExtractionInput:
    """What an extraction read and the source version observed around the read.
    source_version is set only when the same version was seen before and after."""
    source_rid: str
    source_version: Optional[str]
    input_sha256: str
    association: str              # captured_with_input | changed_while_reading | unestablished


_SOURCE_VERSION_OF_RID = (
    f"SELECT {SOURCE_VERSION_SQL} FROM (SELECT $1::text AS rid) x"
    " LEFT JOIN document_ingestion_log l ON l.document_rid = x.rid"
    " LEFT JOIN koi_memories km ON km.rid = x.rid")


async def resolve_source_version(conn, source_rid: str) -> Optional[str]:
    return await conn.fetchval(_SOURCE_VERSION_OF_RID, source_rid)


async def capture_extraction_input(conn, *, source_rid: str,
                                   read_chunks: Callable[[], Awaitable[Sequence[Tuple[int, str]]]]):
    """Read an extraction's input and observe the source version on both sides
    of the read. Returns (chunks, ExtractionInput)."""
    before = await resolve_source_version(conn, source_rid)
    chunks = await read_chunks()
    after = await resolve_source_version(conn, source_rid)
    if before is None:
        association, version = "unestablished", None
    elif before != after:
        association, version = "changed_while_reading", None
    else:
        association, version = "captured_with_input", before
    return chunks, ExtractionInput(source_rid=source_rid, source_version=version,
                                   input_sha256=extraction_input_digest(chunks), association=association)


async def record_extraction_bindings(conn, *, source_rid: str, move_ids: Iterable[Any], run_id: str,
                                     recorded_by: str, extraction_input: Optional[ExtractionInput] = None,
                                     extraction_input_sha256: Optional[str] = None) -> int:
    """Record which source version and exact input each move came from.

    The version is the one captured when the input was read (ExtractionInput),
    never one resolved now. If the source has changed since, the binding is
    recorded as source_changed_during_extraction. Without a captured input the
    association is unestablished and the version unknown: such a binding can
    never be admitted.
    """
    ids = sorted({str(m) for m in move_ids})
    if not ids:
        return 0
    if extraction_input is not None:
        if extraction_input.source_rid != source_rid:
            raise ValueError(f"captured input is for {extraction_input.source_rid}, not {source_rid}")
        input_sha, version_at_read, association = (extraction_input.input_sha256, extraction_input.source_version,
                                                   extraction_input.association)
    elif extraction_input_sha256:
        input_sha, version_at_read, association = extraction_input_sha256, None, "unestablished"
    else:
        raise ValueError("record_extraction_bindings needs the captured ExtractionInput")
    async with conn.transaction():
        version_now = await resolve_source_version(conn, source_rid)
        if association == "captured_with_input" and version_now != version_at_read:
            association = "source_changed_during_extraction"
        await _capture_representations(conn, ids)
        status = await conn.execute(
            f"""INSERT INTO discourse_move_extraction_bindings
                  (move_id, source_rid, source_version_at_extraction, source_version_at_record, association,
                   extraction_input_sha256, representation_sha256, run_id, recorded_by)
                SELECT m.id, m.source_rid, $2, $3, $4, $5, {REPR_SQL.format(a='m')}, $6, $7
                  FROM session_discourse_moves m
                 WHERE m.id = ANY($1::uuid[]) AND m.source_rid = $8
                ON CONFLICT (move_id) DO UPDATE SET
                  source_rid = EXCLUDED.source_rid,
                  source_version_at_extraction = EXCLUDED.source_version_at_extraction,
                  source_version_at_record = EXCLUDED.source_version_at_record,
                  association = EXCLUDED.association,
                  extraction_input_sha256 = EXCLUDED.extraction_input_sha256,
                  representation_sha256 = EXCLUDED.representation_sha256,
                  run_id = EXCLUDED.run_id, recorded_by = EXCLUDED.recorded_by, recorded_at = now()""",
            ids, version_at_read, version_now, association, input_sha, run_id, recorded_by, source_rid)
        written = int(status.split()[-1])
        if written != len(ids):
            raise ValueError(f"{len(ids) - written} of {len(ids)} moves do not belong to {source_rid}")
    return written


# ---------------------------------------------------------------------------
# Embeddings (local or supplied provider; the prototype makes no paid call)
# ---------------------------------------------------------------------------

def _vector_text(vec: Sequence[float]) -> str:
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"


async def _embedding_todo(conn, move_ids: Iterable[Any], *, model: str, text_prefix: str):
    """The moves, their stored (representation, text) stamps for `model`, and
    the ones needing a new vector: missing, or made from another representation
    or text. Shared by embed_moves and plan_discovery."""
    moves = await fetch_moves(conn, move_ids)
    existing = {
        str(r["move_id"]): (r["representation_sha256"], r["text_sha256"])
        for r in await conn.fetch(
            "SELECT move_id, representation_sha256, text_sha256 FROM discourse_move_embeddings "
            "WHERE model = $1 AND move_id = ANY($2::uuid[])", model, list(moves))
    }
    todo = []
    for mid, mv in sorted(moves.items()):
        text = text_prefix + embedding_text(mv["title"], mv["detail"])
        stamp = (mv["representation_sha256"], _sha256(text))
        if existing.get(mid) != stamp:
            todo.append((mid, text, stamp))
    return moves, existing, todo


# The exact stored vector value: sha256 of pgvector's binary send format (int16
# dimension, int16 unused, then the big-endian float4 values). Alias a.
VECTOR_SHA_SQL = "encode(sha256(vector_send({a}.embedding)), 'hex')"


async def _stored_vector_stamps(conn, model: str, move_ids: Iterable[Any], *, lock: bool = False
                                ) -> Dict[str, Dict[str, Any]]:
    """What a plan pins about each stored vector it replaces or ranks with: the
    input hashes it was made from, its dimension column and the hash of its
    exact value. lock=True holds the rows FOR SHARE until commit (not allowed,
    and not used, in a read-only planning transaction)."""
    ids = sorted({str(m) for m in move_ids})
    if not ids:
        return {}
    rows = await conn.fetch(
        f"""SELECT e.move_id, e.representation_sha256, e.text_sha256, e.dimension,
                   {VECTOR_SHA_SQL.format(a='e')} AS vector_sha256
              FROM discourse_move_embeddings e
             WHERE e.model = $1 AND e.move_id = ANY($2::uuid[])
             ORDER BY e.move_id{' FOR SHARE' if lock else ''}""", model, ids)
    return {str(r["move_id"]): {k: r[k] for k in ("representation_sha256", "text_sha256", "dimension", "vector_sha256")}
            for r in rows}


async def _write_embedding(conn, mid: str, model: str, rep: str, digest: str, vector_text: str, dimension: int,
                           recorder: Optional[RunRecorder]) -> None:
    """One vector and its effect row in one savepoint. A replaced vector is kept
    as prior_state so rollback can restore it."""
    async with conn.transaction():
        prior = None
        if recorder:
            prior = await conn.fetchrow(
                "SELECT representation_sha256, text_sha256, dimension, embedding::text AS embedding, "
                "created_at FROM discourse_move_embeddings WHERE move_id = $1 AND model = $2", mid, model)
        stored = await conn.fetchrow(
            f"""INSERT INTO discourse_move_embeddings AS e
                  (move_id, model, representation_sha256, text_sha256, dimension, embedding)
                VALUES ($1, $2, $3, $4, $5, $6::vector)
                ON CONFLICT (move_id, model) DO UPDATE
                   SET representation_sha256 = EXCLUDED.representation_sha256,
                       text_sha256 = EXCLUDED.text_sha256, dimension = EXCLUDED.dimension,
                       embedding = EXCLUDED.embedding, created_at = now()
                RETURNING e.dimension, {VECTOR_SHA_SQL.format(a='e')} AS vector_sha256""",
            mid, model, rep, digest, dimension, vector_text)
        if recorder:
            # The exact value written, so rollback can prove the row is still this run's.
            await recorder.note(conn, "embedding_replaced" if prior else "embedding_inserted",
                                move_id=mid, model=model,
                                after_state={"representation_sha256": rep, "text_sha256": digest,
                                             "dimension": stored["dimension"],
                                             "vector_sha256": stored["vector_sha256"]},
                                prior_state=dict(prior) if prior else None)


async def embed_moves(conn, embedder, move_ids: Iterable[Any], *, model: str, text_prefix: str = "",
                      batch_size: int = 32, recorder: Optional[RunRecorder] = None) -> Dict[str, int]:
    """Store an embedding for every move whose vector for `model` is missing or
    was made from another representation or text. With a recorder, each write
    and its effect row share one savepoint."""
    moves, _, todo = await _embedding_todo(conn, move_ids, model=model, text_prefix=text_prefix)
    for i in range(0, len(todo), batch_size):
        batch = todo[i:i + batch_size]
        vectors = await embedder.embed_batch([t for _, t, _ in batch])
        if len(vectors) != len(batch):
            raise RuntimeError(f"embedder returned {len(vectors)} vectors for {len(batch)} texts")
        for (mid, _, (rep, digest)), vec in zip(batch, vectors):
            await _write_embedding(conn, mid, model, rep, digest, _vector_text(vec), len(vec), recorder)
    return {"moves": len(moves), "embedded": len(todo), "reused": len(moves) - len(todo)}


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

_ELIGIBLE = """
    t.source_type = 'document'
    AND t.superseded_at IS NULL
    AND t.source_rid IS NOT NULL
    AND t.source_rid <> $2
    AND t.move_type = ANY($3::text[])
    AND EXISTS (SELECT 1 FROM document_ingestion_log l WHERE l.document_rid = t.source_rid)
"""


async def _lexical_leg(conn, ctx, query_text: str, source_rid: str, move_types, limit: int):
    tsq = lexical_or_query(query_text)
    if not tsq:
        return []
    vis, vp = _visibility(ctx, "t", 5)
    rows = await conn.fetch(
        f"""SELECT t.id,
                   ts_rank_cd(to_tsvector('english', coalesce(t.title,'')||' '||coalesce(t.detail,'')),
                              websearch_to_tsquery('english', $1)) AS score
              FROM session_discourse_moves t
             WHERE {_ELIGIBLE} AND {vis}
               AND to_tsvector('english', coalesce(t.title,'')||' '||coalesce(t.detail,''))
                   @@ websearch_to_tsquery('english', $1)
             ORDER BY score DESC, t.id
             LIMIT $4""",
        tsq, source_rid, list(move_types), limit, *vp)
    return [{"id": str(r["id"]), "score": float(r["score"])} for r in rows]


async def _semantic_leg(conn, ctx, query_move_id: str, source_rid: str, move_types,
                        model: str, limit: int):
    """Cosine neighbours among visible moves, using only vectors made from the
    CURRENT representation of both the query and the target."""
    vis, vp = _visibility(ctx, "t", 6)
    rows = await conn.fetch(
        f"""SELECT t.id, 1 - (e.embedding <=> q.embedding) AS score
              FROM discourse_move_embeddings q
              JOIN session_discourse_moves qm ON qm.id = q.move_id
              JOIN discourse_move_embeddings e ON e.model = q.model AND e.dimension = q.dimension
              JOIN session_discourse_moves t ON t.id = e.move_id
             WHERE q.move_id = $1 AND q.model = $5
               AND q.representation_sha256 = {REPR_SQL.format(a='qm')}
               AND e.representation_sha256 = {REPR_SQL.format(a='t')}
               AND {_ELIGIBLE} AND {vis}
             ORDER BY e.embedding <=> q.embedding, t.id
             LIMIT $4""",
        query_move_id, source_rid, list(move_types), limit, model, *vp)
    return [{"id": str(r["id"]), "score": float(r["score"])} for r in rows]


async def _semantic_leg_planned(conn, ctx, query_vector: str, dimension: int, source_rid: str, move_types,
                                model: str, limit: int, pending: Sequence[Dict[str, Any]]):
    """_semantic_leg for a plan: the query vector is passed in, and the plan's
    own vectors (not yet stored) take the place of any stored row for the same
    move. Same filters, distance, order and limit."""
    pend = [p for p in pending if p["dimension"] == dimension]
    vis, vp = _visibility(ctx, "t", 10)
    rows = await conn.fetch(
        f"""SELECT t.id, 1 - (v.embedding <=> $1::vector) AS score
              FROM (SELECT p.move_id, p.representation_sha256, p.embedding::vector AS embedding
                      FROM unnest($7::uuid[], $8::text[], $9::text[]) AS p(move_id, representation_sha256, embedding)
                    UNION ALL
                    SELECT e.move_id, e.representation_sha256, e.embedding
                      FROM discourse_move_embeddings e
                     WHERE e.model = $5 AND e.dimension = $6 AND NOT (e.move_id = ANY($7::uuid[]))) v
              JOIN session_discourse_moves t ON t.id = v.move_id
             WHERE v.representation_sha256 = {REPR_SQL.format(a='t')}
               AND {_ELIGIBLE} AND {vis}
             ORDER BY v.embedding <=> $1::vector, t.id
             LIMIT $4""",
        query_vector, source_rid, list(move_types), limit, model, dimension,
        [p["move_id"] for p in pend], [p["representation_sha256"] for p in pend], [p["vector"] for p in pend], *vp)
    return [{"id": str(r["id"]), "score": float(r["score"])} for r in rows]


async def _discovery_cap(conn, cap: Optional[int], expanded_by: Optional[AdmissionAuthority]) -> Optional[int]:
    """Loosening the per-target-source cap (None, 0, or above the default) is the expanded view and needs authority."""
    if not cap or cap > DEFAULT_PER_TARGET_SOURCE_CAP:
        await _check_authority(conn, expanded_by)
        cap = cap or None
    return cap


def _method_params(*, context: CallerContext, embedding_model, move_types, lexical_limit, semantic_limit,
                   per_move_cap, per_target_source_cap, expanded_by) -> Dict[str, Any]:
    return {
        "rrf_k": RRF_K, "lexical": "websearch_to_tsquery-or/ts_rank_cd/english",
        "lexical_limit": lexical_limit, "semantic_limit": semantic_limit,
        "per_move_cap": per_move_cap, "per_target_source_cap": per_target_source_cap,
        "expanded_by": expanded_by.actor if expanded_by else None,
        "embedding_model": embedding_model, "move_types": sorted(move_types),
        "context": context.describe(),
        "eligibility": "document moves, other source_rid, not superseded, source log row present, visible",
        "embedding_freshness": "query and target vectors must match the current representation",
    }


def _discovery_summary(query_moves: int, params: Dict[str, Any]) -> Dict[str, Any]:
    return {"query_moves": query_moves, "inserted": 0, "rediscovered": 0,
            "skipped_not_visible": 0, "skipped_ineligible": 0, "unknown_binding_queries": 0,
            "no_embedding": 0, "stale_query_embedding": 0, "capped_by_target_source": 0,
            "changed_during_discovery": 0, "method": METHOD, "method_params": params}


async def _pick_targets(conn, fused: Sequence[Dict[str, Any]], *, per_move_cap: int,
                        per_target_source_cap: Optional[int], summary: Dict[str, Any]):
    """One query's fused list cut to per_move_cap, at most per_target_source_cap per target source."""
    targets = await fetch_moves(conn, [f["id"] for f in fused])
    picked: List[Dict[str, Any]] = []
    per_source: Dict[str, int] = {}
    for f in fused:
        if len(picked) >= per_move_cap:
            break
        rid = targets[f["id"]]["source"]["rid"]
        if per_target_source_cap and per_source.get(rid, 0) >= per_target_source_cap:
            summary["capped_by_target_source"] += 1
            continue
        per_source[rid] = per_source.get(rid, 0) + 1
        picked.append(f)
    return picked, targets


def _candidate_row(qid: str, q: Dict[str, Any], t: Dict[str, Any], f: Dict[str, Any], *,
                   discovered_by: str, run_id: str, pick: int) -> Dict[str, Any]:
    """One sighting: query qid picked target t at position `pick` (1-based) of its picks in this run."""
    qe, te = q["extraction"], t["extraction"]
    return {
        "candidate_key": candidate_key((qid, q["representation_sha256"], qe["bound_version"]),
                                       (t["id"], t["representation_sha256"], te["bound_version"])),
        "query_move_id": qid, "target_move_id": t["id"],
        "query_source_rid": q["source"]["rid"], "target_source_rid": t["source"]["rid"],
        "query_representation_sha256": q["representation_sha256"],
        "target_representation_sha256": t["representation_sha256"],
        "query_binding_kind": qe["binding_kind"], "target_binding_kind": te["binding_kind"],
        "query_bound_version": qe["bound_version"], "target_bound_version": te["bound_version"],
        "lexical_rank": f["lexical_rank"], "lexical_score": f["lexical_score"],
        "semantic_rank": f["semantic_rank"], "semantic_score": f["semantic_score"],
        "fused_score": f["fused_score"], "discovered_by": discovered_by, "run_id": run_id, "pick": pick,
    }


async def _write_candidate(conn, c: Dict[str, Any], method_params_json: str, recorder: Optional[RunRecorder]):
    """One candidate sighting, the text of both ends and its effect rows in one
    savepoint. A changed end fails the foreign key on its representation.

    A sighting of a stored pair changes only last_seen_at and times_seen: the
    first sighting's run, query, ranks and parameters stay on the row. Each
    sighting's own query and pick go in its effect row (after_state.occurrence),
    which rollback keeps as history."""
    async with conn.transaction():
        new_reps = await _capture_representations(conn, [c["query_move_id"], c["target_move_id"]])
        prior = (await conn.fetchrow(
            "SELECT id, times_seen, last_seen_at FROM discourse_link_candidates WHERE candidate_key = $1",
            c["candidate_key"]) if recorder else None)
        row = await conn.fetchrow(
            """INSERT INTO discourse_link_candidates
                 (candidate_key, query_move_id, target_move_id, query_source_rid, target_source_rid,
                  query_representation_sha256, target_representation_sha256,
                  query_binding_kind, target_binding_kind, query_bound_version, target_bound_version,
                  method, method_params, lexical_rank, lexical_score, semantic_rank, semantic_score,
                  fused_score, discovered_by, run_id)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15,$16,$17,$18,$19,$20)
               ON CONFLICT (candidate_key) DO UPDATE
                  SET last_seen_at = now(),
                      times_seen = discourse_link_candidates.times_seen + 1
               RETURNING id, (xmax = 0) AS inserted, times_seen, last_seen_at""",
            c["candidate_key"], c["query_move_id"], c["target_move_id"], c["query_source_rid"],
            c["target_source_rid"], c["query_representation_sha256"], c["target_representation_sha256"],
            c["query_binding_kind"], c["target_binding_kind"], c["query_bound_version"], c["target_bound_version"],
            METHOD, method_params_json, c["lexical_rank"], c["lexical_score"],
            c["semantic_rank"], c["semantic_score"], c["fused_score"], c["discovered_by"], c["run_id"])
        if recorder:
            for h in new_reps:
                await recorder.note(conn, "representation_inserted", representation_sha256=h)
            after = {"times_seen": row["times_seen"], "last_seen_at": _iso(row["last_seen_at"])}
            if c.get("pick") is not None:
                after["occurrence"] = {"query_move_id": str(c["query_move_id"]), "pick": c["pick"]}
            if row["inserted"]:
                await recorder.note(conn, "candidate_inserted", candidate_id=row["id"], after_state=after)
            else:
                await recorder.note(conn, "candidate_seen", candidate_id=row["id"], after_state=after,
                                    prior_state={"times_seen": prior["times_seen"],
                                                 "last_seen_at": _iso(prior["last_seen_at"])})
    return row


async def discover_candidates(
    conn,
    query_move_ids: Iterable[Any],
    *,
    context: CallerContext,
    discovered_by: str,
    run_id: str,
    embedding_model: Optional[str] = None,
    move_types: Sequence[str] = DEFAULT_MOVE_TYPES,
    lexical_limit: int = 20,
    semantic_limit: int = 20,
    per_move_cap: int = 5,
    per_target_source_cap: Optional[int] = DEFAULT_PER_TARGET_SOURCE_CAP,
    expanded_by: Optional[AdmissionAuthority] = None,
    recorder: Optional[RunRecorder] = None,
) -> Dict[str, Any]:
    """Record cross-source candidates for each visible query move, retrieved
    only from moves the context may see. Writes candidates and the text of their
    ends; never reads or writes judgments.

    per_target_source_cap is an experimental budget: at most that many of one
    query move's candidates come from one target source. It records nothing
    about evidence and drops no move. Loosening it (None, 0, or above the
    default) is the expanded view and needs authority.
    """
    per_target_source_cap = await _discovery_cap(conn, per_target_source_cap, expanded_by)
    params = _method_params(context=context, embedding_model=embedding_model, move_types=move_types,
                            lexical_limit=lexical_limit, semantic_limit=semantic_limit, per_move_cap=per_move_cap,
                            per_target_source_cap=per_target_source_cap, expanded_by=expanded_by)
    queries = await fetch_moves(conn, query_move_ids)
    visible = await visible_ids(conn, context, queries)
    summary = _discovery_summary(len(queries), params)
    for qid, q in sorted(queries.items()):
        if qid not in visible:
            summary["skipped_not_visible"] += 1
            continue
        if not q["eligible"]:
            summary["skipped_ineligible"] += 1
            continue
        if q["extraction"]["bound_version"] is None:
            summary["unknown_binding_queries"] += 1
        lexical = await _lexical_leg(conn, context, embedding_text(q["title"], q["detail"]),
                                     q["source"]["rid"], move_types, lexical_limit)
        semantic: List[Dict[str, Any]] = []
        if embedding_model:
            stored = await conn.fetchval(
                "SELECT representation_sha256 FROM discourse_move_embeddings WHERE move_id = $1 AND model = $2",
                qid, embedding_model)
            if stored is None:
                summary["no_embedding"] += 1
            elif stored != q["representation_sha256"]:
                summary["stale_query_embedding"] += 1
            else:
                semantic = await _semantic_leg(conn, context, qid, q["source"]["rid"], move_types,
                                               embedding_model, semantic_limit)
        picked, targets = await _pick_targets(conn, rrf_fuse(lexical, semantic), per_move_cap=per_move_cap,
                                              per_target_source_cap=per_target_source_cap, summary=summary)
        for pick, f in enumerate(picked, start=1):
            c = _candidate_row(qid, q, targets[f["id"]], f, discovered_by=discovered_by, run_id=run_id, pick=pick)
            try:
                row = await _write_candidate(conn, c, json.dumps(params), recorder)
            except asyncpg.ForeignKeyViolationError:
                # The move's text changed between reading and recording it.
                summary["changed_during_discovery"] += 1
                continue
            summary["inserted" if row["inserted"] else "rediscovered"] += 1
    return summary


# ---------------------------------------------------------------------------
# Two-phase discovery (O2): plan without writing, then apply briefly
# ---------------------------------------------------------------------------

PLAN_FORMAT = "discourse-link-plan/v1"


def _endpoint_pin(mv: Dict[str, Any], visible: bool) -> Dict[str, Any]:
    """What a plan binds each move to. A difference at apply refuses the plan."""
    ext = mv["extraction"]
    return {"source_rid": mv["source"]["rid"], "move_type": mv["move_type"],
            "representation_sha256": mv["representation_sha256"], "eligible": mv["eligible"],
            "binding_kind": ext["binding_kind"], "bound_version": ext["bound_version"],
            "source_version": mv["source"]["version"], "visible": visible}


async def plan_discovery(
    conn,
    embedder,
    query_move_ids: Iterable[Any],
    *,
    context: CallerContext,
    discovered_by: str,
    run_id: str,
    embedding_model: Optional[str] = None,
    text_prefix: str = "",
    move_types: Sequence[str] = DEFAULT_MOVE_TYPES,
    lexical_limit: int = 20,
    semantic_limit: int = 20,
    per_move_cap: int = 5,
    per_target_source_cap: Optional[int] = DEFAULT_PER_TARGET_SOURCE_CAP,
    expanded_by: Optional[AdmissionAuthority] = None,
    batch_size: int = 32,
) -> Dict[str, Any]:
    """Phase 1: what embed_moves followed by discover_candidates would write,
    computed without writing anything. Refuses to run outside a READ ONLY
    transaction, so the database itself rejects any write.

    New vectors are computed exactly as embed_moves would (same todo, batches
    and text) and held in the plan. The semantic leg ranks against the stored
    vectors with the plan's vectors laid over them, which is what the stored
    table would hold after embed_moves. Lexical leg, fusion and caps are the
    functions discover_candidates uses. Each sighting's effect (inserted or
    seen again) is decided against the candidates stored now, in sighting order.
    """
    if await conn.fetchval("SHOW transaction_read_only") != "on":
        raise PlanRefused("planning runs only inside a READ ONLY transaction")
    started = time.monotonic()
    per_target_source_cap = await _discovery_cap(conn, per_target_source_cap, expanded_by)
    params = _method_params(context=context, embedding_model=embedding_model, move_types=move_types,
                            lexical_limit=lexical_limit, semantic_limit=semantic_limit, per_move_cap=per_move_cap,
                            per_target_source_cap=per_target_source_cap, expanded_by=expanded_by)
    head = await conn.fetchrow(
        "SELECT current_database() AS database, clock_timestamp() AS planned_at, "
        "txid_current_snapshot()::text AS snapshot, current_setting('transaction_isolation') AS isolation")
    if embedding_model:
        queries, existing, todo = await _embedding_todo(conn, query_move_ids, model=embedding_model,
                                                        text_prefix=text_prefix)
    else:
        queries, existing, todo = await fetch_moves(conn, query_move_ids), {}, []
    embeddings: List[Dict[str, Any]] = []
    for i in range(0, len(todo), batch_size):
        batch = todo[i:i + batch_size]
        vectors = await embedder.embed_batch([t for _, t, _ in batch])
        if len(vectors) != len(batch):
            raise RuntimeError(f"embedder returned {len(vectors)} vectors for {len(batch)} texts")
        for (mid, _, (rep, digest)), vec in zip(batch, vectors):
            text = _vector_text(vec)
            embeddings.append({
                "move_id": mid, "effect": "embedding_replaced" if mid in existing else "embedding_inserted",
                "representation_sha256": rep, "text_sha256": digest, "dimension": len(vec),
                "prior": None, "vector_sha256": _sha256(text), "vector": text})
    embed_s = time.monotonic() - started
    planned = {e["move_id"]: e for e in embeddings}

    visible = await visible_ids(conn, context, queries)
    summary = _discovery_summary(len(queries), params)
    endpoints = {mid: _endpoint_pin(queries[mid], mid in visible) for mid in planned}
    stored_used: set = set()                    # stored (not planned) vectors the ranking used
    sightings: List[Dict[str, Any]] = []
    for qid, q in sorted(queries.items()):
        if qid not in visible:
            summary["skipped_not_visible"] += 1
            continue
        if not q["eligible"]:
            summary["skipped_ineligible"] += 1
            continue
        if q["extraction"]["bound_version"] is None:
            summary["unknown_binding_queries"] += 1
        endpoints[qid] = _endpoint_pin(q, True)
        lexical = await _lexical_leg(conn, context, embedding_text(q["title"], q["detail"]),
                                     q["source"]["rid"], move_types, lexical_limit)
        semantic: List[Dict[str, Any]] = []
        if embedding_model:
            if qid in planned:
                vector, dim = planned[qid]["vector"], planned[qid]["dimension"]
            else:
                row = await conn.fetchrow(
                    "SELECT representation_sha256, text_sha256, dimension, embedding::text AS vector "
                    "FROM discourse_move_embeddings WHERE move_id = $1 AND model = $2", qid, embedding_model)
                vector = dim = None
                if row is None:
                    summary["no_embedding"] += 1
                elif row["representation_sha256"] != q["representation_sha256"]:
                    summary["stale_query_embedding"] += 1
                else:
                    vector, dim = row["vector"], row["dimension"]
                    stored_used.add(qid)
            if vector is not None:
                semantic = await _semantic_leg_planned(conn, context, vector, dim, q["source"]["rid"], move_types,
                                                       embedding_model, semantic_limit, embeddings)
        picked, targets = await _pick_targets(conn, rrf_fuse(lexical, semantic), per_move_cap=per_move_cap,
                                              per_target_source_cap=per_target_source_cap, summary=summary)
        for pick, f in enumerate(picked, start=1):
            t = targets[f["id"]]
            endpoints[t["id"]] = _endpoint_pin(t, True)          # retrieval returns visible moves only
            if f["semantic_rank"] is not None and t["id"] not in planned:
                stored_used.add(t["id"])
            sightings.append(_candidate_row(qid, q, t, f, discovered_by=discovered_by, run_id=run_id, pick=pick))
    # Pin every stored vector the plan ranked with or will replace, by value. Same snapshot as the ranking.
    stamps = await _stored_vector_stamps(conn, embedding_model, stored_used | set(existing)) if embedding_model else {}
    for e in embeddings:
        e["prior"] = stamps.get(e["move_id"])
        if (e["prior"] is None) != (e["effect"] == "embedding_inserted"):
            raise PlanRefused(f"stored vector of {e['move_id']} changed during planning")

    keys = sorted({s["candidate_key"] for s in sightings})
    stored = {r["candidate_key"]: {"times_seen": r["times_seen"], "last_seen_at": _iso(r["last_seen_at"])}
              for r in await conn.fetch(
                  "SELECT candidate_key, times_seen, last_seen_at FROM discourse_link_candidates "
                  "WHERE candidate_key = ANY($1::text[])", keys)}
    met: set = set()
    for s in sightings:
        s["effect"] = "candidate_seen" if s["candidate_key"] in stored or s["candidate_key"] in met \
            else "candidate_inserted"
        met.add(s["candidate_key"])
        summary["inserted" if s["effect"] == "candidate_inserted" else "rediscovered"] += 1
    reps = {s[k] for s in sightings for k in ("query_representation_sha256", "target_representation_sha256")}
    reps_stored = {r["representation_sha256"] for r in await conn.fetch(
        "SELECT representation_sha256 FROM discourse_claim_representations "
        "WHERE representation_sha256 = ANY($1::text[])", sorted(reps))}
    dims = sorted({e["dimension"] for e in embeddings})
    return {
        "format": PLAN_FORMAT, "run_id": run_id, "database": head["database"],
        "planned_at": head["planned_at"].isoformat(), "snapshot": head["snapshot"], "isolation": head["isolation"],
        "context": context.describe(), "discovered_by": discovered_by,
        "method": METHOD, "method_params": params,
        "embedding_profile": {"model": embedding_model, "text_prefix": text_prefix, "batch_size": batch_size,
                              "dimension": dims[0] if len(dims) == 1 else dims,
                              "embedder": getattr(embedder, "model_id", type(embedder).__name__)},
        "query_move_ids": sorted(queries),
        "summary": summary,
        "planned_effects": dict(Counter([e["effect"] for e in embeddings] + [s["effect"] for s in sightings])),
        "representations_not_yet_stored": len(reps - reps_stored),
        "timings": {"embed_s": round(embed_s, 3), "plan_s": round(time.monotonic() - started, 3)},
        "endpoints": {k: endpoints[k] for k in sorted(endpoints)},
        "stored_embeddings_used": {k: stamps[k] for k in sorted(stored_used)},
        "candidate_priors": {k: stored.get(k) for k in keys},
        "sightings": sightings,
        "embeddings": embeddings,
    }


def _check_plan_shape(plan: Dict[str, Any], run_id: str) -> None:
    """A plan's parts must agree with each other before anything is checked against the database."""
    if plan.get("format") != PLAN_FORMAT:
        raise PlanRefused(f"not a {PLAN_FORMAT} plan")
    if plan["method"] != METHOD or plan["run_id"] != run_id:
        raise PlanRefused("the plan's method or run id differs from this run")
    queries, ends = set(plan["query_move_ids"]), plan["endpoints"]
    profile = plan["embedding_profile"]
    for e in plan["embeddings"]:
        if (e["move_id"] not in queries or e["move_id"] not in ends or e["dimension"] != profile["dimension"]
                or _sha256(e["vector"]) != e["vector_sha256"]):
            raise PlanRefused(f"planned embedding for {e['move_id']} is inconsistent with the plan")
    for s in plan["sightings"]:
        q, t = ends.get(s["query_move_id"]), ends.get(s["target_move_id"])
        if (s["query_move_id"] not in queries or q is None or t is None
                or s["run_id"] != run_id or s["discovered_by"] != plan["discovered_by"]
                or (s["query_representation_sha256"], s["query_bound_version"])
                != (q["representation_sha256"], q["bound_version"])
                or (s["target_representation_sha256"], s["target_bound_version"])
                != (t["representation_sha256"], t["bound_version"])
                or s["candidate_key"] != candidate_key(
                    (s["query_move_id"], s["query_representation_sha256"], s["query_bound_version"]),
                    (s["target_move_id"], s["target_representation_sha256"], s["target_bound_version"]))):
            raise PlanRefused(f"planned candidate {s['candidate_key'][:12]} is inconsistent with the plan")
    # Each query's picks: at most the plan's per-claim limit, and when recorded, exactly 1..n in order.
    cap = plan["method_params"]["per_move_cap"]
    picks: Dict[str, List[Optional[int]]] = {}
    for s in plan["sightings"]:
        picks.setdefault(s["query_move_id"], []).append(s.get("pick"))
    for qid, got in sorted(picks.items()):
        if len(got) > cap or (any(p is not None for p in got) and got != list(range(1, len(got) + 1))):
            raise PlanRefused(f"planned picks for query {qid} are not 1..n within the per-claim limit {cap}")
    effects = Counter([e["effect"] for e in plan["embeddings"]] + [s["effect"] for s in plan["sightings"]])
    if dict(effects) != plan["planned_effects"]:
        raise PlanRefused("the plan's effect counts do not match its embeddings and candidates")


async def apply_plan(conn, plan: Dict[str, Any], *, context: CallerContext, recorder: RunRecorder,
                     expanded_by: Optional[AdmissionAuthority] = None) -> Dict[str, Any]:
    """Phase 2: write one plan's embeddings and candidates inside the caller's
    transaction, which should be short.

    First it locks every move the plan names FOR SHARE, so none can change or be
    deleted before commit, then rechecks each against the plan: present, visible
    to this context, eligible, same representation, binding, bound version and
    source version. It locks FOR SHARE and rechecks every stored vector the plan
    replaces or ranked with: its input hashes, dimension and exact value
    (VECTOR_SHA_SQL). It rechecks the stored state of every candidate it touches
    and the approved write limits. Any difference refuses the whole plan before
    a row is written. A vector row that appears after the check (the one race
    the locks cannot cover, an insert) fails the effect-count check after
    writing. Nothing is re-ranked or substituted, and retrieval is not rerun: a
    plan says nothing about material that arrived after it was made.
    """
    _check_plan_shape(plan, recorder.run_id)
    if plan["database"] != await _database(conn):
        raise PlanRefused(f"the plan was made in {plan['database']}, not {await _database(conn)}")
    if plan["context"] != context.describe():
        raise PlanRefused("the plan was made for another caller context")
    cap = plan["method_params"]["per_target_source_cap"]
    if not cap or cap > DEFAULT_PER_TARGET_SOURCE_CAP:
        await _check_authority(conn, expanded_by)
    model = plan["embedding_profile"]["model"]
    ids = sorted(plan["endpoints"])
    locked = await conn.fetch(
        "SELECT id FROM session_discourse_moves WHERE id = ANY($1::uuid[]) ORDER BY id FOR SHARE", ids)
    current = await fetch_moves(conn, ids)
    visible = await visible_ids(conn, context, ids)
    problems: List[str] = []
    for mid in ids:
        mv = current.get(mid)
        if mv is None:
            problems.append(f"move {mid}: no longer exists")
            continue
        pin, want = _endpoint_pin(mv, mid in visible), plan["endpoints"][mid]
        changed = [k for k in pin if pin[k] != want.get(k)]
        if changed:
            problems.append(f"move {mid}: {', '.join(changed)} changed")
    stamps = await _stored_vector_stamps(conn, model, ids, lock=True) if model else {}

    def vector_change(now, want):
        if now is None or want is None:
            return "appeared" if want is None else "disappeared"
        return ", ".join(k for k in want if now.get(k) != want[k])
    for e in plan["embeddings"]:
        mv = current.get(e["move_id"])
        if stamps.get(e["move_id"]) != e["prior"]:
            problems.append(f"embedding {e['move_id']}: the stored vector it replaces changed since planning "
                            f"({vector_change(stamps.get(e['move_id']), e['prior'])})")
        elif mv and _sha256(plan["embedding_profile"]["text_prefix"]
                            + embedding_text(mv["title"], mv["detail"])) != e["text_sha256"]:
            problems.append(f"embedding {e['move_id']}: text differs from the planned vector's")
    for mid, stamp in plan["stored_embeddings_used"].items():
        if stamps.get(mid) != stamp:
            problems.append(f"embedding {mid}: the stored vector the plan ranked with changed "
                            f"({vector_change(stamps.get(mid), stamp)})")
    priors = plan["candidate_priors"]
    now = {r["candidate_key"]: {"times_seen": r["times_seen"], "last_seen_at": _iso(r["last_seen_at"])}
           for r in await conn.fetch(
               "SELECT candidate_key, times_seen, last_seen_at FROM discourse_link_candidates "
               "WHERE candidate_key = ANY($1::text[])", sorted(priors))}
    problems += [f"candidate {k[:12]}: stored state changed since planning" for k in sorted(priors)
                 if now.get(k) != priors[k]]
    if problems:
        more = f" (+{len(problems) - 20} more)" if len(problems) > 20 else ""
        raise PlanRefused(f"{len(problems)} difference(s) from the plan: " + "; ".join(problems[:20]) + more)
    effects = plan["planned_effects"]
    embedding_writes = effects.get("embedding_inserted", 0) + effects.get("embedding_replaced", 0)
    if embedding_writes > recorder.max_embedding_writes:
        raise RunLimitExceeded(f"{recorder.run_id}: plan has {embedding_writes} embedding writes > approved "
                               f"{recorder.max_embedding_writes}")
    if effects.get("candidate_inserted", 0) > recorder.max_candidate_inserts:
        raise RunLimitExceeded(f"{recorder.run_id}: plan has {effects['candidate_inserted']} candidates > approved "
                               f"{recorder.max_candidate_inserts}")
    before = Counter(recorder.counts)
    for e in plan["embeddings"]:
        await _write_embedding(conn, e["move_id"], model, e["representation_sha256"], e["text_sha256"],
                               e["vector"], e["dimension"], recorder)
    for kind in ("embedding_inserted", "embedding_replaced"):
        if recorder.counts[kind] - before[kind] != effects.get(kind, 0):
            raise PlanRefused(f"a stored vector appeared after the check: {kind} "
                              f"{recorder.counts[kind] - before[kind]}, planned {effects.get(kind, 0)}")
    params_json = json.dumps(plan["method_params"])
    for s in plan["sightings"]:
        row = await _write_candidate(conn, s, params_json, recorder)
        if ("candidate_inserted" if row["inserted"] else "candidate_seen") != s["effect"]:
            raise PlanRefused(f"candidate {s['candidate_key'][:12]} was not {s['effect']} as planned")
    age = await conn.fetchval("SELECT extract(epoch FROM clock_timestamp() - $1::timestamptz)",
                              datetime.fromisoformat(plan["planned_at"]))
    return {"endpoints_locked_for_share": len(locked), "embeddings_written": len(plan["embeddings"]),
            "sightings_written": len(plan["sightings"]), "plan_age_s": round(float(age), 3),
            "retrieval_rerun": False}


# ---------------------------------------------------------------------------
# Proposal and review
# ---------------------------------------------------------------------------

def _check_label(relation: Optional[str], semantics: Optional[str]) -> None:
    if not (relation and semantics):
        raise ReviewRefused("a judgment needs an explicit relation and semantics; similarity supplies neither")
    allowed = RELATION_SEMANTICS.get(semantics)
    if allowed is None:
        raise ReviewRefused(f"unknown semantics {semantics!r}")
    if relation not in allowed:
        raise ReviewRefused(f"relation {relation!r} is not in {semantics} ({sorted(allowed)})")


async def propose_relation(conn, candidate_id: str, *, relation: str, semantics: str,
                           proposed_by: str, rationale: str) -> None:
    """A label suggestion by any caller, agents included. proposed_by is
    caller-declared. Status stays 'candidate'. Not a judgment."""
    _check_label(relation, semantics)
    if not (proposed_by and rationale and rationale.strip()):
        raise ReviewRefused("a proposal needs proposed_by and a rationale")
    done = await conn.fetchval(
        """UPDATE discourse_link_candidates
              SET proposed_relation = $2, proposed_semantics = $3,
                  proposed_by = $4, proposal_rationale = $5
            WHERE id = $1 AND status = 'candidate' RETURNING id""",
        candidate_id, relation, semantics, proposed_by, rationale)
    if done is None:
        raise ReviewRefused(f"candidate {candidate_id} is not open")


_INSERT_RELATION = """
    INSERT INTO discourse_relations
      (from_move_id, to_move_id, relation, semantics,
       from_representation_sha256, to_representation_sha256,
       from_binding_kind, to_binding_kind, from_bound_version, to_bound_version,
       origin, candidate_id, asserted_by, authority_basis, rationale)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
    ON CONFLICT DO NOTHING
    RETURNING id
"""


async def _insert_judgment(conn, ends, *, relation, semantics, origin, candidate_id, authority, rationale):
    """ends: [(move_id, representation, binding_kind, bound_version)] from -> to."""
    await _capture_representations(conn, [ends[0][0], ends[1][0]])
    rel_id = await conn.fetchval(
        _INSERT_RELATION, ends[0][0], ends[1][0], relation, semantics, ends[0][1], ends[1][1],
        ends[0][2], ends[1][2], ends[0][3], ends[1][3], origin, candidate_id,
        authority.actor, authority.basis, rationale)
    if rel_id is None:
        raise ReviewRefused("an identical judgment by this actor already exists (it may be withdrawn)")
    return str(rel_id)


async def review_candidate(conn, candidate_id: str, *, decision: str, authority: AdmissionAuthority,
                           rationale: str, relation: Optional[str] = None,
                           semantics: Optional[str] = None, reverse: bool = False) -> Optional[str]:
    """Admit or decline one open candidate. Returns the new relation id on admit.

    Admission rechecks both ends against the current rows: eligible, unchanged
    text, the bound version the candidate recorded, still current, and a
    recorded extraction binding captured with its input.
    """
    await _check_judgments_enabled(conn)
    if decision not in ("admit", "decline"):
        raise ReviewRefused(f"decision must be admit or decline, not {decision!r}")
    if not (rationale and rationale.strip()):
        raise ReviewRefused("a review needs a rationale")
    async with conn.transaction():
        cand = await conn.fetchrow(
            "SELECT * FROM discourse_link_candidates WHERE id = $1 FOR UPDATE", candidate_id)
        if cand is None:
            raise ReviewRefused(f"no candidate {candidate_id}")
        if cand["status"] != "candidate":
            raise ReviewRefused(f"candidate {candidate_id} is already {cand['status']}")
        await _check_authority(conn, authority, not_actors=(cand["discovered_by"], cand["proposed_by"]))
        if decision == "decline":
            await conn.execute(
                """UPDATE discourse_link_candidates SET status = 'declined', reviewed_by = $2,
                          review_authority = $3, reviewed_at = now(), review_rationale = $4 WHERE id = $1""",
                candidate_id, authority.actor, authority.basis, rationale)
            return None
        _check_label(relation, semantics)
        moves = await fetch_moves(conn, [cand["query_move_id"], cand["target_move_id"]])
        q, t = str(cand["query_move_id"]), str(cand["target_move_id"])
        problems = {
            "query": _end_problems(cand["query_representation_sha256"], cand["query_bound_version"],
                                   moves.get(q), for_admission=True),
            "target": _end_problems(cand["target_representation_sha256"], cand["target_bound_version"],
                                    moves.get(t), for_admission=True),
        }
        if any(problems.values()):
            detail = ", ".join(f"{k} end: {'; '.join(v)}" for k, v in problems.items() if v)
            raise ReviewRefused(f"not admissible ({detail})")
        ends = [(q, cand["query_representation_sha256"], moves[q]["extraction"]["binding_kind"],
                 cand["query_bound_version"]),
                (t, cand["target_representation_sha256"], moves[t]["extraction"]["binding_kind"],
                 cand["target_bound_version"])]
        if reverse:
            ends.reverse()
        rel_id = await _insert_judgment(conn, ends, relation=relation, semantics=semantics,
                                        origin="reviewed_candidate", candidate_id=candidate_id,
                                        authority=authority, rationale=rationale)
        await conn.execute(
            """UPDATE discourse_link_candidates SET status = 'admitted', reviewed_by = $2,
                      review_authority = $3, reviewed_at = now(), review_rationale = $4 WHERE id = $1""",
            candidate_id, authority.actor, authority.basis, rationale)
        return rel_id


async def _current_ends(conn, from_move_id: str, to_move_id: str):
    moves = await fetch_moves(conn, [from_move_id, to_move_id])
    ends = []
    for name, mid in (("from", str(from_move_id)), ("to", str(to_move_id))):
        mv = moves.get(mid)
        if mv is None:
            raise ReviewRefused(f"{name} move {mid} does not exist")
        if mv["admission_blockers"]:
            raise ReviewRefused(f"{name} end not admissible: {'; '.join(mv['admission_blockers'])}")
        ext = mv["extraction"]
        ends.append((mid, mv["representation_sha256"], ext["binding_kind"], ext["bound_version"]))
    return ends


async def record_authored_relation(conn, from_move_id: str, to_move_id: str, *, relation: str,
                                   semantics: str, authority: AdmissionAuthority, rationale: str) -> str:
    """A judgment asserted directly, with no candidate behind it, bound to the
    current representation and recorded binding of both ends."""
    await _check_judgments_enabled(conn)
    _check_label(relation, semantics)
    await _check_authority(conn, authority)
    if not (rationale and rationale.strip()):
        raise ReviewRefused("an authored relation needs a rationale")
    async with conn.transaction():
        ends = await _current_ends(conn, from_move_id, to_move_id)
        return await _insert_judgment(conn, ends, relation=relation, semantics=semantics, origin="authored",
                                      candidate_id=None, authority=authority, rationale=rationale)


# ---------------------------------------------------------------------------
# A3: withdrawal, correction, reopening
# ---------------------------------------------------------------------------

async def _terminal_event(conn, relation_id: str):
    return await conn.fetchrow(
        "SELECT * FROM discourse_review_events WHERE target_kind = 'relation' AND relation_id = $1", relation_id)


async def withdraw_relation(conn, relation_id: str, *, authority: AdmissionAuthority, rationale: str) -> str:
    """Take a judgment out of current counts. The row stays; history shows why."""
    await _check_judgments_enabled(conn)
    await _check_authority(conn, authority, bases=(LOCAL_CREDENTIAL,))
    if not (rationale and rationale.strip()):
        raise ReviewRefused("a withdrawal needs a rationale")
    async with conn.transaction():
        if await conn.fetchval("SELECT id FROM discourse_relations WHERE id = $1 FOR UPDATE", relation_id) is None:
            raise ReviewRefused(f"no judgment {relation_id}")
        if await _terminal_event(conn, relation_id):
            raise ReviewRefused(f"judgment {relation_id} is already withdrawn or corrected")
        return str(await conn.fetchval(
            """INSERT INTO discourse_review_events (target_kind, relation_id, action, actor, authority_basis, rationale)
               VALUES ('relation', $1, 'withdraw', $2, $3, $4) RETURNING id""",
            relation_id, authority.actor, authority.basis, rationale))


async def correct_relation(conn, relation_id: str, *, authority: AdmissionAuthority, rationale: str,
                           relation: str, semantics: str, reverse: bool = False) -> str:
    """Replace a judgment: the old one is marked corrected, and a new one on
    the same two moves is bound to their current text and binding. Returns the
    new judgment's id."""
    await _check_judgments_enabled(conn)
    await _check_authority(conn, authority, bases=(LOCAL_CREDENTIAL,))
    _check_label(relation, semantics)
    if not (rationale and rationale.strip()):
        raise ReviewRefused("a correction needs a rationale")
    async with conn.transaction():
        old = await conn.fetchrow("SELECT * FROM discourse_relations WHERE id = $1 FOR UPDATE", relation_id)
        if old is None:
            raise ReviewRefused(f"no judgment {relation_id}")
        if await _terminal_event(conn, relation_id):
            raise ReviewRefused(f"judgment {relation_id} is already withdrawn or corrected")
        a, b = str(old["from_move_id"]), str(old["to_move_id"])
        ends = await _current_ends(conn, *((b, a) if reverse else (a, b)))
        try:
            new_id = await _insert_judgment(conn, ends, relation=relation, semantics=semantics,
                                            origin="correction", candidate_id=None,
                                            authority=authority, rationale=rationale)
        except ReviewRefused:
            raise ReviewRefused("a correction must change the judgment; to retract it, withdraw it")
        await conn.execute(
            """INSERT INTO discourse_review_events
                 (target_kind, relation_id, action, actor, authority_basis, rationale, replaced_by_relation_id)
               VALUES ('relation', $1, 'correct', $2, $3, $4, $5)""",
            relation_id, authority.actor, authority.basis, rationale, new_id)
        return new_id


async def reopen_candidate(conn, candidate_id: str, *, authority: AdmissionAuthority, rationale: str) -> str:
    """Return a declined candidate to review. Its earlier decision is kept in
    the event's prior_state. Admitted candidates are corrected through their
    judgment instead."""
    await _check_judgments_enabled(conn)
    await _check_authority(conn, authority, bases=(LOCAL_CREDENTIAL,))
    if not (rationale and rationale.strip()):
        raise ReviewRefused("reopening needs a rationale")
    async with conn.transaction():
        cand = await conn.fetchrow("SELECT * FROM discourse_link_candidates WHERE id = $1 FOR UPDATE", candidate_id)
        if cand is None or cand["status"] != "declined":
            raise ReviewRefused(f"candidate {candidate_id} is not declined")
        prior = {k: _iso(cand[k]) for k in ("status", "reviewed_by", "review_authority", "reviewed_at",
                                             "review_rationale")}
        event_id = await conn.fetchval(
            """INSERT INTO discourse_review_events
                 (target_kind, candidate_id, action, actor, authority_basis, rationale, prior_state)
               VALUES ('candidate', $1, 'reopen', $2, $3, $4, $5::jsonb) RETURNING id""",
            candidate_id, authority.actor, authority.basis, rationale, json.dumps(prior))
        await conn.execute(
            """UPDATE discourse_link_candidates SET status = 'candidate', reviewed_by = NULL,
                      review_authority = NULL, reviewed_at = NULL, review_rationale = NULL WHERE id = $1""",
            candidate_id)
        return str(event_id)


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------

async def connected_material(conn, move_id: str, *, context: CallerContext, include_candidates: bool = False,
                             candidate_statuses: Sequence[str] = ("candidate",)) -> Dict[str, Any]:
    """One visible move with its judgments in both directions and (on request)
    its candidates, limited to what the context may see.

    `relations` holds current judgments: not withdrawn or corrected, both ends
    eligible, unchanged text, unchanged bound version, source not changed since
    extraction. Everything else is in `historical_relations`, with reasons and
    the exact text each end had when judged (or a statement that only its hash
    remains). Candidates are a third list with the same freshness check plus
    whether they could be admitted now.
    """
    mid = str(move_id)
    if mid not in await visible_ids(conn, context, [mid]):
        raise NotVisible(mid)
    rels = await conn.fetch(
        "SELECT * FROM discourse_relations WHERE from_move_id = $1 OR to_move_id = $1 "
        "ORDER BY semantics, relation, created_at, id", mid)
    cands = []
    if include_candidates:
        cands = await conn.fetch(
            """SELECT * FROM discourse_link_candidates
                WHERE (query_move_id = $1 OR target_move_id = $1) AND status = ANY($2::text[])
                ORDER BY fused_score DESC, id""",
            mid, list(candidate_statuses))
    vis, vp = _visibility(context, "t", 2)
    open_count = await conn.fetchval(
        f"""SELECT count(*) FROM discourse_link_candidates c
              JOIN session_discourse_moves t
                ON t.id = CASE WHEN c.query_move_id = $1 THEN c.target_move_id ELSE c.query_move_id END
             WHERE (c.query_move_id = $1 OR c.target_move_id = $1) AND c.status = 'candidate' AND {vis}""",
        mid, *vp)

    def other(row, a, b):
        return str(row[b] if str(row[a]) == mid else row[a])

    others = {other(r, "from_move_id", "to_move_id") for r in rels}
    others |= {other(c, "query_move_id", "target_move_id") for c in cands}
    seen = await visible_ids(conn, context, others)
    rels = [r for r in rels if other(r, "from_move_id", "to_move_id") in seen]
    cands = [c for c in cands if other(c, "query_move_id", "target_move_id") in seen]
    moves = await fetch_moves(conn, seen | {mid})
    events = {str(e["relation_id"]): e for e in await conn.fetch(
        "SELECT * FROM discourse_review_events WHERE target_kind = 'relation' AND relation_id = ANY($1::uuid[])",
        [str(r["id"]) for r in rels])}
    reps = await _representations(conn, [h for r in rels for h in (r["from_representation_sha256"],
                                                                    r["to_representation_sha256"])])

    current, historical = [], []
    for r in rels:
        outbound = str(r["from_move_id"]) == mid
        problems = (_end_problems(r["from_representation_sha256"], r["from_bound_version"],
                                  moves.get(str(r["from_move_id"])), for_admission=False)
                    + _end_problems(r["to_representation_sha256"], r["to_bound_version"],
                                    moves.get(str(r["to_move_id"])), for_admission=False))
        ev = events.get(str(r["id"]))
        if ev is not None:
            problems.append("withdrawn" if ev["action"] == "withdraw" else "corrected")
        entry = {
            "relation_id": str(r["id"]), "relation": r["relation"], "semantics": r["semantics"],
            "direction": "outbound" if outbound else "inbound", "origin": r["origin"],
            "asserted_by": r["asserted_by"], "authority_basis": r["authority_basis"],
            "rationale": r["rationale"], "created_at": r["created_at"].isoformat(),
            "candidate_id": str(r["candidate_id"]) if r["candidate_id"] else None,
            "judged": {"from": {**_text_of(reps, r["from_representation_sha256"]),
                                "binding_kind": r["from_binding_kind"], "bound_version": r["from_bound_version"]},
                       "to": {**_text_of(reps, r["to_representation_sha256"]),
                              "binding_kind": r["to_binding_kind"], "bound_version": r["to_bound_version"]}},
            "terminal_event": ({"action": ev["action"], "actor": ev["actor"], "rationale": ev["rationale"],
                                "replaced_by": str(ev["replaced_by_relation_id"]) if ev["replaced_by_relation_id"] else None,
                                "at": ev["created_at"].isoformat()} if ev is not None else None),
            "current": not problems, "not_current_reasons": sorted(set(problems)),
            "other": moves[other(r, "from_move_id", "to_move_id")],
        }
        (current if not problems else historical).append(entry)

    candidates = []
    for c in cands:
        ends = ((c["query_representation_sha256"], c["query_bound_version"], moves.get(str(c["query_move_id"]))),
                (c["target_representation_sha256"], c["target_bound_version"], moves.get(str(c["target_move_id"]))))
        problems = sum((_end_problems(rp, bv, mv, for_admission=False) for rp, bv, mv in ends), [])
        blockers = sum((_end_problems(rp, bv, mv, for_admission=True) for rp, bv, mv in ends), [])
        candidates.append({
            "candidate_id": str(c["id"]), "status": c["status"], "method": c["method"],
            "method_params": json.loads(c["method_params"]) if isinstance(c["method_params"], str) else c["method_params"],
            "scores": {k: c[k] for k in ("fused_score", "lexical_rank", "lexical_score",
                                         "semantic_rank", "semantic_score")},
            "discovered_by": c["discovered_by"], "run_id": c["run_id"], "times_seen": c["times_seen"],
            "proposal": ({"relation": c["proposed_relation"], "semantics": c["proposed_semantics"],
                          "declared_by": c["proposed_by"], "rationale": c["proposal_rationale"]}
                         if c["proposed_by"] else None),
            "current": not problems, "not_current_reasons": sorted(set(problems)),
            "admissible_now": not blockers, "admission_blockers": sorted(set(blockers)),
            "other": moves[other(c, "query_move_id", "target_move_id")],
        })
    return {"move": moves[mid], "context": context.describe(), "relations": current,
            "historical_relations": historical, "candidates": candidates, "open_candidate_count": int(open_count)}


async def search_connected(conn, query: str, *, context: CallerContext, limit: int = 5,
                           include_candidates: bool = False) -> Dict[str, Any]:
    """Text query -> visible matching moves -> each hit's connected material.

    Same lexical semantics as /knowledge/discourse-search (plainto_tsquery,
    ts_rank), but the visibility predicate runs before LIMIT. Filtering that
    endpoint's results afterwards would let hidden moves use up the limit and
    decide which visible moves appear.
    """
    vis, vp = _visibility(context, "t", 3)
    rows = await conn.fetch(
        f"""SELECT t.id FROM session_discourse_moves t
             WHERE t.source_type = 'document' AND t.superseded_at IS NULL AND {vis}
               AND to_tsvector('english', coalesce(t.title,'')||' '||coalesce(t.detail,''))
                   @@ plainto_tsquery('english', $1)
             ORDER BY ts_rank(to_tsvector('english', coalesce(t.title,'')||' '||coalesce(t.detail,'')),
                              plainto_tsquery('english', $1)) DESC, t.created_at DESC, t.id
             LIMIT $2""", query, limit, *vp)
    results = [await connected_material(conn, str(r["id"]), context=context, include_candidates=include_candidates)
               for r in rows]
    return {"query": query, "query_mode": "lexical", "context": context.describe(), "results": results}


async def corroboration_report(conn, move_id: str, *, context: CallerContext) -> Dict[str, Any]:
    """What currently supports a move, within what the context may see.

    Counts only current epistemic `supports` judgments, once per supporting
    move however many actors judged it. Historical judgments are listed, not
    counted. A supporting move is `restates_this_claim` only when a current
    `restates` joins that same move to this one. Independence is never
    evaluated here, so it is reported as unevaluated, not as zero.
    """
    view = await connected_material(conn, move_id, context=context, include_candidates=True)

    def inbound(rows, relation, semantics):
        return [r for r in rows if r["direction"] == "inbound"
                and r["relation"] == relation and r["semantics"] == semantics]

    restating = {r["other"]["id"] for r in inbound(view["relations"], "restates", "lineage/v0")}
    by_move: Dict[str, Dict[str, Any]] = {}
    for r in inbound(view["relations"], "supports", "epistemic-relation/v0"):
        entry = by_move.setdefault(r["other"]["id"], {
            "move_id": r["other"]["id"], "source_rid": r["other"]["source"]["rid"],
            "dependence": "restates_this_claim" if r["other"]["id"] in restating else "not_assessed",
            "judgments": []})
        entry["judgments"].append({"relation_id": r["relation_id"], "asserted_by": r["asserted_by"],
                                   "authority_basis": r["authority_basis"], "origin": r["origin"]})
    historical = [{"relation_id": r["relation_id"], "move_id": r["other"]["id"],
                   "asserted_by": r["asserted_by"], "reasons": r["not_current_reasons"]}
                  for r in inbound(view["historical_relations"], "supports", "epistemic-relation/v0")]
    supporting = sorted(by_move.values(), key=lambda e: e["move_id"])
    return {
        "move_id": view["move"]["id"],
        "context": view["context"],
        "current_supporting_moves": len(supporting),
        "current_supporting_judgments": sum(len(e["judgments"]) for e in supporting),
        "supporting": supporting,
        "historical_supporting_judgments": historical,
        "current_restating_moves": len(restating),
        "independence": {
            "evaluated": False, "independent_sources": None,
            "reason": "no scoped independence or source-lineage assessment exists in this prototype",
        },
        "open_candidates_not_counted": view["open_candidate_count"],
        "naive_linked_count": len(view["relations"]) + len(view["historical_relations"])
                              + view["open_candidate_count"],
    }


# ---------------------------------------------------------------------------
# Read-only reports
# ---------------------------------------------------------------------------

async def retrieval_coverage(conn, model: Optional[str] = None, *, context: CallerContext,
                             move_types: Sequence[str] = DEFAULT_MOVE_TYPES) -> Dict[str, Any]:
    """How much of the visible eligible corpus each leg can reach. SELECTs only.
    Works on a database without the draft tables (semantic reach is then 0)."""
    has_emb = await _has_table(conn, "discourse_move_embeddings")
    has_bind = await _has_table(conn, "discourse_move_extraction_bindings")
    emb = ("LEFT JOIN discourse_move_embeddings e ON e.move_id = m.id AND e.model = $2"
           if has_emb and model else
           "LEFT JOIN (SELECT NULL::uuid AS move_id, NULL::text AS representation_sha256) e ON false")
    bind = "discourse_move_extraction_bindings" if has_bind else _NO_BINDINGS
    start = 3 if has_emb and model else 2
    vis, vp = _visibility(context, "m", start)
    row = await conn.fetchrow(
        f"""SELECT count(*) AS eligible,
                   count(*) FILTER (WHERE to_tsvector('english', coalesce(m.title,'')||' '||coalesce(m.detail,''))
                                          <> ''::tsvector) AS lexical,
                   count(e.move_id) FILTER (WHERE e.representation_sha256 = {REPR_SQL.format(a='m')}) AS semantic_current,
                   count(e.move_id) FILTER (WHERE e.representation_sha256 <> {REPR_SQL.format(a='m')}) AS semantic_stale,
                   count(*) FILTER (WHERE ({BINDING_KIND_SQL}) = 'recorded_extraction') AS binding_recorded_extraction,
                   count(*) FILTER (WHERE ({BINDING_KIND_SQL}) = 'content_addressed_source') AS binding_content_addressed_source,
                   count(*) FILTER (WHERE ({BINDING_KIND_SQL}) = 'inferred_timestamp') AS binding_inferred_timestamp,
                   count(*) FILTER (WHERE ({BINDING_KIND_SQL}) = 'unknown') AS binding_unknown
              FROM session_discourse_moves m
              JOIN document_ingestion_log l ON l.document_rid = m.source_rid
              LEFT JOIN koi_memories km ON km.rid = m.source_rid
              LEFT JOIN {bind} b ON b.move_id = m.id
              {emb}
             WHERE m.source_type = 'document' AND m.superseded_at IS NULL
               AND m.move_type = ANY($1::text[]) AND {vis}""",
        *([list(move_types), model] if has_emb and model else [list(move_types)]), *vp)
    return {"model": model, "context": context.describe(), "embeddings_table": has_emb, **dict(row)}


async def same_work_signals(conn, *, context: CallerContext, min_cosine: float = 0.9,
                            min_pairs: int = 3) -> List[Dict[str, Any]]:
    """Source pairs, both visible, whose candidates look like two copies of one
    work. A signal for a person to confirm. Nothing is merged or hidden."""
    v1, p1 = _visibility(context, "qm", 3)
    v2, p2 = _visibility(context, "tm", 9)
    rows = await conn.fetch(
        f"""SELECT least(c.query_source_rid, c.target_source_rid) AS source_a,
                   greatest(c.query_source_rid, c.target_source_rid) AS source_b,
                   count(*) AS candidates,
                   count(*) FILTER (WHERE c.semantic_score >= $1) AS near_identical,
                   max(c.semantic_score) AS max_cosine
              FROM discourse_link_candidates c
              JOIN session_discourse_moves qm ON qm.id = c.query_move_id
              JOIN session_discourse_moves tm ON tm.id = c.target_move_id
             WHERE {v1} AND {v2}
             GROUP BY 1, 2
            HAVING count(*) FILTER (WHERE c.semantic_score >= $1) >= $2
             ORDER BY near_identical DESC, source_a, source_b""",
        min_cosine, min_pairs, *p1, *p2)
    return [dict(r) for r in rows]


# Fields a seed manifest pins per move. Embedding status is left out: it is
# what a run is allowed to change, and the run's write limits bound it.
MANIFEST_PINNED_FIELDS = (
    "move_id", "source_rid", "move_type", "title", "representation_sha256", "eligible",
    "ineligible_reasons", "lexical_indexable", "source_version", "binding_kind", "bound_version",
    "binding_current", "seed_eligible",
)


async def seed_manifest(conn, source_rids: Sequence[str], *, context: CallerContext,
                        model: Optional[str] = None) -> Dict[str, Any]:
    """Move manifest and eligibility report for a proposed seed set, limited to
    what the context may see. SELECTs only; works without the draft tables.

    Visibility is applied in the query that selects moves, so a hidden move adds
    no entry, title, id, hash or count. A requested source with nothing visible
    is listed as unavailable, the same whether it is hidden or absent.
    """
    vis, vp = _visibility(context, "m", 2)
    rows = await conn.fetch(
        f"""SELECT m.id FROM session_discourse_moves m
             WHERE m.source_type = 'document' AND m.source_rid = ANY($1::text[]) AND {vis}
             ORDER BY m.source_rid, m.turn_range_start NULLS FIRST, m.id""", list(source_rids), *vp)
    ids = [str(r["id"]) for r in rows]
    moves = await fetch_moves(conn, ids)
    emb: Dict[str, str] = {}
    if model and await _has_table(conn, "discourse_move_embeddings"):
        emb = {str(r["move_id"]): r["representation_sha256"] for r in await conn.fetch(
            "SELECT move_id, representation_sha256 FROM discourse_move_embeddings "
            "WHERE model = $1 AND move_id = ANY($2::uuid[])", model, ids)}
    entries = []
    for mid in ids:
        mv = moves[mid]
        ext = mv["extraction"]
        stored = emb.get(mid)
        entries.append({
            "move_id": mid, "source_rid": mv["source"]["rid"], "move_type": mv["move_type"],
            "title": mv["title"], "representation_sha256": mv["representation_sha256"],
            "eligible": mv["eligible"], "ineligible_reasons": mv["ineligible_reasons"],
            "lexical_indexable": mv["lexical_indexable"],
            "source_version": mv["source"]["version"], "source_version_basis": mv["source"]["version_basis"],
            "binding_kind": ext["binding_kind"], "binding_basis": ext["binding_basis"],
            "bound_version": ext["bound_version"], "binding_current": ext["current"],
            "admissible_for_live_judgment": mv["admissible"], "admission_blockers": mv["admission_blockers"],
            "embedding": ("current" if stored == mv["representation_sha256"] else
                          "stale" if stored else "none"),
            "seed_eligible": mv["eligible"] and mv["lexical_indexable"],
        })
    by_source: Dict[str, Dict[str, Any]] = {}
    for e in entries:
        s = by_source.setdefault(e["source_rid"], {"source_rid": e["source_rid"], "moves": 0, "seed_eligible": 0,
                                                   "admissible_for_live_judgment": 0, "binding_kinds": {}})
        s["moves"] += 1
        s["seed_eligible"] += e["seed_eligible"]
        s["admissible_for_live_judgment"] += e["admissible_for_live_judgment"]
        s["binding_kinds"][e["binding_kind"]] = s["binding_kinds"].get(e["binding_kind"], 0) + 1
    return {
        "context": context.describe(), "model": model,
        "requested_sources": list(source_rids),
        "unavailable_sources": sorted(set(source_rids) - set(by_source)),
        "moves": len(entries), "seed_eligible": sum(e["seed_eligible"] for e in entries),
        "admissible_for_live_judgment": sum(e["admissible_for_live_judgment"] for e in entries),
        "sources": [by_source[r] for r in sorted(by_source)],
        "entries": entries,
    }


def manifest_pins(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The per-move fields an approved manifest fixes, in a comparable form."""
    return [{k: e[k] for k in MANIFEST_PINNED_FIELDS} for e in manifest["entries"]]
