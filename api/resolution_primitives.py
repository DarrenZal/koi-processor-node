"""Pure entity resolution primitives.

No module-level state. No external API calls.
Embedding generation is injected as an optional async callable.

Copied from personal_ingest_api.py for use in the KOI-net pipeline
without pulling in the full app module and its side effects.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Awaitable, Callable, List, Optional, Tuple

from api.entity_schema import get_schema_for_type
from api.resolver_shadow import start_attempt

logger = logging.getLogger(__name__)

# Token overlap constants (matches personal_ingest_api.py)
MIN_TOKEN_OVERLAP_RATIO = 0.5
MIN_TOKEN_OVERLAP_COUNT = 2

# `/entities/merge` TOMBSTONES the loser (sets merged_into) rather than deleting it, and
# deliberately keeps its embedding and its aliases so history stays resolvable. That makes
# every merge ever performed leave a row that still matches by name and still competes in
# the ANN. 202 of them exist; 167 share normalized_text with the live survivor.
#
# There are TWO correct responses and picking the wrong one causes a different bug:
#
#   RETRIEVAL (ANN, search results, list endpoints) -> EXCLUDE. The live row is in the same
#   index, so a tombstone is a pure duplicate that costs a slot and can be cited by an LLM.
#
#   RESOLUTION (name -> uri, before writing) -> FOLLOW. Excluding here is actively worse
#   than doing nothing: the lookup misses, falls through to create_new, and mints a THIRD
#   row for a name that already has a canonical home. A tombstone match is positive
#   evidence that this name belongs to the survivor.
#
# Follows transitively, because merges chain. 14 rows in the live registry are two hops
# from a live entity (project-pol.is -> softwareapplication-polis -> concept-pol.is), so a
# single-hop follow lands on another tombstone and looks like it worked.
MAX_MERGE_CHAIN = 10


async def resolve_to_live_uri(conn, uri: Optional[str]) -> Optional[str]:
    """Walk merged_into to the surviving entity. Returns `uri` unchanged if it is live.

    Cycle-safe: a merge cycle (A -> B -> A) would otherwise spin forever inside a request.
    On a cycle or an over-long chain it returns the last URI reached rather than raising —
    a slightly-wrong URI degrades a result, an exception fails a whole ingest.
    """
    if not uri:
        return uri
    seen = {uri}
    current = uri
    for _ in range(MAX_MERGE_CHAIN):
        row = await conn.fetchrow(
            "SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1", current
        )
        if row is None or not row["merged_into"]:
            return current
        nxt = row["merged_into"]
        if nxt in seen:
            logger.warning("merge cycle at %s; stopping on %s", nxt, current)
            return current
        seen.add(nxt)
        current = nxt
    logger.warning("merge chain from %s exceeded %d hops", uri, MAX_MERGE_CHAIN)
    return current


def normalize_entity_text(text: str) -> str:
    """Normalize entity text for comparison."""
    return (
        text.lower()
        .strip()
        .replace("_", " ")
        .replace("-", " ")
        .replace("  ", " ")
        .lstrip("@")
    )


def normalize_alias(alias: Any) -> str:
    """Strip [[...]], lowercase, trim for alias matching.

    Handles wikilinks like [[People/Name|Display]] -> name
    """
    alias = str(alias)
    alias = re.sub(r"\[\[([^\]|]+)(\|[^\]]+)?\]\]", r"\1", alias)
    if "/" in alias:
        alias = alias.rsplit("/", 1)[-1]
    alias = alias.lower().strip()
    return alias


def normalize_alias_list(values: Any) -> list:
    """Normalize a list of alias strings to the canonical matchable form.

    Applies normalize_alias() to each value, drops empties, and dedupes while
    preserving first-seen order. Use this at EVERY alias-write path so stored
    aliases stay in the same form the resolver queries with
    (WHERE $2 = ANY(aliases), $2 = normalize_alias(input)). See plan
    alias-normalization-fix.
    """
    if values is None:
        return []
    if isinstance(values, str):
        values = [values]
    out: list = []
    seen = set()
    for v in values:
        if v is None:
            continue  # skip before normalize_alias (str(None) -> "none")
        n = normalize_alias(v)
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def jaro_winkler_similarity(s1: str, s2: str) -> float:
    """Calculate Jaro-Winkler similarity between two strings."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    len1, len2 = len(s1), len(s2)
    match_distance = max(len1, len2) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0

    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (
        matches / len1
        + matches / len2
        + (matches - transpositions / 2) / matches
    ) / 3

    prefix_len = 0
    for i in range(min(4, min(len1, len2))):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    return jaro + prefix_len * 0.1 * (1 - jaro)


def compute_token_overlap(text1: str, text2: str) -> Tuple[float, int]:
    """Compute token (word) overlap between two texts.

    Returns: (overlap_ratio, overlap_count)
    """
    tokens1 = set(text1.lower().split())
    tokens2 = set(text2.lower().split())
    overlap = tokens1 & tokens2
    overlap_count = len(overlap)
    shorter_len = min(len(tokens1), len(tokens2))
    if shorter_len == 0:
        return 0.0, 0
    return overlap_count / shorter_len, overlap_count


def passes_token_overlap_legacy(text1: str, text2: str, entity_type: str) -> bool:
    """The permissive token-overlap policy used by the shared resolver.

    This name is intentionally explicit.  A stricter ingest policy historically
    existed under the same ``passes_token_overlap_check`` name in
    ``personal_ingest_api`` and shadowed this implementation.  Keeping the two
    policies named independently lets us measure them without changing either
    caller's production behavior.
    """
    schema = get_schema_for_type(entity_type)
    if not schema.require_token_overlap:
        return True

    overlap_ratio, overlap_count = compute_token_overlap(text1, text2)
    tokens1 = text1.lower().split()
    tokens2 = text2.lower().split()
    if len(tokens1) == 1 or len(tokens2) == 1:
        return True
    if overlap_ratio < MIN_TOKEN_OVERLAP_RATIO:
        return False
    if overlap_count < MIN_TOKEN_OVERLAP_COUNT:
        return False
    return True


# =============================================================================
# Shared resolver name guards (P1 — 2026-07-13)
#
# These are the single source of truth for the person-name, distinctive-token
# and semantic-match guards. `personal_ingest_api.py` imports them (no
# duplication) and wires them into its Tier 1.5 / 2a / 2b, and
# `resolve_entity_multi_tier` below applies them in its Tier 2a / 2b.
# =============================================================================

# Generic "scaffolding" tokens that carry little identity signal for
# Organization / Project / Concept names. Two names that share ONLY generic
# tokens (e.g. "University of Guelph" vs "University of Melbourne") are not the
# same entity.
GENERIC_NAME_TOKENS = {
    "of", "the", "and", "for", "a", "in",
    "institute", "university", "college", "foundation", "network",
    "initiative", "project", "program", "alliance", "association",
    "council", "group", "lab", "labs", "camp", "team", "fund", "trust",
    "society", "collective", "coalition", "centre", "center",
}


def passes_person_name_guard(query_norm: str, cand_norm: str) -> bool:
    """Guard Person merges against first-name / single-token collisions.

    Applied wherever a Person candidate would be accepted by fuzzy, semantic,
    or contextual similarity. Registered aliases are resolved earlier
    (Tier 1.1), so this guard can be strict:

    - Multi-token vs single-token (either direction) -> False. A bare first
      name ("Dan", "Calla", "Abel") must not collapse into a full name
      unless a registered alias says so ("Dan" -> Danica Tidewell-Thornbury is handled
      by Tier 1.1, not here).
    - Both multi-token -> require Jaro-Winkler >= 0.85 on the FIRST tokens AND
      >= 0.85 on the LAST tokens, independently. Rejects "Calla Northwick" vs
      "Calla Amberly" (last JW 0.42), "Kaia Orrell" vs "Kaia Tamworth"
      (last 0.43), "Saffi Wilsley" vs "Saffi Wilson" (last 0.848) while
      allowing spelling variants of the same full name.
    - Single vs single -> True; the caller's single-word JW>=0.95 rule
      (passes_token_overlap_strict in personal ingest) is the gate for those.

    Inputs are expected to be normalized (lowercased, hyphen/underscore ->
    space) but the function lowercases defensively.
    """
    q = query_norm.lower().split()
    c = cand_norm.lower().split()
    if not q or not c:
        return False
    q_multi = len(q) >= 2
    c_multi = len(c) >= 2
    if q_multi != c_multi:
        return False  # multi vs single — never merge without a registered alias
    if not q_multi:
        return True   # single vs single — deferred to single-word JW rule
    first_jw = jaro_winkler_similarity(q[0], c[0])
    last_jw = jaro_winkler_similarity(q[-1], c[-1])
    return first_jw >= 0.85 and last_jw >= 0.85


def _distinctive_tokens(text: str) -> set:
    """Tokens of `text` with generic scaffolding words removed."""
    return {t for t in text.lower().split() if t not in GENERIC_NAME_TOKENS}


def passes_distinctive_token_check(text1: str, text2: str) -> bool:
    """Distinctive-token guard for Organization / Project / Concept merges.

    Strips generic tokens (of / the / institute / university / network / ...)
    and reasons about the remaining *distinctive* tokens:

    - Both sides have >=1 distinctive token and the distinctive sets are
      DISJOINT -> reject. E.g. "University of Guelph" vs "University of
      Melbourne" -> {guelph} vs {melbourne}.
    - One side's distinctive set is a STRICT SUPERSET of the other's (a
      token-level extension, e.g. {dweb, cascadia} over {dweb}) -> reject
      unless the full strings are near-identical (JW >= 0.97).

    Otherwise pass (defer to the caller's token-overlap-count / JW rules).
    Returns True when either side has no distinctive tokens (nothing to
    compare) so purely-generic names fall through to other checks.
    """
    d1 = _distinctive_tokens(text1)
    d2 = _distinctive_tokens(text2)
    if not d1 or not d2:
        return True
    if d1.isdisjoint(d2):
        return False
    if d1 < d2 or d2 < d1:  # strict superset in either direction
        if jaro_winkler_similarity(text1.lower(), text2.lower()) < 0.97:
            return False
    return True


_LEADING_DATE_RE = re.compile(r"^\s*(\d{4})[-\s](\d{2})[-\s](\d{2})(?![\d])")


def extract_leading_meeting_date(text: Optional[str]) -> Optional[str]:
    """Return a validated leading YYYY-MM-DD from a raw or normalized title.

    The guard is leading-only by design.  Searching elsewhere in a title would
    treat subject dates as meeting identity.  Missing or malformed dates fall
    through to the surrounding resolution policy; only two parsed and unequal
    dates are a rejection signal.
    """
    if not text:
        return None
    match = _LEADING_DATE_RE.match(text)
    if not match:
        return None
    year, month, day = match.group(1), match.group(2), match.group(3)
    if not (1 <= int(month) <= 12 and 1 <= int(day) <= 31):
        return None
    return f"{year}-{month}-{day}"


def passes_token_overlap_strict(text1: str, text2: str, entity_type: str) -> bool:
    """The strict identity policy historically local to personal ingest.

    This is a behavior-preserving extraction, not a consolidation.  In
    particular, it carries forward the Meeting leading-date guard that prevents
    separate meetings in the same series from collapsing after normalization.
    """
    tokens1 = text1.lower().split()
    tokens2 = text2.lower().split()

    # Single-word guard applies to every schema, including schemas that bypass
    # the multi-word overlap rule.
    if len(tokens1) == 1 or len(tokens2) == 1:
        jw = jaro_winkler_similarity(text1.lower(), text2.lower())
        if jw < 0.95:
            return False
        s1, s2 = text1.lower(), text2.lower()
        if len(s1) != len(s2):
            shorter, longer = (s1, s2) if len(s1) < len(s2) else (s2, s1)
            if longer.startswith(shorter) and len(longer) - len(shorter) >= 2:
                return False
        return True

    if entity_type in ("Organization", "Project", "Concept"):
        if not passes_distinctive_token_check(text1, text2):
            return False

    if entity_type == "Meeting":
        date1 = extract_leading_meeting_date(text1)
        date2 = extract_leading_meeting_date(text2)
        if date1 and date2 and date1 != date2:
            return False

    schema = get_schema_for_type(entity_type)
    if not schema.require_token_overlap:
        if entity_type == "Person":
            return passes_person_name_guard(text1, text2)
        if len(tokens1) == 2 and len(tokens2) == 2:
            last_jw = jaro_winkler_similarity(tokens1[-1], tokens2[-1])
            if last_jw < 0.75:
                return False
        return True

    overlap_ratio, overlap_count = compute_token_overlap(text1, text2)
    return (
        overlap_ratio >= MIN_TOKEN_OVERLAP_RATIO
        and overlap_count >= MIN_TOKEN_OVERLAP_COUNT
    )


def passes_semantic_match_guard(
    entity_type: str,
    query_norm: str,
    match_text_norm: str,
    similarity: float,
    threshold: float,
) -> bool:
    return passes_semantic_match_guard_with_policy(
        entity_type,
        query_norm,
        match_text_norm,
        similarity,
        threshold,
        passes_token_overlap_legacy,
    )


def passes_semantic_match_guard_with_policy(
    entity_type: str,
    query_norm: str,
    match_text_norm: str,
    similarity: float,
    threshold: float,
    token_policy: Callable[[str, str, str], bool],
) -> bool:
    """Composite guard applied to a Tier 2b (semantic) candidate before accept.

    Semantic similarity alone conflates same-first-name people, short generic
    names, and disjoint multi-word names. This layers the name-shape guards on
    top:

    (a) Person -> passes_person_name_guard.
    (b) Any multi-token pair -> the supplied token policy (must share tokens),
        plus the distinctive-token guard for Organization/Project/Concept.
    (c) Short names (query <=2 tokens or <12 chars) -> require the embedding
        similarity to clear threshold by a 0.03 margin (short names embed
        weakly, so demand a stronger signal).
    """
    if entity_type == "Person" and not passes_person_name_guard(query_norm, match_text_norm):
        return False

    q = query_norm.split()
    m = match_text_norm.split()
    if len(q) >= 2 and len(m) >= 2:
        if not token_policy(query_norm, match_text_norm, entity_type):
            return False
    if entity_type in ("Organization", "Project", "Concept"):
        if not passes_distinctive_token_check(query_norm, match_text_norm):
            return False

    if len(q) <= 2 or len(query_norm) < 12:
        if similarity < threshold + 0.03:
            return False
    return True


async def resolve_entity_multi_tier(
    conn,
    entity_name: str,
    entity_type: str,
    mode: str = "exact_alias",
    embed_fn: Optional[Callable[[str], Awaitable[Optional[List[float]]]]] = None,
    resolution_caller: str = "resolution_primitives.resolve_entity_multi_tier",
) -> Tuple[Optional[str], float, str]:
    """Multi-tier resolution, guaranteed to return a LIVE uri.

    Deliberately a wrapper rather than four edits inside the tiers. Every tier below can
    match a tombstone — merges keep the loser's normalized_text, aliases and embedding, so
    exact, alias, fuzzy and semantic are all equally exposed — and patching them
    individually is how the next tier added quietly reintroduces the bug. One choke point
    at the exit cannot be forgotten.

    Confidence and relationship pass through untouched: following a merge does not make
    the match weaker, it makes the answer current.
    """
    uri, confidence, relationship = await _resolve_entity_multi_tier_raw(
        conn,
        entity_name,
        entity_type,
        mode=mode,
        embed_fn=embed_fn,
        resolution_caller=resolution_caller,
    )
    if uri:
        live = await resolve_to_live_uri(conn, uri)
        if live != uri:
            logger.info("resolution followed tombstone %s -> %s for %r",
                        uri, live, entity_name)
        return live, confidence, relationship
    return uri, confidence, relationship


async def _resolve_entity_multi_tier_raw(
    conn,
    entity_name: str,
    entity_type: str,
    mode: str = "exact_alias",
    embed_fn: Optional[Callable[[str], Awaitable[Optional[List[float]]]]] = None,
    resolution_caller: str = "resolution_primitives.resolve_entity_multi_tier",
) -> Tuple[Optional[str], float, str]:
    """Multi-tier entity resolution against entity_registry.

    Tombstone-unaware by design; call `resolve_entity_multi_tier` instead unless you
    specifically need the matched row rather than the surviving one.

    Tiers enabled by mode:
    - "exact": Tier 1 only
    - "exact_alias": Tiers 1 + 1.1
    - "fuzzy": Tiers 1 + 1.1 + 2a
    - "semantic": Tiers 1 + 1.1 + 2a + 2b (requires embed_fn)

    Does NOT include Tier 1.5 (contextual) or Tier 3 (create new entity).
    Returns: (local_uri or None, confidence, relationship)
    """
    normalized = normalize_entity_text(entity_name)

    # --- Tier 1: Exact match on normalized_text ---
    row = await conn.fetchrow(
        """
        SELECT fuseki_uri FROM entity_registry
        WHERE normalized_text = $1 AND entity_type = $2
        """,
        normalized,
        entity_type,
    )
    if row:
        return row["fuseki_uri"], 1.0, "same_as"

    if mode == "exact":
        return None, 0.0, "unresolved"

    # --- Tier 1.1: Alias match ---
    alias_norm = normalize_alias(entity_name)
    rows = await conn.fetch(
        """
        SELECT fuseki_uri, aliases FROM entity_registry
        WHERE entity_type = $1 AND aliases IS NOT NULL
        """,
        entity_type,
    )
    for r in rows:
        aliases = r["aliases"]
        if isinstance(aliases, str):
            import json as _json

            try:
                aliases = _json.loads(aliases)
            except (ValueError, TypeError):
                aliases = [aliases]
        if not isinstance(aliases, list):
            continue
        for a in aliases:
            if normalize_alias(a) == alias_norm:
                return r["fuseki_uri"], 1.0, "same_as"

    if mode == "exact_alias":
        return None, 0.0, "unresolved"

    # --- Tier 2a: Fuzzy (Jaro-Winkler) ---
    schema = get_schema_for_type(entity_type)
    threshold = schema.similarity_threshold

    candidates = await conn.fetch(
        """
        SELECT fuseki_uri, normalized_text FROM entity_registry
        WHERE entity_type = $1
        """,
        entity_type,
    )
    best_uri = None
    best_score = 0.0
    shadow = start_attempt(
        caller=resolution_caller,
        engine="shared_multi_tier",
        entity_type=entity_type,
        query_norm=normalized,
        active_policy="strict",
    )

    for c in candidates:
        cand_norm = c["normalized_text"]
        score = jaro_winkler_similarity(normalized, cand_norm)
        if shadow.sampled and score >= threshold:
            shadow_started = time.perf_counter_ns()
            legacy_accepts = passes_token_overlap_legacy(
                normalized, cand_norm, entity_type
            )
            strict_accepts = passes_token_overlap_strict(
                normalized, cand_norm, entity_type
            )
            # These guards are common to the shared resolver regardless of
            # which token-overlap policy is selected.
            if entity_type == "Person":
                common_accepts = passes_person_name_guard(normalized, cand_norm)
                legacy_accepts = legacy_accepts and common_accepts
                strict_accepts = strict_accepts and common_accepts
            if entity_type in ("Organization", "Project", "Concept"):
                common_accepts = passes_distinctive_token_check(
                    normalized, cand_norm
                )
                legacy_accepts = legacy_accepts and common_accepts
                strict_accepts = strict_accepts and common_accepts
            shadow.observe_candidate(
                uri=c["fuseki_uri"],
                score=score,
                tier="fuzzy",
                legacy_accepts=legacy_accepts,
                strict_accepts=strict_accepts,
                elapsed_ns=time.perf_counter_ns() - shadow_started,
            )
        if score >= threshold and score > best_score:
            # STRICT since 2026-08-23. This tier ran `legacy` until a replay of 1,110
            # resolution attempts across all 13 call sites measured what that cost:
            # on Meeting, legacy was wrong on 89.2% of attempts against strict's 6.6%,
            # because it is greedy best-by-score and a wrong-date meeting in the same
            # series outscores the right one. Person and SpecDoc conflated distinct
            # people and distinct documents the same way (two people sharing a
            # first name, two documents whose titles share a prefix).
            #
            # Location and Organization were the counter-argument: strict declines
            # legitimate short/long merges. That was fixed at the DATA layer instead —
            # the duplicate pairs (victoria/victoria bc, mcgill/mcgill university, ...)
            # were merged, so the short form now resolves at Tier 1 via the tombstone
            # and never reaches this tier. Afterwards every residual Location divergence
            # was legacy accepting something wrong (sidney/sydney, colorado/colorado
            # river, two different IP addresses).
            #
            # NOTE the semantic tier below still uses `legacy`, deliberately: the replay
            # observed fuzzy candidates only, so there is no evidence about semantic and
            # flipping it here would be a claim the measurement does not support.
            # Evidence: evidence/resolver-shadow/.
            if not passes_token_overlap_strict(normalized, cand_norm, entity_type):
                continue
            # Shared P1 guards (same as personal_ingest_api Tier 2a).
            if entity_type == "Person" and not passes_person_name_guard(normalized, cand_norm):
                logger.info(
                    "multi-tier Tier 2a REJECTED (person name guard): %r vs %r",
                    entity_name, cand_norm,
                )
                continue
            if entity_type in ("Organization", "Project", "Concept") and \
                    not passes_distinctive_token_check(normalized, cand_norm):
                logger.info(
                    "multi-tier Tier 2a REJECTED (distinctive token guard): %r vs %r",
                    entity_name, cand_norm,
                )
                continue
            best_score = score
            best_uri = c["fuseki_uri"]

    if best_uri:
        shadow.finish(
            active_uri=best_uri,
            active_outcome="fuzzy",
            legacy_fallback="fallthrough_unobserved",
            strict_fallback="fallthrough_unobserved",
        )
        return best_uri, best_score, "related_to"

    if mode == "fuzzy":
        shadow.finish(
            active_uri=None,
            active_outcome="unresolved",
            legacy_fallback="unresolved",
            strict_fallback="unresolved",
        )
        return None, 0.0, "unresolved"

    # --- Tier 2b: Semantic (embedding similarity) ---
    if not embed_fn:
        shadow.finish(
            active_uri=None,
            active_outcome="unresolved",
            legacy_fallback="unresolved",
            strict_fallback="unresolved",
        )
        return None, 0.0, "unresolved"

    query_embedding = await embed_fn(normalized)
    if not query_embedding:
        shadow.finish(
            active_uri=None,
            active_outcome="unresolved",
            legacy_fallback="unresolved",
            strict_fallback="unresolved",
        )
        return None, 0.0, "unresolved"

    semantic_threshold = schema.semantic_threshold

    # pgvector cosine distance: 1 - distance = similarity.
    # Reads from embedding_3072 (post-2026-04-23 OpenAI 3072-dim migration);
    # halfvec cast required because pgvector full-precision vector ANN caps
    # at 2000 dims. Uses idx_entity_registry_embedding_3072_hnsw.
    sem_row = await conn.fetchrow(
        """
        SELECT fuseki_uri, normalized_text,
               1 - (embedding_3072::halfvec(3072)
                    <=> $1::halfvec(3072)) AS similarity
        FROM entity_registry
        WHERE entity_type = $2 AND embedding_3072 IS NOT NULL
        ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
        LIMIT 1
        """,
        str(query_embedding),
        entity_type,
    )
    if sem_row and sem_row["similarity"] >= semantic_threshold:
        # Semantic candidate must clear the shared name-shape guards before we
        # accept it (matches personal_ingest_api Tier 2b).
        sem_norm = sem_row["normalized_text"] or ""
        similarity = float(sem_row["similarity"])
        # STRICT since 2026-08-23, on the same evidence as the fuzzy tier above and
        # measured separately: a semantic replay of 204 observations found 17 divergences,
        # ALL of them Meeting, and all 17 were legacy accepting a match with a DIFFERENT
        # DATE (one meeting of a series matched to the same series two weeks earlier). Zero same-date
        # counter-examples. Every other entity type diverged 0%, so this is a no-op
        # outside Meeting. Evidence: evidence/resolver-shadow/semantic-20260823.log.
        legacy_accepts = not sem_norm or passes_semantic_match_guard_with_policy(
            entity_type,
            normalized,
            sem_norm,
            similarity,
            semantic_threshold,
            passes_token_overlap_strict,
        )
        if shadow.sampled:
            shadow_started = time.perf_counter_ns()
            strict_accepts = not sem_norm or passes_semantic_match_guard_with_policy(
                entity_type,
                normalized,
                sem_norm,
                similarity,
                semantic_threshold,
                passes_token_overlap_strict,
            )
            shadow.observe_candidate(
                uri=sem_row["fuseki_uri"],
                score=similarity,
                tier="semantic",
                legacy_accepts=legacy_accepts,
                strict_accepts=strict_accepts,
                elapsed_ns=time.perf_counter_ns() - shadow_started,
            )
        if not legacy_accepts:
            logger.info(
                "multi-tier Tier 2b REJECTED (name guard): %r vs %r (sim=%.3f)",
                entity_name, sem_norm, similarity,
            )
            shadow.finish(
                active_uri=None,
                active_outcome="unresolved",
                legacy_fallback="unresolved",
                strict_fallback="unresolved",
            )
            return None, 0.0, "unresolved"
        shadow.finish(
            active_uri=sem_row["fuseki_uri"],
            active_outcome="semantic",
            legacy_fallback="unresolved",
            strict_fallback="unresolved",
        )
        return sem_row["fuseki_uri"], similarity, "related_to"

    shadow.finish(
        active_uri=None,
        active_outcome="unresolved",
        legacy_fallback="unresolved",
        strict_fallback="unresolved",
    )
    return None, 0.0, "unresolved"
