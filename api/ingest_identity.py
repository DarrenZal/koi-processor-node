"""Payload-scoped entity identity for high-integrity document ingestion.

Issue #62. A deep document ingest resolves every fact endpoint against the LIVE
entity registry at write time, and may CREATE missing entities mid-run. An entity
created while writing fact 45 is therefore a fuzzy/semantic candidate for a
*different* endpoint at fact 47 — so the result depends on the order facts happen
to be written in, and distinct members of one import payload silently collapse
into each other.

Measured on the audited Buehler (2024) import (DOI 10.1088/2632-2153/ad7228):
115 distinct typed endpoints, 213 facts, 108 exact + 7 fuzzy resolutions, of which
**6 were wrong self-collapses** and 11 persisted fact endpoints pointed at the
wrong entity. The write succeeded and the structural gate passed.

The importer knows the complete typed endpoint set before it writes anything —
`merge_extractions()` produces the whole payload, and only then does the first
`POST /knowledge/episodes` happen. That is the seam this module occupies:

    collect_endpoints(payload)        # 1. complete distinct (label, type) set
    preflight_endpoints(conn, eps)    # 2. classify each against the live graph
    preregister_missing(http, report) # 3. exact-only create the truly missing
    freeze_endpoint_map(conn, ...)    # 4. pin endpoint -> URI, assert bijection
      ... callers write facts, passing the pinned URIs directly ...
    verify_persisted_graph(conn, ...) # 5. every persisted endpoint is in the map

Steps 2 and 4 BLOCK. Step 5 blocks *before* relational/discourse finalization, so
a failed verification never leaves a document that looks finished.

WHY PINNING THE URI IS THE FIX, AND NOT A HIGHER THRESHOLD
----------------------------------------------------------
The four collapses reproduce today, unchanged, against the current strict guards
(verified 2026-09-13 — see tests/test_ingest_identity.py::test_the_four_reported_
collapses_still_reproduce). They survive because `passes_distinctive_token_check`
rejects DISJOINT distinctive-token sets, and "Anthropic Claude 3 Opus" vs
"Anthropic Claude 3 Sonnet" is not disjoint — it shares {anthropic, claude} and
differs in exactly the one token that distinguishes them. Raising a threshold
trades one arbitrary cut for another and leaves the import contract implicit.

Pinning makes the property STRUCTURAL: once an endpoint is bound to a URI, no
resolver tier runs for it at all, so no threshold can be wrong. Issue #53's tuning
remains worth doing for every other caller; it is not what makes THIS path safe.

RELATIONSHIP TO #61
-------------------
Identity here is keyed on the CURRENT normalization of a canonical label, never on
the stored `entity_registry.normalized_text` alone. Stored values were written by
older normalizers: 1,268 of 36,484 rows disagree with the current function
(re-measured 2026-09-13). `GPT-4` is the repaired example; `GPT-4o`, `GPT-4.1` and
`gpt-5.4` are three that are still live and would each duplicate today under an
`exact_only` registration. Reading both ways is what keeps a preflight from
declaring an endpoint "missing" when it is merely spelled by a previous normalizer.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field, replace
from typing import Any, Callable, Iterable, Optional, Sequence

logger = logging.getLogger(__name__)

# Version of the identity contract this module implements. Recorded on every
# registration and in every receipt, so a graph written under one contract can be
# told apart from one written under another. Bump when the KEY changes (what counts
# as the same endpoint) — not for an unrelated refactor.
IDENTITY_CONTRACT_VERSION = "payload-endpoint-pinning-v1"

# Version of the entity-text normalizer whose output forms the identity key. This
# names the behaviour of api.resolution_primitives.normalize_entity_text as of
# 2026-09-13: lower, strip, '_'->' ', '-'->' ', one non-overlapping double-space
# collapse, leading '@' stripped. Issue #61 exists because this changed once
# without a version, a migration, or a compatibility read.
NORMALIZER_VERSION = "entity-text-norm-2026-09-13"

# Endpoint states. Only these two are safe to proceed from.
STATE_LIVE_EXACT = "one_live_exact"
STATE_MISSING = "missing"
# Everything below BLOCKS unless an explicit audited decision covers it.
STATE_AMBIGUOUS = "ambiguous"              # >1 live same-type identity for one label
STATE_TOMBSTONE_RISK = "tombstone_risk"    # a same-type exact row is merged away
STATE_ALIAS_ONLY = "alias_only"            # reachable only via an alias
STATE_GLOBAL_DUPLICATE = "graph_global_duplicate"  # #61: the graph already holds a dup set
# The label exists live under a DIFFERENT type. Treated as advisory in the first
# version of this module, which was wrong in a way that only showed up against real
# data: `DWeb Berlin` was declared Organization by the extractor while the graph
# held it as a Project, the endpoint classified as MISSING, and preregistration
# would then have minted a SECOND `DWeb Berlin` — an Organization — alongside the
# Project. The contract exists to stop one payload endpoint becoming two identities;
# silently creating the cross-type twin is that same failure wearing a different hat.
#
# It blocks, and only an explicit audited TYPE decision clears it. The operator has
# to say which type the thing actually is, because nothing in the payload can.
STATE_CROSS_TYPE_CONFLICT = "cross_type_conflict"
# No extracted entity, no type_map entry and no audited decision names a type for
# this fact endpoint. The first version defaulted it to `Concept`, preregistered
# it as `Concept` with force_type, and hashed that default into the URI —
# recording `type_defaulted` in evidence and gating nothing (review finding M3:
# 1,045 such endpoints across 239 cached documents, re-derived read-only; 24
# live today ONLY under another type, so a Person referenced only in a fact
# would have become a Concept twin).
# The prompt says every fact endpoint MUST appear in entities[]; an untyped
# endpoint is therefore an extraction defect, and a type is an identity decision
# nobody has made. It blocks, and only an audited TYPE decision types it.
STATE_TYPE_UNDECLARED = "type_undeclared"

BLOCKING_STATES = (
    STATE_AMBIGUOUS,
    STATE_TOMBSTONE_RISK,
    STATE_ALIAS_ONLY,
    STATE_GLOBAL_DUPLICATE,
    STATE_CROSS_TYPE_CONFLICT,
    STATE_TYPE_UNDECLARED,
)

_ENTITY_URI_PREFIX = "orn:personal-koi.entity:"


class IdentityError(RuntimeError):
    """A payload's identity contract cannot be established or has been violated.

    Carries the structured blockers so a caller can emit them as evidence rather
    than only as a log line.
    """

    def __init__(self, message: str, *, blockers: Optional[list] = None):
        super().__init__(message)
        self.blockers = blockers or []


# ─────────────────────────────────────────────────────────────────────────────
# 1. Collect the complete distinct typed endpoint set
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Endpoint:
    """One distinct typed identity the payload intends to reference.

    `name` is the canonical label exactly as the payload spells it. `key` is what
    identity is actually decided on, and is deliberately NOT the raw name.
    """
    name: str
    # None when nothing in the payload (or an audited decision) says what this
    # endpoint IS. A None type cannot be preflighted, preregistered or frozen —
    # every stage refuses it — so an untyped endpoint is structurally unable to
    # reach a write. It used to default to "Concept" here; see STATE_TYPE_UNDECLARED.
    entity_type: Optional[str]
    normalized: str
    # True when the PAYLOAD declared no type for this endpoint. With a decision the
    # endpoint is typed (type_source="audited_decision") and this stays False.
    type_defaulted: bool = False
    # Where the type came from: "entities", "type_map", "audited_decision", or None.
    type_source: Optional[str] = None
    roles: frozenset = field(default_factory=frozenset)  # {"entity","subject","object"}

    @property
    def key(self) -> tuple:
        return (self.normalized, self.entity_type)


def collect_endpoints(
    payload: dict,
    *,
    normalize: Callable[[str], str],
    type_decisions: Optional[dict] = None,
) -> list[Endpoint]:
    """Every distinct (current-normalized label, type) the payload's FACTS reference.

    Endpoints come from fact subjects/objects ONLY; `entities[]` supplies the type
    for an endpoint and is never itself a source of endpoints (a declared-but-
    unreferenced entity has nothing written about it, so there is nothing to bind —
    see the comment at the fact loop below for what unioning entities[] in cost).

    Type precedence for a fact endpoint: the extractor's `entities[]` declaration,
    then the merge's `type_map`, then an audited `type_decisions` entry (keyed by
    label, matched on its normalization). Nothing else. There is deliberately NO
    default type: the type is hashed into a created entity's URI, so a guessed
    type is a permanent identity decision made by a fallback branch. An endpoint
    none of the three names is returned with `entity_type=None` and blocks in
    preflight as `type_undeclared`.

    Raises IdentityError when one normalized label is claimed under two types by
    the extractor itself; that is an extraction defect and guessing which one is
    meant would be inventing an identity decision.
    """
    entities = payload.get("entities") or []
    facts = payload.get("facts") or []
    type_map = payload.get("type_map") or {}
    decided_types = {normalize(k): v for k, v in (type_decisions or {}).items()
                     if isinstance(k, str) and normalize(k)}

    # Collected as SETS, not a dict of last-write-wins. Keying a dict on the
    # normalized name silently resolves a disagreement inside entities[] itself by
    # whichever entry happened to come last — an identity decision made by list
    # order, and permanent once the type is hashed into a URI.
    declared_types: dict[str, set] = {}
    for ent in entities:
        name = (ent.get("name") or "").strip()
        if not name:
            continue
        etype = ent.get("type")
        if not etype:
            continue   # an untyped entities[] entry declares nothing
        declared_types.setdefault(normalize(name), set()).add(etype)

    declared_conflicts = {n: sorted(t) for n, t in declared_types.items() if len(t) > 1}
    if declared_conflicts:
        raise IdentityError(
            "the payload's own entities[] claims one normalized label under multiple "
            "entity types: "
            + json.dumps(dict(list(declared_conflicts.items())[:5]), sort_keys=True),
            blockers=[
                {"state": "payload_type_conflict", "normalized": n, "types": t}
                for n, t in sorted(declared_conflicts.items())
            ],
        )
    declared: dict[str, str] = {n: next(iter(t)) for n, t in declared_types.items()}

    # name -> (entity_type, type_defaulted, roles)
    found: dict[tuple, dict] = {}

    def _add(raw_name: Any, role: str) -> None:
        if not raw_name or not isinstance(raw_name, str):
            return
        name = raw_name.strip()
        if not name:
            return
        norm = normalize(name)
        if not norm:
            return
        if norm in declared:
            etype, source = declared[norm], "entities"
        elif type_map.get(norm):
            etype, source = type_map[norm], "type_map"
        elif norm in decided_types:
            etype, source = decided_types[norm], "audited_decision"
        else:
            etype, source = None, None
        key = (norm, etype)
        slot = found.get(key)
        if slot is None:
            found[key] = {
                "name": name,
                "entity_type": etype,
                "normalized": norm,
                "type_defaulted": source is None,
                "type_source": source,
                "roles": {role},
            }
        else:
            slot["roles"].add(role)
            # Keep the first spelling seen so the pinned label is stable across
            # runs; ordering of `entities[]` is deterministic from the merge.

    # ENDPOINTS COME FROM FACTS ONLY. `entities[]` supplies the TYPE for an
    # endpoint and nothing else.
    #
    # The first version unioned entities[] in as endpoints too, and running the
    # gate over three freshly-ingested substack posts showed what that costs:
    # the extractor declares far more entities than its facts reference — 26
    # declared vs 14 used on one document, 26 vs a similar count on another — so
    # pre-registering the union would mint a dozen registry rows per document that
    # no fact ever points at. That is registry inflation the unpinned path did not
    # cause, introduced by the thing meant to make identity safer.
    #
    # A declared-but-unreferenced entity is not an identity the payload asserts;
    # it is a candidate the extractor mentioned. Nothing needs to be bound for it,
    # because nothing will be written about it.
    for fact in facts:
        _add(fact.get("subject"), "subject")
        _add(fact.get("object"), "object")

    # A normalized label claimed under two different types inside ONE payload is an
    # extraction defect, not an identity decision this module may make.
    by_norm: dict[str, set] = {}
    for (norm, etype) in found:
        by_norm.setdefault(norm, set()).add(etype)
    conflicts = {n: sorted(t) for n, t in by_norm.items() if len(t) > 1}
    if conflicts:
        raise IdentityError(
            "the payload claims one normalized label under multiple entity types; "
            "resolve the extraction before importing: "
            + json.dumps(dict(list(conflicts.items())[:5]), sort_keys=True),
            blockers=[
                {"state": "payload_type_conflict", "normalized": n, "types": t}
                for n, t in sorted(conflicts.items())
            ],
        )

    out = [
        Endpoint(
            name=v["name"],
            entity_type=v["entity_type"],
            normalized=v["normalized"],
            type_defaulted=v["type_defaulted"],
            type_source=v["type_source"],
            roles=frozenset(v["roles"]),
        )
        for v in found.values()
    ]
    out.sort(key=lambda e: (e.normalized, e.entity_type or ""))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 2. Preflight the endpoint set against the live graph
# ─────────────────────────────────────────────────────────────────────────────

# Live means: not merged away, not revoked. `expires_at` is deliberately NOT part
# of liveness — it is a scheduled future state, and an endpoint that expires
# tomorrow is still the right identity to bind today.
_CANDIDATE_SQL = """
    SELECT fuseki_uri, entity_text, entity_type, normalized_text, aliases,
           merged_into, revoked_at
      FROM entity_registry
     WHERE entity_type = ANY($1::text[])
"""

# Same-label candidates under ANY type. This is a SEPARATE query on purpose. The
# one above is type-filtered, so it structurally cannot return a cross-type row —
# an advisory built from its results could never fire, which a test caught by
# asserting the advisory was populated and finding it empty. A check that cannot
# fire is worse than no check, because its silence reads as a clean result.
#
# It is NOT filtered by type at all. The first version excluded the PAYLOAD-WIDE
# set of declared types here (`AND NOT entity_type = ANY(types)`), so one live
# `Project` row declared `Organization` by the payload blocked correctly — until
# any OTHER endpoint in the same payload happened to be a `Project`, at which point
# the same label quietly classified as `missing` and would have been minted as a
# cross-type twin with force_type (review finding B2; every shipped cross-type test
# used a single-endpoint payload). The exclusion is now per ENDPOINT, in Python,
# against the endpoint's OWN type. The same rows also serve as the evidence for an
# untyped endpoint (what does the graph hold under this label, under any type).
#
# Matches on either the stored normalized_text or the CURRENT normalization of the
# canonical label, for the same reason the type-filtered read does (#61).
_ANY_TYPE_SQL = """
    SELECT fuseki_uri, entity_text, entity_type, normalized_text,
           merged_into, revoked_at
      FROM entity_registry
     WHERE (normalized_text = ANY($1::text[])
            OR koi_normalize_entity_text(entity_text) = ANY($1::text[]))
"""

# Rows by URI, any type, live or not — for validating an audited alias decision's
# target (review finding M4: the freeze bound a decided URI with zero validation).
_BY_URI_SQL = """
    SELECT fuseki_uri, entity_text, entity_type, merged_into, revoked_at
      FROM entity_registry
     WHERE fuseki_uri = ANY($1::text[])
"""


@dataclass
class EndpointFinding:
    endpoint: Endpoint
    state: str
    live_uris: list
    tombstoned_uris: list
    alias_uris: list
    cross_type_uris: list
    # Which read found the live row: "stored_norm" when the persisted
    # normalized_text matched, "current_norm" when only the recomputed value did.
    # A "current_norm" hit is a live instance of issue #61 — the row is invisible
    # to a stock exact lookup and would have been duplicated.
    matched_via: Optional[str] = None
    # The audited entity_type an operator supplied for this label, when one cleared
    # a cross-type conflict. It travels ON THE FINDING rather than being re-read
    # from a dict downstream: `preregister_missing` has its own belt-and-braces
    # cross-type refusal, and before this field existed that refusal fired even for
    # a conflict the operator had already decided — so the documented escape hatch
    # ("Supply an audited type decision") could not actually be taken. Issue #68
    # requirement 7: conflicts keep blocking, but a decision must be able to clear
    # them end to end, not just at the first of two gates.
    type_decision: Optional[str] = None

    def as_evidence(self) -> dict:
        return {
            "name": self.endpoint.name,
            "type": self.endpoint.entity_type,
            "normalized": self.endpoint.normalized,
            "state": self.state,
            "matched_via": self.matched_via,
            "type_decision": self.type_decision,
            "type_defaulted": self.endpoint.type_defaulted,
            "type_source": self.endpoint.type_source,
            "roles": sorted(self.endpoint.roles),
            "live_uris": sorted(self.live_uris),
            "tombstoned_uris": sorted(self.tombstoned_uris),
            "alias_uris": sorted(self.alias_uris),
            "cross_type_uris": sorted(self.cross_type_uris),
        }


@dataclass
class PreflightReport:
    findings: list
    normalizer_version: str = NORMALIZER_VERSION
    contract_version: str = IDENTITY_CONTRACT_VERSION

    @property
    def missing(self) -> list:
        return [f for f in self.findings if f.state == STATE_MISSING]

    @property
    def resolved(self) -> list:
        return [f for f in self.findings if f.state == STATE_LIVE_EXACT]

    @property
    def blockers(self) -> list:
        return [f for f in self.findings if f.state in BLOCKING_STATES]

    @property
    def endpoints(self) -> list:
        """The endpoints as classified — what the freeze must be given."""
        return [f.endpoint for f in self.findings]

    def counts(self) -> dict:
        out: dict = {}
        for f in self.findings:
            out[f.state] = out.get(f.state, 0) + 1
        return dict(sorted(out.items()))

    def as_evidence(self) -> dict:
        return {
            "contract_version": self.contract_version,
            "normalizer_version": self.normalizer_version,
            "endpoints": len(self.findings),
            "state_counts": self.counts(),
            "drift_recovered": sum(1 for f in self.findings if f.matched_via == "current_norm"),
            "type_defaulted": sum(1 for f in self.findings if f.endpoint.type_defaulted),
            "findings": [f.as_evidence() for f in self.findings],
        }


def _coerce_aliases(value: Any) -> list:
    if not value:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return [value]
        return parsed if isinstance(parsed, list) else [parsed]
    return list(value)


async def preflight_endpoints(
    conn,
    endpoints: Sequence[Endpoint],
    *,
    normalize: Callable[[str], str],
    normalize_alias_fn: Callable[[Any], str],
    alias_decisions: Optional[dict] = None,
    type_decisions: Optional[dict] = None,
) -> PreflightReport:
    """Classify every endpoint against the live registry. Read-only.

    `alias_decisions` maps a payload label to a URI the operator has explicitly
    audited as the right target (issue #62 requirement 6: "unless an explicit,
    audited alias decision says otherwise"). Supplying one converts that endpoint's
    ALIAS_ONLY block into a resolved binding, and the decision is carried into the
    receipt. Nothing else can clear a block.
    """
    alias_decisions = alias_decisions or {}
    # {payload label -> entity_type}: an audited statement of what the thing IS,
    # required to clear a cross-type conflict. Distinct from alias_decisions, which
    # says which URI a label means; this says which TYPE it is.
    type_decisions = type_decisions or {}
    if not endpoints:
        return PreflightReport(findings=[])

    # An audited type decision types an endpoint the payload left untyped.
    # `collect_endpoints` applies decisions first in production; this covers a
    # caller that built endpoints without them, and it happens BEFORE the
    # candidate read so the decided type's rows are actually fetched. Matched on
    # the normalized label, like everything else here.
    if type_decisions:
        decided_by_norm = {normalize(k): v for k, v in type_decisions.items()
                           if isinstance(k, str) and normalize(k)}
        endpoints = [
            replace(e, entity_type=decided_by_norm[e.normalized], type_defaulted=False,
                    type_source="audited_decision")
            if e.entity_type is None and e.normalized in decided_by_norm else e
            for e in endpoints
        ]

    types = sorted({e.entity_type for e in endpoints if e.entity_type})
    rows = list(await conn.fetch(_CANDIDATE_SQL, types)) if types else []
    labels = sorted({e.normalized for e in endpoints})
    any_type_rows = list(await conn.fetch(_ANY_TYPE_SQL, labels))

    # Index once. Both keys matter: `stored_norm` is what a stock Tier-1 exact
    # lookup sees, `current_norm` is what the label actually normalizes to today.
    by_stored: dict[tuple, list] = {}
    by_current: dict[tuple, list] = {}
    by_alias: dict[tuple, list] = {}
    for row in rows:
        etype = row["entity_type"]
        by_stored.setdefault((row["normalized_text"], etype), []).append(row)
        by_current.setdefault((normalize(row["entity_text"] or ""), etype), []).append(row)
        for alias in _coerce_aliases(row["aliases"]):
            by_alias.setdefault((normalize_alias_fn(alias), etype), []).append(row)

    # Any-type index, built from the DEDICATED query above. Keyed under both
    # spellings so a drifted stored value still surfaces. The per-endpoint type
    # filter is applied at the point of use, never here.
    by_current_any_type: dict[str, list] = {}
    for row in any_type_rows:
        for k in {normalize(row["entity_text"] or ""), row["normalized_text"]}:
            by_current_any_type.setdefault(k, []).append(row)

    def _live(rs: Iterable) -> list:
        return [r for r in rs if r["merged_into"] is None and r["revoked_at"] is None]

    findings: list = []
    for ep in endpoints:
        if ep.entity_type is None:
            # Nothing declared a type and no decision supplied one. Block, and
            # show what the graph holds under this label so the decision can be
            # written in one look. Never classified MISSING: MISSING is what gets
            # preregistered, and there is no type to preregister it as.
            held = _live(by_current_any_type.get(ep.normalized, []))
            findings.append(EndpointFinding(
                endpoint=ep, state=STATE_TYPE_UNDECLARED, live_uris=[],
                tombstoned_uris=[], alias_uris=[],
                cross_type_uris=sorted({r["fuseki_uri"] for r in held})))
            continue

        key = ep.key
        stored_hits = by_stored.get(key, [])
        current_hits = by_current.get(key, [])

        # Union by URI, preserving which read found each — the #61 dual read.
        seen_uris: dict = {}
        for r in stored_hits:
            seen_uris.setdefault(r["fuseki_uri"], ("stored_norm", r))
        for r in current_hits:
            if r["fuseki_uri"] not in seen_uris:
                seen_uris[r["fuseki_uri"]] = ("current_norm", r)

        exact_rows = [r for _via, r in seen_uris.values()]
        live_exact = _live(exact_rows)
        tombstoned = [r for r in exact_rows if r["merged_into"] is not None]
        alias_rows = _live(by_alias.get((normalize_alias_fn(ep.name), ep.entity_type), []))
        # Endpoint-specific: rows for THIS label whose type is not THIS endpoint's.
        cross_type = [r for r in _live(by_current_any_type.get(ep.normalized, []))
                      if r["entity_type"] != ep.entity_type]

        live_uris = sorted({r["fuseki_uri"] for r in live_exact})
        alias_uris = sorted({r["fuseki_uri"] for r in alias_rows} - set(live_uris))
        matched_via = None
        type_decision = None

        # An AUDITED ALIAS DECISION is checked first, ahead of every heuristic
        # branch below. The operator has named the exact URI this label means, so
        # there is nothing left for the classifier to decide — and in particular a
        # sibling tombstone stops being a risk, because the binding no longer
        # depends on following a merge chain.
        #
        # Ordering matters and got this wrong at first: `DWeb Berlin` has a
        # same-label tombstone (from its own retype) and reached TOMBSTONE_RISK
        # before the alias branch, so a decision that named the survivor could not
        # clear it. A decision that cannot be acted on is not an escape hatch.
        decided_uri = alias_decisions.get(ep.name)
        if decided_uri:
            # Live AND of this endpoint's own type. The candidate rows span every
            # type the payload declares, and accepting any of them let a decision
            # bind a Concept endpoint to a live Project row whenever the payload
            # also had a Project somewhere — a binding the pinned write 422s on.
            live_by_uri = {r["fuseki_uri"] for r in _live(rows)
                           if r["entity_type"] == ep.entity_type}
            if decided_uri in live_by_uri:
                findings.append(EndpointFinding(
                    endpoint=ep, state=STATE_LIVE_EXACT, live_uris=[decided_uri],
                    tombstoned_uris=sorted({r["fuseki_uri"] for r in tombstoned}),
                    alias_uris=alias_uris,
                    cross_type_uris=sorted({r["fuseki_uri"] for r in cross_type}),
                    matched_via="audited_alias_decision"))
                continue
            # A decision naming a dead, unknown or wrong-type URI is IGNORED here,
            # not refused: the endpoint falls through to the ordinary classification
            # below, so a same-label live row of its type still resolves it and an
            # unmatched label still goes `missing` — and `preregister_missing` will
            # then MINT a row for it before the freeze reads the decision back and
            # refuses (`alias_decision_not_live` / `alias_decision_type_mismatch`).
            # The refusal is therefore one step later than it should be, and can
            # leave a minted row behind. Known gap, deferred to #69 (transactional
            # rollback/replay is the place to make the mint reversible); an earlier
            # comment here claimed this branch blocks, which it does not.
            logger.warning(
                "audited alias decision for %r names %s, which is not a live "
                "candidate — ignoring the decision", ep.name, decided_uri)

        if len(live_uris) > 1:
            # Issue #61 AC5 / #62 requirement 3. Distinguish the two shapes: rows
            # that already share a stored normalized_text are a pre-existing
            # duplicate set; rows visible to each other ONLY under current
            # normalization are drift-created and invisible to Tier-1 today.
            distinct_stored = {r["normalized_text"] for r in live_exact}
            state = STATE_GLOBAL_DUPLICATE if len(distinct_stored) > 1 else STATE_AMBIGUOUS
        elif len(live_uris) == 1:
            # A sibling tombstone is NOT a blocker when exactly one live row also
            # matches. It was treated as one in the first version of this module,
            # and running the real Buehler payload against the real repaired graph
            # showed why that is wrong: the single blocker over 115 endpoints was
            # `GPT-4` — issue #61's own repaired entity, whose legacy row is
            # tombstoned *because the repair worked*. Blocking there would make a
            # correctly-merged graph permanently un-ingestable, punishing the repair.
            #
            # The risk it was named for is real only on the UNPINNED path, where the
            # writer's Tier-1 could select the tombstone by name. This payload is
            # pinned to the live URI, and `_bind_pinned_uri` refuses a merged-away
            # URI outright, so the tombstone cannot be reached. Recorded as evidence,
            # not as a block.
            state = STATE_LIVE_EXACT
            matched_via = seen_uris[live_uris[0]][0]
        elif tombstoned:
            # No live row, but a same-type exact row that has been merged away. Here
            # the tombstone IS the only thing the label matches, so the payload's
            # identity would depend on following a merge chain that can change under
            # it. That blocks.
            state = STATE_TOMBSTONE_RISK
        elif alias_uris:
            decided = alias_decisions.get(ep.name)
            if decided and decided in alias_uris:
                state = STATE_LIVE_EXACT
                matched_via = "audited_alias_decision"
                live_uris = [decided]
            else:
                state = STATE_ALIAS_ONLY
        elif cross_type:
            # Live under another type. Creating the declared-type twin here is the
            # thing the contract exists to prevent, so it blocks unless an audited
            # type decision names the type explicitly.
            #
            # A decision that names a DIFFERENT type from the one the payload
            # declares does not clear anything — it contradicts the payload, and
            # acting on it would bind the endpoint to a type nobody asserted. Only
            # an exact agreement clears, and it is recorded on the finding so the
            # registration step can honour the same decision.
            decided_type = type_decisions.get(ep.name)
            if decided_type is not None and decided_type != ep.entity_type:
                logger.warning(
                    "audited type decision for %r says %s but the payload declares "
                    "%s — the conflict stands", ep.name, decided_type, ep.entity_type)
            if decided_type == ep.entity_type:
                state, type_decision = STATE_MISSING, decided_type
            else:
                state, type_decision = STATE_CROSS_TYPE_CONFLICT, None
        else:
            state = STATE_MISSING

        findings.append(
            EndpointFinding(
                endpoint=ep,
                state=state,
                live_uris=live_uris,
                tombstoned_uris=sorted({r["fuseki_uri"] for r in tombstoned}),
                alias_uris=alias_uris,
                cross_type_uris=sorted({r["fuseki_uri"] for r in cross_type}),
                matched_via=matched_via,
                type_decision=type_decision,
            )
        )

    return PreflightReport(findings=findings)


def require_no_blockers(report: PreflightReport) -> None:
    """Raise before anything is written if any endpoint cannot be bound safely."""
    blockers = report.blockers
    if not blockers:
        return
    evidence = [f.as_evidence() for f in blockers]
    raise IdentityError(
        f"{len(blockers)} of {len(report.findings)} payload endpoints cannot be bound to a "
        f"single live identity; refusing to write any fact. "
        f"States: {sorted({f.state for f in blockers})}. "
        f"First: {json.dumps(evidence[0], sort_keys=True)[:600]}",
        blockers=evidence,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 3. Exact-only preregistration of the truly missing identities
# ─────────────────────────────────────────────────────────────────────────────

def _register_url(http, post_path: str, base_url: Optional[str]) -> str:
    """The absolute URL a registration POST will go to — or an IdentityError.

    The extractor's client is `provider_async_client()`, which sets no base_url,
    and the first version POSTed the bare path "/register-entity" to it. httpx
    raised `UnsupportedProtocol` before a socket opened — on every strict run with
    at least one missing endpoint, which the preflight's own docstring calls
    "every first ingest". It escaped as a bare traceback because it was neither
    IdentityError nor ExtractionError (review finding B1). Resolve the URL here,
    and refuse with a typed error BEFORE any request when nothing supplies a host.
    """
    if re.match(r"^https?://", post_path):
        return post_path
    if base_url:
        return base_url.rstrip("/") + "/" + post_path.lstrip("/")
    client_base = str(getattr(http, "base_url", "") or "")
    if client_base:
        return client_base.rstrip("/") + "/" + post_path.lstrip("/")
    raise IdentityError(
        f"cannot preregister: {post_path!r} is a relative path and neither a base_url "
        f"argument nor the HTTP client supplies a host; an absolute URL is required "
        f"before any registration is attempted")


async def preregister_missing(
    http,
    report: PreflightReport,
    *,
    post_path: str = "/register-entity",
    base_url: Optional[str] = None,
    timeout: float = 240.0,
) -> dict:
    """Create every MISSING endpoint via exact-only, force-typed registration.

    `exact_only=true` is what keeps THIS call from doing the very thing the module
    prevents — a plain registration would run the fuzzy/semantic tiers and could
    bind the new label to an existing sibling. `force_type=true` suppresses the
    cross-type dedup fallback so a same-name entity of another type does not
    capture it; the advisory cross_type warning still fires and is recorded.

    `base_url` (or an absolute `post_path`, or a client with its own base_url) is
    required — see `_register_url`. Resolved once, before the first POST, so a
    misconfiguration cannot mint half the endpoints and then fail.

    Returns {payload label -> canonical URI} for the endpoints it registered.
    """
    created: dict = {}
    receipts: list = []
    url = _register_url(http, post_path, base_url)
    for finding in report.missing:
        ep = finding.endpoint
        # Belt to the preflight's braces. require_no_blockers() should already have
        # stopped a cross-type conflict, but this function can be called directly
        # and minting the cross-type twin is irreversible once its type is hashed
        # into a URI. Refuse rather than trust the caller ran the gate.
        #
        # The one thing that gets through is the escape hatch the message below
        # names: an audited type decision, recorded on the finding by the preflight
        # that already evaluated it. Before it was carried here, this refusal fired
        # on decided conflicts too — the suggested remedy did nothing, which is a
        # worse failure than no remedy at all.
        if finding.cross_type_uris and finding.type_decision != ep.entity_type:
            raise IdentityError(
                f"refusing to preregister {ep.name!r} as {ep.entity_type}: the label is "
                f"already live under another type ({finding.cross_type_uris[:3]}). Supply an "
                f"audited type decision, or merge/retype the existing identity first.",
                blockers=[{"state": STATE_CROSS_TYPE_CONFLICT, "name": ep.name,
                           "requested_type": ep.entity_type,
                           "existing": finding.cross_type_uris}])
        if finding.cross_type_uris:
            # Reached only with a matching audited decision (the refusal above). The
            # twin is being created on purpose; say so loudly enough that it turns up
            # in a log grep, because the URI it mints cannot be corrected in place.
            logger.warning(
                "minting %r as %s ALONGSIDE %d live row(s) of another type (%s) — "
                "authorised by an audited type decision",
                ep.name, ep.entity_type, len(finding.cross_type_uris),
                finding.cross_type_uris[:3])
        body = {
            "entity_type": ep.entity_type,
            "name": ep.name,
            "properties": {},
            "publication_scope": "local_graph",
            "visibility_scope": "public",
            "force_type": True,
            "exact_only": True,
        }
        response = await http.post(url, json=body, timeout=timeout)
        if response.status_code >= 400:
            raise IdentityError(
                f"exact-only preregistration failed for {ep.name!r} ({ep.entity_type}): "
                f"HTTP {response.status_code} {response.text[:400]}"
            )
        data = response.json()
        if data.get("success") is not True:
            raise IdentityError(f"register-entity reported failure for {ep.name!r}: {data}")
        uri = data.get("canonical_uri")
        if not isinstance(uri, str) or not uri.startswith(_ENTITY_URI_PREFIX):
            raise IdentityError(f"register-entity returned an invalid URI for {ep.name!r}: {uri!r}")
        # A registration that silently rolled back its relationship side effect used
        # to return success=True; that is a separate defect already fixed, but the
        # field is still the only way to know, so it is not ignored here.
        if data.get("relationship_sync_error"):
            raise IdentityError(
                f"register-entity relationship side effect failed for {ep.name!r}: "
                f"{data['relationship_sync_error']}"
            )
        created[ep.name] = uri
        receipts.append({
            "name": ep.name,
            "type": ep.entity_type,
            "canonical_uri": uri,
            "is_new": bool(data.get("is_new")),
            "cross_type_warning": data.get("cross_type_warning"),
            "collision_warning": data.get("collision_warning"),
            "normalizer_version": NORMALIZER_VERSION,
            "contract_version": IDENTITY_CONTRACT_VERSION,
            # Present only when an audited decision was what allowed this mint
            # despite a live same-label row of another type.
            "audited_type_decision": (ep.entity_type if finding.cross_type_uris else None),
        })

    # An exact-only registration that lands on an URI another endpoint already owns
    # means two distinct payload identities just became one. Catch it here, before
    # the freeze, so the error names the registration rather than the map.
    collapsed: dict = {}
    for name, uri in created.items():
        collapsed.setdefault(uri, []).append(name)
    shared = {uri: names for uri, names in collapsed.items() if len(names) > 1}
    if shared:
        raise IdentityError(
            f"exact-only preregistration collapsed distinct payload endpoints onto shared "
            f"URIs: {json.dumps({k: sorted(v) for k, v in shared.items()}, sort_keys=True)}",
            blockers=[{"state": "preregistration_collapse", "uri": k, "names": sorted(v)}
                      for k, v in shared.items()],
        )
    return {"uri_map": created, "receipts": receipts}


# ─────────────────────────────────────────────────────────────────────────────
# 4. Freeze the endpoint -> URI map
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FrozenMap:
    """An immutable payload-endpoint-to-URI binding. Facts must use it directly.

    Lookups are by the CANONICAL KEY, not the raw spelling. `collect_endpoints`
    folds every spelling of one label into one endpoint and keeps the first
    spelling as `name`; the facts keep their own spellings. The first version's
    `uri_for` was a bare `by_name.get(raw)`, so the second spelling of a bound
    endpoint (`gpt_4` after `GPT-4`) came back None and the episode builder raised
    `identity_map_incomplete` — AFTER preregistration had already minted (review
    finding B3; 86 of 1,755 cached documents carry such a pair). `by_key` already
    held the answer and nothing read it.
    """
    by_name: dict          # payload label -> canonical URI
    by_key: dict           # (normalized, type) -> canonical URI
    by_name_type: dict     # payload label -> the entity_type the binding was made on
    evidence: list
    contract_version: str = IDENTITY_CONTRACT_VERSION
    normalizer_version: str = NORMALIZER_VERSION
    # The normalizer the keys were built with. Optional only so hand-built maps in
    # older tests still construct; a map built by `freeze_endpoint_map` always has
    # one, and without it a lookup falls back to the raw-spelling read.
    normalize: Optional[Callable[[str], str]] = field(default=None, repr=False, compare=False)
    _by_norm: Optional[dict] = field(default=None, init=False, repr=False, compare=False)

    def _key_for(self, name: str) -> Optional[tuple]:
        if self.normalize is None:
            return None
        if self._by_norm is None:
            # A payload has exactly one type per normalized label (collect_endpoints
            # raises otherwise), so normalized -> key is a function.
            self._by_norm = {n: (n, t) for (n, t) in self.by_key}
        return self._by_norm.get(self.normalize(name or ""))

    def uri_for(self, name: str) -> Optional[str]:
        hit = self.by_name.get(name)
        if hit is not None:
            return hit
        key = self._key_for(name)
        return self.by_key.get(key) if key else None

    def type_for(self, name: str) -> Optional[str]:
        """The type this endpoint was BOUND on — not the caller's later guess.

        A pinned write must declare the type the freeze used, because the server
        rejects a pin whose declared type disagrees with the entity's. Re-deriving
        the type from a type_map at write time can disagree with the freeze (the
        map is keyed on a normalized name and an endpoint may have been typed by
        default), and that disagreement would surface as a 422 blaming the pin.
        """
        hit = self.by_name_type.get(name)
        if hit is not None:
            return hit
        key = self._key_for(name)
        return key[1] if key else None

    @property
    def distinct_uris(self) -> int:
        return len(set(self.by_name.values()))

    def as_evidence(self) -> dict:
        return {
            "contract_version": self.contract_version,
            "normalizer_version": self.normalizer_version,
            "endpoints": len(self.by_name),
            "distinct_uris": self.distinct_uris,
            "bindings": self.evidence,
        }


async def freeze_endpoint_map(
    conn,
    endpoints: Sequence[Endpoint],
    *,
    normalize: Callable[[str], str],
    expected_uris: Optional[dict] = None,
    alias_decisions: Optional[dict] = None,
) -> FrozenMap:
    """Re-read every endpoint from the registry and pin it. Blocks on ambiguity.

    Deliberately re-reads rather than trusting the preflight + registration
    responses: those describe what SHOULD have happened, and the point of this step
    is to establish what IS true immediately before the first fact write. When
    `expected_uris` is supplied, every disagreement with it is an error — that is
    the check that catches a registration which reported one URI and produced
    another.
    """
    alias_decisions = alias_decisions or {}
    if not endpoints:
        return FrozenMap(by_name={}, by_key={}, by_name_type={}, evidence=[],
                         normalize=normalize)

    types = sorted({e.entity_type for e in endpoints if e.entity_type})
    rows = list(await conn.fetch(_CANDIDATE_SQL, types)) if types else []

    # Every decided URI is read back by URI, whatever its type and whether or not
    # it is live, so the decision can be VALIDATED rather than trusted. The first
    # version bound `alias_decisions[name]` verbatim: no liveness check, no type
    # check, and it skipped the `expected_uris` disagreement check — so a decision
    # naming a merged-away or wrong-type URI passed the freeze and the pinned
    # write then 422'd (review finding M4). Preflight does NOT refuse these — it
    # logs and ignores an unmatched decision (see the alias branch there), so this
    # read-back is the ONLY validation the decision gets before the write. It runs
    # immediately before the write and must be at least as strict as the write.
    decided_uris = sorted({alias_decisions[e.name] for e in endpoints
                           if alias_decisions.get(e.name)})
    by_uri = {r["fuseki_uri"]: r
              for r in (await conn.fetch(_BY_URI_SQL, decided_uris) if decided_uris else [])}

    by_stored: dict[tuple, list] = {}
    by_current: dict[tuple, list] = {}
    for row in rows:
        if row["merged_into"] is not None or row["revoked_at"] is not None:
            continue
        etype = row["entity_type"]
        by_stored.setdefault((row["normalized_text"], etype), []).append(row)
        by_current.setdefault((normalize(row["entity_text"] or ""), etype), []).append(row)

    by_name: dict = {}
    by_key: dict = {}
    by_name_type: dict = {}
    evidence: list = []
    problems: list = []

    for ep in endpoints:
        if ep.entity_type is None:
            # Cannot be reached through the preflight (it blocks first); refused
            # here too so a caller that skipped the gate cannot bind a type-less
            # endpoint to anything.
            problems.append({"state": STATE_TYPE_UNDECLARED, "name": ep.name,
                             "type": None, "candidates": []})
            continue

        decided = alias_decisions.get(ep.name)
        if decided:
            target = by_uri.get(decided)
            if target is None or target["merged_into"] is not None or target["revoked_at"] is not None:
                problems.append({
                    "state": "alias_decision_not_live",
                    "name": ep.name, "type": ep.entity_type, "decided_uri": decided,
                    "reason": ("unknown URI" if target is None
                               else "merged into " + str(target["merged_into"])
                               if target["merged_into"] is not None else "revoked"),
                })
                continue
            if target["entity_type"] != ep.entity_type:
                problems.append({
                    "state": "alias_decision_type_mismatch",
                    "name": ep.name, "type": ep.entity_type, "decided_uri": decided,
                    "target_type": target["entity_type"],
                })
                continue
            if expected_uris is not None and ep.name in expected_uris and expected_uris[ep.name] != decided:
                problems.append({
                    "state": "registration_disagreement",
                    "name": ep.name, "type": ep.entity_type,
                    "registration_returned": expected_uris[ep.name],
                    "registry_holds": decided,
                })
                continue
            by_name[ep.name] = decided
            by_key[ep.key] = decided
            by_name_type[ep.name] = ep.entity_type
            evidence.append({
                "name": ep.name, "type": ep.entity_type, "uri": decided,
                "bound_via": "audited_alias_decision",
                "canonical_label": target["entity_text"],
            })
            continue

        merged = {r["fuseki_uri"]: r for r in by_stored.get(ep.key, [])}
        via = {u: "stored_norm" for u in merged}
        for r in by_current.get(ep.key, []):
            if r["fuseki_uri"] not in merged:
                merged[r["fuseki_uri"]] = r
                via[r["fuseki_uri"]] = "current_norm"

        if len(merged) != 1:
            problems.append({
                "state": STATE_AMBIGUOUS if merged else STATE_MISSING,
                "name": ep.name, "type": ep.entity_type,
                "candidates": sorted(merged),
            })
            continue

        uri = next(iter(merged))
        if expected_uris is not None and ep.name in expected_uris and expected_uris[ep.name] != uri:
            problems.append({
                "state": "registration_disagreement",
                "name": ep.name, "type": ep.entity_type,
                "registration_returned": expected_uris[ep.name],
                "registry_holds": uri,
            })
            continue

        by_name[ep.name] = uri
        by_key[ep.key] = uri
        by_name_type[ep.name] = ep.entity_type
        evidence.append({
            "name": ep.name, "type": ep.entity_type, "uri": uri,
            "bound_via": via[uri],
            "canonical_label": merged[uri]["entity_text"],
        })

    if problems:
        raise IdentityError(
            f"cannot freeze an endpoint map: {len(problems)} of {len(endpoints)} endpoints do not "
            f"have exactly one live same-type identity. "
            f"First: {json.dumps(problems[0], sort_keys=True)[:500]}",
            blockers=problems,
        )

    # Issue #62 requirement 6 — the bijection. Distinct typed payload endpoints must
    # map to distinct URIs. This is the assertion the whole module exists to make,
    # and it is the one the stock structural gate never made.
    collapsed: dict = {}
    for name, uri in by_name.items():
        collapsed.setdefault(uri, []).append(name)
    shared = {
        uri: sorted(names) for uri, names in collapsed.items()
        if len(names) > 1 and not all(alias_decisions.get(n) == uri for n in names)
    }
    if shared:
        raise IdentityError(
            f"distinct payload endpoints resolved to the same identity, with no audited alias "
            f"decision permitting it: "
            f"{json.dumps({k: v for k, v in list(shared.items())[:5]}, sort_keys=True)}",
            blockers=[{"state": "endpoint_collapse", "uri": k, "names": v} for k, v in shared.items()],
        )

    return FrozenMap(by_name=by_name, by_key=by_key, by_name_type=by_name_type,
                     evidence=evidence, normalize=normalize)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Verify the persisted graph against the frozen map
# ─────────────────────────────────────────────────────────────────────────────

_PERSISTED_ENDPOINTS_SQL = """
    SELECT id::text AS id, subject_uri, object_uri, predicate, fact_text
      FROM knowledge_facts
     WHERE episode_id = $1
       AND valid_to IS NULL
"""

# Scoped to the facts THIS run wrote. Episodes are keyed on (source_document,
# group_id) and are SHARED: 11 live episodes are shared by 24 documents. An
# episode-wide read verified a sibling document's facts against this run's map
# and failed — after this run's facts had committed (review finding M5). The
# server returns `fact_ids` for exactly the rows it inserted; verify those.
_PERSISTED_BY_ID_SQL = """
    SELECT id::text AS id, subject_uri, object_uri, predicate, fact_text
      FROM knowledge_facts
     WHERE episode_id = $1
       AND id = ANY($2::uuid[])
"""


@dataclass
class VerifyReport:
    checked_facts: int
    offending: list
    persisted_uris: int
    expected_uris: int
    unused_endpoints: list
    scope: str = "episode"     # "run_fact_ids" when scoped to this run's rows

    @property
    def ok(self) -> bool:
        return not self.offending

    def as_evidence(self) -> dict:
        return {
            "checked_facts": self.checked_facts,
            "persisted_distinct_uris": self.persisted_uris,
            "frozen_distinct_uris": self.expected_uris,
            "offending_facts": len(self.offending),
            "offending_sample": self.offending[:10],
            "unused_endpoints": len(self.unused_endpoints),
            "binding_verified": self.ok,
            "scope": self.scope,
        }


async def verify_persisted_graph(conn, episode_id, frozen: FrozenMap, *,
                                 fact_ids: Optional[Sequence[str]] = None) -> VerifyReport:
    """Every persisted subject/object of THIS RUN's facts must be a pinned URI.

    `fact_ids` are the ids the server reported inserting (`EpisodeCreateResponse.
    fact_ids`). With them, only those rows are checked. Without them the check
    falls back to the whole episode — correct for an unshared episode and a false
    failure on a shared one, which is why the extractor always passes them.

    Run this BEFORE relational/discourse finalization. A document that fails here
    must not receive document_entity_links, discourse moves, or a deep_extracted_at
    stamp — otherwise it reads as complete while pointing at the wrong entities,
    which is exactly what the audited Buehler import did.
    """
    allowed = set(frozen.by_name.values())
    if fact_ids is not None:
        rows = list(await conn.fetch(_PERSISTED_BY_ID_SQL, episode_id, list(fact_ids)))
    else:
        rows = list(await conn.fetch(_PERSISTED_ENDPOINTS_SQL, episode_id))

    offending: list = []
    seen: set = set()
    for row in rows:
        for role in ("subject_uri", "object_uri"):
            uri = row[role]
            if uri is None:
                continue  # literal-object facts have no object_uri
            seen.add(uri)
            if uri not in allowed:
                offending.append({
                    "fact_id": row["id"],
                    "role": role,
                    "persisted_uri": uri,
                    "predicate": row["predicate"],
                    "fact_text": (row["fact_text"] or "")[:160],
                })

    unused = sorted(allowed - seen)
    return VerifyReport(
        checked_facts=len(rows),
        offending=offending,
        persisted_uris=len(seen),
        expected_uris=len(allowed),
        unused_endpoints=unused,
        scope="run_fact_ids" if fact_ids is not None else "episode",
    )


def require_binding_verified(report: VerifyReport) -> None:
    if report.ok:
        return
    raise IdentityError(
        f"{len(report.offending)} persisted fact endpoint(s) are not in the frozen payload map; "
        f"the import is NOT sound and must not be finalized. "
        f"First: {json.dumps(report.offending[0], sort_keys=True)[:500]}",
        blockers=report.offending,
    )
