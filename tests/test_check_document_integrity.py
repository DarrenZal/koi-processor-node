"""The operator gate must run the PRODUCTION merge and report what production does.

Independent review of PR #66 (finding M7): the read-only gate
`scripts/check_document_integrity.py` diverged from the extractor it was meant to
pre-flight. It de-duplicated entities by (name, type) while the extractor coerces
by name → the gate BLOCKED 100 of 1,755 cached documents production would ingest;
its "cross-window coercion(s)" count was structurally 0; `offending_facts: 0` was a
literal; the bijection was never evaluated when any endpoint was missing, yet the
gate exited 0; a sanctioned alias collapse read as a FAIL; and blockers were
truncated to 10 BEFORE being counted. It also never checked that a curated
payload's `curation.document_rid` was the document it was being run against.

ISOLATION. One asyncpg connection in a transaction rolled back at teardown, on the
scratch database — the gate reads `koi_memory_chunks` and
`document_window_extractions`, so a synthetic document is seeded there.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import asyncpg
import pytest

from api import ingest_identity as ident
from api.resolution_primitives import normalize_entity_text
from tests import koi_test_targets as targets

REPO = Path(__file__).resolve().parent.parent
GATE = REPO / "scripts" / "check_document_integrity.py"


def _gate():
    spec = importlib.util.spec_from_file_location("gate_under_test", GATE)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    db = await c.fetchval("SELECT current_database()")
    assert db != "personal_koi", f"refusing to run against {db!r}"
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()
    await c.close()


def _tag():
    return uuid.uuid4().hex[:10]


async def _seed_entity(conn, *, uri, text, etype):
    await conn.execute(
        """INSERT INTO entity_registry
             (fuseki_uri, entity_text, entity_type, normalized_text, source)
           VALUES ($1,$2,$3,$4,'pytest')""",
        uri, text, etype, normalize_entity_text(text))


async def _seed_document(conn, *, rid, windows, chunks_per_window=1):
    """A synthetic document: one chunk per window (default) and one cached window
    extraction per entry in `windows`. Each chunk's text carries every entity name
    its window mentions, so Gate 2's citation_support is satisfied and an exit code
    in these tests reflects Gate 1 (identity), which is what they are about."""
    n_chunks = chunks_per_window * len(windows)
    await conn.execute(
        "INSERT INTO koi_memories (rid, event_type, source_sensor, content) "
        "VALUES ($1, 'NEW', 'pytest', '{}'::jsonb)", rid)
    await conn.execute(
        "INSERT INTO document_ingestion_log (document_rid, content_hash) VALUES ($1, $2)",
        rid, "h-" + rid)
    for i in range(n_chunks):
        names = " ".join(e["name"] for e in (windows[i // chunks_per_window].get("entities") or []))
        await conn.execute(
            "INSERT INTO koi_memory_chunks (chunk_rid, document_rid, chunk_index, total_chunks, content) "
            "VALUES ($1, $2, $3, $4, $5::jsonb)",
            f"{rid}#chunk{i}", rid, i, n_chunks,
            json.dumps({"text": f"chunk {i} mentions {names} and more content words"}))
    for w, raw in enumerate(windows):
        await conn.execute(
            "INSERT INTO document_window_extractions "
            "(document_rid, window_index, char_start, char_end, chunk_index_base, status, raw_json) "
            "VALUES ($1, $2, $3, $4, $5, 'extracted', $6::jsonb)",
            rid, w, w * 100, w * 100 + 99, w * chunks_per_window, json.dumps(raw))


def _ent(name, etype, chunk=0):
    return {"name": name, "type": etype, "first_seen_chunk": chunk, "mention_count": 1}


def _fact(subj, obj, pred="RELATES_TO", chunk=0):
    return {"subject": subj, "predicate": pred, "object": obj, "object_literal": None,
            "fact_text": f"{subj} {pred} {obj}", "chunk_range": [chunk, chunk],
            "confidence": "high"}


def _facts(subj, obj, chunk=0):
    """Four distinct predicates between one pair, so predicate_concentration is
    0.25 rather than the 1.0 a single fact would score."""
    return [_fact(subj, obj, pred=p, chunk=chunk)
            for p in ("RELATES_TO", "PART_OF", "SUPPORTS", "DEFINES")]


@pytest.mark.anyio
class TestTheGateRunsTheProductionMerge:

    async def test_a_cross_window_type_coercion_is_folded_not_blocked(self, conn):
        """Two windows, one label, two types. Production folds them into ONE entity
        by type priority and reports the coercion. The old gate kept both records
        and then raised `payload_type_conflict` on its own artefact."""
        gate = _gate()
        t = _tag()
        rid = f"document:test-{t}"
        label, other = f"Omni Mapping {t}", f"Other Thing {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{t}", text=label,
                           etype="Project")
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-o-{t}", text=other,
                           etype="Concept")
        await _seed_document(conn, rid=rid, windows=[
            {"entities": [_ent(label, "Concept"), _ent(other, "Concept")],
             "facts": _facts(label, other, chunk=0)},
            {"entities": [_ent(label, "Project"), _ent(other, "Concept", 1)],
             "facts": _facts(label, other, chunk=1)},
        ])
        report, code = await gate.run_gate(conn, document_rid=rid, tier="standard")
        assert report["identity"]["status"] == "pass", report["identity"]
        assert code == 0
        conflicts = report["type_contract"]["conflicts"]
        assert len(conflicts) == 1 and conflicts[0]["kept"] == "Project", conflicts
        assert report["identity"]["endpoints"] == 2

    async def test_the_bijection_is_evaluated_even_when_endpoints_are_missing(self, conn):
        """`pass_after_preregistration` used to mean 'the freeze raised on the first
        missing endpoint and nothing else was checked'. The resolvable endpoints
        must still be frozen and their bijection asserted, and every would-be
        mint must be reported WITH the type it would be minted as."""
        gate = _gate()
        t = _tag()
        rid = f"document:test-{t}"
        live, new = f"Live One {t}", f"Brand New {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-{t}", text=live,
                           etype="Concept")
        await _seed_document(conn, rid=rid, windows=[
            {"entities": [_ent(live, "Concept"), _ent(new, "Document")],
             "facts": _facts(live, new)},
        ])
        report, code = await gate.run_gate(conn, document_rid=rid, tier="standard")
        i = report["identity"]
        assert i["status"] == "pass_after_preregistration", i
        assert i["bijective"] is True
        assert i["endpoints"] == 1 and i["distinct_uris"] == 1
        assert i["would_preregister"] == [{"name": new, "type": "Document"}]
        assert code == 0
        _report, strict = await gate.run_gate(conn, document_rid=rid, tier="standard",
                                              strict_preregistration=True)
        assert strict == 2

    async def test_a_sanctioned_alias_collapse_is_not_a_false_fail(self, conn):
        gate = _gate()
        t = _tag()
        rid = f"document:test-{t}"
        a, b = f"First {t}", f"Second {t}"
        shared = f"orn:personal-koi.entity:concept-shared-{t}"
        await _seed_entity(conn, uri=shared, text=f"Shared {t}", etype="Concept")
        await _seed_document(conn, rid=rid, windows=[
            {"entities": [_ent(a, "Concept"), _ent(b, "Concept")],
             "facts": _facts(a, b)},
        ])
        report, code = await gate.run_gate(
            conn, document_rid=rid, tier="standard", alias_decisions={a: shared, b: shared})
        i = report["identity"]
        assert i["status"] == "pass", i
        assert i["bijective"] is True
        assert i["distinct_uris"] == 1 and i["endpoints"] == 2
        assert i["sanctioned_alias_collapses"] == 1
        assert code == 0

    async def test_blockers_are_counted_before_they_are_truncated(self, conn):
        gate = _gate()
        t = _tag()
        rid = f"document:test-{t}"
        names = [f"Clash {k} {t}" for k in range(12)]
        for k, n in enumerate(names):
            await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{k}-{t}", text=n,
                               etype="Project")
        anchor = f"Anchor {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-anchor-{t}",
                           text=anchor, etype="Concept")
        await _seed_document(conn, rid=rid, windows=[
            {"entities": [_ent(anchor, "Concept")] + [_ent(n, "Organization") for n in names],
             "facts": [_fact(anchor, n) for n in names]},
        ])
        report, code = await gate.run_gate(conn, document_rid=rid, tier="standard")
        i = report["identity"]
        assert i["status"] == "blocked"
        assert i["blockers_total"] == 12
        assert len(i["blockers"]) == 10
        assert code == 1

    async def test_check_only_evidence_fabricates_no_verification(self, conn):
        gate = _gate()
        t = _tag()
        rid = f"document:test-{t}"
        a, b = f"A {t}", f"B {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-a-{t}", text=a, etype="Concept")
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-b-{t}", text=b, etype="Concept")
        await _seed_document(conn, rid=rid, windows=[
            {"entities": [_ent(a, "Concept"), _ent(b, "Concept")], "facts": _facts(a, b)},
        ])
        report, _code = await gate.run_gate(conn, document_rid=rid, tier="standard")
        assert "verification" not in report["identity_evidence"]
        integrity = next(d for d in report["quality"]["dimensions"]
                         if d["name"] == "endpoint_integrity")
        assert "check-only" in integrity["detail"]
        assert integrity["value"] == 1.0

    async def test_an_untyped_fact_endpoint_blocks_with_its_type_shown_as_none(self, conn):
        """M3 through the gate: no entities[] entry, no decision → blocked, and the
        printed blocker says type None rather than Concept."""
        gate = _gate()
        t = _tag()
        rid = f"document:test-{t}"
        a = f"A {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-a-{t}", text=a, etype="Concept")
        await _seed_document(conn, rid=rid, windows=[
            {"entities": [_ent(a, "Concept")], "facts": _facts(a, f"Ghost {t}")},
        ])
        report, code = await gate.run_gate(conn, document_rid=rid, tier="standard")
        i = report["identity"]
        assert i["status"] == "blocked" and code == 1
        assert i["blockers"][0]["state"] == ident.STATE_TYPE_UNDECLARED
        assert i["blockers"][0]["type"] is None
        # …and a type decision clears it without anything being minted.
        report2, code2 = await gate.run_gate(conn, document_rid=rid, tier="standard",
                                             type_decisions={f"Ghost {t}": "Person"})
        assert report2["identity"]["status"] == "pass_after_preregistration", report2["identity"]
        assert report2["identity"]["would_preregister"] == [{"name": f"Ghost {t}", "type": "Person"}]
        assert code2 == 0


class TestCuratedPayloadProvenance:

    def test_a_payload_curated_for_another_document_is_refused(self, tmp_path):
        gate = _gate()
        payload = {"entities": [], "facts": [], "discourse": [], "type_map": {},
                   "curation": {"document_rid": "document:other"}}
        problem = gate.payload_provenance_problem(payload, "document:this")
        assert problem and "document:other" in problem and "document:this" in problem
        assert gate.payload_provenance_problem(payload, "document:other") is None
        # A payload with no curation block is not refused, but is named as such.
        assert gate.payload_provenance_problem({"facts": []}, "document:x") is None

    def test_the_cli_exits_3_before_touching_the_database(self, tmp_path):
        p = tmp_path / "curated.json"
        p.write_text(json.dumps({"entities": [], "facts": [], "discourse": [], "type_map": {},
                                 "curation": {"document_rid": "document:other"}}))
        env = dict(os.environ, POSTGRES_URL="postgresql://nobody:@127.0.0.1:1/does_not_exist")
        r = subprocess.run(
            [sys.executable, str(GATE), "--document-rid", "document:this", "--payload", str(p)],
            capture_output=True, text=True, env=env, cwd=str(REPO))
        assert r.returncode == 3, (r.returncode, r.stdout, r.stderr)
        assert "curation.document_rid" in r.stderr
