"""Focused tests for the local review surface (api/discourse_review.py,
scripts/discourse_review.py, scripts/koi-discourse). No database: rows are built
in the shape candidate_rows returns, and the read-only guard is tested with a
stand-in connection."""

import ast
import asyncio
import copy
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from api import discourse_review as dr  # noqa: E402

GARFIELD_DOC = "document:" + "a" * 64
GARFIELD_SUB = "substack-corpus:michaelgarfield:scenius"
LEVIN_DOC = "document:" + "b" * 64
JOHAR = "substack-corpus:indyjohar:a-drone-night-in-munich"
JOHAR2 = "substack-corpus:indyjohar:reforming-time"


def end(move_id, rid, title, *, detail=None, repr_sha="r" * 64, version="sha256:" + "v" * 64,
        kind="content_addressed_source", limitations=("content-addressed source attribution, not recorded extraction provenance",)):
    return {"move_id": move_id, "move_type": "claim", "title": title, "detail": detail, "status": "asserted",
            "chunk_range": [3, 4],
            "representation_sha256": repr_sha, "representation_at_discovery": repr_sha,
            "text_changed_since_discovery": False,
            "source": {"rid": rid, "title": f"title of {rid[:20]}", "url": None, "version": version,
                       "version_basis": "document_ingestion_log.content_hash"},
            "binding": {"kind": kind, "basis": "basis", "association": None, "bound_version": version,
                        "bound_version_at_discovery": version, "current": True, "limitations": list(limitations)}}


def row(cid, a, b, fused):
    return {"candidate_id": cid, "candidate_key": "k" + cid, "run_id": "canary:x", "method": "hybrid-rrf/v0",
            "discovered_by": "system:test", "times_seen": 1,
            "scores": {"fused_score": fused, "lexical_rank": 1, "lexical_score": 0.9, "semantic_rank": 2, "semantic_score": 0.8},
            "a": a, "b": b, "current": True, "not_current_reasons": []}


MAPPINGS = {"same_work": {GARFIELD_DOC: "garfield-scenius", GARFIELD_SUB: "garfield-scenius"},
            "authors": {LEVIN_DOC: ["Michael Levin", "Lyons"], GARFIELD_DOC: ["Michael Garfield"],
                        JOHAR: ["Indy Johar"], JOHAR2: ["Indy Johar"]},
            "author_basis": {LEVIN_DOC: "source title"},
            "path": "test", "sha256": "m" * 64, "prepared_by": "test fixture"}


def rows():
    return [
        row("c1", end("m1", GARFIELD_DOC, "Michael Levin's paper describes the economy as a collective intelligence."),
            end("m2", LEVIN_DOC, "The price system is the economy's cognitive glue."), 0.033),
        row("c2", end("m3", GARFIELD_DOC, "Industry is prokaryotic."), end("m4", GARFIELD_SUB, "Industry is prokaryotic, AI eukaryotic."), 0.032),
        row("c3", end("m5", LEVIN_DOC, "Local optimisation helps the collective."), end("m6", JOHAR, "Local optimisation hurt Munich."), 0.031),
        row("c4", end("m7", LEVIN_DOC, "Prices encode scarcity."), end("m8", JOHAR, "Taxi drivers refused long routes."), 0.030),
        row("c5", end("m9", JOHAR, "Time has three layers."), end("m10", JOHAR2, "Machine time is UTC."), 0.029),
        row("c6", end("m11", GARFIELD_DOC, "Intelligence is fractal."), end("m12", LEVIN_DOC, "All intelligence is collective."), 0.028),
    ]


def batch_of(rows_):
    lanes = dr.queue(rows_, MAPPINGS)
    items = [r for k in dr.LANES for r in lanes[k]]
    return dr.make_batch(items, database="personal_koi", mappings=MAPPINGS, created_at="2026-10-20T00:00:00+00:00")


# ---- lanes, cues, ordering ------------------------------------------------------

def test_same_work_lane_uses_mapping_and_keeps_every_candidate():
    lanes = dr.queue(rows(), MAPPINGS)
    assert [r["candidate_id"] for r in lanes["same_work_mapped"]] == ["c2"]
    assert sum(len(v) for v in lanes.values()) == len(rows())


def test_suggested_same_work_is_its_own_labelled_lane():
    lanes = dr.queue(rows(), {"same_work": {}, "authors": {}}, suggested=[frozenset((GARFIELD_DOC, GARFIELD_SUB))])
    assert [r["candidate_id"] for r in lanes["same_work_suggested"]] == ["c2"]
    assert "Unverified" in dr.render(lanes["same_work_suggested"][0])
    assert lanes["same_work_mapped"] == []


def test_mapping_file_is_loaded_with_its_hash_and_refuses_a_source_in_two_works(tmp_path):
    p = tmp_path / "m.json"
    p.write_text(json.dumps({"prepared_by": "draft", "same_work": {"w1": ["x", "y"]},
                             "authors": {"x": ["A B", " "], "y": {"names": ["C D"], "basis": "source title"}}}))
    m = dr.load_mappings(str(p))
    assert m["same_work"] == {"x": "w1", "y": "w1"} and m["prepared_by"] == "draft"
    assert m["authors"] == {"x": ["A B"], "y": ["C D"]} and m["author_basis"] == {"x": None, "y": "source title"}
    assert len(m["sha256"]) == 64
    assert dr.load_mappings(None)["same_work"] == {}
    p.write_text(json.dumps({"same_work": {"w1": ["x", "y"], "w2": ["y", "z"]}}))
    with pytest.raises(dr.ReviewError):
        dr.load_mappings(str(p))


def test_cues_are_labelled_and_never_claim_dependence():
    c1 = dr.cues(rows()[0], MAPPINGS)
    assert [c["kind"] for c in c1] == ["names_other_source_author"]
    assert c1[0]["side"] == "a" and c1[0]["matched"] == "Levin"
    assert "not verified citation, restatement or dependence" in c1[0]["note"]
    c5 = dr.cues(rows()[4], MAPPINGS)
    assert {c["kind"] for c in c5} == {"shared_author_listed", "same_publication"}
    assert all("does not establish" in c["note"] or "not evidence" in c["note"] for c in c5)
    # Only the notes mention dependence, and only to deny it. No field or value asserts it.
    fields = json.dumps([{k: v for k, v in c.items() if k != "note"} for c in c1 + c5]).lower()
    assert "depend" not in fields and "restat" not in fields and "cite" not in fields
    assert "independent" not in json.dumps([c1, c5]).lower()


def test_name_cue_is_case_sensitive_and_whole_word():
    r = rows()[0]
    r["a"]["title"] = "the Levinson effect and a levin-like idea"
    assert dr.cues(r, MAPPINGS) == []
    r["a"]["title"] = "as (Levin, 2023) argued"
    assert [c["matched"] for c in dr.cues(r, MAPPINGS)] == ["Levin"]


def test_source_diverse_order_interleaves_pairs_and_keeps_all():
    rs = [row(f"x{i}", end(f"a{i}", LEVIN_DOC, "a"), end(f"b{i}", JOHAR, "b"), 0.05 - i * 0.001) for i in range(3)]
    rs += [row(f"y{i}", end(f"c{i}", GARFIELD_DOC, "c"), end(f"d{i}", LEVIN_DOC, "d"), 0.04 - i * 0.001) for i in range(2)]
    fused = [r["candidate_id"] for r in dr.order(rs, "fused")]
    diverse = [r["candidate_id"] for r in dr.order(rs, "source_diverse")]
    assert fused == ["x0", "x1", "x2", "y0", "y1"]
    assert diverse == ["x0", "y0", "x1", "y1", "x2"]
    assert sorted(diverse) == sorted(fused)
    assert diverse == [r["candidate_id"] for r in dr.order(list(reversed(rs)), "source_diverse")]


# ---- what a reviewer sees -------------------------------------------------------

def test_blind_view_and_render_hide_scores_and_run_but_show_passages_versions_and_limits():
    item = dr.queue(rows(), MAPPINGS)["cross_source"][0]
    v = dr.view(item, blind=True)
    for hidden in ("scores", "run_id", "discovered_by", "times_seen", "method"):
        assert hidden not in v
    text = dr.render(item, position=1, total=4, blind=True)
    assert item["a"]["title"] in text and item["b"]["title"] in text
    assert "source version: sha256:" in text and "limitation: content-addressed" in text
    assert "extracted from source chunks 3-4" in text
    assert "authors: Michael Levin, Lyons (mapping: source title)" in text
    unmapped = dict(item, authors=dr.listed_authors(item, {"authors": {}}))
    assert dr.render(unmapped).count("authors: unknown (not in the mapping)") == 2
    assert "Admission is disabled" in text
    assert "fused" not in text and "canary:x" not in text
    assert "fused" in dr.render(item, blind=False)


def test_batch_carries_exact_text_and_versions_but_no_scores():
    b = batch_of(rows())
    assert b["schema"] == dr.BATCH_SCHEMA and len(b["items"]) == 6
    s = json.dumps(b)
    assert "fused_score" not in s and "canary:x" not in s and "lexical_rank" not in s
    it = b["items"][0]
    assert it["a"]["representation_sha256"] == "r" * 64 and it["a"]["source"]["version"].startswith("sha256:")
    assert b["mappings"] == {"path": "test", "sha256": "m" * 64, "prepared_by": "test fixture"}
    other = dr.make_batch([], database="personal_koi", mappings={**MAPPINGS, "sha256": "n" * 64}, created_at="x")
    assert other["batch_id"] != dr.make_batch([], database="personal_koi", mappings=MAPPINGS, created_at="x")["batch_id"]


# ---- annotations ----------------------------------------------------------------

def test_annotation_binds_candidate_text_versions_and_reviewer(tmp_path):
    b = batch_of(rows())
    rec = dr.annotation(b, 1, reviewer="reviewer-a", answer="useful", reason="  shows a citation ", os_user="u",
                        tool_version="abc", created_at="2026-10-20T01:00:00+00:00")
    it = b["items"][0]
    assert rec["candidate"] == {"id": it["candidate_id"], "candidate_key": it["candidate_key"]}
    assert rec["shown"]["a"]["representation_sha256"] == it["a"]["representation_sha256"]
    assert rec["shown"]["b"]["source_version"] == it["b"]["source"]["version"]
    assert rec["reviewer"] == "reviewer-a" and rec["reason"] == "shows a citation" and rec["question"] == dr.QUESTION
    assert rec["annotation_id"] == dr._sha({k: v for k, v in rec.items() if k != "annotation_id"})


def test_append_only_and_supersede(tmp_path):
    path = str(tmp_path / "ann.jsonl")
    b = batch_of(rows())
    first = dr.annotation(b, 1, reviewer="reviewer-a", answer="marginal", created_at="t1")
    dr.append_annotation(path, first)
    before = Path(path).read_bytes()
    second = dr.annotation(b, 1, reviewer="reviewer-a", answer="useful", supersedes=first["annotation_id"], created_at="t2")
    dr.append_annotation(path, second)
    after = Path(path).read_bytes()
    assert after.startswith(before) and after.count(b"\n") == 2
    assert [a["answer"] for a in dr.latest(dr.read_annotations(path))] == ["useful"]


def test_refusals_write_nothing(tmp_path):
    path = tmp_path / "ann.jsonl"
    b = batch_of(rows())
    with pytest.raises(dr.ReviewError):
        dr.annotation(b, 1, reviewer="  ", answer="useful")
    with pytest.raises(dr.ReviewError):
        dr.annotation(b, 1, reviewer="reviewer-a", answer="supports")
    with pytest.raises(dr.ReviewError):
        dr.annotation(b, 99, reviewer="reviewer-a", answer="useful")
    rec = dr.annotation(b, 1, reviewer="reviewer-a", answer="useful", created_at="t1")
    dr.append_annotation(str(path), rec)
    with pytest.raises(dr.ReviewError):          # exact duplicate
        dr.append_annotation(str(path), rec)
    other = dr.annotation(b, 2, reviewer="reviewer-a", answer="useful", supersedes=rec["annotation_id"], created_at="t2")
    with pytest.raises(dr.ReviewError):          # supersedes another candidate's annotation
        dr.append_annotation(str(path), other)
    assert Path(path).read_text().count("\n") == 1


def test_damaged_or_edited_file_is_refused(tmp_path):
    path = tmp_path / "ann.jsonl"
    b = batch_of(rows())
    dr.append_annotation(str(path), dr.annotation(b, 1, reviewer="reviewer-a", answer="useful", created_at="t1"))
    path.write_text(path.read_text().replace('"useful"', '"not_useful"'))
    with pytest.raises(dr.ReviewError, match="does not match its id"):
        dr.read_annotations(str(path))
    path.write_text('{"partial": ')
    with pytest.raises(dr.ReviewError, match="newline"):
        dr.append_annotation(str(path), dr.annotation(b, 2, reviewer="reviewer-a", answer="useful", created_at="t3"))


def test_binding_problems_catch_changed_text_and_versions():
    b = batch_of(rows())
    it = b["items"][0]
    current = copy.deepcopy(next(r for r in rows() if r["candidate_id"] == it["candidate_id"]))
    current["candidate_key"] = it["candidate_key"]
    assert dr.binding_problems(current, it) == []
    current["b"]["representation_sha256"] = "z" * 64
    current["a"]["source"]["version"] = "sha256:" + "w" * 64
    probs = dr.binding_problems(current, it)
    assert "b: claim text changed since the batch was made" in probs
    assert "a: source version changed since the batch was made" in probs
    assert dr.binding_problems(None, it) == ["candidate is no longer open or visible"]


def test_review_loop_records_skips_and_quits(tmp_path):
    path = str(tmp_path / "ann.jsonl")
    b = batch_of(rows())
    answers = iter(["x", "u", "relevant", "s", "c", "", "q"])
    said = []
    n = dr.review_loop(b, reviewer="reviewer-a", path=path, verify=lambda it: [], ask=lambda _: next(answers), say=said.append)
    got = dr.read_annotations(path)
    assert n == 2 and [a["answer"] for a in got] == ["useful", "needs_context"]
    assert [a["position"] for a in got] == [1, 3] and got[0]["reason"] == "relevant" and got[1]["reason"] is None
    assert any("Please answer" in s for s in said)
    blocked = iter(["u", ""])
    n2 = dr.review_loop(b, reviewer="reviewer-a", path=path, verify=lambda it: ["a: claim text changed"],
                        ask=lambda _: next(blocked, "q"), say=said.append)
    assert n2 == 0 and len(dr.read_annotations(path)) == 2


# ---- read-only and no-write guarantees -------------------------------------------

class _Conn:
    def __init__(self, ro):
        self.ro, self.queries = ro, []

    async def fetchval(self, sql, *a):
        self.queries.append(sql)
        return self.ro

    async def fetch(self, sql, *a):
        self.queries.append(sql)
        return []


def test_candidate_rows_refuses_outside_a_read_only_transaction():
    conn = _Conn("off")
    with pytest.raises(dr.ReviewError):
        asyncio.run(dr.candidate_rows(conn))
    assert conn.queries == ["SHOW transaction_read_only"]


def test_no_write_sql_in_the_review_code():
    write = re.compile(r"\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|ALTER\s+TABLE|CREATE\s+(TABLE|INDEX)|DROP\s+|TRUNCATE)\b", re.I)
    for f in (ROOT / "api" / "discourse_review.py", ROOT / "scripts" / "discourse_review.py"):
        strings = [n.value for n in ast.walk(ast.parse(f.read_text())) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        assert not [s for s in strings if write.search(s)], f


def test_wrapper_routes_review_commands_and_prints_usage():
    env = dict(os.environ, KOI_DISCOURSE_PYTHON=sys.executable, KOI_DISCOURSE_REPO_DIR=str(ROOT))
    out = subprocess.run(["bash", str(ROOT / "scripts" / "koi-discourse"), "--help"], capture_output=True, text=True, env=env)
    assert out.returncode == 0
    assert "koi-discourse annotate" in out.stdout and "Overrides:" in out.stdout and "set -euo" not in out.stdout
    bad = subprocess.run(["bash", str(ROOT / "scripts" / "koi-discourse"), "judge"], capture_output=True, text=True, env=env)
    assert bad.returncode == 2
    help_ = subprocess.run([sys.executable, "-B", str(ROOT / "scripts" / "discourse_review.py"), "annotate", "--help"],
                           capture_output=True, text=True, env=env)
    assert help_.returncode == 0 and "--reviewer" in help_.stdout


def test_wrapper_reaches_the_review_cli(tmp_path):
    """The annotations command needs no database, so it proves the route end to end."""
    path = tmp_path / "ann.jsonl"
    b = batch_of(rows())
    dr.append_annotation(str(path), dr.annotation(b, 2, reviewer="reviewer-a", answer="marginal", created_at="t1"))
    env = dict(os.environ, KOI_DISCOURSE_PYTHON=sys.executable, KOI_DISCOURSE_REPO_DIR=str(ROOT))
    out = subprocess.run(["bash", str(ROOT / "scripts" / "koi-discourse"), "annotations", "--annotations", str(path)],
                         capture_output=True, text=True, env=env)
    assert out.returncode == 0, out.stderr
    assert "marginal" in out.stdout and "1 current answer(s)" in out.stdout


def _load_cli():
    import importlib.util
    spec = importlib.util.spec_from_file_location("discourse_review_cli", ROOT / "scripts" / "discourse_review.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_cli_batch_records_the_mapping_and_refuses_to_overwrite(tmp_path, monkeypatch):
    """Drives cmd_batch with the database read stubbed: the batch must carry the mapping file's hash."""
    cli = _load_cli()
    mp = tmp_path / "mapping.json"
    mp.write_text(json.dumps({"prepared_by": "test", "same_work": {}, "authors": {LEVIN_DOC: ["Michael Levin"]}}))
    lanes = dr.queue(rows(), dr.load_mappings(str(mp)))
    picked = [lanes["cross_source"][0]]

    async def fake_readonly(dsn, fn):
        return "personal_koi", picked
    monkeypatch.setattr(cli, "_readonly", fake_readonly)
    out = tmp_path / "batch.json"
    args = type("A", (), {"out": str(out), "mappings": str(mp), "candidate_id": None, "from_queue": 1,
                          "order": "source_diverse", "note": "n", "dsn": "unused",
                          "run_id": None, "seen_in_run": None, "tier": "all"})()   # what argparse gives when omitted
    cli.cmd_batch(args)
    b = json.loads(out.read_text())
    assert "selection" not in b                                        # no selection: the batch is as before
    assert b["schema"] == dr.BATCH_SCHEMA and not (tmp_path / "batch.binding.json").exists()   # one file
    assert b["mappings"]["sha256"] == dr.load_mappings(str(mp))["sha256"] and b["mappings"]["prepared_by"] == "test"
    assert b["items"][0]["authors"]["b"]["names"] == ["Michael Levin"]
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        cli.cmd_batch(args)
    args.out, args.binding_out = str(tmp_path / "other.json"), str(tmp_path / "x.binding.json")
    with pytest.raises(SystemExit, match="only for a filtered batch"):
        cli.cmd_batch(args)
    assert not (tmp_path / "other.json").exists()


# ---- occurrence tiers (I2) ------------------------------------------------------

def occ(row, run, pick, cap, *, sighting="first", end="a"):
    """One sighting of `row`, as occurrences() reads it: `end` is the stored end whose search found it."""
    tier, basis = dr.occurrence_tier(pick, cap)
    return {"run_id": run, "sighting": sighting, "query_move_id": row[end]["move_id"] if end else None,
            "query_end": end, "pick": pick, "run_per_move_cap": cap, "tier": tier, "tier_basis": basis}


def c1_seen_from_both_ends():
    """rows()[0]: stored A = m1 (Garfield), B = m2 (Levin). First found by A's search; later met from m2's side."""
    r = rows()[0]
    r["occurrences"] = [occ(r, "canary:x", 3, 5), occ(r, "ingest:9:1", 8, 10, sighting="again", end="b")]
    return r


def test_a_tier_comes_from_one_sightings_recorded_pick():
    from api import discourse_discovery as dd
    assert dr.FIRST_PICKS == dd.PROFILE["per_move_cap"] == 5                    # the default's picks are the first five
    assert dr.occurrence_tier(1, 10) == dr.occurrence_tier(5, 10) == ("first_five", "recorded pick")
    assert dr.occurrence_tier(6, 10) == dr.occurrence_tier(10, 10) == ("additional", "recorded pick")
    assert dr.occurrence_tier(None, 5) == ("first_five", "pick not recorded; the run kept at most 5 per claim")
    assert dr.occurrence_tier(None, 10) == dr.occurrence_tier(None, None) == ("unknown", "pick not recorded")
    r = c1_seen_from_both_ends()
    by_candidate, _ = dr.review_items([r, rows()[1]])                          # --run-id or no run filter
    assert [(i["candidate_id"], i["tier"]) for i in by_candidate] == [("c1", "first_five"), ("c2", "unknown")]
    assert all("anchor" not in i for i in by_candidate)
    by_sighting, _ = dr.review_items([r], seen_in_runs=["canary:x", "ingest:9:1"])
    assert [(i["sighting"]["run_id"], i["tier"]) for i in by_sighting] == [("canary:x", "first_five"),
                                                                           ("ingest:9:1", "additional")]
    assert dr.tier_counts(by_sighting) == {"first_five": 1, "additional": 1, "unknown": 0}
    assert dr.review_items([r], seen_in_runs=["ingest:9:1"], tier="first_five")[0] == []
    legacy = rows()[0]
    legacy["occurrences"] = [occ(legacy, "canary:x", None, 5, sighting="again", end=None)]   # query never recorded
    assert dr.review_items([legacy], seen_in_runs=["canary:x"]) == ([], 1)
    with pytest.raises(dr.ReviewError):
        dr.review_items([r], tier="picks_6_to_10")


def test_a_sighting_from_the_stored_b_is_reviewed_with_b_as_the_anchor():
    """Regression: a later sighting whose query was the stored B (m2) was shown and recorded as A=m1."""
    r = c1_seen_from_both_ends()
    items, _ = dr.review_items([r], seen_in_runs=["ingest:9:1"], tier="additional")
    lanes = dr.queue(items, MAPPINGS)
    picked = [i for v in lanes.values() for i in v]
    batch = dr.make_batch(picked, database="personal_koi", mappings=MAPPINGS, created_at="x",
                          selection={"run_ids": None, "seen_in_runs": ["ingest:9:1"], "tier": "additional"})
    it = batch["items"][0]
    assert (it["a"]["move_id"], it["b"]["move_id"]) == ("m2", "m1")             # the sighting's query is A
    assert it["anchor"] == {"move_id": "m2", "stored_end": "b"}
    assert it["sighting"] == {"run_id": "ingest:9:1", "query_move_id": "m2"}
    assert [(c["kind"], c["side"]) for c in it["cues"]] == [("names_other_source_author", "b")]   # cue follows its text
    text = dr.render(it, position=1, total=1, blind=True)
    assert text.index(r["b"]["title"]) < text.index(r["a"]["title"])           # m2's claim is shown as A
    assert "B's text contains 'Levin'" in text
    for hidden in ("pick", "additional", "first_five", "ingest:9:1", "anchor", "tier"):
        assert hidden not in text
    for hidden in ("pick", "additional", "first_five", "tier"):
        assert hidden not in json.dumps(batch["items"])
    rec = dr.annotation(batch, 1, reviewer="agent:test", answer="useful", created_at="t1")
    assert rec["shown"]["orientation"] == dr.SIGHTING_ORIENTATION == "A=query of the selected sighting, B=the claim it picked"
    assert (rec["shown"]["a"]["move_id"], rec["shown"]["b"]["move_id"]) == ("m2", "m1")
    assert rec["anchor"] == it["anchor"] and rec["sighting"] == it["sighting"]
    assert rec["candidate"] == {"id": "c1", "candidate_key": "kc1"}            # the candidate itself is unchanged
    assert r["a"]["move_id"] == "m1" and r["run_id"] == "canary:x"              # and so is its stored first sighting
    # Bound to the sighting and to the exact ends, compared end for end with the stored row.
    assert dr.binding_problems(r, it) == []
    changed = copy.deepcopy(r)
    changed["a"]["representation_sha256"] = "z" * 64                           # stored A, which this item shows as B
    assert dr.binding_problems(changed, it) == ["b: claim text changed since the batch was made"]
    rolled_back = copy.deepcopy(r)
    rolled_back["occurrences"] = rolled_back["occurrences"][:1]
    assert dr.binding_problems(rolled_back, it) == [
        "the selected sighting is no longer current (its run was rolled back, or it never was)"]


def test_a_sighting_from_the_stored_a_keeps_the_stored_direction_and_candidate_reviews_are_unchanged():
    r = c1_seen_from_both_ends()
    items, _ = dr.review_items([r], seen_in_runs=["canary:x"])                 # control: the stored query's own search
    batch = dr.make_batch(dr.queue(items, MAPPINGS)["cross_source"], database="personal_koi", mappings=MAPPINGS,
                          created_at="x")
    it = batch["items"][0]
    assert (it["a"]["move_id"], it["b"]["move_id"], it["anchor"]) == ("m1", "m2", {"move_id": "m1", "stored_end": "a"})
    rec = dr.annotation(batch, 1, reviewer="agent:test", answer="useful", created_at="t1")
    assert rec["shown"]["orientation"] == dr.SIGHTING_ORIENTATION and rec["shown"]["a"]["move_id"] == "m1"
    assert dr.binding_problems(r, it) == []
    # A candidate review (no --seen-in-run) is exactly what I1 recorded: no anchor, no sighting, stored orientation.
    plain = dr.make_batch(dr.queue(dr.review_items([r])[0], MAPPINGS)["cross_source"], database="personal_koi",
                          mappings=MAPPINGS, created_at="x")
    assert plain["batch_id"] == batch_of([rows()[0]])["batch_id"]
    old = dr.annotation(plain, 1, reviewer="agent:test", answer="useful", created_at="t1")
    assert set(old) == {"schema", "question", "answer", "reason", "reviewer", "os_user", "created_at", "batch_id",
                        "position", "blind", "database", "candidate", "shown", "tool", "supersedes", "annotation_id"}
    assert old["shown"]["orientation"] == dr.STORED_ORIENTATION == "A=query, B=target"


def test_reviews_of_the_two_directions_stay_separate(tmp_path):
    r = c1_seen_from_both_ends()
    items, _ = dr.review_items([r], seen_in_runs=["canary:x", "ingest:9:1"])
    batch = dr.make_batch(items, database="personal_koi", created_at="x")
    assert [(i["a"]["move_id"], i["anchor"]["stored_end"]) for i in batch["items"]] == [("m1", "a"), ("m2", "b")]
    path = str(tmp_path / "ann.jsonl")
    one = dr.annotation(batch, 1, reviewer="agent:test", answer="useful", created_at="t1")
    two = dr.annotation(batch, 2, reviewer="agent:test", answer="not_useful", created_at="t1")
    assert one["annotation_id"] != two["annotation_id"]
    assert (one["anchor"], one["sighting"]) == ({"move_id": "m1", "stored_end": "a"},
                                                {"run_id": "canary:x", "query_move_id": "m1"})
    assert (two["anchor"], two["sighting"]) == ({"move_id": "m2", "stored_end": "b"},
                                                {"run_id": "ingest:9:1", "query_move_id": "m2"})
    dr.append_annotation(path, one)
    dr.append_annotation(path, two)
    assert sorted(a["answer"] for a in dr.latest(dr.read_annotations(path))) == ["not_useful", "useful"]
    across = dr.annotation(batch, 2, reviewer="agent:test", answer="useful", supersedes=one["annotation_id"],
                           created_at="t2")
    with pytest.raises(dr.ReviewError, match="studying the same claim A"):
        dr.append_annotation(path, across)
    same = dr.annotation(batch, 2, reviewer="agent:test", answer="marginal", supersedes=two["annotation_id"],
                         created_at="t2")
    dr.append_annotation(path, same)                                            # control: same direction supersedes
    assert sorted(a["answer"] for a in dr.latest(dr.read_annotations(path))) == ["marginal", "useful"]


def test_blind_hides_sightings_and_the_operator_view_labels_each_one():
    r = c1_seen_from_both_ends()
    r["occurrences"].append(occ(r, "canary:old", None, 5))
    item = dr.queue(dr.review_items([r], seen_in_runs=["ingest:9:1"])[0], MAPPINGS)["cross_source"][0]
    for hidden in ("occurrences", "tier", "anchor", "sighting", "scores", "run_id"):
        assert hidden not in dr.view(item, blind=True) and hidden in dr.view(item, blind=False)
    blind = dr.render(item, blind=True)
    for hidden in ("occurrence", "pick", "first_five", "additional", "ingest:", "canary:", "tier", "anchor"):
        assert hidden not in blind
    shown = dr.render(item, blind=False)
    assert "anchor: A, the query of the selected sighting (run ingest:9:1, stored B): additional" in shown
    assert ("occurrence (first sighting): run canary:x, B's search, pick 3 (limit 5 per claim): "
            "first_five (recorded pick)") in shown                              # restated for the swapped ends
    assert ("occurrence (seen again): run ingest:9:1, A's search, pick 8 (limit 10 per claim): "
            "additional (recorded pick)") in shown
    assert "pick ? (limit 5 per claim): first_five (pick not recorded; the run kept at most 5 per claim)" in shown
    assert f"tier note: {dr.TIER_NOTE}" in shown
    assert "not a judgment" in dr.TIER_NOTE and "not a lasting property" in dr.TIER_NOTE


def test_cli_queue_and_batch_keep_the_two_run_filters_apart(monkeypatch, capsys, tmp_path):
    cli = _load_cli()
    monkeypatch.delenv("KOI_DISCOURSE_REVIEW_MAPPINGS", raising=False)
    rs = rows()
    rs[0] = c1_seen_from_both_ends()
    rs[2]["occurrences"] = [occ(rs[2], "ingest:9:1", 2, 10)]
    rs[3]["occurrences"] = [occ(rs[3], "ingest:1:1", 8, 10)]                   # additional, but in another run
    calls = []

    class Conn:
        async def fetchval(self, sql, *a):
            return "personal_koi"

    async def fake_readonly(dsn, fn):
        return await fn(Conn())

    async def fake_rows(conn, run_ids=None, candidate_ids=None, seen_in_runs=None):
        calls.append({"run_ids": run_ids, "seen_in_runs": seen_in_runs})
        return [x for x in rs if not seen_in_runs or any(o["run_id"] in seen_in_runs for o in x.get("occurrences", []))]

    async def no_suggestions(conn):
        return []
    monkeypatch.setattr(cli, "_readonly", fake_readonly)
    monkeypatch.setattr(dr, "candidate_rows", fake_rows)
    monkeypatch.setattr(dr, "suggested_same_work", no_suggestions)
    args = type("A", (), {"dsn": "unused", "run_id": None, "seen_in_run": ["ingest:9:1"], "tier": "additional",
                          "order": "fused", "lane": "all", "limit": 10, "mappings": None, "show_scores": True,
                          "format": "json"})()
    cli.cmd_queue(args)
    out = json.loads(capsys.readouterr().out)
    assert calls[-1] == {"run_ids": None, "seen_in_runs": ["ingest:9:1"]}
    assert out["unit"] == "sightings" and out["tier_counts"] == {"first_five": 1, "additional": 1, "unknown": 0}
    listed = [r for lane in out["lanes"].values() for r in lane]
    assert [(r["candidate_id"], r["a"]["move_id"]) for r in listed] == [("c1", "m2")]   # anchored at the sighting
    args.run_id, args.seen_in_run, args.tier = ["canary:x"], None, "all"
    cli.cmd_queue(args)
    out = json.loads(capsys.readouterr().out)
    assert calls[-1] == {"run_ids": ["canary:x"], "seen_in_runs": None} and out["unit"] == "candidates"
    args.format, args.show_scores, args.run_id, args.seen_in_run, args.tier = "text", False, None, ["ingest:9:1"], "additional"
    cli.cmd_queue(args)
    text = capsys.readouterr().out
    assert "tier=additional" in text and "sightings by tier: first_five=1  additional=1  unknown=0" in text
    assert "pick 8" not in text and "ingest:9:1" not in text.split("\n", 2)[2]  # blind items show no pick or run
    (tmp_path / "reviewer").mkdir()
    (tmp_path / "operator").mkdir()
    out_path, binding_path = tmp_path / "reviewer" / "b.json", tmp_path / "operator" / "b.binding.json"
    bargs = type("B", (), {"dsn": "unused", "candidate_id": None, "run_id": None, "seen_in_run": ["ingest:9:1"],
                           "tier": "additional", "from_queue": 5, "order": "fused", "mappings": None, "note": None,
                           "out": str(out_path), "binding_out": str(binding_path)})()
    cli.cmd_batch(bargs)
    # S1: a filtered batch is a reviewer file plus a binding file; the selection and anchor live in the binding.
    reviewer = json.loads(out_path.read_text())
    binding = json.loads(binding_path.read_text())
    assert reviewer["schema"] == dr.REVIEWER_SCHEMA and "selection" not in reviewer
    assert binding["selection"] == {"run_ids": None, "seen_in_runs": ["ingest:9:1"], "tier": "additional"}
    assert [i["a"]["move_id"] for i in reviewer["items"]] == ["m2"]
    assert [(i["anchor"]["stored_end"], i["sighting"]["run_id"]) for i in binding["items"]] == [("b", "ingest:9:1")]


# ---- reviewer file and binding file (S1) -----------------------------------------

RUN = "ingest:9:1"
# Keys and values a reviewer file must never carry: retrieval runs, ranks, picks, tiers, the
# selection, stored-end metadata, and the candidate identity that would let a reviewer look them up.
FORBIDDEN_KEYS = {"candidate_id", "candidate_key", "scores", "fused_score", "lexical_rank", "lexical_score",
                  "semantic_rank", "semantic_score", "run_id", "run_ids", "seen_in_runs", "pick", "tier",
                  "tier_basis", "tier_counts", "anchor", "sighting", "stored_end", "selection", "occurrences",
                  "queue_position", "salt", "database", "created_at", "method", "discovered_by", "times_seen"}
FORBIDDEN_TOP = FORBIDDEN_KEYS | {"note", "mappings"}            # a cue's "note" is its fixed caveat text


def _walk(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield "key", k
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)
    else:
        yield "value", obj


REVIEWER_TOP = {"schema", "question", "question_text", "answers", "blind", "orientation", "items",
                "binding_commitment", "reviewer_batch_id"}
REVIEWER_ITEM = {"position", "item_id", "lane", "authors", "cues", "a", "b"}
REVIEWER_END = {"move_id", "move_type", "title", "detail", "status", "chunk_range", "representation_sha256",
                "source", "binding"}


def assert_reviewer_file_is_blind(raw: bytes, *, run_ids, candidate_ids, salt=None):
    text = raw.decode("utf-8")
    shape = json.loads(raw)                                    # a whitelist, so a new field cannot slip in
    assert set(shape) == REVIEWER_TOP
    for it in shape["items"]:
        assert set(it) == REVIEWER_ITEM and set(it["authors"]) <= {"a", "b"}
        for side in ("a", "b"):
            assert set(it[side]) == REVIEWER_END
            assert set(it[side]["source"]) == {"rid", "title", "url", "version", "version_basis"}
            assert set(it[side]["binding"]) == {"kind", "association", "bound_version", "current", "limitations"}
    if salt:
        assert salt not in text
    reviewer = json.loads(raw)
    found = list(_walk(reviewer))
    assert not ({k for kind, k in found if kind == "key"} & FORBIDDEN_KEYS)
    assert not (set(reviewer) & FORBIDDEN_TOP)
    assert all(set(c) <= {"kind", "names", "publication", "side", "name", "matched", "basis", "note"}
               for i in reviewer["items"] for c in i["cues"])
    values = {v for kind, v in found if kind == "value"}
    assert not (values & (set(dr.TIERS) | set(run_ids) | set(candidate_ids)))
    for s in list(run_ids) + list(dr.TIERS) + [json.dumps(c) for c in candidate_ids]:
        assert s not in text, s


def mixed_rows():
    """One run (RUN, limit 10) that met c1 from its stored B at pick 8, c3 at pick 2 and c4 at pick 7."""
    rs = rows()
    rs[0] = c1_seen_from_both_ends()
    rs[2]["occurrences"] = [occ(rs[2], RUN, 2, 10)]
    rs[3]["occurrences"] = [occ(rs[3], RUN, 7, 10)]
    return rs


def _cli_batch(tmp_path, monkeypatch, *, tier, name):
    cli = _load_cli()
    monkeypatch.delenv("KOI_DISCOURSE_REVIEW_MAPPINGS", raising=False)
    rs = mixed_rows()
    mp = tmp_path / "mapping.json"
    mp.write_text(json.dumps({"prepared_by": "test", "same_work": {}, "authors": {LEVIN_DOC: ["Michael Levin"]}}))
    for d in ("reviewer", "operator"):
        (tmp_path / d).mkdir(exist_ok=True)

    class Conn:
        async def fetchval(self, sql, *a):
            return "personal_koi"

    async def fake_readonly(dsn, fn):
        return await fn(Conn())

    async def fake_rows(conn, run_ids=None, candidate_ids=None, seen_in_runs=None):
        return [x for x in rs if not seen_in_runs or any(o["run_id"] in seen_in_runs for o in x.get("occurrences", []))]

    async def no_suggestions(conn):
        return []
    monkeypatch.setattr(cli, "_readonly", fake_readonly)
    monkeypatch.setattr(dr, "candidate_rows", fake_rows)
    monkeypatch.setattr(dr, "suggested_same_work", no_suggestions)
    out, bpath = tmp_path / "reviewer" / f"{name}.json", tmp_path / "operator" / f"{name}.binding.json"
    args = type("B", (), {"dsn": "unused", "candidate_id": None, "run_id": None, "seen_in_run": [RUN], "tier": tier,
                          "from_queue": 10, "order": "fused", "mappings": str(mp), "note": "limit-10 trial",
                          "out": str(out), "binding_out": str(bpath)})()
    cli.cmd_batch(args)
    return cli, args, out, bpath, rs


@pytest.mark.parametrize("tier", ["all", "additional"])
def test_the_serialized_reviewer_file_carries_the_question_texts_and_sources_and_nothing_else(tmp_path, monkeypatch,
                                                                                               tier):
    """Mixed-tier (all) and filtered (additional) batches, read back from the files the CLI wrote."""
    _, _, out, bpath, rs = _cli_batch(tmp_path, monkeypatch, tier=tier, name=f"b-{tier}")
    raw, binding = out.read_bytes(), json.loads(bpath.read_text())
    assert_reviewer_file_is_blind(raw, run_ids=[RUN, "canary:x"], candidate_ids=[r["candidate_id"] for r in rs],
                                  salt=binding["salt"])
    reviewer = json.loads(raw)
    assert reviewer["schema"] == dr.REVIEWER_SCHEMA and reviewer["question_text"] == dr.QUESTION_TEXT
    assert reviewer["answers"] == list(dr.ANSWERS) and reviewer["orientation"] == dr.REVIEWER_ORIENTATION
    by_a = {i["a"]["move_id"]: i for i in reviewer["items"]}
    expected = {"all": {"m2", "m5", "m7"}, "additional": {"m2", "m7"}}[tier]
    assert set(by_a) == expected                                       # c1 is shown from its sighting's query, m2
    c1 = by_a["m2"]
    assert (c1["b"]["move_id"], c1["a"]["title"]) == ("m1", "The price system is the economy's cognitive glue.")
    assert c1["a"]["source"] == {"rid": LEVIN_DOC, "title": f"title of {LEVIN_DOC[:20]}", "url": None,
                                 "version": "sha256:" + "v" * 64, "version_basis": "document_ingestion_log.content_hash"}
    assert c1["b"]["binding"]["kind"] == "content_addressed_source" and c1["b"]["binding"]["limitations"]
    # Cues follow the shown direction: B (m1, stored A) names A's listed author.
    assert [(c["kind"], c["side"], c["matched"]) for c in c1["cues"]] == [("names_other_source_author", "b", "Levin")]
    assert c1["authors"]["a"]["names"] == ["Michael Levin"] and c1["authors"]["b"] is None
    # The binding file holds the tier key, the anchors and the selection.
    assert binding["selection"] == {"run_ids": None, "seen_in_runs": [RUN], "tier": None if tier == "all" else tier}
    assert binding["note"] == "limit-10 trial" and binding["database"] == "personal_koi"
    tiers = {b["candidate_id"]: (b["tier"], b["anchor"]["stored_end"]) for b in binding["items"]}
    assert tiers == ({"c1": ("additional", "b"), "c3": ("first_five", "a"), "c4": ("additional", "a")} if tier == "all"
                     else {"c1": ("additional", "b"), "c4": ("additional", "a")})
    assert hashlib.sha256(raw).hexdigest() == binding["reviewer_file_sha256"]
    assert dr.bind_split(raw, binding)["batch_id"] == reviewer["reviewer_batch_id"]


def test_reviewer_positions_follow_the_salted_ids_not_the_queue_order():
    items = dr.queue(dr.review_items(rows())[0], MAPPINGS)["cross_source"]       # fused order c1, c3, c4, ...
    raw, binding = dr.make_split_batch(items, database="d", selection={"x": 1}, salt="s" * 64, created_at="t")
    reviewer = json.loads(raw)
    assert [i["item_id"] for i in reviewer["items"]] == sorted(i["item_id"] for i in reviewer["items"])
    queue_order = [b["queue_position"] for b in binding["items"]]
    assert sorted(queue_order) == list(range(1, len(items) + 1)) and queue_order != sorted(queue_order)
    again, _ = dr.make_split_batch(items, database="d", selection={"x": 1}, salt="t" * 64, created_at="t")
    assert [i["a"]["move_id"] for i in json.loads(again)["items"]] != [i["a"]["move_id"] for i in reviewer["items"]]


def _split(salt="s" * 64):
    items, _ = dr.review_items(mixed_rows(), seen_in_runs=[RUN])
    return dr.make_split_batch(dr.queue(items, MAPPINGS)["cross_source"], database="personal_koi",
                               selection={"run_ids": None, "seen_in_runs": [RUN], "tier": None}, salt=salt,
                               created_at="t")


def _rehash(binding):
    body = {k: v for k, v in binding.items() if k != "binding_id"}
    return {**body, "binding_id": dr._sha(body)}


def _forge(raw, binding):
    """Both files rewritten to agree on everything except the reviewer file's item ids."""
    reviewer = json.loads(raw)
    body = {k: v for k, v in reviewer.items() if k != "reviewer_batch_id"}
    body["binding_commitment"] = dr.binding_commitment(binding["salt"], binding)
    new_raw = dr.reviewer_file_bytes({**body, "reviewer_batch_id": dr._sha(body)})
    return new_raw, _rehash({**binding, "reviewer_batch_id": dr._sha(body),
                             "reviewer_file_sha256": hashlib.sha256(new_raw).hexdigest()})


def test_a_missing_mismatched_or_tampered_binding_file_is_refused():
    raw, binding = _split()
    assert len(dr.bind_split(raw, binding)["items"]) == 3                      # control: the pair binds
    refusals = []

    def refused(r, b, match):
        with pytest.raises(dr.ReviewError, match=match):
            dr.bind_split(r, b)
        refusals.append(match)
    refused(raw, None, "needs its binding file")
    edited = copy.deepcopy(binding)
    edited["items"][0]["tier"] = "first_five"                                  # the tier key changed, id kept
    refused(raw, edited, "binding file does not match its own id")
    other_raw, other = _split(salt="o" * 64)                                   # another batch of the same items
    refused(raw, other, "belongs to another reviewer file")
    swapped = copy.deepcopy(binding)                                           # anchor flipped, id recomputed
    it = swapped["items"][0]
    it["anchor"] = {"move_id": it["anchor"]["move_id"], "stored_end": {"a": "b", "b": "a"}[it["anchor"]["stored_end"]]}
    refused(raw, _rehash(swapped), "contents are not the ones its reviewer file was made with")
    moved = copy.deepcopy(binding)                                             # two items' bindings exchanged
    moved["items"][0]["candidate_id"], moved["items"][1]["candidate_id"] = (moved["items"][1]["candidate_id"],
                                                                            moved["items"][0]["candidate_id"])
    refused(raw, _rehash(moved), "contents are not the ones its reviewer file was made with")
    fewer = copy.deepcopy(binding)
    fewer["items"].pop()
    refused(raw, _rehash(fewer), "contents are not the ones its reviewer file was made with")
    # A forgery that also rewrites the reviewer file's commitment and ids, keeping the items a reviewer
    # saw: the salted item ids still refuse it.
    refused(*_forge(raw, swapped), "do not describe the same item")
    refused(*_forge(raw, moved), "do not describe the same item")
    refused(*_forge(raw, fewer), "list different items")
    reviewer = json.loads(raw)
    reviewer["items"][0]["a"]["title"] = "an edited claim"                     # reviewer file edited, id kept
    refused(dr.reviewer_file_bytes(reviewer), binding, "reviewer file does not match its own id")
    body = {k: v for k, v in reviewer.items() if k != "reviewer_batch_id"}
    refused(dr.reviewer_file_bytes({**body, "reviewer_batch_id": dr._sha(body)}), binding,
            "belongs to another reviewer file")                                # edited and re-hashed
    compact = json.dumps(json.loads(raw), separators=(",", ":")).encode()      # same content, other bytes
    refused(compact, binding, "bytes are not the ones")
    # Edits a reviewer could not see but the trial analysis reads, each with the binding's own id recomputed.
    retiered = copy.deepcopy(binding)
    retiered["items"][0]["tier"] = {"first_five": "additional", "additional": "first_five"}[retiered["items"][0]["tier"]]
    refused(raw, _rehash(retiered), "contents are not the ones its reviewer file was made with")
    for field, value in (("selection", {"run_ids": None, "seen_in_runs": ["ingest:2:1"], "tier": None}),
                         ("note", "edited"), ("database", "another_db")):
        refused(raw, _rehash({**binding, field: value}), "contents are not the ones its reviewer file was made with")
    requeued = copy.deepcopy(binding)
    requeued["items"][0]["queue_position"], requeued["items"][1]["queue_position"] = (
        requeued["items"][1]["queue_position"], requeued["items"][0]["queue_position"])
    refused(raw, _rehash(requeued), "contents are not the ones its reviewer file was made with")
    unsalted = {k: v for k, v in binding.items() if k != "salt"}
    refused(raw, _rehash(unsalted), "does not have the fields a binding file has")
    keyless = copy.deepcopy(binding)
    del keyless["items"][0]["tier"]
    refused(raw, _rehash(keyless), "does not have the fields a binding file has")
    refused(raw, ["not", "a", "binding"], "not a koi-discourse-review-binding/v1 file")
    assert len(refusals) == 20


def test_a_split_annotation_is_v2_keeps_the_sighting_direction_and_binds_like_v1(tmp_path):
    raw, binding = _split()
    batch = dr.bind_split(raw, binding)
    item = next(i for i in batch["items"] if i["a"]["move_id"] == "m2")      # c1 from its stored B
    rec = dr.annotation(batch, item["position"], reviewer="agent:a", answer="useful", created_at="t1")
    assert rec["schema"] == dr.SPLIT_ANNOTATION_SCHEMA and rec["batch_id"] == json.loads(raw)["reviewer_batch_id"]
    assert (rec["binding_id"], rec["item_id"]) == (binding["binding_id"], item["item_id"])
    assert rec["shown"]["orientation"] == dr.SIGHTING_ORIENTATION
    assert (rec["shown"]["a"]["move_id"], rec["anchor"], rec["sighting"]) == (
        "m2", {"move_id": "m2", "stored_end": "b"}, {"run_id": RUN, "query_move_id": "m2"})
    current = c1_seen_from_both_ends()                                         # stored orientation, as read now
    assert dr.binding_problems(current, item) == []
    rolled_back = dict(current, occurrences=[o for o in current["occurrences"] if o["run_id"] != RUN])
    assert dr.binding_problems(rolled_back, item) == [
        "the selected sighting is no longer current (its run was rolled back, or it never was)"]
    path = str(tmp_path / "ann.jsonl")
    dr.append_annotation(path, rec)
    assert dr.read_annotations(path) == [rec]
    later = dr.annotation(batch, item["position"], reviewer="agent:a", answer="marginal", created_at="t2",
                          supersedes=rec["annotation_id"])
    dr.append_annotation(path, later)
    assert dr.latest(dr.read_annotations(path)) == [later]


def test_one_file_batches_and_their_annotations_keep_their_identities():
    """Golden ids computed by the I2 code (becf145) from these fixtures: a plain batch, and an I2 one-file
    batch with a sighting and a selection. Their contents and format are unchanged, so their ids are too.
    (The first annotation id was recomputed when the fixture's reviewer name changed.)"""
    b = batch_of(rows())
    a = dr.annotation(b, 1, reviewer="reviewer-a", answer="useful", reason="r", os_user="u", tool_version="v",
                      created_at="t1")
    items, _ = dr.review_items([c1_seen_from_both_ends()], seen_in_runs=[RUN])
    lb = dr.make_batch(dr.queue(items, MAPPINGS)["cross_source"], database="personal_koi", mappings=MAPPINGS,
                       created_at="x", selection={"run_ids": None, "seen_in_runs": [RUN], "tier": None})
    la = dr.annotation(lb, 1, reviewer="agent:test", answer="useful", created_at="t1")
    assert (b["batch_id"], a["annotation_id"], a["schema"]) == (
        "962d5f33e20fffacf04479595d26383b4fd1b87d2fe6d85be5ecdd3307944cc0",
        "213269a683ab17a5edac3b37824d866d52f7d4b0208a928772084e276e952654", dr.ANNOTATION_SCHEMA)
    assert (lb["batch_id"], la["annotation_id"]) == (
        "2e0df6172e874b6950b01f21fb76cab7f914853be8480d0f3086a0cd47c0357d",
        "d02331463f54124bad33acd5fd40e9e6c9f76e9bbb79e75b9be8ac1a18984214")
    assert "binding_id" not in a and "item_id" not in la


def test_cli_annotate_refuses_without_the_binding_and_records_with_it(tmp_path, monkeypatch, capsys):
    cli, _, out, bpath, rs = _cli_batch(tmp_path, monkeypatch, tier="all", name="trial")
    monkeypatch.setattr(cli, "_verify", lambda dsn, items: {i["position"]: [] for i in items})
    ann = tmp_path / "operator" / "ann.jsonl"
    items = json.loads(out.read_text())["items"]
    base = {"dsn": "unused", "batch": str(out), "item": 1, "answer": "useful", "reason": None,
            "reviewer": "agent:a", "annotations": str(ann), "supersedes": None, "item_id": items[0]["item_id"][:8]}
    with pytest.raises(dr.ReviewError, match="needs its binding file"):
        cli.cmd_annotate(type("A", (), {**base, "binding": None})())
    with pytest.raises(SystemExit, match="does not exist"):
        cli.cmd_annotate(type("A", (), {**base, "binding": str(tmp_path / "missing.json")})())
    (tmp_path / "bad.json").write_text("{not json")
    with pytest.raises(SystemExit, match="is not JSON"):
        cli.cmd_annotate(type("A", (), {**base, "binding": str(tmp_path / "bad.json")})())
    with pytest.raises(SystemExit, match="cannot read binding file"):
        cli.cmd_annotate(type("A", (), {**base, "binding": str(tmp_path)})())   # a directory
    with pytest.raises(dr.ReviewError, match="needs the item id"):
        cli.cmd_annotate(type("A", (), {**base, "binding": str(bpath), "item_id": None})())
    with pytest.raises(dr.ReviewError, match="does not match the item at this position"):
        cli.cmd_annotate(type("A", (), {**base, "binding": str(bpath), "item_id": items[1]["item_id"][:8]})())
    with pytest.raises(SystemExit, match="outside the reviewer file's directory"):
        cli.cmd_annotate(type("A", (), {**base, "binding": str(bpath),
                                        "annotations": str(out.parent / "ann.jsonl")})())
    assert not ann.exists() and not (out.parent / "ann.jsonl").exists()      # nothing recorded
    cli.cmd_annotate(type("A", (), {**base, "binding": str(bpath)})())
    (rec,) = dr.read_annotations(str(ann))
    assert rec["schema"] == dr.SPLIT_ANNOTATION_SCHEMA and rec["binding_id"] == json.loads(bpath.read_text())["binding_id"]
    # show without the binding renders exactly the reviewer file: no candidate id, run or tier.
    capsys.readouterr()
    cli.cmd_show(type("S", (), {"dsn": "unused", "batch": str(out), "binding": None, "item": None, "verify": False})())
    shown = capsys.readouterr().out
    assert shown.count("item ") == 3 and "candidate c" not in shown                # "authors: unknown" is not a tier
    assert not any(s in shown for s in [RUN, "canary:x", "first_five", "additional", "tier", "pick"])
    with pytest.raises(dr.ReviewError, match="needs its binding file"):
        cli.cmd_show(type("S", (), {"dsn": "unused", "batch": str(out), "binding": None, "item": None, "verify": True})())


def test_cli_one_file_batches_refuse_a_binding_and_warn_when_they_carry_sightings(tmp_path, monkeypatch, capsys):
    cli = _load_cli()
    items, _ = dr.review_items([c1_seen_from_both_ends()], seen_in_runs=[RUN])
    legacy = dr.make_batch(dr.queue(items, MAPPINGS)["cross_source"], database="personal_koi", mappings=MAPPINGS,
                           created_at="x", selection={"run_ids": None, "seen_in_runs": [RUN], "tier": None})
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(legacy, indent=1) + "\n")
    monkeypatch.setattr(cli, "_verify", lambda dsn, items: {i["position"]: [] for i in items})
    cli.cmd_show(type("S", (), {"dsn": "unused", "batch": str(path), "binding": None, "item": None, "verify": False})())
    assert "operator only; do not give it to a reviewer" in capsys.readouterr().out
    ann = tmp_path / "ann.jsonl"
    base = {"dsn": "unused", "batch": str(path), "item": 1, "answer": "useful", "reason": None,
            "reviewer": "agent:a", "annotations": str(ann), "supersedes": None}
    with pytest.raises(SystemExit, match="one-file batch; it has no binding file"):
        cli.cmd_annotate(type("A", (), {**base, "binding": str(path)})())
    cli.cmd_annotate(type("A", (), {**base, "binding": None})())             # still annotates as in I2
    (rec,) = dr.read_annotations(str(ann))
    assert rec["schema"] == dr.ANNOTATION_SCHEMA and rec["batch_id"] == legacy["batch_id"] and rec["anchor"]["stored_end"] == "b"


def test_cli_filtered_batch_names_its_binding_file_outside_the_reviewer_directory(tmp_path, monkeypatch):
    cli, args, out, bpath, _ = _cli_batch(tmp_path, monkeypatch, tier="all", name="first")
    assert out.exists() and bpath.exists()
    tried = []

    def refused(match, **changes):
        a = type("B", (), {**{k: getattr(args, k) for k in dir(args) if not k.startswith("_")}, **changes})()
        with pytest.raises(SystemExit, match=match):
            cli.cmd_batch(a)
        tried.append(changes.get("out", args.out))
    nxt = tmp_path / "reviewer" / "next.json"
    refused("name the binding file with --binding-out", out=str(nxt), binding_out=None)
    refused("outside the reviewer file's directory", out=str(nxt), binding_out=str(tmp_path / "reviewer" / "n.binding.json"))
    (tmp_path / "reviewer" / "sub").mkdir()
    refused("outside the reviewer file's directory", out=str(nxt),
            binding_out=str(tmp_path / "reviewer" / "sub" / "n.binding.json"))
    refused("refusing to overwrite", out=str(nxt), binding_out=str(bpath))              # an existing binding file
    refused("refusing to overwrite", out=str(out), binding_out=str(tmp_path / "operator" / "n.binding.json"))
    assert not nxt.exists() and not (tmp_path / "operator" / "n.binding.json").exists()
    # The binding file cannot be written: the reviewer file just written is removed again.
    lost = type("B", (), {**{k: getattr(args, k) for k in dir(args) if not k.startswith("_")},
                          "out": str(nxt), "binding_out": str(tmp_path / "missing-dir" / "n.binding.json")})()
    with pytest.raises(SystemExit, match="cannot write .*nothing was kept"):
        cli.cmd_batch(lost)
    assert not nxt.exists()
    assert len(tried) == 5


def test_an_answer_cannot_land_on_the_same_position_of_another_reviewer_file(tmp_path, monkeypatch):
    """Two reviewer files of the same items number them 1..N in different salted orders. An answer
    recorded with the item id the reviewer saw in one file is refused against the other."""
    cli, _, out_a, bind_a, _ = _cli_batch(tmp_path, monkeypatch, tier="all", name="a")
    _, _, out_b, bind_b, _ = _cli_batch(tmp_path, monkeypatch, tier="all", name="b")
    monkeypatch.setattr(cli, "_verify", lambda dsn, items: {i["position"]: [] for i in items})
    seen_in_a = json.loads(out_a.read_text())["items"][0]
    ann = tmp_path / "operator" / "ann.jsonl"
    args = type("A", (), {"dsn": "unused", "batch": str(out_b), "binding": str(bind_b), "item": 1,
                          "item_id": seen_in_a["item_id"][:12], "answer": "useful", "reason": None,
                          "reviewer": "agent:a", "annotations": str(ann), "supersedes": None})()
    with pytest.raises(dr.ReviewError, match="another item or another reviewer file"):
        cli.cmd_annotate(args)
    args.batch, args.binding = str(out_a), str(bind_a)
    cli.cmd_annotate(args)
    (rec,) = dr.read_annotations(str(ann))
    assert rec["item_id"] == seen_in_a["item_id"] and rec["shown"]["a"]["move_id"] == seen_in_a["a"]["move_id"]


def test_the_review_loop_on_a_reviewer_file_records_v2_annotations(tmp_path, monkeypatch):
    cli, _, out, bpath, _ = _cli_batch(tmp_path, monkeypatch, tier="all", name="loop")
    batch = cli._load_batch(str(out), str(bpath), need_binding=True)
    path = str(tmp_path / "operator" / "loop.jsonl")
    keys = iter(["u", "", "n", "why not", "q"])
    shown = []
    n = dr.review_loop(batch, reviewer="person:x", path=path, verify=lambda it: [], ask=lambda _: next(keys),
                       say=shown.append)
    recs = dr.read_annotations(path)
    assert n == 2 and [r["answer"] for r in recs] == ["useful", "not_useful"]
    assert all(r["schema"] == dr.SPLIT_ANNOTATION_SCHEMA and r["batch_id"] == batch["batch_id"] for r in recs)
    assert not any(s in "\n".join(shown) for s in (RUN, "first_five", "additional", "candidate c", "stored"))


def test_folders_are_compared_by_identity_not_by_spelling(tmp_path, monkeypatch):
    (tmp_path / "Reviewer").mkdir()
    if not (tmp_path / "reviewer").exists():
        pytest.skip("case-sensitive volume: Reviewer and reviewer are two folders")
    cli, args, out, bpath, _ = _cli_batch(tmp_path, monkeypatch, tier="all", name="case")
    same = type("B", (), {**{k: getattr(args, k) for k in dir(args) if not k.startswith("_")},
                          "out": str(tmp_path / "Reviewer" / "c.json"),
                          "binding_out": str(tmp_path / "reviewer" / "c.binding.json")})()
    with pytest.raises(SystemExit, match="outside the reviewer file's directory"):
        cli.cmd_batch(same)
    assert not (tmp_path / "Reviewer" / "c.json").exists()


def test_a_binding_file_found_in_the_reviewer_directory_is_refused(tmp_path, monkeypatch):
    cli, _, out, bpath, _ = _cli_batch(tmp_path, monkeypatch, tier="all", name="leak")
    moved = out.parent / "leak.binding.json"
    moved.write_bytes(bpath.read_bytes())                    # someone put it where reviewers read
    with pytest.raises(SystemExit, match="binding file is inside the reviewer file's directory"):
        cli._load_batch(str(out), str(moved), need_binding=True)


def test_a_failed_binding_write_keeps_neither_file(tmp_path, monkeypatch):
    """The binding file is created, then its write fails (a full disk): both files are removed."""
    cli, args, _, _, _ = _cli_batch(tmp_path, monkeypatch, tier="all", name="seed")
    out, bpath = tmp_path / "reviewer" / "full.json", tmp_path / "operator" / "full.binding.json"
    real_open = open

    class Full:
        def __init__(self, f):
            self.f = f

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.f.close()
            return False

        def write(self, data):
            self.f.write(data[:10])
            raise OSError(28, "No space left on device", str(bpath))

    def fake_open(path, mode="r", *a, **k):
        f = real_open(path, mode, *a, **k)
        return Full(f) if Path(path) == bpath else f
    monkeypatch.setattr(cli, "open", fake_open, raising=False)
    full = type("B", (), {**{k: getattr(args, k) for k in dir(args) if not k.startswith("_")},
                          "out": str(out), "binding_out": str(bpath)})()
    with pytest.raises(SystemExit, match="No space left on device; nothing was kept"):
        cli.cmd_batch(full)
    assert not out.exists() and not bpath.exists()


def test_a_pair_sighted_from_the_same_claim_by_two_runs_is_refused_in_one_reviewer_file():
    r = rows()[0]
    r["occurrences"] = [occ(r, RUN, 8, 10, sighting="again", end="b"), occ(r, "ingest:10:1", 3, 10, sighting="again", end="b")]
    items, _ = dr.review_items([r], seen_in_runs=[RUN, "ingest:10:1"])
    assert len(items) == 2 and items[0]["a"]["move_id"] == items[1]["a"]["move_id"] == "m2"
    with pytest.raises(dr.ReviewError, match="the same A and B .* one batch per run"):
        dr.make_split_batch(items, database="d", selection={"x": 1}, salt="s" * 64, created_at="t")
    assert len(json.loads(dr.make_split_batch(items[:1], database="d", selection={"x": 1}, salt="s" * 64,
                                              created_at="t")[0])["items"]) == 1
