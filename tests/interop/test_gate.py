"""KOI-net interop gate: one case, run by tests/interop/run_gate.py. Not run on its own.

run_gate.py starts our node (a throwaway database, a scratch vault) and one stock koi-net
node whose first contact is ours, then runs this module in .venv-koinet with:

    KOI_TEST_API_URL       our node (127.0.0.1, never 8351)
    KOI_TEST_DATABASE_URL  our node's database, koi_test_interop_<case>
    KOI_TEST_PEER_API_URL  the stock node, when it is a full node
    KOI_GATE_*             the case: stock node type, membrane, roots and logs

Each test is one step of the protocol, observed from the state on both sides: our
database and log, the stock node's rid-lib cache and log. A step that has not happened
by its deadline raises StepNotReached. A step that an open divergence blocks is a strict
xfail limited to StepNotReached, so a setup problem fails outright and a fix makes the
test fail until its marker is removed and the conformance register is updated.

A missing variable fails the test. Nothing here skips.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from pathlib import Path

import httpx
import psycopg
import pytest
from rid_lib.ext import Cache
from rid_lib.ext.utils import sha256_hash
from rid_lib.types import KoiNetEdge, KoiNetNode
from ruamel.yaml import YAML

from koi_net.protocol.edge import EdgeProfile, EdgeStatus
from koi_net.protocol.node import NodeProfile

from tests import koi_test_targets as targets

DEADLINE = float(os.environ.get("KOI_GATE_STEP_TIMEOUT", "25"))


class StepNotReached(AssertionError):
    """A protocol step did not happen before its deadline."""


def _var(name: str) -> str:
    value = targets.configured(name) if name.startswith("KOI_TEST_") else os.environ.get(name, "").strip()
    if not value:
        pytest.fail(f"{name} is not set: run this module through tests/interop/run_gate.py")
    return value


CASE = os.environ.get("KOI_GATE_CASE", "")
KIND = os.environ.get("KOI_GATE_STOCK_KIND", "")
MEMBRANE_ON = os.environ.get("KOI_GATE_MEMBRANE", "") == "on"


def _record(what: str, outcome: str) -> None:
    """Append where a step stopped to KOI_GATE_STEP_LOG; run_gate.py prints it per case."""
    path = os.environ.get("KOI_GATE_STEP_LOG")
    if path:
        test = os.environ.get("PYTEST_CURRENT_TEST", "?").split(" ")[0].rsplit("::", 1)[-1]
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"case": CASE, "test": test, "step": what, "outcome": outcome}) + "\n")


def _stock_alive() -> bool:
    """Whether the stock node run_gate.py started is still running (a zombie counts as exited)."""
    pid = os.environ.get("KOI_GATE_STOCK_PID", "").strip()
    if not pid.isdigit():
        pytest.fail("KOI_GATE_STOCK_PID is not set: run this module through tests/interop/run_gate.py")
    r = subprocess.run(["ps", "-o", "stat=", "-p", pid], capture_output=True, text=True)
    stat = r.stdout.strip()
    return bool(stat) and not stat.startswith("Z")


def _handshake_refused() -> bool:
    """Our node answered the stock node's handshake broadcast with 403: divergence 18's membrane."""
    return _log_has(Path(_var("KOI_GATE_OURS_LOG")), r'"POST /koi-net/events/broadcast HTTP/1\.1" 403')


def _until(what: str, probe, timeout: float = DEADLINE, setup: bool = False, needs_stock: bool = True):
    """Return probe()'s first truthy value.

    On timeout a step under test raises StepNotReached (what a divergence xfail accepts);
    a precondition (setup=True) fails the test outright, so an xfail cannot hide it.

    The stock node must stay up. If it has exited, the step cannot happen: that is
    StepNotReached only when the membrane is on and our node refused its handshake with a
    403 (divergence 18, after which a stock node shuts down). Any other exit fails the
    test outright, whatever its marker. A wait on our own node's state passes
    needs_stock=False, because it does not depend on the stock node."""
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            _record(what, "reached")
            return value
        if needs_stock and not _stock_alive():
            if MEMBRANE_ON and _handshake_refused() and not setup:
                _record(what, "not-reached: stock node exited after the 403 (18)")
                raise StepNotReached(f"{what}: the stock node exited after our node refused its handshake (18)")
            _record(what, "setup-failed: stock node exited")
            pytest.fail(f"setup: {what}: the stock node exited (see KOI_GATE_STOCK_LOG)")
        if time.monotonic() >= deadline:
            _record(what, "setup-failed" if setup else "not-reached")
            if setup:
                pytest.fail(f"setup: {what}: not reached in {timeout:.0f}s")
            raise StepNotReached(f"{what}: not reached in {timeout:.0f}s")
        time.sleep(0.5)


def _blocked_by(*divergences: str, when: bool = True):
    unique = sorted({d for d in divergences if d}, key=int)
    if not unique:
        return lambda test: test
    reason = "open divergence(s) " + ", ".join(unique) + " (docs/architecture/koi-protocol-conformance.md)"
    return pytest.mark.xfail(when, strict=True, raises=StepNotReached, reason=reason)


# --- fixtures -------------------------------------------------------------------

@pytest.fixture(scope="module")
def ours():
    url = _var("KOI_TEST_API_URL")
    health = httpx.get(f"{url}/koi-net/health", timeout=10).json()["node"]
    if not health["node_name"].startswith("conformance-"):
        pytest.fail(f"refusing: our node {health['node_name']!r} is not a conformance-* throwaway node")
    rid = KoiNetNode.from_string(_var("KOI_GATE_OURS_RID"))
    if str(rid) != health["node_rid"]:
        pytest.fail(f"setup: KOI_GATE_OURS_RID {rid} is not the node at {url} ({health['node_rid']})")
    return {"url": url, "rid": rid, "log": Path(_var("KOI_GATE_OURS_LOG")),
            "vault": Path(_var("KOI_GATE_VAULT")), "admin": _var("KOI_GATE_ADMIN_TOKEN")}


@pytest.fixture(scope="module")
def stock():
    root = Path(_var("KOI_GATE_STOCK_ROOT"))
    if KIND not in ("full", "partial"):
        pytest.fail(f"KOI_GATE_STOCK_KIND is {KIND!r}, expected full or partial")

    def node_rid():
        data = YAML(typ="safe").load((root / "config.yaml").read_text())
        return (data.get("koi_net") or {}).get("node_rid")

    rid = KoiNetNode.from_string(_until("stock node writes its node_rid to config.yaml", node_rid, timeout=30, setup=True, needs_stock=False))
    return {"root": root, "rid": rid, "cache": Cache(str(root / ".rid_cache")), "log": Path(_var("KOI_GATE_STOCK_LOG"))}


@pytest.fixture(scope="module")
def db():
    conn = psycopg.connect(_var("KOI_TEST_DATABASE_URL"), autocommit=True)
    name = conn.execute("SELECT current_database()").fetchone()[0]
    if not name.startswith("koi_test_interop_"):
        pytest.fail(f"refusing: connected to {name!r}, not a koi_test_interop_* database")
    yield conn
    conn.close()


def _log_has(path: Path, pattern: str) -> bool:
    try:
        return re.search(pattern, path.read_text(errors="replace")) is not None
    except FileNotFoundError:
        return False


def _edge_rid(source: KoiNetNode, target: KoiNetNode) -> KoiNetEdge:
    """Spec section 3.2.1, as koi_net.protocol.edge.generate_edge_bundle derives it."""
    return KoiNetEdge(sha256_hash(str(source) + str(target)))


# --- 1. handshake ---------------------------------------------------------------

def test_handshake_broadcast_accepted(ours, stock, db):
    """The stock node joins by broadcasting FORGET + NEW of its own bundle to its first contact."""
    _until("our node answers the stock node's handshake broadcast with 200",
           lambda: _log_has(ours["log"], r'"POST /koi-net/events/broadcast HTTP/1\.1" 200'))
    # (membrane on: _until raises StepNotReached only after it has seen our 403 and the
    # stock node's exit, so any other reason for no 200 fails outright)
    _until("our node records the stock node",
           lambda: db.execute("SELECT 1 FROM koi_net_nodes WHERE node_rid = %s", (str(stock["rid"]),)).fetchone())


# --- 2. the stock node learns our profile ----------------------------------------

def test_stock_node_caches_our_node_bundle(ours, stock):
    bundle = _until("stock node caches our KoiNetNode bundle", lambda: stock["cache"].read(ours["rid"]))
    profile = bundle.validate_contents(NodeProfile)
    assert profile.public_key, "our cached profile carries no public key"


# --- 3. edge negotiation --------------------------------------------------------

def test_edge_to_stock_node_approved_on_both_sides(ours, stock, db):
    """The stock node subscribes to us: edge source=ours, target=stock, APPROVED by both."""
    edge_rid = _edge_rid(ours["rid"], stock["rid"])
    edge_type = "WEBHOOK" if KIND == "full" else "POLL"

    def our_row():
        return db.execute(
            "SELECT edge_type, status FROM koi_net_edges WHERE edge_rid = %s AND source_node = %s AND target_node = %s",
            (str(edge_rid), str(ours["rid"]), str(stock["rid"])),
        ).fetchone()

    def theirs():
        b = stock["cache"].read(edge_rid)
        return b if b and b.validate_contents(EdgeProfile).status == EdgeStatus.APPROVED else None

    row = _until(f"our node stores edge {edge_rid}", our_row)
    if MEMBRANE_ON:
        # D1: under the membrane a subscription waits for an operator.
        assert row == (edge_type, "PROPOSED"), f"our edge row is {row}, expected ({edge_type}, PROPOSED) before approval"
        r = httpx.post(f"{ours['url']}/koi-net/edges/approve", timeout=20,
                       headers={"Authorization": f"Bearer {ours['admin']}"}, json={"edge_rid": str(edge_rid)})
        if r.status_code != 200:
            pytest.fail(f"setup: edges/approve HTTP {r.status_code} {r.text[:300]}")
        row = _until(f"our node marks edge {edge_rid} APPROVED", lambda: (lambda r: r if r and r[1] == "APPROVED" else None)(our_row()))
    assert row == (edge_type, "APPROVED"), f"our edge row is {row}, expected ({edge_type}, APPROVED)"
    edge = _until("stock node caches the edge as APPROVED", theirs).validate_contents(EdgeProfile)
    assert edge.source == ours["rid"] and edge.target == stock["rid"]


# --- 4. fetch -------------------------------------------------------------------

def test_stock_node_fetches_rids_and_bundles_from_us(ours, stock):
    """After proposing an edge, a stock node catches up with /rids/fetch for the types it
    subscribed to. It fetches bundles only for listed RIDs it does not hold; our node lists
    its own RID, which the stock node already has, so /bundles/fetch is exercised by the
    conformance suite instead."""
    _until("stock node calls our /rids/fetch", lambda: _log_has(ours["log"], r'"POST /koi-net/rids/fetch HTTP/1\.1" 200'))
    # A 200 alone proves nothing under the membrane, which also answers 200.
    refused = re.escape(f"rids/fetch: no approved poll edge for {stock['rid']}")
    if MEMBRANE_ON:
        # The catch-up comes before an operator has approved the subscription: refused.
        _until("our node refuses the catch-up before approval (membrane)", lambda: _log_has(ours["log"], refused))
    else:
        assert not _log_has(ours["log"], refused), "membrane off, yet the catch-up was refused"


# --- 5. an event of ours reaches the stock node -----------------------------------

def _publish_vault_note(ours, stock) -> tuple[str, str]:
    """Write a note into our scratch vault and queue it for the stock node, as vault sync does."""
    rel = f"Shared/gate-{CASE}.md"
    body = f"# gate {CASE}\n\nvault-file event for the stock node.\n"
    (ours["vault"] / rel).write_text(body)
    headers = {"Authorization": f"Bearer {ours['admin']}"}
    r = httpx.post(f"{ours['url']}/koi-net/vault-sync/configure", headers=headers, timeout=20,
                   json={"peer": str(stock["rid"]), "shared_folder": "Shared", "enabled": True})
    if r.status_code != 200:
        pytest.fail(f"setup: vault-sync/configure HTTP {r.status_code} {r.text[:300]}")
    r = httpx.post(f"{ours['url']}/koi-net/vault-sync/trigger", headers=headers, timeout=60)
    if r.status_code != 200:
        pytest.fail(f"setup: vault-sync/trigger HTTP {r.status_code} {r.text[:300]}")
    return rel, body


def test_vault_file_event_reaches_stock_node(ours, stock, db):
    rel, body = _publish_vault_note(ours, stock)
    _until("our node queues a vault-file event for the stock node", lambda: db.execute(
        "SELECT 1 FROM koi_net_events WHERE target_node = %s AND rid LIKE %s",
        (str(stock["rid"]), f"%{rel}"),
    ).fetchone(), timeout=10, setup=True, needs_stock=False)

    def cached():
        for rid in stock["cache"].list_rids():
            if str(rid).endswith(rel):
                return stock["cache"].read(rid)
        return None

    bundle = _until(f"stock node caches the vault-file bundle for {rel}", cached)
    assert (bundle.contents or {}).get("markdown") == body, "cached vault-file contents differ from the note"


# --- 6. ours -> stock: our node subscribes and polls (full stock nodes only) ------

if KIND == "full":
    def test_our_node_polls_stock_node(ours, stock, db):
        """D4: with KOI_NET_RID_TYPES_OF_INTEREST set, our node proposes a POLL edge to a full
        contact (source=stock, target=ours) and polls it once an operator approves it."""
        edge_rid = _edge_rid(stock["rid"], ours["rid"])
        # Register 26: the stock node's approval alone does not open the edge,
        # membrane or not (KOI_NET_DEFER_UNKNOWN_HANDSHAKE defaults to true). An operator does.
        _until(f"the stock node approves our proposal {edge_rid}; ours waits for an operator", lambda: db.execute(
            "SELECT 1 FROM koi_net_edges WHERE edge_rid = %s AND status = 'PROPOSED' "
            "AND (metadata->>'peer_approved')::boolean", (str(edge_rid),)).fetchone())
        r = httpx.post(f"{ours['url']}/koi-net/edges/approve", timeout=20,
                       headers={"Authorization": f"Bearer {ours['admin']}"}, json={"edge_rid": str(edge_rid)})
        if r.status_code != 200:
            pytest.fail(f"setup: edges/approve HTTP {r.status_code} {r.text[:300]}")
        _until(f"our node stores edge {edge_rid} as APPROVED", lambda: db.execute(
            "SELECT 1 FROM koi_net_edges WHERE edge_rid = %s AND status = 'APPROVED' AND edge_type = 'POLL'",
            (str(edge_rid),),
        ).fetchone())
        # Our poller's own request lines (httpx), to the stock node's URL, answered 200, twice.
        peer = re.escape(_var("KOI_TEST_PEER_API_URL"))
        line = rf'HTTP Request: POST {peer}/koi-net/events/poll "HTTP/1\.1 200 OK"'
        _until("our node polls the stock node and gets 200, twice",
               lambda: len(re.findall(line, ours["log"].read_text(errors="replace"))) >= 2)
        # The poller verifies each signed reply and logs a warning when a poll fails; an empty
        # reply that verifies logs nothing. No POLL warning may name the stock node.
        failures = [l for l in ours["log"].read_text(errors="replace").splitlines()
                    if l.startswith("WARNING:api.koi_poller:") and str(stock["rid"]) in l and "WEBHOOK" not in l]
        assert not failures, failures[:3]
