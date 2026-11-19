"""Every write to koi_net_edges under api/ is a guarded peer write or an operator route.

Peer-driven writes used to read a row and then
wrote it with no status predicate, so an operator's concurrent revoke could be overwritten.
They now go through three Negotiator methods that re-check the stored state; /handshake's
two upserts never change status and change rid_types only on a PROPOSED row; the rest
are operator routes. A new writer anywhere else fails here, so the next one is not fixed one
call site at a time.
"""
import ast
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WRITE = re.compile(r"\b(UPDATE\s+koi_net_edges|INSERT\s+INTO\s+koi_net_edges)\b", re.I)

ALLOWED = Counter({
    # peer-driven, guarded (re-check the stored state; RETURNING; None when it no longer matches)
    ("api/koi_net_negotiation.py", "Negotiator._peer_edge_update"): 1,
    ("api/koi_net_negotiation.py", "Negotiator._peer_edge_insert"): 1,
    ("api/koi_net_negotiation.py", "Negotiator._peer_forget_pair"): 1,  # narrows only, keeps REJECTED
    # peer-driven, /handshake: status never changes on conflict; rid_types only while PROPOSED
    ("api/koi_net_router.py", "handshake"): 2,
    # operator routes (admin guard)
    ("api/koi_net_router.py", "approve_edge"): 1,
    ("api/koi_net_router.py", "reject_edge"): 1,
    ("api/koi_net_router.py", "reopen_edge"): 1,
})


def _writers():
    found, texts = Counter(), {}
    for path in sorted((ROOT / "api").rglob("*.py")):
        rel = str(path.relative_to(ROOT))
        tree = ast.parse(path.read_text())

        def visit(node, scope):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    visit(child, scope + [child.name])
                    continue
                text = None
                if isinstance(child, ast.Constant) and isinstance(child.value, str):
                    text = child.value
                elif isinstance(child, ast.JoinedStr):  # an f-string is one statement: {} marks a value
                    text = "".join(v.value if isinstance(v, ast.Constant) else "{}" for v in child.values)
                if text is not None and WRITE.search(text):
                    key = (rel, ".".join(scope) or "<module>")
                    found[key] += 1
                    texts.setdefault(key, []).append(" ".join(text.split()))
                    continue
                visit(child, scope)

        visit(tree, [])
    return found, texts


def test_every_edge_writer_is_guarded_or_an_operator_route():
    found, _ = _writers()
    assert found, "no koi_net_edges writer found at all: the scan itself is broken"
    assert found == ALLOWED, f"unexpected writers: {dict(found - ALLOWED)}; missing: {dict(ALLOWED - found)}"


def test_the_guarded_writers_carry_their_predicates():
    _, texts = _writers()
    (update,) = texts[("api/koi_net_negotiation.py", "Negotiator._peer_edge_update")]
    assert "status NOT IN ('REVOKED', 'REJECTED') ELSE status = $2::text END" in update and "RETURNING edge_rid" in update
    (insert,) = texts[("api/koi_net_negotiation.py", "Negotiator._peer_edge_insert")]
    assert "WHERE koi_net_edges.status NOT IN ('REVOKED', 'REJECTED') RETURNING edge_rid" in insert
    (forget,) = texts[("api/koi_net_negotiation.py", "Negotiator._peer_forget_pair")]
    assert "SET status = 'REVOKED'" in forget and "status NOT IN ('REVOKED', 'REJECTED')" in forget
    for upsert in texts[("api/koi_net_router.py", "handshake")]:
        conflict = upsert.split("ON CONFLICT", 1)[1]
        assert "status =" not in conflict.replace("koi_net_edges.status = 'PROPOSED'", ""), upsert
        assert "AND koi_net_edges.status = 'PROPOSED' THEN EXCLUDED.rid_types" in conflict, upsert
