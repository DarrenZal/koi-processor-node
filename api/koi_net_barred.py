"""Which nodes an operator has barred, shared by the router, the negotiator and the poller.

A node an operator rejected or revoked stays out, and so does its key: under any other RID
(the name part of a node RID is free, so one key can hold many), and in any encoding a
client loads as the same key. Register entry 26.

Every check takes the caller's connection, so it reads the same database state the caller
then acts on.
"""
from __future__ import annotations

from typing import Any, Optional

from api.koi_envelope import load_public_key_from_der_b64, public_key_to_der_b64

BARRED_NODE_STATUSES = ("rejected", "revoked")
# The SQL form, for queries that filter nodes: "status NOT IN ('rejected', 'revoked')".
BARRED_SQL_LIST = "('rejected', 'revoked')"


def canonical_key_b64(der_b64: Any) -> Optional[str]:
    """A public key's canonical SPKI DER (base64), or None when it does not parse. Equal for
    every encoding a client loads as the same key (whitespace, a compressed point)."""
    try:
        key = load_public_key_from_der_b64(der_b64)
    except Exception:
        return None
    return public_key_to_der_b64(key) if key is not None else None


async def barred_keys(conn) -> set:
    rows = await conn.fetch(
        "SELECT public_key FROM koi_net_nodes WHERE status = ANY($1::text[]) AND public_key IS NOT NULL",
        list(BARRED_NODE_STATUSES),
    )
    return {k for k in (canonical_key_b64(r["public_key"]) for r in rows) if k}


async def key_is_barred(conn, der_b64: Any) -> bool:
    canonical = canonical_key_b64(der_b64)
    return canonical is not None and canonical in await barred_keys(conn)


async def node_is_barred(conn, node_rid: Optional[str]) -> bool:
    """The node is rejected or revoked, or holds a key that is under another RID."""
    if not node_rid:
        return False
    row = await conn.fetchrow("SELECT status, public_key FROM koi_net_nodes WHERE node_rid = $1", node_rid)
    if not row:
        return False
    if row["status"] in BARRED_NODE_STATUSES:
        return True
    return bool(row["public_key"]) and await key_is_barred(conn, row["public_key"])
