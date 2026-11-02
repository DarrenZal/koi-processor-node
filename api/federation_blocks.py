"""Never send blocked records to a peer again (migration 137).

A peer upserts every event it receives, so a record removed on a peer comes back with the next
event this node queues for it. federation_rid_blocks holds the sha256 of exact identifiers (task
keys, entity URIs, doclink RIDs). An event is withheld when its RID, or any string value in its
contents or manifest, hashes to a row, so a new event that only refers to a blocked record (a
doclink to a blocked entity) is withheld too. Rows hold hashes only, because some identifiers carry
a name.

The guard fails closed. The migration is applied before this code, so a missing table means the
block list was lost, not that nothing is blocked: callers queue and serve nothing instead.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, FrozenSet, Iterator, Optional


class FederationBlocksUnavailable(RuntimeError):
    """federation_rid_blocks is missing; queue and serve nothing."""


def rid_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _strings(obj: Any) -> Iterator[str]:
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _strings(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            yield from _strings(v)


def _decoded(raw: Any) -> Any:
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return raw
    return raw


async def blocked_hashes(conn) -> FrozenSet[str]:
    """All blocked hashes. Raises FederationBlocksUnavailable when the table is missing."""
    present = await conn.fetchval("SELECT to_regclass('federation_rid_blocks') IS NOT NULL")
    if not present:
        raise FederationBlocksUnavailable("federation_rid_blocks is missing; federation fails closed")
    return frozenset(r["rid_sha256"] for r in await conn.fetch("SELECT rid_sha256 FROM federation_rid_blocks"))


def is_blocked(blocked: FrozenSet[str], rid: Optional[str], contents: Any = None, manifest: Any = None) -> bool:
    """True when the RID or any string value in contents or manifest is blocked."""
    if not blocked:
        return False
    if rid and rid_sha256(rid) in blocked:
        return True
    return any(rid_sha256(s) in blocked for part in (contents, manifest) for s in _strings(_decoded(part)))
