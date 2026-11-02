"""KOI write-attribution log (C14).

A pure-ASGI middleware that writes one ``koi_write_log`` row per request that
either carries an ``X-KOI-Client`` header or arrives from the tunnel address in
``KOI_RECORDED_CLIENT_IP``, whatever its method. It records no other request, so peer polls
and untagged Mac-local requests add no rows (r-0428).

It is pure ASGI, not ``BaseHTTPMiddleware``: the serving Starlette is 0.27.0,
where ``BaseHTTPMiddleware`` leaves the handler waiting for a body the middleware
already consumed (RQ34). We buffer and replay the request body, insert the row
before the handler, return 503 if that insert fails, and fill the status in a
``finally`` (RQ34). Ids are never guessed (RQ46): ``affected_ids`` come from the
route's id source named in ``koi-routes.tsv`` (RQ35, RQ41), and a response that
is non-2xx, streamed, over 1 MB, or not JSON gets null ``affected_ids`` with an
``ids_reason`` saying why. A route the map lacks (a 404, the trailing-slash 307,
or a 405) records ``no_id_source`` (Y5), so it is never confused with a
middleware failure, which leaves both columns null.

The middleware never writes the request body itself, only its sha256.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
from typing import Any, Awaitable, Callable, Optional

# The Mac tunnel address. A request from here is recorded even without a tag,
# because the tunnel cannot set the header for sessions that do not (C14, C15).
# KOI_RECORDED_CLIENT_IP overrides it; an empty value records tagged requests only.
RECORDED_CLIENT_IP = os.environ.get("KOI_RECORDED_CLIENT_IP", "")

# A recorded request body over this size gets 413 before the handler runs (RQ34).
MAX_REQUEST_BODY = 32 * 1024 * 1024
# The middleware copies at most this much of a response body for id extraction.
# It never buffers the whole response (RQ46).
MAX_RESPONSE_PEEK = 1 * 1024 * 1024

# ids_reason values (RQ46, Y5).
REASON_ERROR = "error"
REASON_STREAMED = "streamed"
REASON_TOO_LARGE = "too_large"
REASON_NON_JSON = "non_json"
REASON_NO_ID_SOURCE = "no_id_source"

Scope = dict
Receive = Callable[[], Awaitable[dict]]
Send = Callable[[dict], Awaitable[None]]


# --------------------------------------------------------------------------- #
# Route map (koi-routes.tsv)
# --------------------------------------------------------------------------- #

class IdSource:
    """Where the middleware takes ``affected_ids`` for one route (RQ35)."""

    __slots__ = ("kind", "field", "rw")

    def __init__(self, kind: str, field: Optional[str], rw: str):
        # kind: "response" (field read from the 2xx JSON response body),
        #       "request"  (field read from a URL path param, else the JSON
        #                   request body), or "none".
        self.kind = kind
        self.field = field
        self.rw = rw  # "write" | "read" (informational; every recorded request is logged)

    def __repr__(self) -> str:  # pragma: no cover - debug only
        return f"IdSource(kind={self.kind!r}, field={self.field!r}, rw={self.rw!r})"


class RouteMap:
    """The (method, path_format) -> IdSource table built from a regen-prod commit.

    A pair missing from the map is treated as a write with id source ``none``
    (a route missing from the table counts as a write, RQ38/line 448), so the
    middleware still logs it and records ``no_id_source``.
    """

    def __init__(self, commit: Optional[str], rows: dict[tuple[str, str], IdSource]):
        self.commit = commit
        self._rows = rows

    def lookup(self, method: str, path_format: Optional[str]) -> Optional[IdSource]:
        if path_format is None:
            return None
        return self._rows.get((method.upper(), path_format))

    def __len__(self) -> int:
        return len(self._rows)

    def pairs(self) -> set[tuple[str, str]]:
        return set(self._rows.keys())


def unmapped_routes(app_routes, route_map: Optional["RouteMap"]) -> list[tuple[str, str]]:
    """Return every mounted (method, path_format) pair absent from the route map.

    Used by the app's startup check to print routes added to KOI but not yet in
    ``koi-routes.tsv`` (they log as ``no_id_source`` until reviewed, RQ38).
    """
    from fastapi.routing import APIRoute  # local import keeps this module light

    mapped = route_map.pairs() if route_map is not None else set()
    out: list[tuple[str, str]] = []
    for r in app_routes:
        if isinstance(r, APIRoute):
            for method in (r.methods - {"HEAD", "OPTIONS"}):
                if (method, r.path_format) not in mapped:
                    out.append((method, r.path_format))
    return out


def _parse_id_source(spec: str, rw: str) -> IdSource:
    spec = spec.strip()
    if spec in ("", "-", "none"):
        return IdSource("none", None, rw)
    if ":" in spec:
        kind, field = spec.split(":", 1)
        kind = kind.strip().lower()
        if kind in ("response", "request"):
            return IdSource(kind, field.strip(), rw)
    # Anything unrecognised is conservatively treated as no id source.
    return IdSource("none", None, rw)


def load_route_map(path: str) -> RouteMap:
    """Parse koi-routes.tsv.

    Format: a leading comment line ``# regen-prod <sha>`` (RQ41), optional other
    comment lines, then tab-separated rows ``method<TAB>path_format<TAB>rw<TAB>id_source``.
    ``rw`` is ``write`` or ``read``; ``id_source`` is ``response:<field>``,
    ``request:<field>`` or ``none`` (reads use ``none``).
    """
    commit: Optional[str] = None
    rows: dict[tuple[str, str], IdSource] = {}
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            if line.startswith("#"):
                low = line.lstrip("#").strip()
                if low.lower().startswith("regen-prod"):
                    parts = low.split()
                    if len(parts) >= 2:
                        commit = parts[1]
                continue
            fields = line.split("\t")
            if len(fields) < 4:
                raise ValueError(f"koi-routes.tsv malformed row: {line!r}")
            method, path_format, rw, id_spec = (
                fields[0].strip().upper(),
                fields[1].strip(),
                fields[2].strip().lower(),
                fields[3],
            )
            rows[(method, path_format)] = _parse_id_source(id_spec, rw)
    return RouteMap(commit, rows)


# --------------------------------------------------------------------------- #
# Id extraction
# --------------------------------------------------------------------------- #

def _as_id_list(value: Any) -> Optional[list[str]]:
    """Coerce an extracted value to a text[] of ids, or None when absent."""
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        out = [str(v) for v in value if v is not None]
        return out
    return [str(value)]


def extract_ids(doc: Any, field: str) -> Optional[list[str]]:
    """Read ``field`` (a dotted path, with ``[]`` for array fan-out) from a
    parsed JSON document and return it as a list of id strings.

    Examples: ``id``; ``data.task_key``; ``results[].uri``.
    """
    cur: Any = doc
    for seg in field.split("."):
        fan = seg.endswith("[]")
        key = seg[:-2] if fan else seg
        if key:
            if isinstance(cur, list):
                cur = [c.get(key) if isinstance(c, dict) else None for c in cur]
            elif isinstance(cur, dict):
                cur = cur.get(key)
            else:
                return None
        if fan and not isinstance(cur, list):
            # The path asked to fan out over an array but the node is not one.
            return None if cur is None else _as_id_list(cur)
    return _as_id_list(cur)


# --------------------------------------------------------------------------- #
# Middleware
# --------------------------------------------------------------------------- #

def _header(scope: Scope, name: bytes) -> Optional[str]:
    name = name.lower()
    for k, v in scope.get("headers") or []:
        if k.lower() == name:
            try:
                return v.decode("latin-1")
            except Exception:
                return None
    return None


def _content_type_is_json(headers: list[tuple[bytes, bytes]]) -> bool:
    for k, v in headers or []:
        if k.lower() == b"content-type":
            ct = v.decode("latin-1").split(";", 1)[0].strip().lower()
            return ct == "application/json" or ct.endswith("+json")
    return False


class KoiWriteLogMiddleware:
    """Pure-ASGI write-attribution logger (C14, RQ34, RQ35, RQ41, RQ46, Y5)."""

    def __init__(
        self,
        app,
        pool_provider: Callable[[], Any],
        route_map: RouteMap,
        max_request_body: int = MAX_REQUEST_BODY,
        max_response_peek: int = MAX_RESPONSE_PEEK,
    ):
        self.app = app
        # Late-bound so the global asyncpg pool created in startup is picked up.
        self._pool_provider = pool_provider
        self.route_map = route_map
        self.max_request_body = max_request_body
        self.max_response_peek = max_response_peek

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            # Non-http scopes (lifespan, websocket) pass straight through.
            await self.app(scope, receive, send)
            return

        client_tag = _header(scope, b"x-koi-client")
        client = scope.get("client")
        client_ip = client[0] if client else None
        recorded = client_tag is not None or (bool(RECORDED_CLIENT_IP) and client_ip == RECORDED_CLIENT_IP)
        if not recorded:
            # Untagged request not from the tunnel: record nothing, do not touch
            # the database (so it is unaffected even if the table is missing).
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "")

        # 1. Buffer the request body, enforcing the 32 MB cap (413) before the
        #    handler runs, and compute its sha256. Never store the body.
        body = bytearray()
        too_large = False
        while True:
            message = await receive()
            if message["type"] == "http.request":
                chunk = message.get("body", b"")
                if chunk:
                    body.extend(chunk)
                    if len(body) > self.max_request_body:
                        too_large = True
                        # Drain nothing further; refuse now.
                        break
                if not message.get("more_body", False):
                    break
            elif message["type"] == "http.disconnect":
                break

        if too_large:
            await self._send_simple(send, 413, b"request body too large")
            return

        request_body = bytes(body)
        body_sha256 = hashlib.sha256(request_body).hexdigest()

        # 2. Insert the row before the handler. 503 if that fails (RQ34).
        pool = self._pool_provider()
        try:
            row_id = await self._insert_row(
                pool, method, scope.get("path", ""), client_tag, client_ip, body_sha256
            )
        except Exception:
            await self._send_simple(send, 503, b"write log unavailable")
            return

        # 3. Replay the buffered body as one http.request message, then delegate
        #    every later receive() to the original so http.disconnect arrives.
        replay_done = False

        async def replay_receive() -> dict:
            nonlocal replay_done
            if not replay_done:
                replay_done = True
                return {"type": "http.request", "body": request_body, "more_body": False}
            return await receive()

        # 4. Wrap send to capture status and peek at the response (<= 1 MB),
        #    passing every message through unchanged and without delay (RQ46).
        cap = _ResponseCapture(self.max_response_peek)

        async def capture_send(message: dict) -> None:
            cap.observe(message)
            await send(message)

        status: Optional[int] = None
        raised = False
        try:
            await self.app(scope, replay_receive, capture_send)
        except Exception:
            raised = True
            raise
        finally:
            # scope["route"] is set for a matched route even when the handler
            # raised, and for a 405 it is the path's route for another method,
            # so the method is part of the key (RQ41).
            route = scope.get("route")
            path_format = getattr(route, "path_format", None)
            path_params = scope.get("path_params") or {}
            status = cap.status if cap.status is not None else (500 if raised else None)
            # A failure in here must leave ids_reason null (Y5), so wrap it.
            affected_ids: Optional[list[str]] = None
            ids_reason: Optional[str] = None
            try:
                affected_ids, ids_reason = self._resolve_ids(
                    method, path_format, status, raised, cap, request_body, path_params
                )
            except Exception:
                affected_ids, ids_reason = None, None
            try:
                await self._finalize_row(pool, row_id, status, affected_ids, ids_reason)
            except Exception:
                pass

    # ----- id resolution ---------------------------------------------------- #

    def _resolve_ids(
        self,
        method: str,
        path_format: Optional[str],
        status: Optional[int],
        raised: bool,
        cap: "_ResponseCapture",
        request_body: bytes,
        path_params: Optional[dict] = None,
    ) -> tuple[Optional[list[str]], Optional[str]]:
        path_params = path_params or {}
        src = self.route_map.lookup(method, path_format)
        # No route, a pair missing from the map, or id source "none" -> no_id_source.
        if src is None or src.kind == "none":
            return None, REASON_NO_ID_SOURCE
        # Checked in order: error, then (for response) streamed, too_large, non_json.
        if raised or status is None or not (200 <= status < 300):
            return None, REASON_ERROR
        if src.kind == "request":
            field = src.field or ""
            # A request-sourced id can live in the URL path (e.g. /tasks/{task_key},
            # /claims/{rid}/anchor) or in the request body. Path params win, and do
            # not require a JSON body.
            if field in path_params and path_params[field] is not None:
                return [str(path_params[field])], None
            if not cap.request_was_json(request_body):
                return None, REASON_NON_JSON
            try:
                doc = json.loads(request_body.decode("utf-8"))
            except Exception:
                return None, REASON_NON_JSON
            return extract_ids(doc, field), None
        # response-sourced
        if cap.streamed:
            return None, REASON_STREAMED
        if cap.too_large:
            return None, REASON_TOO_LARGE
        if not cap.response_is_json():
            return None, REASON_NON_JSON
        try:
            doc = json.loads(cap.first_body.decode("utf-8"))
        except Exception:
            return None, REASON_NON_JSON
        return extract_ids(doc, src.field or ""), None

    # ----- database --------------------------------------------------------- #

    async def _insert_row(
        self, pool, method, path, client, client_ip, body_sha256
    ) -> Any:
        async with pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO koi_write_log
                    (method, path, client, client_ip, body_sha256)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
                """,
                method,
                path,
                client,
                client_ip,
                body_sha256,
            )

    async def _finalize_row(self, pool, row_id, status, affected_ids, ids_reason) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE koi_write_log
                   SET status = $2, affected_ids = $3, ids_reason = $4
                 WHERE id = $1
                """,
                row_id,
                status,
                affected_ids,
                ids_reason,
            )

    # ----- helpers ---------------------------------------------------------- #

    @staticmethod
    async def _send_simple(send: Send, status: int, body: bytes) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"text/plain; charset=utf-8"),
                    (b"content-length", str(len(body)).encode("latin-1")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body, "more_body": False})


class _ResponseCapture:
    """Observes the outgoing ASGI messages to capture status and peek the body.

    Never buffers the whole response: keeps at most ``max_peek`` bytes of the
    first body message and counts whether more than one body message arrived.
    """

    def __init__(self, max_peek: int):
        self.max_peek = max_peek
        self.status: Optional[int] = None
        self.headers: list[tuple[bytes, bytes]] = []
        self.first_body = b""
        self.body_messages = 0
        self.streamed = False
        self.too_large = False
        self._request_ct_json: Optional[bool] = None

    def observe(self, message: dict) -> None:
        mtype = message.get("type")
        if mtype == "http.response.start":
            self.status = message.get("status")
            self.headers = message.get("headers") or []
        elif mtype == "http.response.body":
            chunk = message.get("body", b"") or b""
            if self.body_messages == 0:
                if len(chunk) > self.max_peek:
                    self.first_body = chunk[: self.max_peek]
                    self.too_large = True
                else:
                    self.first_body = chunk
            self.body_messages += 1
            more = message.get("more_body", False)
            # A body delivered in more than one message is "streamed" (RQ46).
            if self.body_messages > 1:
                self.streamed = True
            if self.body_messages == 1 and more:
                # First message says more is coming -> it is streamed.
                self.streamed = True

    def response_is_json(self) -> bool:
        return _content_type_is_json(self.headers)

    def request_was_json(self, request_body: bytes) -> bool:
        # The request content-type is not on the response headers; decide by a
        # parse attempt, which is what the handler itself would rely on.
        try:
            json.loads(request_body.decode("utf-8"))
            return True
        except Exception:
            return False
