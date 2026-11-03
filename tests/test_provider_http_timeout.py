#!/usr/bin/env python3
"""Proves the provider-HTTP hardening actually FIRES.

Config inspection is not proof. On 2026-07-15 the wedged callers *looked* configured —
they had `timeout=300` — and still hung for 40 minutes. So these tests stand up real
sockets that misbehave in the two ways observed in the incident and assert the client
raises promptly instead of hanging.

  1. SILENT SERVER — accepts the connection, reads the request, then never replies.
     The read timeout must fire.
  2. HALF-CLOSED / CLOSE_WAIT REUSE — the server accepts, replies, then sends FIN. The
     socket sits in the pool half-closed. A later request must not hang on it. This is
     the exact shape from the incident, and the defence is `keepalive_expiry`, not the
     timeout.
  3. LIMITS REACH THE POOL — regression test for a bug in the fix itself: httpx uses
     `limits=` only to build its DEFAULT transport, so `Client(limits=…, transport=…)`
     silently DISCARDS them. That shipped keepalive_expiry=5.0 (httpx default) while
     looking correct.

No network access and no API keys required.

Run:  <venv>/bin/python tests/test_provider_http_timeout.py
"""
import asyncio
import pathlib
import socket
import sys
import threading
import time

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import httpx  # noqa: E402
from api.provider_http import (  # noqa: E402
    PROVIDER_KEEPALIVE_EXPIRY, provider_async_client, provider_limits, provider_sync_client,
    provider_timeout, provider_transport,
)

RESULTS = []


def check(cond, label):
    RESULTS.append(bool(cond))
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")


def _serve(handler):
    """Start a throwaway TCP server on a free port; returns (port, stop_fn)."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    port = srv.getsockname()[1]
    stop = threading.Event()

    def loop():
        srv.settimeout(0.5)
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=handler, args=(conn,), daemon=True).start()
        srv.close()

    threading.Thread(target=loop, daemon=True).start()
    return port, stop.set


def _silent(conn):
    """Accept, read the request, then hold the socket open and never answer."""
    try:
        conn.recv(65535)
        while True:
            time.sleep(0.5)
    except OSError:
        pass


def _reply_then_fin(conn):
    """Answer once, then close our side — the client's socket goes to CLOSE_WAIT."""
    try:
        conn.recv(65535)
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
        time.sleep(0.05)
        conn.close()
    except OSError:
        pass


async def main() -> int:
    print("1. limits actually reach the connection pool (regression: httpx discards")
    print("   `limits=` when an explicit `transport=` is also passed)")
    want = provider_limits().keepalive_expiry
    for label, cli in (("async", provider_async_client()), ("sync", provider_sync_client())):
        got = cli._transport._pool._keepalive_expiry
        check(got == want, f"{label} client keepalive_expiry={got} (want {want}, httpx default is 5.0)")
    check(provider_transport()._pool._keepalive_expiry == want, "transport carries the limits itself")

    print("\n2. read timeout FIRES against a server that never replies")
    port, stop = _serve(_silent)
    try:
        t0 = time.monotonic()
        raised = None
        async with provider_async_client(read=2.0) as c:
            try:
                await c.get(f"http://127.0.0.1:{port}/hang")
            except Exception as e:  # noqa: BLE001
                raised = e
        dt = time.monotonic() - t0
        check(isinstance(raised, httpx.ReadTimeout), f"raised ReadTimeout (got {type(raised).__name__})")
        check(dt < 6.0, f"returned in {dt:.1f}s, not hanging (bounded by read=2.0)")
    finally:
        stop()

    print("\n3. a half-closed (CLOSE_WAIT) pooled socket does not hang the next request")
    port, stop = _serve(_reply_then_fin)
    try:
        async with provider_async_client(read=3.0) as c:
            r1 = await c.get(f"http://127.0.0.1:{port}/one")
            check(r1.status_code == 200, "first request succeeds")
            await asyncio.sleep(0.3)  # let the FIN land; socket is now half-closed
            t0 = time.monotonic()
            ok = False
            try:
                r2 = await c.get(f"http://127.0.0.1:{port}/two")
                ok = r2.status_code == 200
            except httpx.HTTPError:
                ok = True  # failing fast is an acceptable outcome; hanging is not
            dt = time.monotonic() - t0
            check(ok, "second request completed or failed fast (did not hang)")
            check(dt < 6.0, f"second request resolved in {dt:.1f}s")
    finally:
        stop()

    print("\n4. every phase is bounded (a scalar timeout is what failed in the incident)")
    t = provider_timeout()
    check(all(v is not None for v in (t.connect, t.read, t.write, t.pool)),
          f"connect={t.connect} read={t.read} write={t.write} pool={t.pool}")
    check(PROVIDER_KEEPALIVE_EXPIRY > 0, f"keepalive_expiry configured ({PROVIDER_KEEPALIVE_EXPIRY}s)")

    print(f"\n{sum(RESULTS)}/{len(RESULTS)} passed")
    return 0 if all(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
