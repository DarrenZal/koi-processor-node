"""Hardened HTTP settings for OUTBOUND PROVIDER calls (OpenAI, Anthropic, …).

Why this module exists (gaiaaiagent/koi-processor#36)
-----------------------------------------------------
On 2026-07-15 a silently-dropped provider socket wedged the **shared** personal-koi
service on :8351 fleet-wide for ~40 minutes — every agent and session lost KOI search,
ingest, tasks and the morning brief. The remote sent FIN, the local end sat in
CLOSE_WAIT, and the caller never raised.

Two things made an ordinary dropped socket into an outage:

1. **No enforced ceiling.** `OpenAI(api_key=...)` was constructed with SDK defaults —
   `Timeout(connect=5, read=600, write=600, pool=600)` with `max_retries=2`. That is
   **600s x 3 attempts = 30 minutes** of hanging on a single embed call, which is the
   observed wedge almost exactly.
2. **A single scalar `timeout=` does not reliably fire on a half-closed POOLED socket.**
   Callers with `timeout=300` were observed hanging for 40 minutes. A reused CLOSE_WAIT
   connection can outlive its nominal timeout.

Note the embedding calls run under `asyncio.to_thread`, so they do not block the event
loop directly — they exhaust the default ThreadPoolExecutor instead. Once those workers
are all parked on dead sockets, every `to_thread` caller stalls and the service is
effectively down even though the loop is technically alive. That is why the asyncpg pool
was found sitting idle-in-ClientRead while Postgres itself was perfectly healthy.

The three defences here, which are meant to be used together
-----------------------------------------------------------
* **Explicit per-phase timeouts** (`connect`/`read`/`write`/`pool`) rather than one
  scalar, so every phase is independently bounded.
* **Short `keepalive_expiry`**, so a socket that died while idle is evicted from the pool
  instead of being handed to the next request. This is the one that addresses the actual
  CLOSE_WAIT reuse; timeouts alone only bound the damage.
* **TCP keepalive socket options**, so the OS itself detects a peer that went away
  without a clean FIN.

Everything is env-tunable — raise the read ceiling rather than removing it.
"""
from __future__ import annotations

import os
import socket
from typing import Any, Dict, List, Optional, Tuple

import httpx

# Per-phase ceilings. `read` is the one that matters for a wedged provider: healthy
# OpenAI embedding latency is well under 1s, and a 100-item batch measured ~5s, so the
# 120s default is ~24x headroom while still failing 15x faster than the SDK default.
PROVIDER_CONNECT_TIMEOUT = float(os.getenv("PROVIDER_CONNECT_TIMEOUT", "10"))
PROVIDER_READ_TIMEOUT = float(os.getenv("PROVIDER_READ_TIMEOUT", "120"))
PROVIDER_WRITE_TIMEOUT = float(os.getenv("PROVIDER_WRITE_TIMEOUT", "30"))
PROVIDER_POOL_TIMEOUT = float(os.getenv("PROVIDER_POOL_TIMEOUT", "10"))

# Evict pooled sockets this many seconds after they go idle. The CLOSE_WAIT reuse is the
# failure mode, so keeping this well under a typical provider/LB idle-close window is the
# point; reconnect cost is a TLS handshake, which is cheap next to a fleet outage.
PROVIDER_KEEPALIVE_EXPIRY = float(os.getenv("PROVIDER_KEEPALIVE_EXPIRY", "30"))
PROVIDER_MAX_CONNECTIONS = int(os.getenv("PROVIDER_MAX_CONNECTIONS", "20"))
PROVIDER_MAX_KEEPALIVE = int(os.getenv("PROVIDER_MAX_KEEPALIVE", "5"))

# SDK-level retries multiply the read ceiling, so bound them explicitly too:
# worst case becomes read x (1 + retries) instead of 600 x 3.
PROVIDER_MAX_RETRIES = int(os.getenv("PROVIDER_MAX_RETRIES", "2"))


def provider_timeout(read: Optional[float] = None) -> httpx.Timeout:
    """Explicit per-phase timeout. Pass `read` to override just that phase."""
    return httpx.Timeout(
        connect=PROVIDER_CONNECT_TIMEOUT,
        read=PROVIDER_READ_TIMEOUT if read is None else float(read),
        write=PROVIDER_WRITE_TIMEOUT,
        pool=PROVIDER_POOL_TIMEOUT,
    )


def provider_limits() -> httpx.Limits:
    return httpx.Limits(
        max_connections=PROVIDER_MAX_CONNECTIONS,
        max_keepalive_connections=PROVIDER_MAX_KEEPALIVE,
        keepalive_expiry=PROVIDER_KEEPALIVE_EXPIRY,
    )


def _keepalive_socket_options() -> List[Tuple[int, int, int]]:
    """TCP keepalive so the OS surfaces a peer that vanished without a clean FIN.

    Idle 30s, probe every 10s, 3 failed probes -> dead. Only sets options the running
    platform actually defines (TCP_KEEPIDLE is Linux; macOS uses TCP_KEEPALIVE).
    """
    opts: List[Tuple[int, int, int]] = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    for name, value in (("TCP_KEEPIDLE", 30), ("TCP_KEEPALIVE", 30),
                        ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 3)):
        opt = getattr(socket, name, None)
        if opt is not None:
            opts.append((socket.IPPROTO_TCP, opt, value))
    return opts


def provider_transport(*, asynchronous: bool = True, retries: int = 0):
    """httpx transport carrying BOTH the connection limits and the keepalive options.

    The limits MUST be passed in here, not to the Client. httpx uses `limits=` only to
    build its DEFAULT transport, so `Client(limits=..., transport=...)` silently DISCARDS
    them — verified: keepalive_expiry came back 5.0 (the httpx default) instead of the
    configured 30. That would have quietly removed the one defence that actually fixes
    CLOSE_WAIT reuse while looking correctly configured.
    """
    cls = httpx.AsyncHTTPTransport if asynchronous else httpx.HTTPTransport
    try:
        return cls(retries=retries, limits=provider_limits(),
                   socket_options=_keepalive_socket_options())
    except TypeError:
        # httpx < 0.25 has no socket_options; keep the limits, drop only the extra.
        return cls(retries=retries, limits=provider_limits())


def provider_async_client(*, read: Optional[float] = None, **kwargs: Any) -> httpx.AsyncClient:
    """`httpx.AsyncClient` with all three defences applied. Caller-supplied kwargs win."""
    opts: Dict[str, Any] = {
        "timeout": provider_timeout(read),
        # NOTE: limits live INSIDE the transport (see provider_transport) — passing
        # them here as well would be silently ignored by httpx.
        "transport": provider_transport(asynchronous=True),
    }
    opts.update(kwargs)
    return httpx.AsyncClient(**opts)


def provider_sync_client(*, read: Optional[float] = None, **kwargs: Any) -> httpx.Client:
    """Sync variant — for SDKs (e.g. `openai.OpenAI`) that take an `http_client`."""
    opts: Dict[str, Any] = {
        "timeout": provider_timeout(read),
        # NOTE: limits live INSIDE the transport (see provider_transport) — passing
        # them here as well would be silently ignored by httpx.
        "transport": provider_transport(asynchronous=False),
    }
    opts.update(kwargs)
    return httpx.Client(**opts)
