#!/usr/bin/env python3
"""
KOI Protocol-compliant Event Bridge Poller
Implements the KOI-net protocol by polling the Coordinator (full node) for events
and processing them through the Event Bridge pipeline.

This makes Event Bridge act as a partial node that polls for events per KOI protocol.

With --run it loops forever: it polls the coordinator's event queue, forwards each event
to the event bridge and the code graph service, and confirms (acknowledges) the delivered
ones. With no arguments or --dry-run it only prints its configuration.
"""

import argparse
import asyncio
import sys
import httpx
import logging
import json
import hashlib
import os
from datetime import datetime
import time
from dotenv import load_dotenv

from koi_envelope import load_private_key_from_env, sign_envelope
from forwarder_delivery import (
    Disposition,
    classify_delivery,
    write_dead_letter,
    backoff_delay,
    max_retries,
)

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("koi.forwarder")

# Configuration from environment with sensible defaults
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8005")
EVENT_BRIDGE_URL = os.getenv("EVENT_BRIDGE_URL", "http://localhost:8004")  # Semantic Event Bridge
EVENT_BRIDGE_ENDPOINT = os.getenv("EVENT_BRIDGE_ENDPOINT", "/events/process")
CODE_GRAPH_URL = os.getenv("CODE_GRAPH_URL", "http://localhost:8350")
CODE_GRAPH_ENDPOINT = os.getenv("CODE_GRAPH_ENDPOINT", "/process-koi-event")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "2"))
NODE_ID = os.getenv("NODE_ID", "event-bridge-forwarder")
COORDINATOR_NODE_ID = os.getenv("COORDINATOR_NODE_ID")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "10"))
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "300"))  # 5 minutes

# Service key sent to the configured internal targets (event bridge, code graph,
# coordinator). Set as a default header on the client below, so it only reaches
# the env-configured URLs this forwarder talks to.
SERVICE_KEY = os.getenv("KOI_SERVICE_KEY")

# Envelope signing
ENVELOPE_PRIVATE_KEY = load_private_key_from_env()
ENVELOPE_SIGN = bool(ENVELOPE_PRIVATE_KEY) and os.getenv("KOI_ENVELOPE_SIGN", "true").lower() not in ("0", "false", "no")


async def check_endpoint_health(client: httpx.AsyncClient, name: str, url: str) -> bool:
    """Check if an endpoint is healthy and responding"""
    try:
        # Try health endpoint first, then root
        for endpoint in ["/health", "/"]:
            try:
                response = await client.get(f"{url}{endpoint}", timeout=5.0)
                if response.status_code == 200:
                    logger.info(f"✅ {name} is healthy at {url}")
                    return True
            except:
                continue
        logger.error(f"❌ {name} at {url} is NOT responding!")
        return False
    except Exception as e:
        logger.error(f"❌ {name} health check failed: {e}")
        return False


async def startup_health_checks(client: httpx.AsyncClient) -> bool:
    """Run health checks on all endpoints at startup"""
    logger.info("=" * 60)
    logger.info("Running startup health checks...")
    logger.info("=" * 60)

    all_healthy = True

    # Check coordinator
    if not await check_endpoint_health(client, "Coordinator", COORDINATOR_URL):
        all_healthy = False

    # Check event bridge
    if not await check_endpoint_health(client, "Event Bridge", EVENT_BRIDGE_URL):
        all_healthy = False
        logger.error(f"⚠️  CRITICAL: Event Bridge not responding! Events will NOT be processed!")

    # Check code graph (optional, don't fail if missing)
    await check_endpoint_health(client, "Code Graph", CODE_GRAPH_URL)

    logger.info("=" * 60)
    if all_healthy:
        logger.info("✅ All critical endpoints are healthy")
    else:
        logger.error("❌ Some critical endpoints are NOT healthy!")
    logger.info("=" * 60)

    return all_healthy

async def forward_events():
    """Poll coordinator for events and forward to Event Bridge"""

    default_headers = {"X-KOI-Service-Key": SERVICE_KEY} if SERVICE_KEY else {}
    async with httpx.AsyncClient(timeout=30.0, headers=default_headers) as client:
        # Log configuration
        logger.info("=" * 60)
        logger.info("KOI Event Forwarder Configuration:")
        logger.info(f"  Coordinator: {COORDINATOR_URL}")
        logger.info(f"  Event Bridge: {EVENT_BRIDGE_URL}{EVENT_BRIDGE_ENDPOINT}")
        logger.info(f"  Code Graph: {CODE_GRAPH_URL}{CODE_GRAPH_ENDPOINT}")
        logger.info(f"  Poll Interval: {POLL_INTERVAL}s")
        logger.info(f"  Max Concurrent: {MAX_CONCURRENT}")
        logger.info("=" * 60)

        # Run startup health checks
        await startup_health_checks(client)

        logger.info(f"Starting event forwarder: {COORDINATOR_URL} -> {EVENT_BRIDGE_URL}")
        last_health_check = time.time()

        while True:
            await poll_once(client)

            await asyncio.sleep(POLL_INTERVAL)

            # Periodic health checks
            if time.time() - last_health_check > HEALTH_CHECK_INTERVAL:
                logger.info("Running periodic health check...")
                await startup_health_checks(client)
                last_health_check = time.time()


def normalize_event(event):
    """Normalize a coordinator event into the bridge's expected shape."""
    if "bundle" in event:
        return event

    manifest = event.get("manifest") or {}
    contents = event.get("contents") or {}
    content_hash = manifest.get("sha256_hash") or manifest.get("content_hash")
    if not content_hash:
        content_hash = hashlib.sha256(json.dumps(contents, sort_keys=True).encode()).hexdigest()

    bundle = {
        "rid": event.get("rid"),
        "manifest": {
            "rid": event.get("rid"),
            "timestamp": manifest.get("timestamp") or event.get("timestamp") or datetime.now().isoformat(),
            "content_hash": content_hash,
            "size_bytes": manifest.get("size_bytes") or len(json.dumps(contents).encode()),
            "content_type": manifest.get("content_type") or "application/json",
            "version": manifest.get("version") or "1.0",
            "metadata": manifest.get("metadata") or {}
        },
        "contents": contents
    }
    return {
        "event_type": event.get("event_type"),
        "rid": event.get("rid"),
        "source_node": event.get("source_node", "coordinator"),
        "timestamp": event.get("timestamp") or datetime.now().isoformat(),
        "bundle": bundle
    }


async def _forward_one(client, event):
    """Forward one normalized event to the bridge (and, best-effort, the code
    graph). Returns (Disposition, detail)."""
    eb_task = client.post(f"{EVENT_BRIDGE_URL}{EVENT_BRIDGE_ENDPOINT}", json=event)
    cg_task = client.post(f"{CODE_GRAPH_URL}{CODE_GRAPH_ENDPOINT}", json=event)
    eb_response, cg_response = await asyncio.gather(eb_task, cg_task, return_exceptions=True)

    # Code graph is best effort: never blocks delivery or confirmation.
    if isinstance(cg_response, Exception):
        logger.debug(f"Code Graph connection error: {cg_response}")
    elif getattr(cg_response, "status_code", None) != 200:
        logger.debug(f"Code Graph error {getattr(cg_response, 'status_code', '?')}")

    if isinstance(eb_response, Exception):
        return classify_delivery(None, None), f"connection error: {eb_response}"

    status = eb_response.status_code
    body_success = None
    detail = f"HTTP {status}"
    if status == 200:
        try:
            eb_payload = eb_response.json()
            body_success = bool(eb_payload.get("success", False))
            if not body_success:
                detail = eb_payload.get("error") or "HTTP 200: success=false"
        except Exception as e:
            body_success = False
            detail = f"non-JSON response (HTTP {status}): {e}"
    return classify_delivery(status, body_success), detail


async def _forward_batch(client, pending):
    """Forward a list of (event, event_id). Returns (confirm_ids, retryable).

    Dead-lettered events are written out and added to confirm_ids so the
    coordinator stops redelivering them. Retryable events are returned to be
    retried without polling again.
    """
    results = await asyncio.gather(*[
        _forward_one(client, normalize_event(ev)) for ev, _ in pending
    ])
    confirm_ids = []
    retryable = []
    for (ev, event_id), (disposition, detail) in zip(pending, results):
        rid = ev.get("rid", "unknown")
        if disposition == Disposition.CONFIRM:
            logger.info(f"Delivered event: {rid}")
            if event_id is not None:
                confirm_ids.append(event_id)
        elif disposition == Disposition.DEAD_LETTER:
            path = write_dead_letter(ev, event_id, detail)
            logger.error(f"Dead-lettered event {rid} ({detail}) -> {path}")
            if event_id is not None:
                confirm_ids.append(event_id)
        else:  # RETRY
            logger.warning(f"Transient failure for event {rid}: {detail}")
            retryable.append((ev, event_id))
    return confirm_ids, retryable


async def _confirm(client, event_ids) -> int:
    """Confirm delivery of the given event ids. Returns the confirmed count."""
    if not event_ids:
        return 0
    try:
        confirm_payload = {
            "node_id": NODE_ID,
            "event_ids": event_ids,
            "timestamp": datetime.now().isoformat()
        }
        if ENVELOPE_SIGN:
            target_node = COORDINATOR_NODE_ID or "coordinator"
            confirm_payload = sign_envelope(confirm_payload, NODE_ID, target_node, ENVELOPE_PRIVATE_KEY)

        confirm_response = await client.post(
            f"{COORDINATOR_URL}/events/confirm",
            json=confirm_payload
        )
        if confirm_response.status_code == 200:
            confirm_data = confirm_response.json()
            if isinstance(confirm_data, dict) and "payload" in confirm_data:
                confirm_data = confirm_data["payload"]
            count = int(confirm_data.get("confirmed_count", 0))
            logger.info(f"Confirmed delivery of {count} events")
            return count
        logger.error(f"Failed to confirm delivery: {confirm_response.status_code}")
    except Exception as e:
        logger.error(f"Error confirming delivery: {e}")
    return 0


async def poll_once(client: httpx.AsyncClient) -> int:
    """Poll once, forward the batch, and retry transient failures with backoff
    WITHOUT polling again, so an outage, a 5xx or a missing/rejected key never
    silently drops events. Permanent client errors (400/422) are dead-lettered
    and confirmed. Returns the confirmed event count."""
    confirmed_count = 0
    try:
        poll_payload = {
            "type": "poll_events",
            "limit": MAX_CONCURRENT,
            "node_id": NODE_ID,
            "include_event_ids": True
        }
        if ENVELOPE_SIGN:
            target_node = COORDINATOR_NODE_ID or "coordinator"
            poll_payload = sign_envelope(poll_payload, NODE_ID, target_node, ENVELOPE_PRIVATE_KEY)

        response = await client.post(
            f"{COORDINATOR_URL}/events/poll",
            json=poll_payload
        )

        if response.status_code == 200:
            poll_response = response.json()
            if isinstance(poll_response, dict) and "payload" in poll_response:
                poll_response = poll_response["payload"]

            events = poll_response.get("events", [])
            event_ids = poll_response.get("event_ids", [])

            if events:
                logger.info(f"Received {len(events)} events from coordinator")
                pending = [
                    (events[i], event_ids[i] if i < len(event_ids) else None)
                    for i in range(len(events))
                ]
                attempt = 0
                max_attempts = max_retries()
                while True:
                    confirm_ids, retryable = await _forward_batch(client, pending)
                    confirmed_count += await _confirm(client, confirm_ids)
                    if not retryable:
                        pending = []
                        break
                    if attempt >= max_attempts:
                        pending = retryable  # exhausted; dead-letter below
                        break
                    delay = backoff_delay(attempt)
                    logger.critical(
                        "Forwarder: %d event(s) undelivered (transient). Holding the "
                        "batch and retrying in %.1fs without polling (attempt %d/%d).",
                        len(retryable), delay, attempt + 1, max_attempts,
                    )
                    await asyncio.sleep(delay)
                    attempt += 1
                    pending = retryable

                if pending:
                    # Retries exhausted. Dead-letter (preserve) and confirm, so one
                    # persistent condition - a wrong key, a deterministic 500 - cannot
                    # wedge the whole pipeline forever. Still never a silent drop: each
                    # is logged CRITICAL and written to the dead-letter JSONL.
                    exhausted_ids = []
                    for ev, event_id in pending:
                        path = write_dead_letter(
                            ev, event_id, f"retry attempts exhausted ({max_attempts})"
                        )
                        logger.critical(
                            "Forwarder: dead-lettered %s after %d attempts -> %s",
                            ev.get("rid", "unknown"), max_attempts, path,
                        )
                        if event_id is not None:
                            exhausted_ids.append(event_id)
                    confirmed_count += await _confirm(client, exhausted_ids)

        elif response.status_code == 404:
            logger.debug("No events available")
        else:
            logger.error(f"Coordinator poll error: {response.status_code}")

    except Exception as e:
        logger.error(f"Error polling coordinator: {e}")

    return confirmed_count

def cli_main(argv=None) -> int:
    """Command-line entry: prints the configuration unless --run is given."""
    parser = argparse.ArgumentParser(
        description="KOI-net partial-node forwarder: poll the coordinator, forward events to the "
                    "event bridge and code graph service, confirm them. Default is a dry run "
                    "that contacts nothing.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the configuration and exit (the default; wins over --run)")
    parser.add_argument("--run", action="store_true",
                        help="Start the polling loop (consumes and acknowledges queue events)")
    args = parser.parse_args(argv)

    print(f"Coordinator: {COORDINATOR_URL} (poll /events/poll as {NODE_ID}, up to {MAX_CONCURRENT} "
          f"events every {POLL_INTERVAL}s, confirm via /events/confirm)")
    print(f"Event bridge: {EVENT_BRIDGE_URL}{EVENT_BRIDGE_ENDPOINT}")
    print(f"Code graph: {CODE_GRAPH_URL}{CODE_GRAPH_ENDPOINT}")
    print(f"Envelope signing: {'on' if ENVELOPE_SIGN else 'off'}")
    if args.dry_run or not args.run:
        print("\nDry run: nothing was polled, forwarded or confirmed. Re-run with --run to start.")
        return 0
    try:
        asyncio.run(forward_events())
    except KeyboardInterrupt:
        logger.info("Forwarder stopped")
    return 0


if __name__ == "__main__":
    sys.exit(cli_main())
