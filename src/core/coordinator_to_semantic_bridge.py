#!/usr/bin/env python3
"""
Bridge to forward KOI Coordinator events to Semantic Event Bridge

With --run it loops forever: it polls the coordinator's event queue (COORDINATOR_URL,
default http://localhost:8005), forwards each event to the semantic bridge and confirms
(acknowledges) the delivered ones. With no arguments or --dry-run it only prints its
configuration.
"""

import argparse
import asyncio
import sys
import hashlib
import httpx
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8005")
SEMANTIC_BRIDGE_URL = os.getenv("SEMANTIC_BRIDGE_URL", "http://localhost:8004")
SEMANTIC_BRIDGE_ENDPOINT = os.getenv("SEMANTIC_BRIDGE_ENDPOINT", "/events/process")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))
NODE_ID = os.getenv("NODE_ID", "semantic-bridge-connector")
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "10"))


async def poll_and_forward():
    """Poll coordinator for events and forward to semantic bridge"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Register with coordinator
        logger.info(f"Registering with coordinator as {NODE_ID}")

        while True:
            try:
                # Poll for events from coordinator
                poll_payload = {
                    "type": "poll_events",
                    "limit": MAX_EVENTS,
                    "node_id": NODE_ID,
                    "include_event_ids": True
                }
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

                    def normalize_event(event):
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

                    confirmed_event_ids = []

                    for idx, event in enumerate(events):
                        event_id = event_ids[idx] if idx < len(event_ids) else None

                        # Skip heartbeat events
                        if "heartbeat" in event.get("rid", "").lower():
                            continue

                        event = normalize_event(event)
                        logger.info(f"Forwarding event: {event.get('rid')}")

                        try:
                            # Forward to semantic bridge
                            sem_response = await client.post(
                                f"{SEMANTIC_BRIDGE_URL}{SEMANTIC_BRIDGE_ENDPOINT}",
                                json=event,
                                timeout=60.0
                            )

                            # Handle both 200 (success) and non-200 (failure) responses
                            result = sem_response.json()
                            success = False
                            if isinstance(result, dict):
                                success = bool(result.get("success", sem_response.status_code == 200))

                            if success:
                                logger.info(f"Processed: {result.get('success')} - Chunks: {result.get('chunks_created')}")
                                if event_id:
                                    confirmed_event_ids.append(event_id)
                            else:
                                error_msg = result.get('error', 'unknown') if isinstance(result, dict) else str(result)
                                logger.error(f"Semantic bridge error (HTTP {sem_response.status_code}): {error_msg}")

                        except Exception as e:
                            logger.error(f"Error forwarding event: {e}")
                            logger.error(f"Event structure: {json.dumps(event, indent=2)[:500]}")

                    if confirmed_event_ids:
                        try:
                            confirm_payload = {
                                "node_id": NODE_ID,
                                "event_ids": confirmed_event_ids,
                                "timestamp": datetime.now().isoformat()
                            }
                            confirm_response = await client.post(
                                f"{COORDINATOR_URL}/events/confirm",
                                json=confirm_payload
                            )
                            if confirm_response.status_code == 200:
                                confirm_data = confirm_response.json()
                                if isinstance(confirm_data, dict) and "payload" in confirm_data:
                                    confirm_data = confirm_data["payload"]
                                logger.info(f"Confirmed delivery of {confirm_data.get('confirmed_count', 0)} events")
                            else:
                                logger.error(f"Failed to confirm delivery: {confirm_response.status_code}")
                        except Exception as e:
                            logger.error(f"Error confirming delivery: {e}")

            except Exception as e:
                logger.error(f"Poll error: {e}")

            await asyncio.sleep(POLL_INTERVAL)


async def main():
    logger.info("Starting Coordinator to Semantic Bridge connector...")
    await poll_and_forward()


def cli_main(argv=None) -> int:
    """Command-line entry: prints the configuration unless --run is given."""
    parser = argparse.ArgumentParser(
        description="Poll the KOI coordinator's event queue, forward events to the semantic "
                    "bridge and confirm them. Default is a dry run that contacts nothing.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the configuration and exit (the default; wins over --run)")
    parser.add_argument("--run", action="store_true",
                        help="Start the polling loop (consumes and acknowledges queue events)")
    args = parser.parse_args(argv)

    print(f"Coordinator: {COORDINATOR_URL} (poll /events/poll as {NODE_ID}, up to {MAX_EVENTS} "
          f"events every {POLL_INTERVAL}s, confirm via /events/confirm)")
    print(f"Semantic bridge: {SEMANTIC_BRIDGE_URL}{SEMANTIC_BRIDGE_ENDPOINT}")
    if args.dry_run or not args.run:
        print("\nDry run: nothing was polled, forwarded or confirmed. Re-run with --run to start.")
        return 0
    asyncio.run(main())
    return 0


if __name__ == "__main__":
    sys.exit(cli_main())
