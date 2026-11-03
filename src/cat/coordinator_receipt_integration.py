#!/usr/bin/env python3
"""
Coordinator CAT Receipt Integration
Adds CAT receipt creation when the coordinator receives sensor data

Run as a script it is a demo: with --apply it inserts one test 'discourse-sensor'
receipt into koi_transformation_receipts in the database POSTGRES_URL names (no
cleanup). With no arguments or --dry-run it only prints what it would do.
"""

import asyncpg
import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

def _postgres_dsn() -> Dict[str, Any]:
    """Connection settings from the environment; there is no built-in default."""
    url = os.environ.get("POSTGRES_URL")
    if not url:
        raise RuntimeError("POSTGRES_URL is not set and no db_config was passed")
    return {"dsn": url}


class CoordinatorReceiptManager:
    """Manages CAT receipt creation at the coordinator level"""

    def __init__(self, db_config: Dict[str, Any] = None):
        self.db_config = db_config or _postgres_dsn()
        self.conn = None

    async def connect(self):
        """Connect to database"""
        if not self.conn:
            self.conn = await asyncpg.connect(**self.db_config)

    async def close(self):
        """Close database connection"""
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def create_sensor_collection_receipt(
        self,
        sensor_name: str,
        rid: str,
        content_hash: str,
        source_url: Optional[str] = None,
        document_count: int = 1,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a CAT receipt when coordinator receives data from a sensor

        Args:
            sensor_name: Name of the sensor that collected the data
            rid: Resource Identifier of the collected content
            content_hash: SHA-256 hash of the content
            source_url: Original source URL if available
            document_count: Number of documents in the bundle
            metadata: Additional metadata

        Returns:
            receipt_id: The generated CAT receipt ID
        """
        await self.connect()

        try:
            # Generate receipt ID
            timestamp = datetime.now(timezone.utc).isoformat()
            receipt_content = f"sensor_collection:{sensor_name}:{rid}:{timestamp}"
            receipt_id = hashlib.sha256(receipt_content.encode()).hexdigest()

            # Prepare metadata
            if metadata is None:
                metadata = {}

            metadata.update({
                "sensor": sensor_name,
                "collection_time": timestamp,
                "document_count": document_count
            })

            if source_url:
                metadata["source_url"] = source_url

            # Insert CAT receipt
            await self.conn.execute("""
                INSERT INTO koi_transformation_receipts (
                    receipt_id,
                    transformation_type,
                    input_rid,
                    output_rid,
                    source_sensor,
                    event_type,
                    processor_name,
                    processor_version,
                    chunks_created,
                    metadata,
                    created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (receipt_id) DO NOTHING
            """,
                receipt_id,
                "sensor_collection",
                None,  # No input RID for initial collection
                rid,   # Output is the collected document RID
                sensor_name,
                "NEW",
                "KOI Coordinator",
                "1.0.0",
                document_count,
                json.dumps(metadata),
                datetime.now(timezone.utc)
            )

            logger.info(f"Created sensor collection receipt {receipt_id} for {sensor_name}: {rid}")
            return receipt_id

        except Exception as e:
            logger.error(f"Error creating sensor collection receipt: {e}")
            return ""

    async def create_coordinator_forwarding_receipt(
        self,
        input_rid: str,
        output_rid: str,
        target_service: str,
        sensor_name: str,
        event_type: str = "NEW",
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a CAT receipt when coordinator forwards to Event Bridge

        Args:
            input_rid: Input RID from sensor
            output_rid: Output RID (often same as input)
            target_service: Service being forwarded to (e.g., "event-bridge")
            sensor_name: Original sensor name
            event_type: Type of event (NEW, UPDATE, FORGET)
            metadata: Additional metadata

        Returns:
            receipt_id: The generated CAT receipt ID
        """
        await self.connect()

        try:
            # Generate receipt ID
            timestamp = datetime.now(timezone.utc).isoformat()
            receipt_content = f"coordinator_forward:{input_rid}:{target_service}:{timestamp}"
            receipt_id = hashlib.sha256(receipt_content.encode()).hexdigest()

            # Prepare metadata
            if metadata is None:
                metadata = {}

            metadata.update({
                "forwarded_to": target_service,
                "forwarding_time": timestamp,
                "original_sensor": sensor_name
            })

            # Insert CAT receipt
            await self.conn.execute("""
                INSERT INTO koi_transformation_receipts (
                    receipt_id,
                    transformation_type,
                    input_rid,
                    output_rid,
                    source_sensor,
                    event_type,
                    processor_name,
                    processor_version,
                    metadata,
                    created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (receipt_id) DO NOTHING
            """,
                receipt_id,
                "coordinator_forwarding",
                input_rid,
                output_rid,
                sensor_name,
                event_type,
                "KOI Coordinator",
                "1.0.0",
                json.dumps(metadata),
                datetime.now(timezone.utc)
            )

            logger.info(f"Created forwarding receipt {receipt_id}: {input_rid} -> {target_service}")
            return receipt_id

        except Exception as e:
            logger.error(f"Error creating forwarding receipt: {e}")
            return ""


# Example integration with coordinator
async def add_receipts_to_coordinator_event(
    event_data: Dict[str, Any],
    receipt_manager: CoordinatorReceiptManager
) -> None:
    """
    Add CAT receipts when coordinator processes an event

    Args:
        event_data: The event data from sensor
        receipt_manager: The receipt manager instance
    """
    try:
        # Extract key information
        sensor_name = event_data.get("source_node", "unknown")
        rid = event_data.get("rid", "")
        bundle = event_data.get("bundle", {})

        if bundle and rid:
            manifest = bundle.get("manifest", {})
            content_hash = manifest.get("content_hash", "")
            metadata = manifest.get("metadata", {})

            # Create sensor collection receipt
            collection_receipt = await receipt_manager.create_sensor_collection_receipt(
                sensor_name=sensor_name,
                rid=rid,
                content_hash=content_hash,
                source_url=metadata.get("url"),
                document_count=1,
                metadata=metadata
            )

            # Create forwarding receipt when sending to Event Bridge
            forwarding_receipt = await receipt_manager.create_coordinator_forwarding_receipt(
                input_rid=rid,
                output_rid=rid,
                target_service="event-bridge",
                sensor_name=sensor_name,
                event_type=event_data.get("event_type", "NEW"),
                metadata={"collection_receipt": collection_receipt}
            )

            logger.info(f"Created receipts for {rid}: collection={collection_receipt}, forwarding={forwarding_receipt}")

    except Exception as e:
        logger.error(f"Error adding receipts to coordinator event: {e}")


async def _example_receipt() -> None:
    """Demo: insert one test sensor-collection receipt and print its id."""
    manager = CoordinatorReceiptManager()

    # Test sensor collection receipt
    receipt_id = await manager.create_sensor_collection_receipt(
        sensor_name="discourse-sensor",
        rid="orn:discourse.post:forum.regen.network/123/456",
        content_hash="abc123def456",
        source_url="https://forum.regen.network/t/test/123",
        document_count=1,
        metadata={"topic": "governance"}
    )

    print(f"Created receipt: {receipt_id}")

    await manager.close()


def _masked_dsn(dsn: Optional[str]) -> str:
    """The DSN with any password replaced by ***."""
    if not dsn:
        return "(POSTGRES_URL is not set)"
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(dsn)
    if parts.password is None:
        return dsn
    netloc = parts.netloc.rsplit("@", 1)
    userinfo = netloc[0].split(":", 1)[0]
    return urlunsplit(parts._replace(netloc=f"{userinfo}:***@{netloc[1]}"))


def cli_main(argv=None) -> int:
    """Command-line entry: a dry run unless --apply is given."""
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(
        description="Coordinator CAT receipt demo: insert one test receipt into "
                    "koi_transformation_receipts in the database POSTGRES_URL names. "
                    "Default is a dry run.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Connect and insert the test receipt")
    args = parser.parse_args(argv)

    print(f"Target: {_masked_dsn(os.environ.get('POSTGRES_URL'))}")
    print("Steps: insert one 'discourse-sensor' sensor_collection receipt into "
          "koi_transformation_receipts (not removed afterwards)")
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was written. Re-run with --apply to do this.")
        return 0
    asyncio.run(_example_receipt())
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(cli_main())
