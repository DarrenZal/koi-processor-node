"""
Enhanced CAT Receipt Chain with Jena Integration
Stores receipts in both PostgreSQL (operational) and Jena (provenance graph)

Run as a script it is a one-time migration: with --apply it reads up to 100 rows of
koi_transformation_receipts that have no jena_receipt_uri from --db-url (default the
eliza database on localhost:5433), writes each to Jena on localhost:3030 and UPDATEs the
row with its Jena URIs. With no arguments or --dry-run it only prints what it would do.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import asyncpg
import asyncio
from dataclasses import dataclass, asdict

import sys
import os

DEFAULT_MIGRATION_DB_URL = "postgresql://postgres:postgres@localhost:5433/eliza"


def _masked_dsn(dsn: str) -> str:
    """The DSN with any password replaced by ***."""
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(dsn)
    if parts.password is None:
        return dsn
    host = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit(parts._replace(netloc=f"{parts.username}:***@{host}"))


def _build_parser():
    import argparse

    parser = argparse.ArgumentParser(
        description="One-time migration of CAT receipts from PostgreSQL to Jena. "
                    "Default is a dry run that contacts nothing.")
    parser.add_argument("--db-url", default=DEFAULT_MIGRATION_DB_URL,
                        help="PostgreSQL DSN to migrate from (default: the eliza database on "
                             "localhost:5433)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Write up to 100 receipts to Jena and UPDATE their rows")
    return parser


def _print_plan(args) -> bool:
    """Print the target and steps. True when this is a dry run (nothing should run)."""
    print(f"Source: {_masked_dsn(args.db_url)}")
    print("Jena: http://localhost:3030/koi")
    print("Steps: read up to 100 koi_transformation_receipts rows with jena_receipt_uri IS NULL, "
          "write each to Jena, then UPDATE the row with its Jena URIs")
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was read or written. Re-run with --apply to do this.")
        return True
    return False


if __name__ == "__main__":
    # --help, argument errors and the dry run finish here, before the optional rdflib
    # import below. Only --apply goes on to the migration at the end of the file.
    _MAIN_ARGS = _build_parser().parse_args()
    if _print_plan(_MAIN_ARGS):
        sys.exit(0)

# Import Jena integration
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from provenance.jena_integration import JenaProvenanceManager

logger = logging.getLogger(__name__)


@dataclass
class CATReceiptV2:
    """Enhanced CAT Receipt with dual storage"""
    receipt_id: str
    transformation_type: str
    input_rid: str
    output_rid: str
    parent_receipt_id: Optional[str]
    processor_name: str
    source_sensor: str
    timestamp: str
    metadata: Dict[str, Any]
    content_hash: str
    jena_activity_uri: Optional[str] = None
    jena_receipt_uri: Optional[str] = None


class CATReceiptChainV2:
    """
    Enhanced CAT Receipt Chain Manager
    Stores in PostgreSQL for operations and Jena for provenance
    """

    def __init__(self, db_url: str, jena_endpoint: str = None):
        self.db_url = db_url
        self.pool = None
        self.jena = JenaProvenanceManager(
            query_endpoint=jena_endpoint or "http://localhost:3030/koi/sparql"
        )
        logger.info("CAT receipt chain v2 initialized with Jena integration")

    async def initialize(self):
        """Initialize database connection pool"""
        self.pool = await asyncpg.create_pool(self.db_url)
        logger.info("Database pool initialized")

    async def close(self):
        """Close database pool"""
        if self.pool:
            await self.pool.close()

    async def create_receipt(self,
                           transformation_type: str,
                           input_rid: str,
                           output_rid: str,
                           processor: str,
                           source_sensor: str = None,
                           parent_receipt_id: Optional[str] = None,
                           metadata: Optional[Dict[str, Any]] = None) -> CATReceiptV2:
        """
        Create a CAT receipt and store in both PostgreSQL and Jena

        Args:
            transformation_type: Type of transformation
            input_rid: Input artifact RID
            output_rid: Output artifact RID
            processor: Processor/agent name
            source_sensor: Original sensor source
            parent_receipt_id: Parent receipt if chained
            metadata: Additional metadata

        Returns:
            Created CAT receipt
        """
        metadata = metadata or {}

        # Generate receipt ID
        timestamp = datetime.now(timezone.utc).isoformat()
        receipt_content = f"{transformation_type}:{input_rid}:{output_rid}:{processor}:{timestamp}"
        receipt_id = hashlib.sha256(receipt_content.encode()).hexdigest()

        # Create receipt object
        receipt = CATReceiptV2(
            receipt_id=receipt_id,
            transformation_type=transformation_type,
            input_rid=input_rid,
            output_rid=output_rid,
            parent_receipt_id=parent_receipt_id,
            processor_name=processor,
            source_sensor=source_sensor or "unknown",
            timestamp=timestamp,
            metadata=metadata,
            content_hash=hashlib.sha256(f"{input_rid}{output_rid}".encode()).hexdigest()
        )

        # Store in PostgreSQL
        await self._store_postgresql(receipt)

        # Store in Jena asynchronously
        try:
            activity_uri, receipt_uri = await self._store_jena(receipt)
            receipt.jena_activity_uri = activity_uri
            receipt.jena_receipt_uri = receipt_uri

            # Update PostgreSQL with Jena URIs
            await self._update_jena_uris(receipt_id, activity_uri, receipt_uri)

        except Exception as e:
            logger.error(f"Failed to store in Jena: {e}")
            # Continue even if Jena fails - PostgreSQL is primary for operations

        logger.info(f"Created CAT receipt: {receipt_id[:16]}... for {transformation_type}")
        return receipt

    async def _store_postgresql(self, receipt: CATReceiptV2):
        """Store receipt in PostgreSQL"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO koi_transformation_receipts (
                    receipt_id, transformation_type, parent_receipt_id,
                    input_rid, output_rid, source_sensor, processor_name,
                    metadata, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (receipt_id) DO NOTHING
            """,
                receipt.receipt_id,
                receipt.transformation_type,
                receipt.parent_receipt_id,
                receipt.input_rid,
                receipt.output_rid,
                receipt.source_sensor,
                receipt.processor_name,
                json.dumps(receipt.metadata),
                datetime.fromisoformat(receipt.timestamp.replace('Z', '+00:00'))
            )

    async def _store_jena(self, receipt: CATReceiptV2) -> tuple[str, str]:
        """Store receipt and provenance in Jena"""

        # First, ensure artifacts exist in Jena
        await self.jena.store_artifact(
            rid=receipt.input_rid,
            artifact_type="artifact",
            metadata={
                "source_sensor": receipt.source_sensor,
                "content_hash": receipt.content_hash
            }
        )

        await self.jena.store_artifact(
            rid=receipt.output_rid,
            artifact_type="artifact",
            metadata={
                "source_sensor": receipt.source_sensor,
                "content_hash": receipt.content_hash
            },
            parent_rid=receipt.input_rid
        )

        # Store transformation with CAT receipt
        activity_uri, receipt_uri = await self.jena.store_transformation(
            transformation_type=receipt.transformation_type,
            input_rid=receipt.input_rid,
            output_rid=receipt.output_rid,
            processor=receipt.processor_name,
            metadata=receipt.metadata
        )

        return activity_uri, receipt_uri

    async def _update_jena_uris(self, receipt_id: str, activity_uri: str, receipt_uri: str):
        """Update PostgreSQL with Jena URIs"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE koi_transformation_receipts
                SET jena_activity_uri = $2, jena_receipt_uri = $3
                WHERE receipt_id = $1
            """, receipt_id, activity_uri, receipt_uri)

    async def get_chain(self, rid: str, source: str = "both") -> List[Dict[str, Any]]:
        """
        Get receipt chain for a RID

        Args:
            rid: Resource identifier
            source: 'postgresql', 'jena', or 'both'

        Returns:
            List of receipts in the chain
        """
        receipts = []

        if source in ["postgresql", "both"]:
            receipts.extend(await self._get_postgresql_chain(rid))

        if source in ["jena", "both"]:
            jena_receipts = await self.jena.get_cat_receipts(rid)
            receipts.extend(jena_receipts)

        # Deduplicate if querying both
        if source == "both":
            seen = set()
            unique_receipts = []
            for r in receipts:
                key = r.get("receipt_id") or r.get("hash")
                if key and key not in seen:
                    seen.add(key)
                    unique_receipts.append(r)
            receipts = unique_receipts

        return receipts

    async def _get_postgresql_chain(self, rid: str) -> List[Dict[str, Any]]:
        """Get receipt chain from PostgreSQL"""
        async with self.pool.acquire() as conn:
            records = await conn.fetch("""
                WITH RECURSIVE chain AS (
                    SELECT * FROM koi_transformation_receipts
                    WHERE input_rid = $1 OR output_rid = $1

                    UNION ALL

                    SELECT r.* FROM koi_transformation_receipts r
                    JOIN chain c ON r.output_rid = c.input_rid
                )
                SELECT * FROM chain
                ORDER BY created_at ASC
            """, rid)

            return [dict(r) for r in records]

    async def get_provenance(self, rid: str) -> Dict[str, Any]:
        """
        Get complete provenance from Jena

        Args:
            rid: Resource identifier

        Returns:
            Complete provenance information
        """
        return await self.jena.query_provenance(rid)

    async def verify_chain(self, rid: str) -> Dict[str, Any]:
        """
        Verify integrity of receipt chain

        Args:
            rid: Resource identifier

        Returns:
            Verification report
        """
        # Get receipts from both sources
        pg_receipts = await self._get_postgresql_chain(rid)
        jena_receipts = await self.jena.get_cat_receipts(rid)

        # Verify consistency
        pg_ids = {r["receipt_id"] for r in pg_receipts}
        jena_hashes = {r["hash"] for r in jena_receipts}

        return {
            "postgresql_count": len(pg_receipts),
            "jena_count": len(jena_receipts),
            "consistent": pg_ids == jena_hashes,
            "missing_in_jena": list(pg_ids - jena_hashes),
            "missing_in_postgresql": list(jena_hashes - pg_ids),
            "verified": len(pg_ids & jena_hashes) == len(pg_ids)
        }

    async def create_extraction_receipt(self,
                                       source_rid: str,
                                       extraction_type: str,
                                       extracted_data: Dict[str, Any],
                                       model: str,
                                       confidence: float = 1.0) -> str:
        """
        Create receipt for entity/relation extraction

        Args:
            source_rid: Source document RID
            extraction_type: Type of extraction
            extracted_data: Extracted entities/relations
            model: Model used
            confidence: Confidence score

        Returns:
            Receipt ID
        """
        # Generate output RID for extracted data
        output_rid = f"extracted:{extraction_type}:{hashlib.sha256(json.dumps(extracted_data).encode()).hexdigest()[:16]}"

        # Create transformation receipt
        receipt = await self.create_receipt(
            transformation_type=f"{extraction_type}_extraction",
            input_rid=source_rid,
            output_rid=output_rid,
            processor=model,
            metadata={
                "extraction_type": extraction_type,
                "confidence": confidence,
                "entity_count": len(extracted_data.get("entities", [])),
                "relation_count": len(extracted_data.get("relations", []))
            }
        )

        # Store extracted data in Jena
        await self.jena.store_extraction(
            source_rid=source_rid,
            extraction_type=extraction_type,
            extracted_data=extracted_data,
            model=model,
            confidence=confidence
        )

        return receipt.receipt_id


async def migrate_existing_receipts(db_url: Optional[str] = None):
    """
    Migrate existing PostgreSQL receipts to Jena
    One-time migration script
    """
    db_url = db_url or DEFAULT_MIGRATION_DB_URL
    chain = CATReceiptChainV2(db_url)
    await chain.initialize()

    async with chain.pool.acquire() as conn:
        receipts = await conn.fetch("""
            SELECT * FROM koi_transformation_receipts
            WHERE jena_receipt_uri IS NULL
            ORDER BY created_at
            LIMIT 100
        """)

        for receipt in receipts:
            try:
                # Create receipt object
                r = CATReceiptV2(
                    receipt_id=receipt["receipt_id"],
                    transformation_type=receipt["transformation_type"],
                    input_rid=receipt["input_rid"],
                    output_rid=receipt["output_rid"],
                    parent_receipt_id=receipt["parent_receipt_id"],
                    processor_name=receipt["processor_name"],
                    source_sensor=receipt["source_sensor"],
                    timestamp=receipt["created_at"].isoformat(),
                    metadata=json.loads(receipt["metadata"] or "{}"),
                    content_hash=""
                )

                # Store in Jena
                activity_uri, receipt_uri = await chain._store_jena(r)

                # Update PostgreSQL
                await chain._update_jena_uris(receipt["receipt_id"], activity_uri, receipt_uri)

                logger.info(f"Migrated receipt {receipt['receipt_id'][:16]}...")

            except Exception as e:
                logger.error(f"Failed to migrate receipt {receipt['receipt_id']}: {e}")

    await chain.close()
    logger.info("Migration complete")


def cli_main(argv=None) -> int:
    """Command-line entry: a dry run unless --apply is given."""
    args = _build_parser().parse_args(argv)
    if _print_plan(args):
        return 0
    asyncio.run(migrate_existing_receipts(args.db_url))
    return 0


if __name__ == "__main__":
    # --apply (the plan was printed and the dry run handled at the top of the file).
    asyncio.run(migrate_existing_receipts(_MAIN_ARGS.db_url))
    sys.exit(0)
