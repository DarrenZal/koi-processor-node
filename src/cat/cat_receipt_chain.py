"""
CAT (Content Authentication Technology) Receipt Chain
Tracks all transformations and provenance in the KOI pipeline

Run as a script it is a demo: with --apply it creates the cat_receipts table in the
database POSTGRES_URL names and inserts three example receipts (no cleanup). With no
arguments or --dry-run it only prints what it would do.
"""

import json
import os
import hashlib
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from pathlib import Path
import asyncpg
from dataclasses import dataclass, asdict

def _postgres_dsn() -> Dict[str, Any]:
    """Connection settings from the environment; there is no built-in default."""
    url = os.environ.get("POSTGRES_URL")
    if not url:
        raise RuntimeError("POSTGRES_URL is not set and no db_config was passed")
    return {"dsn": url}


@dataclass
class CATReceipt:
    """
    Individual CAT receipt for a transformation
    """
    rid: str  # Receipt ID (orn:cat:...)
    type: str  # Transformation type
    timestamp: str  # ISO format timestamp
    parent_rid: Optional[str]  # Previous receipt in chain
    content_cid: Optional[str]  # Content identifier
    transformation: Dict[str, Any]  # Details of the transformation
    metadata: Dict[str, Any]  # Additional metadata
    hash: str  # Hash of the receipt for integrity

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class CATReceiptChain:
    """
    Manages CAT receipt chains for content transformations
    """

    def __init__(self, db_config: Dict[str, Any] = None):
        self.logger = logging.getLogger(__name__)
        self.db_config = db_config or _postgres_dsn()
        self.db_pool = None

    async def initialize(self):
        """Initialize database connection pool"""
        try:
            self.db_pool = await asyncpg.create_pool(**self.db_config)
            await self._ensure_tables()
            self.logger.info("CAT receipt chain initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize CAT chain: {e}")

    async def _ensure_tables(self):
        """Ensure CAT receipt tables exist"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS cat_receipts (
                    rid TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    parent_rid TEXT,
                    content_cid TEXT,
                    transformation JSONB NOT NULL,
                    metadata JSONB DEFAULT '{}',
                    hash TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    FOREIGN KEY (parent_rid) REFERENCES cat_receipts(rid)
                )
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cat_parent_rid ON cat_receipts(parent_rid);
                CREATE INDEX IF NOT EXISTS idx_cat_content_cid ON cat_receipts(content_cid);
                CREATE INDEX IF NOT EXISTS idx_cat_type ON cat_receipts(type);
                CREATE INDEX IF NOT EXISTS idx_cat_timestamp ON cat_receipts(timestamp);
            """)

    def generate_rid(self, transformation_type: str, content: str) -> str:
        """Generate a unique RID for a CAT receipt"""
        timestamp = datetime.now(timezone.utc).isoformat()
        hash_input = f"{transformation_type}:{content}:{timestamp}"
        hash_value = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
        return f"orn:cat:{transformation_type}:{hash_value}"

    def calculate_hash(self, receipt_data: Dict[str, Any]) -> str:
        """Calculate hash of receipt data for integrity"""
        # Remove hash field if present
        data = {k: v for k, v in receipt_data.items() if k != 'hash'}
        json_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()

    async def create_receipt(
        self,
        transformation_type: str,
        parent_rid: Optional[str] = None,
        content_cid: Optional[str] = None,
        transformation_details: Dict[str, Any] = None,
        metadata: Dict[str, Any] = None
    ) -> CATReceipt:
        """
        Create a new CAT receipt for a transformation

        Args:
            transformation_type: Type of transformation (e.g., 'extraction', 'chunking')
            parent_rid: RID of parent receipt in chain
            content_cid: Content identifier
            transformation_details: Details about the transformation
            metadata: Additional metadata

        Returns:
            Created CAT receipt
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        rid = self.generate_rid(transformation_type, content_cid or timestamp)

        receipt_data = {
            "rid": rid,
            "type": transformation_type,
            "timestamp": timestamp,
            "parent_rid": parent_rid,
            "content_cid": content_cid,
            "transformation": transformation_details or {},
            "metadata": metadata or {}
        }

        # Calculate hash
        receipt_data["hash"] = self.calculate_hash(receipt_data)

        receipt = CATReceipt(**receipt_data)

        # Store in database
        await self.store_receipt(receipt)

        return receipt

    async def store_receipt(self, receipt: CATReceipt):
        """Store CAT receipt in database"""
        if not self.db_pool:
            await self.initialize()

        try:
            async with self.db_pool.acquire() as conn:
                # Convert timestamp string to datetime object
                timestamp_dt = datetime.fromisoformat(receipt.timestamp.replace('Z', '+00:00')) if isinstance(receipt.timestamp, str) else receipt.timestamp

                await conn.execute("""
                    INSERT INTO cat_receipts
                    (rid, type, timestamp, parent_rid, content_cid, transformation, metadata, hash)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (rid) DO NOTHING
                """,
                    receipt.rid,
                    receipt.type,
                    timestamp_dt,
                    receipt.parent_rid,
                    receipt.content_cid,
                    json.dumps(receipt.transformation),
                    json.dumps(receipt.metadata),
                    receipt.hash
                )
                self.logger.debug(f"Stored CAT receipt: {receipt.rid}")
        except Exception as e:
            self.logger.error(f"Failed to store CAT receipt: {e}")

    async def get_receipt(self, rid: str) -> Optional[CATReceipt]:
        """Retrieve a CAT receipt by RID"""
        if not self.db_pool:
            await self.initialize()

        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT * FROM cat_receipts WHERE rid = $1
                """, rid)

                if row:
                    return CATReceipt(
                        rid=row['rid'],
                        type=row['type'],
                        timestamp=row['timestamp'].isoformat(),
                        parent_rid=row['parent_rid'],
                        content_cid=row['content_cid'],
                        transformation=json.loads(row['transformation']),
                        metadata=json.loads(row['metadata']),
                        hash=row['hash']
                    )
        except Exception as e:
            self.logger.error(f"Failed to retrieve CAT receipt: {e}")

        return None

    async def get_chain(self, rid: str) -> List[CATReceipt]:
        """Get complete receipt chain from a given RID"""
        chain = []
        current_rid = rid

        while current_rid:
            receipt = await self.get_receipt(current_rid)
            if receipt:
                chain.append(receipt)
                current_rid = receipt.parent_rid
            else:
                break

        # Return in chronological order (oldest first)
        return list(reversed(chain))

    async def verify_chain(self, rid: str) -> Dict[str, Any]:
        """Verify integrity of a receipt chain"""
        chain = await self.get_chain(rid)
        verification = {
            "valid": True,
            "chain_length": len(chain),
            "receipts": [],
            "errors": []
        }

        for i, receipt in enumerate(chain):
            # Verify hash
            calculated_hash = self.calculate_hash(receipt.to_dict())
            hash_valid = calculated_hash == receipt.hash

            # Verify parent link
            parent_valid = True
            if i > 0:
                parent_valid = receipt.parent_rid == chain[i-1].rid

            receipt_verification = {
                "rid": receipt.rid,
                "hash_valid": hash_valid,
                "parent_valid": parent_valid
            }

            if not hash_valid:
                verification["valid"] = False
                verification["errors"].append(f"Invalid hash for {receipt.rid}")

            if not parent_valid:
                verification["valid"] = False
                verification["errors"].append(f"Invalid parent link for {receipt.rid}")

            verification["receipts"].append(receipt_verification)

        return verification

    # Transformation-specific receipt creators

    async def create_sensor_receipt(
        self,
        sensor_name: str,
        source_url: str,
        content_cid: str,
        document_count: int,
        metadata: Dict[str, Any] = None
    ) -> CATReceipt:
        """Create receipt for sensor data collection"""
        return await self.create_receipt(
            transformation_type="sensor_collection",
            content_cid=content_cid,
            transformation_details={
                "sensor": sensor_name,
                "source_url": source_url,
                "document_count": document_count,
                "extraction_time": datetime.now(timezone.utc).isoformat()
            },
            metadata=metadata
        )

    async def create_chunking_receipt(
        self,
        parent_rid: str,
        content_cid: str,
        chunk_count: int,
        chunk_size: int,
        overlap: int,
        chunking_strategy: str = "default",
        source_type: str = None
    ) -> CATReceipt:
        """Create receipt for intelligent content chunking"""
        return await self.create_receipt(
            transformation_type="chunking",
            parent_rid=parent_rid,
            content_cid=content_cid,
            transformation_details={
                "chunk_count": chunk_count,
                "chunk_size": chunk_size,
                "overlap": overlap,
                "method": "intelligent_context_aware",
                "strategy": chunking_strategy,
                "source_type": source_type
            }
        )

    async def create_extraction_receipt(
        self,
        parent_rid: str,
        content_cid: str,
        model: str,
        ontology: str,
        entities_count: int,
        relationships_count: int,
        metadata_resolution: Dict[str, Any] = None
    ) -> CATReceipt:
        """Create receipt for LLM extraction with metadata resolution"""
        transformation_details = {
            "model": model,
            "ontology": ontology,
            "entities_extracted": entities_count,
            "relationships_extracted": relationships_count,
            "extraction_time": datetime.now(timezone.utc).isoformat()
        }

        # Add metadata resolution details if provided
        if metadata_resolution:
            transformation_details["metadata_resolution"] = {
                "conflicts_resolved": len(metadata_resolution.get('conflicts', [])),
                "confidence_scores": metadata_resolution.get('confidence_scores', {}),
                "resolution_methods": metadata_resolution.get('resolution_method', {})
            }

        return await self.create_receipt(
            transformation_type="llm_extraction",
            parent_rid=parent_rid,
            content_cid=content_cid,
            transformation_details=transformation_details
        )

    async def create_embedding_receipt(
        self,
        parent_rid: str,
        content_cid: str,
        model: str,
        dimension: int
    ) -> CATReceipt:
        """Create receipt for embedding generation"""
        return await self.create_receipt(
            transformation_type="embedding",
            parent_rid=parent_rid,
            content_cid=content_cid,
            transformation_details={
                "model": model,
                "dimension": dimension,
                "generation_time": datetime.now(timezone.utc).isoformat()
            }
        )

    async def create_graph_receipt(
        self,
        parent_rid: str,
        content_cid: str,
        triples_added: int,
        store_type: str
    ) -> CATReceipt:
        """Create receipt for knowledge graph integration"""
        return await self.create_receipt(
            transformation_type="graph_integration",
            parent_rid=parent_rid,
            content_cid=content_cid,
            transformation_details={
                "triples_added": triples_added,
                "store_type": store_type,
                "integration_time": datetime.now(timezone.utc).isoformat()
            }
        )

    async def get_provenance_report(self, rid: str) -> Dict[str, Any]:
        """Generate complete provenance report for content"""
        chain = await self.get_chain(rid)
        verification = await self.verify_chain(rid)

        report = {
            "final_rid": rid,
            "chain_valid": verification["valid"],
            "chain_length": len(chain),
            "transformations": [],
            "timeline": [],
            "sources": set(),
            "models": set()
        }

        for receipt in chain:
            # Track transformations
            report["transformations"].append({
                "type": receipt.type,
                "rid": receipt.rid,
                "timestamp": receipt.timestamp,
                "details": receipt.transformation
            })

            # Build timeline
            report["timeline"].append({
                "timestamp": receipt.timestamp,
                "event": f"{receipt.type} transformation",
                "rid": receipt.rid
            })

            # Track sources
            if receipt.type == "sensor_collection":
                if "source_url" in receipt.transformation:
                    report["sources"].add(receipt.transformation["source_url"])

            # Track models used
            if "model" in receipt.transformation:
                report["models"].add(receipt.transformation["model"])

        # Convert sets to lists for JSON serialization
        report["sources"] = list(report["sources"])
        report["models"] = list(report["models"])

        return report

    async def cleanup_old_receipts(self, days: int = 30):
        """Clean up old receipts"""
        if not self.db_pool:
            await self.initialize()

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.execute("""
                    DELETE FROM cat_receipts
                    WHERE created_at < NOW() - INTERVAL '%s days'
                    RETURNING rid
                """, days)

                deleted_count = len(result)
                self.logger.info(f"Cleaned up {deleted_count} old CAT receipts")
                return deleted_count
        except Exception as e:
            self.logger.error(f"Failed to cleanup old receipts: {e}")
            return 0


# Example usage
async def main():
    """Example CAT receipt chain usage"""

    chain = CATReceiptChain()
    await chain.initialize()

    # Create sensor receipt
    sensor_receipt = await chain.create_sensor_receipt(
        sensor_name="discourse",
        source_url="https://forum.regen.network",
        content_cid="QmXxx123",
        document_count=10,
        metadata={"topic": "governance"}
    )
    print(f"Sensor receipt: {sensor_receipt.rid}")

    # Create chunking receipt
    chunk_receipt = await chain.create_chunking_receipt(
        parent_rid=sensor_receipt.rid,
        content_cid="QmYyy456",
        chunk_count=5,
        chunk_size=1000,
        overlap=200
    )
    print(f"Chunking receipt: {chunk_receipt.rid}")

    # Create extraction receipt
    extraction_receipt = await chain.create_extraction_receipt(
        parent_rid=chunk_receipt.rid,
        content_cid="QmZzz789",
        model="mistral:7b",
        ontology="discourse",
        entities_count=12,
        relationships_count=8
    )
    print(f"Extraction receipt: {extraction_receipt.rid}")

    # Get provenance report
    report = await chain.get_provenance_report(extraction_receipt.rid)
    print(f"\nProvenance Report:")
    print(json.dumps(report, indent=2))

    # Verify chain
    verification = await chain.verify_chain(extraction_receipt.rid)
    print(f"\nChain verification: {'VALID' if verification['valid'] else 'INVALID'}")


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
        description="CAT receipt chain demo: create cat_receipts and insert three example "
                    "receipts into the database POSTGRES_URL names. Default is a dry run.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Connect, CREATE TABLE IF NOT EXISTS cat_receipts and insert the examples")
    args = parser.parse_args(argv)

    print(f"Target: {_masked_dsn(os.environ.get('POSTGRES_URL'))}")
    print("Steps: CREATE TABLE IF NOT EXISTS cat_receipts (and its indexes), insert 3 example "
          "receipts (sensor, chunking, extraction) that are not removed afterwards, then read "
          "the chain back")
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was written. Re-run with --apply to do this.")
        return 0
    asyncio.run(main())
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(cli_main())