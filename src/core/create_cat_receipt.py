"""
CAT Receipt creation function for KOI Event Bridge
Tracks complete provenance of data transformations
"""

import hashlib
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import asyncpg
from loguru import logger


async def create_cat_receipt(
    conn: asyncpg.Connection,
    transformation_type: str,
    input_rid: str,
    output_rid: str,
    input_cid: Optional[str] = None,
    output_cid: Optional[str] = None,
    processor_name: str = "KOI Event Bridge v2",
    processor_version: str = "2.0.0",
    chunks_created: int = 0,
    embeddings_created: int = 0,
    entities_extracted: int = 0,
    source_sensor: Optional[str] = None,
    event_type: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    processing_duration_ms: Optional[int] = None
) -> str:
    """
    Create a CAT (Content Addressable Transformation) receipt for provenance tracking

    Args:
        conn: Database connection
        transformation_type: Type of transformation (e.g., "koi_to_memory", "memory_to_embedding")
        input_rid: Input Resource Identifier
        output_rid: Output Resource Identifier
        input_cid: Input Content Identifier (optional)
        output_cid: Output Content Identifier (optional)
        processor_name: Name of the processing component
        processor_version: Version of the processor
        chunks_created: Number of chunks created
        embeddings_created: Number of embeddings created
        entities_extracted: Number of entities extracted
        source_sensor: Source sensor name
        event_type: Event type (NEW, UPDATE, FORGET)
        metadata: Additional metadata as JSON
        processing_duration_ms: Processing time in milliseconds

    Returns:
        receipt_id: The generated receipt ID
    """
    try:
        # Check if this exact transformation already exists (deduplication)
        # BUT only skip if the output memory is still active (not superseded)
        existing = await conn.fetchrow("""
            SELECT r.receipt_id, m.superseded_at
            FROM koi_transformation_receipts r
            LEFT JOIN koi_memories m ON m.rid = r.output_rid
            WHERE r.input_rid = $1 AND r.output_rid = $2 AND r.transformation_type = $3
            ORDER BY r.created_at DESC
            LIMIT 1
        """, input_rid, output_rid, transformation_type)

        if existing:
            # Only skip if the output memory is NOT superseded
            if existing['superseded_at'] is None:
                logger.info(f"✓ DUPLICATE RECEIPT: {transformation_type} {input_rid} → {output_rid} - SKIPPING (existing: {existing['receipt_id'][:8]}...)")
                return existing['receipt_id']
            else:
                logger.info(f"✓ SUPERSEDED RECEIPT: {transformation_type} {input_rid} → {output_rid} - Creating new receipt (old was superseded at {existing['superseded_at']})")

        # Generate receipt ID using SHA-256 of key fields
        receipt_content = f"{transformation_type}:{input_rid}:{output_rid}:{datetime.now(timezone.utc).isoformat()}"
        receipt_id = hashlib.sha256(receipt_content.encode()).hexdigest()

        # Prepare metadata
        if metadata is None:
            metadata = {}

        # Add timestamp to metadata
        metadata['timestamp'] = datetime.now(timezone.utc).isoformat()

        logger.info(f"✓ NEW RECEIPT: {transformation_type} {input_rid[:50]} → {output_rid[:50]} (ID: {receipt_id[:8]}...)")

        # Insert into koi_transformation_receipts table
        await conn.execute("""
            INSERT INTO koi_transformation_receipts (
                receipt_id,
                transformation_type,
                input_rid,
                input_cid,
                output_rid,
                output_cid,
                processor_name,
                processor_version,
                chunks_created,
                embeddings_created,
                entities_extracted,
                source_sensor,
                event_type,
                metadata,
                processing_duration_ms,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (receipt_id) DO NOTHING
        """,
            receipt_id,
            transformation_type,
            input_rid,
            input_cid,
            output_rid,
            output_cid,
            processor_name,
            processor_version,
            chunks_created,
            embeddings_created,
            entities_extracted,
            source_sensor,
            event_type,
            json.dumps(metadata),
            processing_duration_ms,
            datetime.now(timezone.utc)
        )

        logger.info(f"Created CAT receipt {receipt_id} for {transformation_type}: {input_rid} -> {output_rid}")
        return receipt_id

    except Exception as e:
        logger.error(f"Error creating CAT receipt: {e}")
        # Don't fail the main process if receipt creation fails
        return ""


async def create_embedding_receipt(
    conn: asyncpg.Connection,
    memory_id: str,
    rid: str,
    embedding_model: str = "bge-large-en-v1.5",
    embedding_dim: int = 1024,
    source_sensor: Optional[str] = None,
    processing_time_ms: Optional[int] = None
) -> str:
    """
    Create a CAT receipt specifically for embedding generation

    Args:
        conn: Database connection
        memory_id: UUID of the memory
        rid: Resource Identifier of the content
        embedding_model: Model used for embedding
        embedding_dim: Dimension of the embedding
        source_sensor: Source sensor name
        processing_time_ms: Time taken to generate embedding

    Returns:
        receipt_id: The generated receipt ID
    """

    metadata = {
        "memory_id": str(memory_id),
        "embedding_model": embedding_model,
        "embedding_dim": embedding_dim
    }

    return await create_cat_receipt(
        conn=conn,
        transformation_type="memory_to_bge_embedding",
        input_rid=rid,
        output_rid=f"embedding:{rid}:{embedding_model}",
        processor_name=f"BGE Server ({embedding_model})",
        processor_version="1.0.0",
        embeddings_created=1,
        source_sensor=source_sensor,
        metadata=metadata,
        processing_duration_ms=processing_time_ms
    )


async def create_kg_extraction_receipt(
    conn: asyncpg.Connection,
    memory_rid: str,
    extraction_rid: str,
    extraction_type: str,
    entities_extracted: int = 0,
    statements_extracted: int = 0,
    confidence_avg: float = 0.0,
    ontology_version: str = "op-v1.1",
    extractor_version: str = "1.0.0",
    tokens_consumed: int = 0,
    cost_usd: float = 0.0,
    source_url: Optional[str] = None,
    source_sensor: Optional[str] = None,
    processing_time_ms: Optional[int] = None,
    additional_metadata: Optional[Dict[str, Any]] = None
) -> str:
    """
    Create a CAT receipt specifically for Knowledge Graph extraction

    Args:
        conn: Database connection
        memory_rid: RID of the source memory/content
        extraction_rid: RID of the extraction output
        extraction_type: Type of KG extraction (passA, passB, entity_resolution, nanopub_creation, contradiction_detection)
        entities_extracted: Number of entities extracted
        statements_extracted: Number of statements/claims extracted
        confidence_avg: Average confidence score (0.0-1.0)
        ontology_version: Ontology version used (default: op-v1.1)
        extractor_version: Version of the KG extractor
        tokens_consumed: Number of LLM tokens used
        cost_usd: Cost in USD for processing
        source_url: Source URL of the content (for provenance)
        source_sensor: Source sensor name
        processing_time_ms: Time taken for extraction
        additional_metadata: Any additional metadata to include

    Returns:
        receipt_id: The generated receipt ID
    """

    # Build KG-specific metadata
    metadata = {
        "entities_extracted": entities_extracted,
        "statements_extracted": statements_extracted,
        "confidence_avg": confidence_avg,
        "ontology_version": ontology_version,
        "tokens_consumed": tokens_consumed,
        "cost_usd": cost_usd
    }

    # Add source URL if provided (for provenance tracking)
    if source_url:
        metadata["source_url"] = source_url

    # Merge in any additional metadata
    if additional_metadata:
        metadata.update(additional_metadata)

    # Determine transformation type based on extraction type
    transformation_type_map = {
        "passA": "kg_extraction_passA",
        "passB": "kg_extraction_passB",
        "entity_resolution": "kg_entity_resolution",
        "nanopub_creation": "kg_nanopub_creation",
        "contradiction_detection": "kg_contradiction_detection"
    }

    transformation_type = transformation_type_map.get(
        extraction_type,
        f"kg_extraction_{extraction_type}"
    )

    return await create_cat_receipt(
        conn=conn,
        transformation_type=transformation_type,
        input_rid=memory_rid,
        output_rid=extraction_rid,
        processor_name=f"KG Extractor ({extraction_type})",
        processor_version=extractor_version,
        entities_extracted=entities_extracted,
        source_sensor=source_sensor,
        metadata=metadata,
        processing_duration_ms=processing_time_ms
    )