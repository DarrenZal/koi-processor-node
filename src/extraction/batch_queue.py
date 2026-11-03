"""
Batch Processing Queue for OpenAI Extraction
Manages queuing of documents for batch processing to optimize costs
"""

import os
import json
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from pathlib import Path
import asyncpg
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class BatchQueue:
    """
    Manages a queue of documents for batch LLM processing
    Stores queue in PostgreSQL for persistence
    """

    def __init__(self, db_url: str = None):
        self.db_url = db_url or os.getenv(
            'DATABASE_URL',
            'postgresql://postgres:postgres@localhost:5433/eliza'
        )
        self.pool = None

    async def initialize(self):
        """Initialize database connection and tables"""
        self.pool = await asyncpg.create_pool(self.db_url)

        async with self.pool.acquire() as conn:
            # Create batch queue table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS llm_batch_queue (
                    id SERIAL PRIMARY KEY,
                    rid TEXT NOT NULL,
                    content TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    metadata JSONB,
                    status TEXT DEFAULT 'queued',
                    queued_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    processed_at TIMESTAMP WITH TIME ZONE,
                    result JSONB,
                    error TEXT,
                    batch_id TEXT,
                    cost_estimate DECIMAL(10, 6)
                )
            ''')

            # Create indexes
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_batch_queue_status
                ON llm_batch_queue(status)
            ''')
            await conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_batch_queue_batch_id
                ON llm_batch_queue(batch_id)
            ''')

            logger.info("Batch queue table initialized")

    async def add_to_queue(
        self,
        rid: str,
        content: str,
        source_type: str,
        metadata: Dict[str, Any] = None
    ) -> int:
        """Add a document to the batch queue"""

        # Estimate cost (GPT-4o-mini: $0.15/1M input, $0.60/1M output)
        # Assume ~3x expansion for prompt + response
        input_tokens = len(content.split()) * 1.3  # Rough token estimate
        output_tokens = 500  # Estimated response size
        cost_estimate = (input_tokens / 1_000_000 * 0.15) + (output_tokens / 1_000_000 * 0.60)

        async with self.pool.acquire() as conn:
            queue_id = await conn.fetchval('''
                INSERT INTO llm_batch_queue
                (rid, content, source_type, metadata, cost_estimate)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
            ''', rid, content, source_type, json.dumps(metadata or {}), cost_estimate)

            logger.info(f"Added to batch queue: {rid} (ID: {queue_id}, Est. cost: ${cost_estimate:.6f})")
            return queue_id

    async def get_queued_items(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get queued items for batch processing"""

        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT id, rid, content, source_type, metadata, cost_estimate
                FROM llm_batch_queue
                WHERE status = 'queued'
                ORDER BY queued_at
                LIMIT $1
            ''', limit)

            return [dict(row) for row in rows]

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Get statistics about the batch queue"""

        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow('''
                SELECT
                    COUNT(*) FILTER (WHERE status = 'queued') as queued_count,
                    COUNT(*) FILTER (WHERE status = 'processing') as processing_count,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                    SUM(cost_estimate) FILTER (WHERE status = 'queued') as estimated_cost,
                    MIN(queued_at) FILTER (WHERE status = 'queued') as oldest_queued
                FROM llm_batch_queue
            ''')

            return dict(stats)

    async def create_batch(self, item_ids: List[int]) -> str:
        """Create a batch from queued items"""

        batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

        async with self.pool.acquire() as conn:
            await conn.execute('''
                UPDATE llm_batch_queue
                SET status = 'processing',
                    batch_id = $1
                WHERE id = ANY($2)
            ''', batch_id, item_ids)

        logger.info(f"Created batch {batch_id} with {len(item_ids)} items")
        return batch_id

    async def update_batch_results(
        self,
        batch_id: str,
        results: Dict[int, Dict[str, Any]]
    ):
        """Update batch processing results"""

        async with self.pool.acquire() as conn:
            for item_id, result in results.items():
                if 'error' in result:
                    await conn.execute('''
                        UPDATE llm_batch_queue
                        SET status = 'failed',
                            processed_at = NOW(),
                            error = $1
                        WHERE id = $2
                    ''', result['error'], item_id)
                else:
                    await conn.execute('''
                        UPDATE llm_batch_queue
                        SET status = 'completed',
                            processed_at = NOW(),
                            result = $1
                        WHERE id = $2
                    ''', json.dumps(result), item_id)

        logger.info(f"Updated results for batch {batch_id}")

    async def get_completed_results(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get completed results that haven't been integrated yet"""

        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT rid, result, processed_at
                FROM llm_batch_queue
                WHERE status = 'completed'
                AND result IS NOT NULL
                ORDER BY processed_at DESC
                LIMIT $1
            ''', limit)

            return [dict(row) for row in rows]

    async def export_batch_for_api(self, batch_id: str) -> str:
        """Export batch in JSONL format for OpenAI Batch API"""

        async with self.pool.acquire() as conn:
            items = await conn.fetch('''
                SELECT id, rid, content, source_type, metadata
                FROM llm_batch_queue
                WHERE batch_id = $1
            ''', batch_id)

        # Create JSONL file for OpenAI Batch API
        batch_file = Path(f"/tmp/{batch_id}.jsonl")

        with open(batch_file, 'w') as f:
            for item in items:
                # Create request in OpenAI Batch API format
                request = {
                    "custom_id": f"item_{item['id']}",
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": "gpt-4o-mini",
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a semantic extraction system that outputs only valid JSON."
                            },
                            {
                                "role": "user",
                                "content": self._build_prompt(
                                    item['content'],
                                    item['source_type'],
                                    json.loads(item['metadata'])
                                )
                            }
                        ],
                        "temperature": 0.3,
                        "max_tokens": 1000,
                        "response_format": {"type": "json_object"}
                    }
                }
                f.write(json.dumps(request) + '\n')

        logger.info(f"Exported batch {batch_id} to {batch_file}")
        return str(batch_file)

    def _build_prompt(self, content: str, source_type: str, metadata: Dict) -> str:
        """Build extraction prompt (simplified version)"""

        content_snippet = content[:3000] if len(content) > 3000 else content

        return f"""Extract structured data from this {source_type} content.

CONTENT:
{content_snippet}

Extract and return JSON with:
1. Metadata fields with confidence scores (0.0-1.0)
2. Entities (HumanActor, Claim, Evidence, Question)
3. Relationships between entities
4. Discourse type classification

JSON structure:
{{
  "metadata": {{
    "title": {{"value": "...", "confidence": 0.9}},
    "author": {{"value": "...", "confidence": 0.8}},
    "published_date": {{"value": "ISO date", "confidence": 0.7}},
    "tags": {{"value": ["tag1"], "confidence": 0.8}}
  }},
  "entities": [{{"type": "HumanActor", "name": "..."}}],
  "relationships": [{{"subject": "e1", "predicate": "supports", "object": "e2"}}],
  "summary": "one sentence"
}}"""

    async def cleanup_old_completed(self, days: int = 7):
        """Clean up old completed items"""

        async with self.pool.acquire() as conn:
            deleted = await conn.fetchval('''
                DELETE FROM llm_batch_queue
                WHERE status = 'completed'
                AND processed_at < NOW() - INTERVAL '%s days'
                RETURNING COUNT(*)
            ''', days)

        logger.info(f"Cleaned up {deleted} old completed items")


# FastAPI endpoints for batch management
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Batch Queue API")
queue = BatchQueue()


class QueueItem(BaseModel):
    rid: str
    content: str
    source_type: str
    metadata: Dict[str, Any] = {}


@app.on_event("startup")
async def startup():
    await queue.initialize()


@app.post("/queue/add")
async def add_to_queue(item: QueueItem):
    """Add item to batch queue"""
    queue_id = await queue.add_to_queue(
        item.rid,
        item.content,
        item.source_type,
        item.metadata
    )
    return {"queue_id": queue_id, "status": "queued"}


@app.get("/queue/stats")
async def get_queue_stats():
    """Get batch queue statistics"""
    stats = await queue.get_queue_stats()
    return stats


@app.get("/queue/items")
async def get_queued_items(limit: int = 100):
    """Get queued items"""
    items = await queue.get_queued_items(limit)
    return {"items": items, "count": len(items)}


@app.post("/queue/process-batch")
async def process_batch(max_items: int = 100):
    """Create and process a batch"""

    # Get queued items
    items = await queue.get_queued_items(max_items)

    if not items:
        raise HTTPException(status_code=404, detail="No items in queue")

    # Create batch
    item_ids = [item['id'] for item in items]
    batch_id = await queue.create_batch(item_ids)

    # Export for OpenAI Batch API
    batch_file = await queue.export_batch_for_api(batch_id)

    return {
        "batch_id": batch_id,
        "items_count": len(items),
        "batch_file": batch_file,
        "total_cost_estimate": sum(item['cost_estimate'] for item in items)
    }


@app.get("/queue/results")
async def get_results(limit: int = 100):
    """Get completed extraction results"""
    results = await queue.get_completed_results(limit)
    return {"results": results, "count": len(results)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8006)