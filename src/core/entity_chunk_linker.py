"""
Entity-Chunk Linker for Hybrid RAG
Links extracted entities back to their source text chunks.
Enables precise retrieval: Graph → Entity → Chunk → Text Snippet
"""

import asyncio
import asyncpg
import json
import re
import os
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


@dataclass
class EntityChunkLink:
    """A link between an entity and a text chunk."""
    entity_name: str
    entity_type: str
    chunk_rid: str
    chunk_index: int
    document_rid: str
    text_snippet: str
    char_offset: int
    confidence: float


@dataclass
class EntityLocation:
    """Full location info for an entity in the corpus."""
    entity_name: str
    entity_type: str
    document_rid: str
    document_title: str
    occurrences: List[EntityChunkLink]
    total_mentions: int


class EntityChunkLinker:
    """
    Links extracted entities to their source text chunks.

    The 'Hybrid Bridge' that connects:
    - Global Context (Knowledge Graph / Jena)
    - Local Precision (Text Chunks / Postgres)
    """

    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool

    async def link_entity_to_chunks(
        self,
        entity_name: str,
        document_rid: Optional[str] = None,
        context_chars: int = 150
    ) -> List[EntityChunkLink]:
        """
        Find all chunk locations where an entity appears.

        Args:
            entity_name: The entity name to search for
            document_rid: Optional filter to specific document
            context_chars: Characters of context around mention

        Returns:
            List of EntityChunkLink with exact locations
        """
        links = []

        async with self.db_pool.acquire() as conn:
            # First, find extractions containing this entity
            query = """
                SELECT
                    cat_receipt_rid,
                    extracted_content
                FROM koi_adaptive_extractions
                WHERE extracted_content IS NOT NULL
                  AND extracted_content::text ILIKE $1
            """
            params = [f'%{entity_name}%']

            if document_rid:
                query += " AND extracted_content->>'document_rid' = $2"
                params.append(document_rid)

            rows = await conn.fetch(query, *params)

            for row in rows:
                content = row['extracted_content']
                if isinstance(content, str):
                    content = json.loads(content)

                # Check if entity is actually in this extraction
                entities = content.get('entities', [])
                entity_match = None
                for e in entities:
                    if e.get('name', '').lower() == entity_name.lower():
                        entity_match = e
                        break

                if not entity_match:
                    # Try partial match
                    for e in entities:
                        if entity_name.lower() in e.get('name', '').lower():
                            entity_match = e
                            break

                if not entity_match:
                    continue

                doc_rid = content.get('document_rid')
                chunk_rids = content.get('source_chunk_rids', [])

                if not chunk_rids:
                    continue

                # Get chunks and search for entity mentions
                chunks = await conn.fetch("""
                    SELECT
                        chunk_rid,
                        chunk_index,
                        content->>'text' as text
                    FROM koi_memory_chunks
                    WHERE chunk_rid = ANY($1)
                    ORDER BY chunk_index
                """, chunk_rids)

                for chunk in chunks:
                    text = chunk['text'] or ''

                    # Find all occurrences of entity in chunk
                    pattern = re.compile(re.escape(entity_name), re.IGNORECASE)
                    for match in pattern.finditer(text):
                        start = max(0, match.start() - context_chars)
                        end = min(len(text), match.end() + context_chars)
                        snippet = text[start:end]

                        # Add ellipsis if truncated
                        if start > 0:
                            snippet = "..." + snippet
                        if end < len(text):
                            snippet = snippet + "..."

                        links.append(EntityChunkLink(
                            entity_name=entity_match.get('name'),
                            entity_type=entity_match.get('type', 'Entity'),
                            chunk_rid=chunk['chunk_rid'],
                            chunk_index=chunk['chunk_index'],
                            document_rid=doc_rid,
                            text_snippet=snippet,
                            char_offset=match.start(),
                            confidence=entity_match.get('confidence', 0.8)
                        ))

        return links

    async def get_entity_location(
        self,
        entity_name: str,
        context_chars: int = 150
    ) -> Optional[EntityLocation]:
        """
        Get full location info for an entity across all documents.
        """
        links = await self.link_entity_to_chunks(entity_name, context_chars=context_chars)

        if not links:
            return None

        # Get document titles
        doc_rids = list(set(l.document_rid for l in links if l.document_rid))

        async with self.db_pool.acquire() as conn:
            titles = {}
            if doc_rids:
                rows = await conn.fetch("""
                    SELECT DISTINCT document_rid, metadata->>'title' as title
                    FROM koi_memory_chunks
                    WHERE document_rid = ANY($1)
                """, doc_rids)
                titles = {r['document_rid']: r['title'] for r in rows}

        first_link = links[0]
        return EntityLocation(
            entity_name=first_link.entity_name,
            entity_type=first_link.entity_type,
            document_rid=first_link.document_rid,
            document_title=titles.get(first_link.document_rid, 'Unknown'),
            occurrences=links,
            total_mentions=len(links)
        )

    async def build_entity_chunk_index(self, limit: int = 1000) -> Dict[str, List[str]]:
        """
        Build an index mapping entity names to chunk RIDs.

        Returns:
            Dict mapping entity_name -> [chunk_rid, ...]
        """
        index = {}

        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT extracted_content
                FROM koi_adaptive_extractions
                WHERE extracted_content IS NOT NULL
                  AND extracted_content->>'source_chunk_rids' IS NOT NULL
                LIMIT $1
            """, limit)

            for row in rows:
                content = row['extracted_content']
                if isinstance(content, str):
                    content = json.loads(content)

                entities = content.get('entities', [])
                chunk_rids = content.get('source_chunk_rids', [])

                for entity in entities:
                    name = entity.get('name', '').lower()
                    if name:
                        if name not in index:
                            index[name] = []
                        index[name].extend(chunk_rids)

        # Deduplicate
        for name in index:
            index[name] = list(set(index[name]))

        logger.info(f"Built index with {len(index)} entities")
        return index

    async def get_random_entity(self) -> Optional[Dict[str, Any]]:
        """Get a random entity from the extractions for testing."""
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT extracted_content
                FROM koi_adaptive_extractions
                WHERE extracted_content IS NOT NULL
                  AND extracted_content->>'source_chunk_rids' IS NOT NULL
                  AND (extracted_content->'entities')::jsonb IS NOT NULL
                ORDER BY RANDOM()
                LIMIT 1
            """)

            if row:
                content = row['extracted_content']
                if isinstance(content, str):
                    content = json.loads(content)

                entities = content.get('entities', [])
                if entities:
                    import random
                    entity = random.choice(entities)
                    entity['_document_rid'] = content.get('document_rid')
                    return entity

        return None


async def verify_entity_linking():
    """Verification test: Pick random entity and prove chunk linking works."""

    POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@localhost:5433/eliza")
    pool = await asyncpg.create_pool(POSTGRES_URL)

    linker = EntityChunkLinker(pool)

    print("=" * 60)
    print("ENTITY-TO-CHUNK LINKING VERIFICATION TEST")
    print("=" * 60)
    print()

    # Get random entity
    entity = await linker.get_random_entity()
    if not entity:
        print("ERROR: No entities found with chunk links")
        await pool.close()
        return

    entity_name = entity.get('name')
    entity_type = entity.get('type')
    doc_rid = entity.get('_document_rid')

    print(f"RANDOM ENTITY SELECTED:")
    print(f"  Name: {entity_name}")
    print(f"  Type: {entity_type}")
    print(f"  Source Doc: {doc_rid}")
    print()

    # Find chunk links
    print("SEARCHING FOR CHUNK OCCURRENCES...")
    print()

    location = await linker.get_entity_location(entity_name)

    if not location:
        print(f"No chunk occurrences found for '{entity_name}'")
        await pool.close()
        return

    print(f"RESULTS:")
    print(f"  Total mentions found: {location.total_mentions}")
    print(f"  Document: {location.document_title}")
    print()

    print("TEXT SNIPPETS (proving entity appears in source chunks):")
    print("-" * 60)

    for i, link in enumerate(location.occurrences[:5]):  # Show up to 5
        print(f"\n[{i+1}] Chunk {link.chunk_index} (offset {link.char_offset})")
        print(f"    Chunk RID: {link.chunk_rid[:30]}...")
        print(f"    Text: \"{link.text_snippet}\"")

    if location.total_mentions > 5:
        print(f"\n... and {location.total_mentions - 5} more occurrences")

    print()
    print("=" * 60)
    print("VERIFICATION COMPLETE: Entity successfully linked to source chunks!")
    print("=" * 60)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(verify_entity_linking())
