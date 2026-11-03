"""
OpenAI GPT-4o-mini based semantic extraction service for KOI processor
Cost-effective extraction using OpenAI API with batch processing support

FIX-002: Uses shared prompt builder and type normalization
"""

import asyncio
import json
import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import httpx
from pathlib import Path
import hashlib

# FIX-002: Import shared prompt builder and type normalization
try:
    from extraction.prompt_builder import build_extraction_prompt, get_system_message
    from core.entity_types import normalize_type, LLM_ALLOWED_TYPES, is_llm_allowed_type
except ImportError:
    try:
        from src.extraction.prompt_builder import build_extraction_prompt, get_system_message
        from src.core.entity_types import normalize_type, LLM_ALLOWED_TYPES, is_llm_allowed_type
    except ImportError:
        # Fallback for standalone testing
        build_extraction_prompt = None
        get_system_message = None
        normalize_type = lambda x: x.upper() if x else "ENTITY"
        LLM_ALLOWED_TYPES = {"PERSON", "ORGANIZATION", "PROJECT", "CONCEPT", "TECHNOLOGY", "CLAIM", "EVIDENCE", "QUESTION", "LOCATION", "EVENT"}
        is_llm_allowed_type = lambda x: x.upper() in LLM_ALLOWED_TYPES

# Import ontology utilities
try:
    import rdflib
    from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS
except ImportError:
    print("Warning: rdflib not installed. Install with: pip install rdflib")
    rdflib = None


class OpenAIExtractor:
    """
    Extracts structured metadata and entities from text using OpenAI GPT-4o-mini
    Supports both immediate and batch processing for cost optimization
    """

    def __init__(
        self,
        api_key: str = None,
        model: str = "gpt-4o-mini",
        use_batch_api: bool = False,
        ontology_dir: str = "/opt/projects/koi-research/ontologies"
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = os.getenv("OPENAI_EXTRACT_MODEL", model)
        self.use_batch_api = use_batch_api
        self.ontology_dir = Path(ontology_dir)
        self.logger = logging.getLogger(__name__)

        # OpenAI API endpoints
        self.base_url = "https://api.openai.com/v1"
        self.chat_endpoint = f"{self.base_url}/chat/completions"
        self.batch_endpoint = f"{self.base_url}/batches"

        # Load ontologies
        self.unified_ontology = self._load_ontology("regen-unified-ontology.ttl")
        self.source_ontologies = self._load_source_ontologies()

        # Define namespaces
        self.REGEN = Namespace("https://regen.network/ontology#")
        self.DISCOURSE = Namespace("https://regen.network/ontology/discourse#")
        self.TWITTER = Namespace("https://regen.network/ontology/twitter#")
        self.MEDIUM = Namespace("https://regen.network/ontology/medium#")
        self.GITHUB = Namespace("https://regen.network/ontology/github#")

    def _load_ontology(self, filename: str) -> Optional[Any]:
        """Load an ontology file into RDF graph"""
        if not rdflib:
            return None

        try:
            g = Graph()
            ontology_path = self.ontology_dir / filename
            if ontology_path.exists():
                g.parse(str(ontology_path), format="turtle")
                self.logger.info(f"Loaded ontology: {filename}")
                return g
        except Exception as e:
            self.logger.error(f"Error loading ontology {filename}: {e}")
        return None

    def _load_source_ontologies(self) -> Dict[str, Any]:
        """Load all source-specific ontologies"""
        ontologies = {}
        source_dir = self.ontology_dir / "source-specific"

        if source_dir.exists():
            for file in source_dir.glob("*.ttl"):
                source_name = file.stem.replace("-ontology", "")
                ontologies[source_name] = self._load_ontology(f"source-specific/{file.name}")

        return ontologies

    async def extract_metadata(
        self,
        content: str,
        source_type: str,
        existing_metadata: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Extract semantic metadata from content using OpenAI GPT-4o-mini

        Args:
            content: Text content to analyze
            source_type: Type of source (discourse, twitter, medium, etc.)
            existing_metadata: Basic metadata already extracted by sensors

        Returns:
            Enhanced metadata with semantic entities and relationships
        """

        # Build extraction prompt
        prompt = self._build_extraction_prompt(content, source_type, existing_metadata)

        try:
            if self.use_batch_api:
                # Queue for batch processing
                extraction = await self._queue_for_batch(prompt)
            else:
                # Immediate processing
                extraction = await self._call_openai(prompt)

            # Parse and validate extraction
            metadata = self._parse_extraction(extraction, source_type)

            # Merge with existing metadata
            if existing_metadata:
                metadata = {**existing_metadata, **metadata}

            # Add extraction provenance
            metadata["llm_extraction"] = {
                "model": self.model,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source_ontology": source_type,
                "batch_mode": self.use_batch_api
            }

            return metadata

        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            return existing_metadata or {}

    def _build_extraction_prompt(
        self,
        content: str,
        source_type: str,
        metadata: Dict[str, Any] = None
    ) -> str:
        """Build ontology-based extraction prompt for GPT-4o-mini.

        FIX-002: Uses shared prompt builder for consistency across extractors.
        """
        # FIX-002: Use shared prompt builder if available
        if build_extraction_prompt is not None:
            return build_extraction_prompt(
                content=content,
                source_type=source_type,
                metadata=metadata,
                max_content_length=3000  # OpenAI has large context, use 3000 chars
            )

        # Fallback: original inline prompt (shouldn't be reached in normal operation)
        content_snippet = content[:3000] if len(content) > 3000 else content
        return f"""Extract structured data from this {source_type} content.

CONTENT:
{content_snippet}

Return JSON with entities (PERSON, ORGANIZATION, PROJECT, CONCEPT, TECHNOLOGY, CLAIM, EVIDENCE, QUESTION, LOCATION, EVENT), relationships, and summary.
Return ONLY valid JSON."""

    async def _call_openai(self, prompt: str) -> Dict[str, Any]:
        """Call OpenAI API for immediate processing.

        FIX-002: max_tokens is now configurable via OPENAI_EXTRACT_MAX_TOKENS env var.
        """

        self.logger.info(f"[OPENAI] Starting API call with model {self.model}")
        self.logger.info(f"[OPENAI] Prompt length: {len(prompt)} characters")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # FIX-002: Make max_tokens configurable to prevent truncation issues
        max_tokens = int(os.getenv("OPENAI_EXTRACT_MAX_TOKENS", "4096"))

        # FIX-002: Use shared system message if available
        system_content = get_system_message() if get_system_message else "You are a semantic extraction system that outputs only valid JSON."

        request_data = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": system_content
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.3,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"}  # Ensures JSON response
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                import time
                start_time = time.time()

                self.logger.info(f"[OPENAI] Sending request to {self.chat_endpoint}")
                response = await client.post(
                    self.chat_endpoint,
                    headers=headers,
                    json=request_data
                )

                elapsed = time.time() - start_time
                self.logger.info(f"[OPENAI] Response received in {elapsed:.2f} seconds")

                if response.status_code == 200:
                    result = response.json()
                    content = result["choices"][0]["message"]["content"]

                    # Parse JSON response
                    extraction = json.loads(content)

                    # Log usage for cost tracking
                    usage = result.get("usage", {})
                    self.logger.info(f"[OPENAI] Tokens used - Prompt: {usage.get('prompt_tokens', 0)}, "
                                   f"Completion: {usage.get('completion_tokens', 0)}, "
                                   f"Total: {usage.get('total_tokens', 0)}")

                    # Estimate cost (GPT-4o-mini pricing: $0.15/1M input, $0.60/1M output)
                    input_cost = (usage.get('prompt_tokens', 0) / 1_000_000) * 0.15
                    output_cost = (usage.get('completion_tokens', 0) / 1_000_000) * 0.60
                    total_cost = input_cost + output_cost
                    self.logger.info(f"[OPENAI] Estimated cost: ${total_cost:.6f}")

                    return extraction
                else:
                    self.logger.error(f"[OPENAI] API error: {response.status_code}")
                    self.logger.error(f"[OPENAI] Response: {response.text}")
                    return {}

            except json.JSONDecodeError as e:
                self.logger.error(f"[OPENAI] JSON decode error: {e}")
                return {}
            except Exception as e:
                self.logger.error(f"[OPENAI] Unexpected error: {type(e).__name__}: {str(e)}")
                return {}

    async def _queue_for_batch(self, prompt: str) -> Dict[str, Any]:
        """Queue request for batch processing (for cost optimization)"""

        # This would typically write to a JSONL file for batch processing
        # and return a placeholder response
        self.logger.info("[OPENAI] Queuing for batch processing")

        # For now, return empty dict - implement full batch later
        return {
            "batch_queued": True,
            "message": "Queued for batch processing"
        }

    def _parse_extraction(self, extraction: Dict[str, Any], source_type: str) -> Dict[str, Any]:
        """Parse and validate OpenAI extraction with confidence scores.

        FIX-002: Normalizes entity types to canonical uppercase,
        preserves confidence scores, and drops non-LLM-allowed types.
        """

        metadata = {
            "semantic_extraction": extraction,
            "source_type": source_type
        }

        # Extract metadata with confidence scores
        if "metadata" in extraction:
            llm_metadata = extraction["metadata"]
            metadata["llm_extracted_metadata"] = {}
            metadata["llm_metadata_confidence"] = {}

            for field, info in llm_metadata.items():
                if isinstance(info, dict) and "value" in info:
                    metadata["llm_extracted_metadata"][field] = info["value"]
                    metadata["llm_metadata_confidence"][field] = info.get("confidence", 0.5)

        # FIX-002: Extract entities with type normalization and filtering
        if "entities" in extraction:
            normalized_entities = []
            for e in extraction["entities"]:
                raw_type = e.get("type", "")
                normalized_type = normalize_type(raw_type)

                # FIX-002: Drop entities with non-LLM-allowed types
                if not is_llm_allowed_type(normalized_type):
                    self.logger.debug(f"[OPENAI] Dropping entity '{e.get('name', '')}' with non-allowed type '{normalized_type}' (raw: '{raw_type}')")
                    continue

                # Build entity dict with canonical type and preserved confidence
                entity_dict = {
                    "name": e.get("name", ""),
                    "type": normalized_type,  # FIX-002: Canonical uppercase type
                }

                # FIX-002: Preserve confidence if present
                if "confidence" in e:
                    entity_dict["confidence"] = e["confidence"]

                # Preserve properties/metadata if present
                if "properties" in e:
                    entity_dict["properties"] = e["properties"]
                if "metadata" in e:
                    entity_dict["metadata"] = e["metadata"]
                if "content" in e:
                    entity_dict["content"] = e["content"]

                normalized_entities.append(entity_dict)

            metadata["extracted_entities"] = normalized_entities

        # FIX-002: Extract relationships with optional type normalization
        if "relationships" in extraction:
            normalized_relationships = []
            for r in extraction["relationships"]:
                rel_dict = {
                    "subject": r.get("subject", ""),
                    "predicate": r.get("predicate", ""),
                    "object": r.get("object", ""),
                }

                # Preserve confidence if present
                if "confidence" in r:
                    rel_dict["confidence"] = r["confidence"]

                # Normalize subject_type and object_type if present
                if "subject_type" in r:
                    rel_dict["subject_type"] = normalize_type(r["subject_type"])
                if "object_type" in r:
                    rel_dict["object_type"] = normalize_type(r["object_type"])

                normalized_relationships.append(rel_dict)

            metadata["extracted_relationships"] = normalized_relationships

        # Extract discourse elements
        for field in ["claims", "evidence", "questions", "discourse_type", "summary"]:
            if field in extraction:
                metadata[field] = extraction[field]

        return metadata

    async def extract_batch(
        self,
        documents: List[Dict[str, Any]],
        batch_size: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Extract metadata from multiple documents in batches

        Args:
            documents: List of documents with content and metadata
            batch_size: Number of documents to process per batch

        Returns:
            List of documents with enhanced metadata
        """

        enhanced_docs = []

        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]

            # Process batch concurrently
            tasks = [
                self.extract_metadata(
                    doc.get("content", ""),
                    doc.get("source_type", "unknown"),
                    doc.get("metadata", {})
                )
                for doc in batch
            ]

            results = await asyncio.gather(*tasks)

            # Merge results back into documents
            for doc, metadata in zip(batch, results):
                doc["metadata"] = metadata
                enhanced_docs.append(doc)

            self.logger.info(f"Processed batch {i//batch_size + 1}: {len(batch)} documents")

            # Add small delay between batches to avoid rate limits
            if i + batch_size < len(documents):
                await asyncio.sleep(1)

        return enhanced_docs

    def generate_cat_receipt(
        self,
        original_rid: str,
        extraction_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate CAT receipt for LLM extraction transformation

        Args:
            original_rid: RID of original content
            extraction_metadata: Metadata from extraction

        Returns:
            CAT receipt documenting the transformation
        """

        # Create transformation hash
        transformation_data = {
            "original_rid": original_rid,
            "model": self.model,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "extraction": extraction_metadata
        }

        transformation_hash = hashlib.sha256(
            json.dumps(transformation_data, sort_keys=True).encode()
        ).hexdigest()

        receipt = {
            "rid": f"orn:cat:extraction:{transformation_hash[:16]}",
            "type": "llm_extraction",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "parent_rid": original_rid,
            "transformation": {
                "model": self.model,
                "ontology": extraction_metadata.get("source_type"),
                "entities_extracted": len(extraction_metadata.get("extracted_entities", [])),
                "relationships_extracted": len(extraction_metadata.get("extracted_relationships", [])),
            },
            "hash": transformation_hash
        }

        return receipt


# Example usage
async def main():
    """Example of using the OpenAI extractor"""

    extractor = OpenAIExtractor()

    # Example discourse content
    content = """
    I've been thinking about the carbon credit methodology we're using.
    The current approach seems to undervalue biodiversity co-benefits.
    We should consider adopting the Verra VM0042 standard which better
    captures ecosystem services beyond just carbon sequestration.

    @alice mentioned in the last meeting that their project saw a 30%
    increase in credit value after switching methodologies. This could
    be significant for our farmers.
    """

    metadata = await extractor.extract_metadata(
        content,
        "discourse",
        {"author": "bob", "post_id": "123", "topic": "Methodology Discussion"}
    )

    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
