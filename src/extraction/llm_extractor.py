"""
LLM-based semantic extraction service for KOI processor
Uses Mistral 7B via Ollama for ontology-driven entity and relationship extraction

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

# Week 13: Import predicate guard for relationship validation
try:
    from extraction.predicate_guard import filter_relationships as apply_predicate_guard
except ImportError:
    try:
        from src.extraction.predicate_guard import filter_relationships as apply_predicate_guard
    except ImportError:
        # Fallback: no predicate guard
        apply_predicate_guard = None

# Import ontology utilities
try:
    import rdflib
    from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS
except ImportError:
    print("Warning: rdflib not installed. Install with: pip install rdflib")
    rdflib = None


class OntologyLLMExtractor:
    """
    Extracts structured metadata and entities from text using LLM and ontologies
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        model: str = "mistral:7b",
        ontology_dir: str = "/opt/projects/koi-research/ontologies"
    ):
        self.ollama_url = ollama_url
        self.model = model
        self.ontology_dir = Path(ontology_dir)
        self.logger = logging.getLogger(__name__)

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
        Extract semantic metadata from content using LLM and ontologies

        Args:
            content: Text content to analyze
            source_type: Type of source (discourse, twitter, medium, etc.)
            existing_metadata: Basic metadata already extracted by sensors

        Returns:
            Enhanced metadata with semantic entities and relationships
        """

        # Get source-specific ontology
        source_ontology = self.source_ontologies.get(source_type)

        # Build extraction prompt
        prompt = self._build_extraction_prompt(content, source_type, existing_metadata)

        try:
            # Call Ollama API
            extraction = await self._call_ollama(prompt)

            # Parse and validate extraction
            metadata = self._parse_extraction(extraction, source_type)

            # Merge with existing metadata
            if existing_metadata:
                metadata = {**existing_metadata, **metadata}

            # Add extraction provenance
            metadata["llm_extraction"] = {
                "model": self.model,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source_ontology": source_type
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
        """Build LLM prompt for extraction based on ontology.

        FIX-002: Uses shared prompt builder for consistency across extractors.
        """
        # FIX-002: Use shared prompt builder if available
        if build_extraction_prompt is not None:
            return build_extraction_prompt(
                content=content,
                source_type=source_type,
                metadata=metadata,
                max_content_length=1500  # Ollama/Mistral has smaller context, use 1500 chars
            )

        # Fallback: minimal inline prompt (shouldn't be reached in normal operation)
        content_snippet = content[:1500] if len(content) > 1500 else content
        return f"""Extract structured data from this {source_type} content.

CONTENT:
{content_snippet}

Return JSON with entities (PERSON, ORGANIZATION, PROJECT, CONCEPT, TECHNOLOGY, CLAIM, EVIDENCE, QUESTION, LOCATION, EVENT), relationships, and summary.
Return ONLY valid JSON."""

    async def _call_ollama(self, prompt: str) -> Dict[str, Any]:
        """Call Ollama API for LLM inference"""

        # Debug logging
        self.logger.info(f"[OLLAMA] Starting API call at {self.ollama_url}")
        self.logger.info(f"[OLLAMA] Model: {self.model}")
        self.logger.info(f"[OLLAMA] Prompt length: {len(prompt)} characters")
        self.logger.debug(f"[OLLAMA] Full prompt: {prompt[:200]}...")

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                request_data = {
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    # REMOVED "format": "json" - causes Mistral to hang!
                    "options": {
                        "temperature": 0.3,  # Lower temp for structured output
                        "top_p": 0.9,
                        "seed": 42,  # For reproducibility
                        "num_predict": 256  # Reduced for faster response
                    }
                }

                self.logger.info(f"[OLLAMA] Request prepared with num_predict={request_data['options']['num_predict']}")
                self.logger.info(f"[OLLAMA] Sending POST request to {self.ollama_url}/api/generate")

                import time
                start_time = time.time()

                response = await client.post(
                    f"{self.ollama_url}/api/generate",
                    json=request_data,
                    timeout=30.0
                )

                elapsed = time.time() - start_time
                self.logger.info(f"[OLLAMA] Response received in {elapsed:.2f} seconds")
                response.raise_for_status()

                result = response.json()
                self.logger.info(f"[OLLAMA] Raw response keys: {list(result.keys())}")
                self.logger.info(f"[OLLAMA] Response 'done': {result.get('done', 'missing')}")

                # Parse the response text as JSON
                try:
                    response_text = result.get("response", "{}")
                    self.logger.info(f"[OLLAMA] Response text length: {len(response_text)} chars")
                    self.logger.debug(f"[OLLAMA] Response text (first 500): {response_text[:500]}")

                    extraction = json.loads(response_text)
                    self.logger.info(f"[OLLAMA] JSON parsed successfully!")
                    self.logger.info(f"[OLLAMA] Extracted keys: {list(extraction.keys())}")
                    self.logger.info(f"[OLLAMA] Entities found: {len(extraction.get('entities', []))}")
                    return extraction
                except json.JSONDecodeError as e:
                    self.logger.error(f"[OLLAMA] JSON decode error: {e}")
                    self.logger.error(f"[OLLAMA] Failed text was: {response_text[:100]}...")
                    # Try to extract JSON from response
                    text = result.get("response", "")
                    start = text.find("{")
                    end = text.rfind("}") + 1
                    if start >= 0 and end > start:
                        try:
                            extracted = json.loads(text[start:end])
                            self.logger.info(f"[OLLAMA] Recovered JSON from malformed response")
                            return extracted
                        except Exception as recover_err:
                            self.logger.error(f"[OLLAMA] Recovery failed: {recover_err}")
                    return {}

            except httpx.TimeoutException as timeout_err:
                self.logger.error(f"[OLLAMA] TIMEOUT after 30 seconds!")
                self.logger.error(f"[OLLAMA] Timeout details: {str(timeout_err)}")
                self.logger.error(f"[OLLAMA] This usually means Ollama is stuck processing")
                self.logger.error(f"[OLLAMA] Check with: ps aux | grep ollama")
                return {}
            except httpx.HTTPStatusError as http_err:
                self.logger.error(f"[OLLAMA] HTTP error: {http_err.response.status_code}")
                self.logger.error(f"[OLLAMA] Response: {http_err.response.text[:200]}")
                return {}
            except Exception as e:
                self.logger.error(f"[OLLAMA] Unexpected error: {type(e).__name__}: {str(e)}")
                self.logger.error(f"[OLLAMA] Failed prompt was: {prompt[:200]}...")
                import traceback
                self.logger.error(f"[OLLAMA] Traceback: {traceback.format_exc()}")
                return {}

    def _parse_extraction(self, extraction: Dict[str, Any], source_type: str) -> Dict[str, Any]:
        """Parse and validate LLM extraction with confidence scores.

        FIX-002: Normalizes entity types to canonical uppercase,
        PRESERVES confidence scores (was being dropped), and filters non-LLM-allowed types.
        """

        metadata = {
            "semantic_extraction": extraction,
            "source_type": source_type
        }

        # Extract metadata with confidence scores
        if "metadata" in extraction:
            llm_metadata = extraction["metadata"]
            # Store both the extracted values and confidence scores
            metadata["llm_extracted_metadata"] = {}
            metadata["llm_metadata_confidence"] = {}

            for field, info in llm_metadata.items():
                if isinstance(info, dict) and "value" in info:
                    metadata["llm_extracted_metadata"][field] = info["value"]
                    metadata["llm_metadata_confidence"][field] = info.get("confidence", 0.5)
                    # Also store reasoning if needed for debugging
                    if "reasoning" in info:
                        metadata.setdefault("llm_metadata_reasoning", {})[field] = info["reasoning"]

        # FIX-002: Extract entities with type normalization and filtering
        entities = extraction.get("entities", [])
        if entities:
            normalized_entities = []
            for e in entities:
                raw_type = e.get("type", "")
                normalized_type = normalize_type(raw_type)

                # FIX-002: Drop entities with non-LLM-allowed types
                if not is_llm_allowed_type(normalized_type):
                    self.logger.debug(f"[OLLAMA] Dropping entity '{e.get('name', '')}' with non-allowed type '{normalized_type}' (raw: '{raw_type}')")
                    continue

                # Build entity dict with canonical type
                entity_dict = {
                    "name": e.get("name", ""),
                    "type": normalized_type,  # FIX-002: Canonical uppercase type
                }

                # FIX-002: PRESERVE confidence (this was the bug - confidence was being dropped)
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
        relationships = extraction.get("relationships", [])
        if relationships:
            normalized_relationships = []
            for r in relationships:
                rel_dict = {
                    "subject": r.get("subject", ""),
                    "predicate": r.get("predicate", ""),
                    "object": r.get("object", ""),
                }

                # FIX-002: Preserve confidence if present
                if "confidence" in r:
                    rel_dict["confidence"] = r["confidence"]

                # Normalize subject_type and object_type if present
                if "subject_type" in r:
                    rel_dict["subject_type"] = normalize_type(r["subject_type"])
                if "object_type" in r:
                    rel_dict["object_type"] = normalize_type(r["object_type"])

                normalized_relationships.append(rel_dict)

            # Week 13: Apply predicate guard to validate/normalize predicates
            # Week 16 (FIX-015): Added type validation to prevent operates→CONCEPT etc.
            if apply_predicate_guard is not None:
                strict_mode = os.getenv("PREDICATE_GUARD_STRICT", "false").lower() == "true"
                validate_types = os.getenv("PREDICATE_GUARD_VALIDATE_TYPES", "false").lower() == "true"
                strict_types = os.getenv("PREDICATE_GUARD_STRICT_TYPES", "false").lower() == "true"
                normalized_relationships = apply_predicate_guard(
                    normalized_relationships,
                    strict=strict_mode,
                    validate_types=validate_types,
                    strict_types=strict_types
                )

            metadata["extracted_relationships"] = normalized_relationships

        # Extract discourse elements (including questions now)
        if extraction.get("claims"):
            metadata["claims"] = extraction["claims"]
        if extraction.get("evidence"):
            metadata["evidence"] = extraction["evidence"]
        if extraction.get("questions"):
            metadata["questions"] = extraction["questions"]

        # Extract essence alignment
        if extraction.get("essence_alignment"):
            metadata["essence_alignment"] = extraction["essence_alignment"]

        # Add discourse type
        if extraction.get("discourse_type"):
            metadata["discourse_type"] = extraction["discourse_type"]

        # Add summary
        if extraction.get("summary"):
            metadata["semantic_summary"] = extraction["summary"]

        return metadata

    async def extract_batch(
        self,
        documents: List[Dict[str, Any]],
        batch_size: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Extract metadata from multiple documents in batches

        Args:
            documents: List of documents with content and metadata
            batch_size: Number of documents to process concurrently

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
    """Example of using the LLM extractor"""

    extractor = OntologyLLMExtractor()

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
