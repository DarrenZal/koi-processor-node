"""
Google Gemini 3-based semantic extraction service for KOI processor.
Uses the new Gemini 3 SDK with thinking levels and disabled safety filters.

FIX-002: Uses shared prompt builder and type normalization for consistency
with OpenAI extractor.
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from google import genai
from google.genai import types

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


logger = logging.getLogger(__name__)


class GeminiExtractor:
    """
    Extracts structured metadata and entities from text using Google Gemini 3.
    Uses new SDK pattern with thinking levels and safety settings.

    FIX-002 Compliant: Uses shared prompt builder and type normalization.
    """

    def __init__(
        self,
        api_key: str = None,
        model: str = None,
    ):
        """
        Initialize GeminiExtractor.

        Args:
            api_key: Google API key (or set GEMINI_API_KEY/GOOGLE_API_KEY env var)
            model: Model name (default: gemini-3-flash-preview, or GEMINI_MODEL env var)
        """
        self.api_key = api_key or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY or GOOGLE_API_KEY required")

        # Configuration from environment variables
        self.model = model or os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
        self.max_content_length = int(os.getenv("GEMINI_EXTRACT_MAX_CONTENT_LENGTH", "3000"))
        self.max_tokens = int(os.getenv("GEMINI_EXTRACT_MAX_TOKENS", "4096"))
        self.disable_safety = os.getenv("GEMINI_DISABLE_SAFETY", "true").lower() == "true"

        # Thinking level: low for fast extraction, high for complex reasoning
        # Options: "low", "medium" (Flash only), "high" (default), "minimal" (Flash only)
        self.thinking_level = os.getenv("GEMINI_THINKING_LEVEL", "low")

        self.logger = logging.getLogger(__name__)
        self.last_usage = {}

        # Initialize Gemini client
        self._init_client()

    def _init_client(self):
        """Initialize Google Gemini 3 client."""
        # New Gemini 3 SDK uses Client pattern
        self.client = genai.Client(api_key=self.api_key)
        self.logger.info(f"[GEMINI] Initialized with model {self.model}, thinking_level={self.thinking_level}, safety_disabled={self.disable_safety}")

    def _get_safety_settings(self) -> Optional[list]:
        """
        Get safety settings for Gemini API.

        Returns:
            Safety settings list to disable filters, or None for defaults
        """
        if not self.disable_safety:
            return None

        # Disable all safety filters for Regen/BioFi content
        # These topics often trigger false positives on financial/agricultural content
        return [
            types.SafetySetting(
                category="HARM_CATEGORY_HARASSMENT",
                threshold="BLOCK_NONE"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_HATE_SPEECH",
                threshold="BLOCK_NONE"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
                threshold="BLOCK_NONE"
            ),
            types.SafetySetting(
                category="HARM_CATEGORY_DANGEROUS_CONTENT",
                threshold="BLOCK_NONE"
            ),
        ]

    def _get_generation_config(self) -> types.GenerateContentConfig:
        """Get generation config with JSON mode and thinking level."""
        # Get system message from shared prompt builder
        system_instruction = get_system_message() if get_system_message else (
            "You are a semantic extraction system that outputs only valid JSON. "
            "Extract entities with their types in UPPERCASE exactly as specified."
        )

        config_kwargs = {
            "max_output_tokens": self.max_tokens,
            "response_mime_type": "application/json",
            "temperature": 1.0,  # Gemini 3 strongly recommends keeping at 1.0
            "system_instruction": system_instruction,
            "thinking_config": types.ThinkingConfig(
                thinking_level=self.thinking_level
            ),
        }

        # Add safety settings if needed
        safety_settings = self._get_safety_settings()
        if safety_settings:
            config_kwargs["safety_settings"] = safety_settings

        return types.GenerateContentConfig(**config_kwargs)

    async def extract_metadata(
        self,
        content: str,
        source_type: str,
        existing_metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Extract semantic metadata from content using Google Gemini 3.

        Args:
            content: Text content to analyze
            source_type: Type of source (discourse, twitter, medium, etc.)
            existing_metadata: Basic metadata already extracted by sensors

        Returns:
            Enhanced metadata with semantic entities and relationships.
            Output contract matches OpenAI extractor.
        """
        if not content or not content.strip():
            return self._build_empty_result(source_type, existing_metadata)

        # FIX-002: Build prompt using shared builder
        prompt = self._build_extraction_prompt(content, source_type, existing_metadata)

        try:
            start_time = time.time()

            # Call Gemini API
            raw_response = await self._call_gemini(prompt)

            elapsed = time.time() - start_time

            if not raw_response:
                self.logger.warning("[GEMINI] Empty response from API")
                return self._build_empty_result(source_type, existing_metadata)

            # Parse JSON response
            extraction = self._extract_json(raw_response)

            # FIX-002: Parse and normalize extraction with type filtering
            result = self._parse_extraction(extraction, source_type)

            # Add latency and token usage
            result["latency_ms"] = elapsed * 1000
            if self.last_usage:
                result["token_usage"] = self.last_usage

            # Merge with existing metadata if provided
            if existing_metadata:
                # Put existing metadata first, then overlay extraction results
                merged = {**existing_metadata}
                for key, value in result.items():
                    merged[key] = value
                return merged

            return result

        except Exception as e:
            self.logger.error(f"[GEMINI] Extraction failed: {type(e).__name__}: {e}")
            return self._build_empty_result(source_type, existing_metadata, error=str(e))

    def _build_extraction_prompt(
        self,
        content: str,
        source_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Build extraction prompt using shared FIX-002 builder.

        Args:
            content: Text content to analyze
            source_type: Type of source
            metadata: Optional existing metadata

        Returns:
            Complete extraction prompt string
        """
        # FIX-002: Use shared prompt builder if available
        if build_extraction_prompt is not None:
            return build_extraction_prompt(
                content=content,
                source_type=source_type,
                metadata=metadata,
                max_content_length=self.max_content_length
            )

        # Fallback: minimal prompt (shouldn't be reached in normal operation)
        content_snippet = content[:self.max_content_length] if len(content) > self.max_content_length else content
        return f"""Extract structured data from this {source_type} content.

CONTENT:
{content_snippet}

Return JSON with entities (PERSON, ORGANIZATION, PROJECT, CONCEPT, TECHNOLOGY, CLAIM, EVIDENCE, QUESTION, LOCATION, EVENT), relationships, and summary.
Return ONLY valid JSON."""

    async def _call_gemini(self, prompt: str) -> str:
        """
        Call Gemini 3 API and return raw response text.

        Args:
            prompt: The extraction prompt

        Returns:
            Raw response text from Gemini
        """
        self.logger.info(f"[GEMINI] Starting API call with model {self.model}")
        self.logger.info(f"[GEMINI] Prompt length: {len(prompt)} characters")

        try:
            # Get generation config
            config = self._get_generation_config()

            # New Gemini 3 SDK uses client.models.generate_content()
            # Wrap synchronous call in thread for async compatibility
            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.model,
                contents=prompt,
                config=config
            )

            # Check for empty/blocked response
            if not response.candidates or not response.candidates[0].content.parts:
                feedback = getattr(response, 'prompt_feedback', None)
                self.logger.error(f"[GEMINI] Empty response. Feedback: {feedback}")
                return ""

            # Extract token usage
            if hasattr(response, 'usage_metadata') and response.usage_metadata:
                usage = response.usage_metadata
                self.last_usage = {
                    'prompt_tokens': getattr(usage, 'prompt_token_count', 0),
                    'completion_tokens': getattr(usage, 'candidates_token_count', 0),
                    'total_tokens': getattr(usage, 'total_token_count', 0)
                }
                self.logger.info(f"[GEMINI] Tokens used - Prompt: {self.last_usage.get('prompt_tokens', 0)}, "
                               f"Completion: {self.last_usage.get('completion_tokens', 0)}, "
                               f"Total: {self.last_usage.get('total_tokens', 0)}")
            else:
                self.last_usage = {}

            # Extract text from response
            return response.text

        except Exception as e:
            self.logger.error(f"[GEMINI] API call failed: {type(e).__name__}: {e}")
            raise

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """
        Robustly extract JSON from response text.

        Handles:
        - Plain JSON
        - Markdown code blocks (```json ... ```)
        - Nested JSON in text

        Args:
            text: Raw response text

        Returns:
            Parsed JSON dict, or empty dict on failure
        """
        if not text:
            return {}

        text = text.strip()

        # 1. Try strict JSON parse first
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 2. Try extracting from markdown code blocks
        try:
            match = re.search(r"```(?:json)?\s*(.*?)\s*```", text, re.DOTALL)
            if match:
                json_str = match.group(1)
                return json.loads(json_str)
        except Exception:
            pass

        # 3. Fallback: Find first { and last }
        try:
            start = text.find('{')
            end = text.rfind('}')
            if start != -1 and end != -1 and end > start:
                json_str = text[start:end + 1]
                return json.loads(json_str)
        except Exception:
            pass

        self.logger.warning(f"[GEMINI] Failed to parse JSON. Preview: {text[:200]}")
        return {}

    def _parse_extraction(
        self,
        extraction: Dict[str, Any],
        source_type: str
    ) -> Dict[str, Any]:
        """
        Parse and validate Gemini extraction with FIX-002 compliance.

        Normalizes entity types to canonical uppercase,
        filters non-LLM-allowed types, and builds output contract
        matching OpenAI extractor.

        Args:
            extraction: Raw parsed JSON from Gemini
            source_type: Type of source

        Returns:
            Normalized metadata dict with wrapper keys
        """
        # Build result with wrapper keys matching OpenAI extractor
        result = {
            "semantic_extraction": True,
            "source_type": source_type,
            "llm_extraction": {
                "model": self.model,
                "provider": "google",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "extracted_entities": [],
            "extracted_relationships": [],
        }

        if not extraction:
            return result

        # Extract metadata with confidence scores
        if "metadata" in extraction:
            llm_metadata = extraction["metadata"]
            result["llm_extracted_metadata"] = {}
            result["llm_metadata_confidence"] = {}

            for field, info in llm_metadata.items():
                if isinstance(info, dict) and "value" in info:
                    result["llm_extracted_metadata"][field] = info["value"]
                    result["llm_metadata_confidence"][field] = info.get("confidence", 0.5)

        # FIX-002: Process entities with type normalization and filtering
        if "entities" in extraction:
            for entity in extraction["entities"]:
                name = entity.get("name", "")
                if isinstance(name, str):
                    name = name.strip()
                else:
                    name = str(name) if name else ""

                if not name:
                    continue

                raw_type = entity.get("type", "")
                normalized_type = normalize_type(raw_type)

                # FIX-002: Drop entities with non-LLM-allowed types
                if not is_llm_allowed_type(normalized_type):
                    self.logger.debug(f"[GEMINI] Dropping entity '{name}' with non-allowed type '{normalized_type}' (raw: '{raw_type}')")
                    continue

                # Build entity dict with canonical type
                entity_dict = {
                    "name": name,
                    "type": normalized_type,  # FIX-002: Canonical uppercase type
                }

                # Preserve confidence if present
                if "confidence" in entity:
                    entity_dict["confidence"] = entity["confidence"]

                # Preserve optional fields
                if "properties" in entity:
                    entity_dict["properties"] = entity["properties"]
                if "metadata" in entity:
                    entity_dict["metadata"] = entity["metadata"]
                if "content" in entity:
                    entity_dict["content"] = entity["content"]

                result["extracted_entities"].append(entity_dict)

        # FIX-002: Process relationships with optional type normalization
        if "relationships" in extraction:
            for rel in extraction["relationships"]:
                subject = rel.get("subject", "")
                predicate = rel.get("predicate", "")
                obj = rel.get("object", "")

                if isinstance(subject, str):
                    subject = subject.strip()
                if isinstance(predicate, str):
                    predicate = predicate.strip()
                if isinstance(obj, str):
                    obj = obj.strip()

                # Skip incomplete relationships
                if not all([subject, predicate, obj]):
                    continue

                rel_dict = {
                    "subject": subject,
                    "predicate": predicate,
                    "object": obj,
                }

                # Preserve confidence if present
                if "confidence" in rel:
                    rel_dict["confidence"] = rel["confidence"]

                # Normalize subject_type and object_type if present
                if "subject_type" in rel:
                    rel_dict["subject_type"] = normalize_type(rel["subject_type"])
                if "object_type" in rel:
                    rel_dict["object_type"] = normalize_type(rel["object_type"])

                result["extracted_relationships"].append(rel_dict)

        # Extract discourse elements
        for field in ["claims", "evidence", "questions", "discourse_type", "summary"]:
            if field in extraction:
                result[field] = extraction[field]

        return result

    def _build_empty_result(
        self,
        source_type: str,
        existing_metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Build empty result with proper wrapper keys.

        Args:
            source_type: Type of source
            existing_metadata: Optional existing metadata to merge
            error: Optional error message

        Returns:
            Empty result dict with proper structure
        """
        result = {
            "semantic_extraction": True,
            "source_type": source_type,
            "llm_extraction": {
                "model": self.model,
                "provider": "google",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "extracted_entities": [],
            "extracted_relationships": [],
        }

        if error:
            result["error"] = error

        if existing_metadata:
            return {**existing_metadata, **result}

        return result


# Example usage
async def main():
    """Example of using the Gemini 3 extractor."""

    extractor = GeminiExtractor()

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
