"""Entity resolver with multi-tier waterfall lookup.

FIX-006 Enhancements:
- Per-type semantic thresholds (PERSON: 0.92, ORG: 0.95, etc.)
- Tier 1.x fuzzy string matching using rapidfuzz
- Improved normalization via shared entity_normalizer module

SCOPE: this is NOT dead code and is NOT the resolver for the personal
node (personal_koi). It is the dedup engine for the semantic event-bridge
KG (the `eliza` database), reached via graph_integration.py ->
koi_event_bridge_semantic.py. The personal node's entity resolution lives in
api/personal_ingest_api.py::resolve_entity() instead -- that function has
its own tiers, its own thresholds, and does not import anything from this
module. This file's occurrence_count UPDATE targets the event-bridge schema,
which the personal node's schema does not share, so checking it against
personal_koi alone makes it look unreachable. Tests and scripts import it,
and removing it would silently disable dedup on the event-bridge pipeline.
Keep it; do not merge its thresholds with the personal node's.
"""

import logging
import json
from typing import Optional, Dict, List, Any, Tuple
import os

try:
    import psycopg2
    from psycopg2.extras import Json
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

try:
    from rapidfuzz import fuzz
    from rapidfuzz.distance import JaroWinkler
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False

from .uri_generator import DeterministicURIGenerator
from .entity_normalizer import normalize_entity_name, normalize_for_canonical_lookup, is_single_token_name


# FIX-006: Per-type semantic similarity thresholds
DEFAULT_TYPE_THRESHOLDS = {
    "PERSON": 0.92,         # Lower to catch name variations
    "ORGANIZATION": 0.95,   # Higher to avoid false merges
    "CONCEPT": 0.90,        # Lower for semantic similarity
    "CLAIM": 0.98,          # Very high - claims should be nearly identical
    "PROJECT": 0.93,
    "TECHNOLOGY": 0.93,
    "LOCATION": 0.95,
    "EVENT": 0.94,
    "DEFAULT": 0.95,
}

# FIX-006: Per-type fuzzy string matching thresholds
DEFAULT_FUZZY_THRESHOLDS = {
    "PERSON": 0.93,         # Jaro-Winkler threshold for names (raised from 0.88 to reduce false positives)
    "ORGANIZATION": 0.85,   # Token-based ratio for orgs
    "PROJECT": 0.85,
    "TECHNOLOGY": 0.85,
    "DEFAULT": 0.85,
}


class EntityResolver:
    """
    Multi-tier entity lookup and deduplication.

    FIX-006 Enhanced tiers:
    - Tier 1: Exact Match (Postgres B-Tree, microseconds)
    - Tier 1.5: Canonical Mapping (alias registry lookup)
    - Tier 1.x: Fuzzy String Match (rapidfuzz, type-specific algorithms)
    - Tier 2: Semantic Match (pgvector HNSW, per-type thresholds)
    - Tier 3: Create New (deterministic URI)

    Why this works:
    - Tier 1 handles exact duplicates (fast path)
    - Tier 1.5 handles known aliases from canonical_entities.json
    - Tier 1.x handles string variations (Gregory_Regen -> gregory regen)
    - Tier 2 handles semantic variations ("IBM" = "International Business Machines")
    - Tier 3 ensures new entities get unique, reproducible URIs

    Postgres is the "Source of Truth" for entity identity.
    """

    def __init__(
        self,
        db_config: Dict[str, Any],
        openai_api_key: str = None,
        fuzzy_threshold: float = None,  # Deprecated, use type_thresholds
        embedding_model: str = "text-embedding-ada-002",
        type_thresholds: Dict[str, float] = None,
        fuzzy_string_thresholds: Dict[str, float] = None,
        enable_fuzzy_tier: bool = True,
    ):
        """
        Initialize entity resolver.

        Args:
            db_config: Postgres connection config {host, port, database, user, password}
            openai_api_key: OpenAI API key for embeddings
            fuzzy_threshold: DEPRECATED - use type_thresholds instead
            embedding_model: OpenAI embedding model
            type_thresholds: Per-type semantic similarity thresholds (FIX-006)
            fuzzy_string_thresholds: Per-type fuzzy string thresholds (FIX-006)
            enable_fuzzy_tier: Enable Tier 1.x fuzzy string matching (default: True)
        """
        if not HAS_PSYCOPG2:
            raise ImportError("psycopg2 is required for EntityResolver. Install with: pip install psycopg2-binary")

        self.db_config = db_config
        self.uri_gen = DeterministicURIGenerator()
        self.logger = logging.getLogger(__name__)
        self._canonical_mappings = self._load_canonical_mappings()

        # FIX-006: Per-type semantic thresholds
        self.type_thresholds = {**DEFAULT_TYPE_THRESHOLDS}
        if type_thresholds:
            self.type_thresholds.update(type_thresholds)

        # FIX-006: Per-type fuzzy string thresholds
        self.fuzzy_string_thresholds = {**DEFAULT_FUZZY_THRESHOLDS}
        if fuzzy_string_thresholds:
            self.fuzzy_string_thresholds.update(fuzzy_string_thresholds)

        # Backward compatibility: if old fuzzy_threshold is passed, use it as DEFAULT
        if fuzzy_threshold is not None:
            self.type_thresholds["DEFAULT"] = fuzzy_threshold
            self.logger.warning(
                f"fuzzy_threshold parameter is deprecated. Use type_thresholds instead. "
                f"Setting DEFAULT threshold to {fuzzy_threshold}"
            )

        # FIX-006: Enable/disable fuzzy string tier
        self.enable_fuzzy_tier = enable_fuzzy_tier and HAS_RAPIDFUZZ
        if enable_fuzzy_tier and not HAS_RAPIDFUZZ:
            self.logger.warning("rapidfuzz not installed. Tier 1.x fuzzy matching disabled. Install with: pip install rapidfuzz")

        # OpenAI client for embeddings
        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if HAS_OPENAI and self.openai_api_key:
            self.openai_client = OpenAI(api_key=self.openai_api_key)
        else:
            self.openai_client = None
            self.logger.warning("OpenAI client not available. Tier 2 semantic matching disabled.")

        self.embedding_model = embedding_model

        # Statistics
        self.stats = {
            "tier1_exact_hits": 0,
            "tier1_5_canonical_hits": 0,
            "tier1x_fuzzy_hits": 0,  # FIX-006: New tier
            "tier2_semantic_hits": 0,
            "tier3_new_entities": 0,
            "race_condition_hits": 0,
            "embedding_errors": 0,
            "single_token_skips": 0,  # FIX-006: Track single-token safety skips
        }

    def _get_connection(self):
        """Get a database connection."""
        return psycopg2.connect(**self.db_config)

    def _load_canonical_mappings(self) -> Dict[Tuple[str, str], str]:
        """
        Load canonical entity mappings from data/canonical_entities.json.

        FIX-006: Uses normalize_for_canonical_lookup for consistent normalization.

        Returns:
            Dict mapping (normalized_alias, entity_type) -> canonical_name
        """
        try:
            from pathlib import Path

            canonical_path = Path(__file__).parents[2] / "data" / "canonical_entities.json"
            if not canonical_path.exists():
                self.logger.warning(f"Canonical mappings not found: {canonical_path}")
                return {}

            with open(canonical_path, "r") as f:
                data = json.load(f)

            lookup: Dict[Tuple[str, str], str] = {}
            for section, entities in data.get("entities", {}).items():
                for _, entry in entities.items():
                    canonical_name = entry.get("canonical_name")
                    entity_type = entry.get("entity_type")
                    aliases = entry.get("aliases", [])
                    if not canonical_name or not entity_type:
                        continue

                    entity_type_upper = entity_type.upper()

                    # Map canonical name (with FIX-006 normalization)
                    normalized_canonical = normalize_for_canonical_lookup(canonical_name, entity_type_upper)
                    lookup[(normalized_canonical, entity_type_upper)] = canonical_name

                    # Map aliases (with FIX-006 normalization)
                    for alias in aliases:
                        normalized_alias = normalize_for_canonical_lookup(alias, entity_type_upper)
                        # Don't overwrite existing mappings (first mapping wins)
                        if (normalized_alias, entity_type_upper) not in lookup:
                            lookup[(normalized_alias, entity_type_upper)] = canonical_name

            self.logger.info(f"Loaded {len(lookup)} canonical mappings for Tier 1.5 resolution")
            return lookup
        except Exception as e:
            self.logger.warning(f"Failed to load canonical mappings: {e}")
            return {}

    def _tier1_5_canonical_lookup(self, cursor, entity_text: str, entity_type: str):
        """
        Tier 1.5: Canonical mapping lookup using alias registry.

        FIX-006: Uses normalize_for_canonical_lookup for consistent matching.

        Returns:
            (entity_id, fuseki_uri, canonical_text) if found, else None
        """
        if not self._canonical_mappings:
            return None

        entity_type_upper = entity_type.upper()

        # FIX-006: Use consistent normalization for lookup
        normalized_lookup_key = normalize_for_canonical_lookup(entity_text, entity_type_upper)
        canonical_name = self._canonical_mappings.get((normalized_lookup_key, entity_type_upper))
        if not canonical_name:
            return None

        normalized_canonical = self.uri_gen.normalize_name(canonical_name)
        cursor.execute(
            """
            SELECT id, fuseki_uri, entity_text, occurrence_count
            FROM entity_registry
            WHERE normalized_text = %s AND entity_type = %s
            """,
            (normalized_canonical, entity_type_upper),
        )
        match = cursor.fetchone()
        if not match:
            return None

        entity_id, uri, canonical_text, count = match
        cursor.execute(
            """
            UPDATE entity_registry
            SET occurrence_count = occurrence_count + 1,
                last_seen_at = NOW()
            WHERE fuseki_uri = %s
            """,
            (uri,),
        )
        self.stats["tier1_5_canonical_hits"] += 1
        self.logger.debug(f"Tier 1.5 canonical: '{entity_text}' -> '{canonical_text}'")
        return entity_id, uri, canonical_text

    def _threshold_for_type(self, entity_type: str) -> float:
        """
        FIX-006: Get semantic similarity threshold for entity type.

        Args:
            entity_type: Entity type (PERSON, ORGANIZATION, etc.)

        Returns:
            Threshold value for Tier 2 matching
        """
        return self.type_thresholds.get(
            entity_type.upper(),
            self.type_thresholds.get("DEFAULT", 0.95)
        )

    def _fuzzy_threshold_for_type(self, entity_type: str) -> float:
        """
        FIX-006: Get fuzzy string threshold for entity type.

        Args:
            entity_type: Entity type (PERSON, ORGANIZATION, etc.)

        Returns:
            Threshold value for Tier 1.x matching
        """
        return self.fuzzy_string_thresholds.get(
            entity_type.upper(),
            self.fuzzy_string_thresholds.get("DEFAULT", 0.85)
        )

    def _tier1x_fuzzy_lookup(self, cursor, entity_text: str, entity_type: str) -> Optional[Tuple[int, str, str, float]]:
        """
        FIX-006 Tier 1.x: Fuzzy string matching using rapidfuzz.

        Uses type-specific algorithms:
        - PERSON: Jaro-Winkler similarity (good for names with typos/variations)
        - ORGANIZATION: Token-based ratio (order-insensitive for "Regen Network" vs "Network Regen")

        Performance: Only checks same-type candidates with length within 50% of query.

        Args:
            cursor: Database cursor
            entity_text: Entity name to match
            entity_type: Entity type

        Returns:
            (entity_id, fuseki_uri, canonical_text, score) if found, else None
        """
        if not self.enable_fuzzy_tier or not HAS_RAPIDFUZZ:
            return None

        entity_type_upper = entity_type.upper()
        normalized_query = normalize_entity_name(entity_text, entity_type_upper)

        # Safety: Skip single-token PERSON names (too risky for auto-merge)
        if entity_type_upper == "PERSON" and is_single_token_name(normalized_query):
            # Only allow if explicitly in canonical registry
            if (normalized_query, entity_type_upper) not in self._canonical_mappings:
                self.stats["single_token_skips"] += 1
                self.logger.debug(f"Tier 1.x skip: single-token PERSON '{entity_text}' not in canonical registry")
                return None

        threshold = self._fuzzy_threshold_for_type(entity_type_upper)
        query_len = len(normalized_query)

        # Prefilter: same type, length within 50% of query (cheap DB filter)
        min_len = max(1, int(query_len * 0.5))
        max_len = int(query_len * 1.5) + 5  # +5 for short names

        cursor.execute(
            """
            SELECT id, fuseki_uri, entity_text, normalized_text
            FROM entity_registry
            WHERE entity_type = %s
              AND LENGTH(normalized_text) BETWEEN %s AND %s
            ORDER BY occurrence_count DESC
            LIMIT 100
            """,
            (entity_type_upper, min_len, max_len),
        )
        candidates = cursor.fetchall()

        if not candidates:
            return None

        best_match = None
        best_score = 0.0

        for entity_id, uri, canonical_text, normalized_text in candidates:
            # Apply type-specific similarity algorithm
            if entity_type_upper == "PERSON":
                # Jaro-Winkler is good for name variations
                score = JaroWinkler.normalized_similarity(normalized_query, normalized_text)
            else:
                # Token-based ratio is order-insensitive (good for orgs/projects)
                score = fuzz.token_sort_ratio(normalized_query, normalized_text) / 100.0

            if score > best_score and score >= threshold:
                best_score = score
                best_match = (entity_id, uri, canonical_text, score)

        if best_match:
            entity_id, uri, canonical_text, score = best_match

            # Update occurrence count
            cursor.execute(
                """
                UPDATE entity_registry
                SET occurrence_count = occurrence_count + 1,
                    last_seen_at = NOW()
                WHERE fuseki_uri = %s
                """,
                (uri,),
            )

            self.stats["tier1x_fuzzy_hits"] += 1
            self.logger.debug(
                f"Tier 1.x fuzzy: '{entity_text}' -> '{canonical_text}' "
                f"(score: {score:.3f}, threshold: {threshold})"
            )
            return best_match

        return None

    def get_or_create_entity(
        self,
        entity_text: str,
        entity_type: str,
        metadata: Dict = None
    ) -> Dict[str, Any]:
        """
        Resolve entity using three-tier waterfall.

        Args:
            entity_text: Entity name
            entity_type: Entity type (PERSON, ORGANIZATION, etc.)
            metadata: Optional additional metadata

        Returns:
            {
                "uri": "https://...",
                "matched": True/False,
                "match_method": "tier1_exact" | "tier2_semantic" | "tier3_new",
                "match_score": 1.0 (exact) or 0.0-1.0 (similarity),
                "entity_text": "canonical name from registry"
            }
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Normalize for consistent matching
        normalized = self.uri_gen.normalize_name(entity_text)
        entity_type_upper = entity_type.upper()

        try:
            # -------------------------------------------------------------------
            # TIER 1: EXACT MATCH (fastest)
            # -------------------------------------------------------------------
            cursor.execute("""
                SELECT id, fuseki_uri, entity_text, occurrence_count
                FROM entity_registry
                WHERE normalized_text = %s AND entity_type = %s
            """, (normalized, entity_type_upper))

            match = cursor.fetchone()
            if match:
                entity_id, uri, canonical_text, count = match

                # Update occurrence count
                cursor.execute("""
                    UPDATE entity_registry
                    SET occurrence_count = occurrence_count + 1,
                        last_seen_at = NOW()
                    WHERE fuseki_uri = %s
                """, (uri,))
                conn.commit()

                self.stats["tier1_exact_hits"] += 1
                self.logger.debug(f"Tier 1 hit: '{entity_text}' -> '{canonical_text}'")

                return {
                    "uri": uri,
                    "entity_id": entity_id,
                    "matched": True,
                    "match_method": "tier1_exact",
                    "match_score": 1.0,
                    "entity_text": canonical_text
                }

            # -------------------------------------------------------------------
            # TIER 1.5: CANONICAL MAPPING (deterministic alias resolution)
            # -------------------------------------------------------------------
            canonical_match = self._tier1_5_canonical_lookup(cursor, entity_text, entity_type_upper)
            if canonical_match:
                entity_id, uri, canonical_text = canonical_match
                conn.commit()
                return {
                    "uri": uri,
                    "entity_id": entity_id,
                    "matched": True,
                    "match_method": "tier1_5_canonical",
                    "match_score": 1.0,
                    "entity_text": canonical_text
                }

            # -------------------------------------------------------------------
            # FIX-006 TIER 1.x: FUZZY STRING MATCH (rapidfuzz)
            # -------------------------------------------------------------------
            fuzzy_match = self._tier1x_fuzzy_lookup(cursor, entity_text, entity_type_upper)
            if fuzzy_match:
                entity_id, uri, canonical_text, score = fuzzy_match
                conn.commit()
                return {
                    "uri": uri,
                    "entity_id": entity_id,
                    "matched": True,
                    "match_method": "tier1x_fuzzy",
                    "match_score": float(score),
                    "entity_text": canonical_text
                }

            # -------------------------------------------------------------------
            # TIER 2: SEMANTIC MATCH (smart) - FIX-006: per-type thresholds
            # -------------------------------------------------------------------
            semantic_threshold = self._threshold_for_type(entity_type_upper)

            if self.openai_client:
                try:
                    embedding = self._generate_embedding(entity_text)

                    cursor.execute("""
                        SELECT id, fuseki_uri, entity_text,
                               1 - (embedding <=> %s::vector) AS similarity
                        FROM entity_registry
                        WHERE 1 - (embedding <=> %s::vector) > %s
                          AND entity_type = %s
                        ORDER BY similarity DESC
                        LIMIT 1
                    """, (embedding, embedding, semantic_threshold, entity_type_upper))

                    match = cursor.fetchone()
                    if match:
                        entity_id, uri, canonical_text, score = match

                        # Update occurrence count
                        cursor.execute("""
                            UPDATE entity_registry
                            SET occurrence_count = occurrence_count + 1,
                                last_seen_at = NOW()
                            WHERE fuseki_uri = %s
                        """, (uri,))
                        conn.commit()

                        self.stats["tier2_semantic_hits"] += 1
                        self.logger.debug(
                            f"Tier 2 hit: '{entity_text}' -> '{canonical_text}' "
                            f"(similarity: {score:.3f})"
                        )

                        return {
                            "uri": uri,
                            "entity_id": entity_id,
                            "matched": True,
                            "match_method": "tier2_semantic",
                            "match_score": float(score),
                            "entity_text": canonical_text
                        }
                except Exception as e:
                    self.stats["embedding_errors"] += 1
                    self.logger.warning(f"Embedding generation failed: {e}")
                    # Fall through to Tier 3
                    embedding = None
            else:
                embedding = None

            # -------------------------------------------------------------------
            # TIER 3: CREATE NEW ENTITY
            # -------------------------------------------------------------------
            new_uri = self.uri_gen.generate_uri(entity_text, entity_type_upper)

            # Generate embedding if we don't have one
            if embedding is None and self.openai_client:
                try:
                    embedding = self._generate_embedding(entity_text)
                except Exception as e:
                    self.logger.error(f"Cannot create entity without embedding: {e}")
                    # Generate a placeholder embedding (zeros) - not ideal but allows operation
                    embedding = [0.0] * 1536

            if embedding is None:
                # No OpenAI client - use zeros placeholder
                embedding = [0.0] * 1536

            try:
                cursor.execute("""
                    INSERT INTO entity_registry
                    (fuseki_uri, entity_text, entity_type, normalized_text, embedding, metadata)
                    VALUES (%s, %s, %s, %s, %s::vector, %s)
                    ON CONFLICT (normalized_text, entity_type) DO UPDATE
                    SET occurrence_count = entity_registry.occurrence_count + 1,
                        last_seen_at = NOW()
                    RETURNING id, fuseki_uri, (xmax = 0) AS inserted
                """, (
                    new_uri,
                    entity_text,
                    entity_type_upper,
                    normalized,
                    embedding,
                    Json(metadata or {})
                ))

                result = cursor.fetchone()
                conn.commit()

                if result:
                    entity_id, final_uri, was_inserted = result

                    if was_inserted:
                        self.stats["tier3_new_entities"] += 1
                        self.logger.debug(f"Tier 3 new: '{entity_text}' -> {new_uri}")

                        return {
                            "uri": final_uri,
                            "entity_id": entity_id,
                            "matched": False,
                            "match_method": "tier3_new",
                            "match_score": 1.0,
                            "entity_text": entity_text
                        }
                    else:
                        # Race condition! Another thread just inserted this entity
                        self.stats["race_condition_hits"] += 1
                        self.logger.debug(f"Race condition resolved: '{entity_text}'")

                        # Get the actual entity text from the existing record
                        cursor.execute("""
                            SELECT entity_text FROM entity_registry
                            WHERE normalized_text = %s AND entity_type = %s
                        """, (normalized, entity_type_upper))
                        existing = cursor.fetchone()
                        canonical_text = existing[0] if existing else entity_text

                        return {
                            "uri": final_uri,
                            "entity_id": entity_id,
                            "matched": True,
                            "match_method": "tier1_exact",  # Effectively an exact match
                            "match_score": 1.0,
                            "entity_text": canonical_text
                        }

            except psycopg2.IntegrityError as e:
                # Fallback: If anything goes wrong, try exact match one more time
                conn.rollback()
                self.logger.debug(f"IntegrityError, falling back to exact match: {e}")

                cursor.execute("""
                    SELECT id, fuseki_uri, entity_text
                    FROM entity_registry
                    WHERE normalized_text = %s AND entity_type = %s
                """, (normalized, entity_type_upper))

                fallback_match = cursor.fetchone()
                if fallback_match:
                    entity_id, uri, canonical_text = fallback_match
                    self.stats["race_condition_hits"] += 1
                    return {
                        "uri": uri,
                        "entity_id": entity_id,
                        "matched": True,
                        "match_method": "tier1_exact",
                        "match_score": 1.0,
                        "entity_text": canonical_text
                    }
                else:
                    # Re-raise if truly unexpected error
                    raise

            # Should not reach here - fallback with entity_id=None
            return {
                "uri": new_uri,
                "entity_id": None,
                "matched": False,
                "match_method": "tier3_new",
                "match_score": 1.0,
                "entity_text": entity_text
            }

        finally:
            cursor.close()
            conn.close()

    def _generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for entity text using OpenAI.

        CRITICAL: This method embeds ONLY the normalized entity name.

        DO NOT embed:
        - Surrounding context
        - Full sentence where entity appeared
        - Entity description from source document

        The registry represents the ideal entity, not a specific mention.

        Args:
            text: Entity name (will be normalized before embedding)

        Returns:
            1536-dimensional embedding vector
        """
        if not self.openai_client:
            raise RuntimeError("OpenAI client not configured")

        # Normalize entity name before embedding
        normalized = self.uri_gen.normalize_name(text)

        response = self.openai_client.embeddings.create(
            model=self.embedding_model,
            input=normalized  # Embed normalized name ONLY
        )
        return response.data[0].embedding

    def get_stats(self) -> Dict[str, Any]:
        """Get lookup statistics."""
        total = (
            self.stats["tier1_exact_hits"] +
            self.stats["tier1_5_canonical_hits"] +
            self.stats.get("tier1x_fuzzy_hits", 0) +  # FIX-006
            self.stats["tier2_semantic_hits"] +
            self.stats["tier3_new_entities"]
        )

        if total == 0:
            return {
                **self.stats,
                "total_lookups": 0,
                "tier1_hit_rate": 0.0,
                "tier1_5_hit_rate": 0.0,
                "tier1x_hit_rate": 0.0,  # FIX-006
                "tier2_hit_rate": 0.0,
                "tier3_new_rate": 0.0,
                "type_thresholds": self.type_thresholds,  # FIX-006
                "fuzzy_string_thresholds": self.fuzzy_string_thresholds,  # FIX-006
            }

        return {
            **self.stats,
            "total_lookups": total,
            "tier1_hit_rate": round(self.stats["tier1_exact_hits"] / total, 4),
            "tier1_5_hit_rate": round(self.stats["tier1_5_canonical_hits"] / total, 4),
            "tier1x_hit_rate": round(self.stats.get("tier1x_fuzzy_hits", 0) / total, 4),  # FIX-006
            "tier2_hit_rate": round(self.stats["tier2_semantic_hits"] / total, 4),
            "tier3_new_rate": round(self.stats["tier3_new_entities"] / total, 4),
            "type_thresholds": self.type_thresholds,  # FIX-006
            "fuzzy_string_thresholds": self.fuzzy_string_thresholds,  # FIX-006
        }

    def reset_stats(self):
        """Reset statistics counters."""
        self.stats = {k: 0 for k in self.stats}

    def get_entity_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        """
        Get entity details by URI.

        Args:
            uri: Entity URI

        Returns:
            Entity details or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT fuseki_uri, entity_text, entity_type, normalized_text,
                       first_seen_at, last_seen_at, occurrence_count, metadata
                FROM entity_registry
                WHERE fuseki_uri = %s
            """, (uri,))

            result = cursor.fetchone()
            if result:
                return {
                    "uri": result[0],
                    "entity_text": result[1],
                    "entity_type": result[2],
                    "normalized_text": result[3],
                    "first_seen_at": result[4].isoformat() if result[4] else None,
                    "last_seen_at": result[5].isoformat() if result[5] else None,
                    "occurrence_count": result[6],
                    "metadata": result[7]
                }
            return None

        finally:
            cursor.close()
            conn.close()

    def search_entities(
        self,
        query: str,
        entity_type: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search entities by text (prefix match).

        Args:
            query: Search query
            entity_type: Optional type filter
            limit: Max results

        Returns:
            List of matching entities
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            normalized_query = self.uri_gen.normalize_name(query)

            if entity_type:
                cursor.execute("""
                    SELECT fuseki_uri, entity_text, entity_type, occurrence_count
                    FROM entity_registry
                    WHERE normalized_text LIKE %s
                      AND entity_type = %s
                    ORDER BY occurrence_count DESC
                    LIMIT %s
                """, (normalized_query + '%', entity_type.upper(), limit))
            else:
                cursor.execute("""
                    SELECT fuseki_uri, entity_text, entity_type, occurrence_count
                    FROM entity_registry
                    WHERE normalized_text LIKE %s
                    ORDER BY occurrence_count DESC
                    LIMIT %s
                """, (normalized_query + '%', limit))

            results = []
            for row in cursor.fetchall():
                results.append({
                    "uri": row[0],
                    "entity_text": row[1],
                    "entity_type": row[2],
                    "occurrence_count": row[3]
                })
            return results

        finally:
            cursor.close()
            conn.close()
