"""
Metadata Resolution System
Resolves conflicts between sensor and LLM extracted metadata using confidence scores
"""

import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
import dateutil.parser
from difflib import SequenceMatcher


class MetadataResolver:
    """
    Resolves metadata conflicts between sensor and LLM extraction
    using confidence-weighted resolution strategies
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def resolve_metadata(
        self,
        sensor_metadata: Dict[str, Any],
        llm_metadata: Dict[str, Any],
        llm_confidence: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Resolve metadata from sensor and LLM extraction

        Args:
            sensor_metadata: Metadata extracted by sensor
            llm_metadata: Metadata extracted by LLM
            llm_confidence: Confidence scores for LLM metadata

        Returns:
            Resolved metadata with tracking of sources and conflicts
        """
        resolved = {
            # Core resolved fields
            "title": None,
            "author": None,
            "published_date": None,
            "organization": None,
            "url": None,
            "tags": [],

            # Tracking metadata
            "resolution_method": {},
            "confidence_scores": {},
            "conflicts": [],
            "sources": {
                "sensor": sensor_metadata,
                "llm": llm_metadata
            }
        }

        # Resolve each field with appropriate strategy
        resolved["title"], resolved["confidence_scores"]["title"] = self._resolve_title(
            sensor_metadata, llm_metadata, llm_confidence
        )

        resolved["author"], resolved["confidence_scores"]["author"] = self._resolve_author(
            sensor_metadata, llm_metadata, llm_confidence
        )

        resolved["published_date"], resolved["confidence_scores"]["published_date"] = self._resolve_date(
            sensor_metadata, llm_metadata, llm_confidence
        )

        resolved["organization"], resolved["confidence_scores"]["organization"] = self._resolve_organization(
            sensor_metadata, llm_metadata, llm_confidence
        )

        resolved["url"], resolved["confidence_scores"]["url"] = self._resolve_url(
            sensor_metadata, llm_metadata, llm_confidence
        )

        resolved["tags"] = self._resolve_tags(
            sensor_metadata, llm_metadata, llm_confidence
        )

        # Add any additional fields from either source
        self._merge_additional_fields(resolved, sensor_metadata, llm_metadata)

        return resolved

    def _resolve_title(
        self,
        sensor_meta: Dict[str, Any],
        llm_meta: Dict[str, Any],
        llm_conf: Dict[str, float]
    ) -> Tuple[Optional[str], float]:
        """Resolve title with confidence"""
        sensor_title = sensor_meta.get("title")
        llm_title = llm_meta.get("title")
        llm_title_conf = llm_conf.get("title", 0.5)

        # Sensor confidence based on source
        sensor_conf = self._get_sensor_confidence(sensor_meta, "title")

        if not sensor_title and not llm_title:
            return None, 0.0

        if not sensor_title:
            return llm_title, llm_title_conf

        if not llm_title:
            return sensor_title, sensor_conf

        # Both exist - check similarity
        similarity = self._string_similarity(sensor_title, llm_title)

        if similarity > 0.8:
            # Very similar - prefer longer/more complete
            if len(llm_title) > len(sensor_title):
                return llm_title, max(sensor_conf, llm_title_conf)
            return sensor_title, max(sensor_conf, llm_title_conf)

        # Different titles - use confidence
        if llm_title_conf > sensor_conf + 0.2:
            return llm_title, llm_title_conf
        return sensor_title, sensor_conf

    def _resolve_author(
        self,
        sensor_meta: Dict[str, Any],
        llm_meta: Dict[str, Any],
        llm_conf: Dict[str, float]
    ) -> Tuple[Optional[str], float]:
        """Resolve author with confidence"""
        sensor_author = sensor_meta.get("author")
        llm_author = llm_meta.get("author")
        llm_author_conf = llm_conf.get("author", 0.5)

        sensor_conf = self._get_sensor_confidence(sensor_meta, "author")

        if not sensor_author and not llm_author:
            return None, 0.0

        if not sensor_author:
            return llm_author, llm_author_conf

        if not llm_author:
            return sensor_author, sensor_conf

        # Check if names refer to same person
        if self._names_match(sensor_author, llm_author):
            # Prefer more complete name
            if len(llm_author) > len(sensor_author):
                return llm_author, max(sensor_conf, llm_author_conf)
            return sensor_author, max(sensor_conf, llm_author_conf)

        # Different authors - trust LLM for context understanding
        if llm_author_conf > 0.7:
            return llm_author, llm_author_conf
        return sensor_author, sensor_conf

    def _resolve_date(
        self,
        sensor_meta: Dict[str, Any],
        llm_meta: Dict[str, Any],
        llm_conf: Dict[str, float]
    ) -> Tuple[Optional[datetime], float]:
        """Resolve publication date with confidence"""
        sensor_date = self._parse_date(sensor_meta.get("published_at") or sensor_meta.get("created_at"))
        llm_date = self._parse_date(llm_meta.get("published_date"))
        llm_date_conf = llm_conf.get("published_date", 0.5)

        sensor_conf = sensor_meta.get("published_confidence", 0.7)

        if not sensor_date and not llm_date:
            return None, 0.0

        if not sensor_date:
            return llm_date, llm_date_conf

        if not llm_date:
            return sensor_date, sensor_conf

        # Check if dates are close
        if sensor_date and llm_date:
            diff = abs((sensor_date - llm_date).total_seconds())
            if diff < 86400:  # Within 24 hours
                # Dates agree - high confidence
                return sensor_date, min(1.0, max(sensor_conf, llm_date_conf) * 1.1)

        # Dates differ significantly - prefer sensor for API data
        if sensor_conf > 0.8:
            return sensor_date, sensor_conf
        return llm_date, llm_date_conf

    def _resolve_organization(
        self,
        sensor_meta: Dict[str, Any],
        llm_meta: Dict[str, Any],
        llm_conf: Dict[str, float]
    ) -> Tuple[Optional[str], float]:
        """Resolve organization with confidence"""
        sensor_org = sensor_meta.get("organization") or sensor_meta.get("forum")
        llm_org = llm_meta.get("organization")
        llm_org_conf = llm_conf.get("organization", 0.5)

        if not sensor_org and not llm_org:
            return None, 0.0

        if not sensor_org:
            return llm_org, llm_org_conf

        if not llm_org:
            return sensor_org, 0.8  # High confidence for platform data

        # Both exist - check similarity
        if self._string_similarity(sensor_org, llm_org) > 0.7:
            return sensor_org, 0.9  # High confidence when they agree

        # Prefer sensor for platform/forum info
        return sensor_org, 0.8

    def _resolve_url(
        self,
        sensor_meta: Dict[str, Any],
        llm_meta: Dict[str, Any],
        llm_conf: Dict[str, float]
    ) -> Tuple[Optional[str], float]:
        """Resolve URL with confidence"""
        sensor_url = sensor_meta.get("url")
        llm_url = llm_meta.get("url")
        llm_url_conf = llm_conf.get("url", 0.5)

        if not sensor_url and not llm_url:
            return None, 0.0

        if not sensor_url:
            return llm_url, llm_url_conf

        if not llm_url:
            return sensor_url, 0.95  # Very high confidence for sensor URLs

        # Both exist - prefer sensor (it actually accessed the URL)
        return sensor_url, 0.95

    def _resolve_tags(
        self,
        sensor_meta: Dict[str, Any],
        llm_meta: Dict[str, Any],
        llm_conf: Dict[str, float]
    ) -> List[str]:
        """Merge tags from both sources"""
        sensor_tags = sensor_meta.get("tags", [])
        if isinstance(sensor_tags, str):
            sensor_tags = [sensor_tags]

        llm_tags = llm_meta.get("tags", [])
        if isinstance(llm_tags, str):
            llm_tags = [llm_tags]

        # Combine and deduplicate
        all_tags = set(sensor_tags) | set(llm_tags)
        return sorted(list(all_tags))

    def _get_sensor_confidence(self, sensor_meta: Dict[str, Any], field: str) -> float:
        """Estimate confidence for sensor-extracted field"""
        # Check if field was from API or scraping
        if field in ["title", "author", "published_at", "created_at"]:
            # These usually come from API - high confidence
            if sensor_meta.get(field):
                return 0.85
        elif field == "url":
            # URL from sensor is definitive
            return 0.95

        # Default moderate confidence
        return 0.7

    def _string_similarity(self, s1: Optional[str], s2: Optional[str]) -> float:
        """Calculate string similarity score"""
        if not s1 or not s2:
            return 0.0
        return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()

    def _names_match(self, name1: str, name2: str) -> bool:
        """Check if two names refer to same person"""
        if not name1 or not name2:
            return False

        # Exact match
        if name1.lower() == name2.lower():
            return True

        # One might be subset of other (e.g., "John" vs "John Smith")
        if name1.lower() in name2.lower() or name2.lower() in name1.lower():
            return True

        # Check last names match (assuming "First Last" format)
        parts1 = name1.split()
        parts2 = name2.split()
        if len(parts1) > 1 and len(parts2) > 1:
            if parts1[-1].lower() == parts2[-1].lower():
                return True

        return False

    def _parse_date(self, date_str: Any) -> Optional[datetime]:
        """Parse date from various formats"""
        if not date_str:
            return None

        if isinstance(date_str, datetime):
            return date_str

        try:
            return dateutil.parser.parse(str(date_str))
        except:
            return None

    def _merge_additional_fields(
        self,
        resolved: Dict[str, Any],
        sensor_meta: Dict[str, Any],
        llm_meta: Dict[str, Any]
    ):
        """Merge additional fields not in core set"""
        # Fields to skip (already resolved)
        skip_fields = {
            "title", "author", "published_date", "published_at",
            "created_at", "organization", "forum", "url", "tags"
        }

        # Add sensor fields
        for key, value in sensor_meta.items():
            if key not in skip_fields and key not in resolved:
                resolved[f"sensor_{key}"] = value

        # Add LLM fields
        for key, value in llm_meta.items():
            if key not in skip_fields and key not in resolved:
                resolved[f"llm_{key}"] = value