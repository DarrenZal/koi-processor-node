"""
Entity quality filtering module for pipeline.

Wraps the existing EntityQualityFilter to work within the pipeline framework.
"""

from typing import Dict, Any
import logging

from ..base import PostProcessingModule
from ..context import ProcessingContext

logger = logging.getLogger(__name__)


class EntityQualityFilterModule(PostProcessingModule):
    """
    Module that filters entities using pattern-based rules.

    Blocks:
    - Pronouns (we, they, it, etc.)
    - Generic nouns (user, farmer, project, etc.)
    - Technical patterns (URLs, localhost, function names, etc.)
    - Sentence fragments
    - Numerics
    - Tautological entities (name equals type)
    - Lowercase single-word PERSON entities
    - Template IDs (JIRA/issue keys, ERC standards)
    - Boilerplate phrases and placeholder PERSON values

    Configuration:
        additional_stop_words: Extra words to block
        whitelist: Words that should never be blocked
        max_name_length: Maximum entity name length (default: 80)
        max_word_count: Maximum words in entity name (default: 8)
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)

        # Try to import and use existing EntityQualityFilter
        self._filter = None
        try:
            from ...improvements import EntityQualityFilter, FilterConfig

            filter_config = FilterConfig(
                additional_stop_words=set(self.config.get('additional_stop_words', [])),
                whitelist=set(self.config.get('whitelist', [])),
                max_name_length=self.config.get('max_name_length', 80),
                max_word_count=self.config.get('max_word_count', 8),
                verbose=self.config.get('verbose', False)
            )
            self._filter = EntityQualityFilter(filter_config)
        except ImportError:
            logger.warning("EntityQualityFilter not available, using inline implementation")
            self._init_inline_filter()

    def _init_inline_filter(self):
        """Initialize inline filter patterns."""
        import re

        self._stop_words = {
            # Pronouns
            'i', 'me', 'my', 'we', 'us', 'our', 'you', 'your',
            'he', 'him', 'his', 'she', 'her', 'they', 'them', 'their', 'it', 'its',
            # Generic nouns
            'people', 'person', 'user', 'users', 'member', 'members',
            'team', 'teams', 'group', 'groups', 'community', 'communities',
            'project', 'projects', 'thing', 'things', 'stuff',
            'farmer', 'farmers', 'developer', 'developers',
            'validator', 'validators', 'contributor', 'contributors'
        }
        self._boilerplate_blocklist = {
            "testing instructions",
            "dry principles",
            "test plan",
            "acceptance criteria",
            "definition of done",
            "success criteria",
            "knowledge network expands with data ingestion",
            "strengthening collective intelligence",
            "building regenerative economies",
            "more information needed",
            "further research required",
            "additional context",
            "n/a",
            "tbd",
            "todo",
        }
        self._placeholder_persons = {"public users", "unknown", "anonymous"}

        self._generic_patterns = [
            re.compile(r'^(the |a |an |our |their |my |your )', re.IGNORECASE),
            re.compile(r'^(some|many|few|all|most|several) ', re.IGNORECASE),
        ]

        self._sentence_patterns = [
            re.compile(r'\b(is|are|was|were|has|have|had|will|would|could|should)\b', re.IGNORECASE),
            re.compile(r'[.!?;]'),
        ]

        self._technical_patterns = [
            re.compile(r'^https?://'),
            re.compile(r'^localhost'),
            re.compile(r':\d{2,5}$'),
            re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'),
            re.compile(r'^regen1[a-z0-9]{38,}$'),
            re.compile(r'\.(md|js|ts|py|go|json)$', re.IGNORECASE),
        ]
        self._jira_pattern = re.compile(r'^[A-Z]+-\d+$', re.IGNORECASE)
        self._erc_pattern = re.compile(r'^ERC-\d+$', re.IGNORECASE)

        self._max_length = self.config.get('max_name_length', 80)
        self._max_words = self.config.get('max_word_count', 8)

    def get_name(self) -> str:
        return "EntityQualityFilter"

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Filter entities using pattern-based rules."""

        entities_to_remove = []
        for entity in context.entities:
            is_valid, reasons = self._filter_with_reasons(
                entity.name,
                entity.type
            )

            if not is_valid:
                entities_to_remove.append((entity, reasons))
                self.stats['entities_blocked'] += 1

                # Track by reason
                for reason in reasons:
                    self.stats[f'blocked_{reason}'] += 1

        # Block low-quality entities
        for entity, reasons in entities_to_remove:
            reason_str = ', '.join(reasons)
            context.block_entity(entity, reason_str, self.get_name())

        return context

    def _filter_with_reasons(self, name: str, entity_type: str) -> tuple:
        """Apply all filters and return reasons for blocking."""
        if self._filter:
            return self._filter.filter_with_reasons(name, entity_type)

        # Inline implementation
        reasons = []
        normalized = name.strip()
        normalized_lower = normalized.lower()

        # Stop word check
        if normalized_lower in self._stop_words:
            reasons.append("stop_word")

        # Numeric only check
        if normalized.isdigit():
            reasons.append("numeric_only")

        # Tautological check
        if entity_type and normalized_lower.rstrip('s') == entity_type.strip().lower().rstrip('s'):
            reasons.append("tautological")

        # Lowercase single-word PERSON check
        if entity_type and entity_type.upper() == 'PERSON':
            stripped = normalized
            if ' ' not in stripped and stripped and stripped[0].islower():
                reasons.append("lowercase_person")

            if normalized_lower in self._placeholder_persons:
                reasons.append("placeholder_person")

        # Template/ID patterns
        if self._erc_pattern.match(normalized):
            reasons.append("erc_standard")
        elif self._jira_pattern.match(normalized):
            reasons.append("jira_issue_id")

        # Known boilerplate phrases
        if any(phrase in normalized_lower for phrase in self._boilerplate_blocklist):
            reasons.append("boilerplate")

        # Generic pattern check
        for pattern in self._generic_patterns:
            if pattern.search(normalized):
                reasons.append("generic_pattern")
                break

        # Sentence-like check
        for pattern in self._sentence_patterns:
            if pattern.search(normalized):
                reasons.append("sentence_like")
                break

        # Technical pattern check
        for pattern in self._technical_patterns:
            if pattern.search(normalized):
                reasons.append("technical_pattern")
                break

        # Length check
        if len(normalized) > self._max_length or len(normalized.split()) > self._max_words:
            reasons.append("too_long")

        is_valid = len(reasons) == 0
        return is_valid, reasons


# Export
__all__ = ['EntityQualityFilterModule']
