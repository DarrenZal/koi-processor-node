"""
Entity Quality Filter Module for Regen KOI

Filters out low-quality entities extracted by LLMs, including:
- Pronouns (we, they, it)
- Generic nouns (people, user, member)
- Numeric-only entities (2030, 35)
- Tautological entities (name equals type)
- Lowercase single-word PERSON entities
- Generic person patterns (the character, our friends)
- Sentence-like entities (contains verbs, too long)
- FIX-002: Git commits/changelog lines when typed as CLAIM/EVIDENCE/QUESTION
- FIX-002: AI systems mis-typed as PERSON (should be TECHNOLOGY)
- FIX-002: Generic event words when typed as EVENT
- FIX-011: Blockchain names mis-typed as LOCATION
- FIX-013: Pipeline/code module names mis-typed as PROCESS (Week 6)
- FIX-014: Abstract environmental concepts mis-typed as MATERIAL (Week 6)

This module is adapted from an earlier knowledge graph extraction system
with Regen-specific additions for forum and community content.

Usage:
    from src.knowledge_graph.improvements import EntityQualityFilter

    filter = EntityQualityFilter()

    # Single entity
    passes, reason = filter.filter_entity({"name": "we", "type": "PERSON"})
    # passes = False, reason = "stop_word"

    # Batch filtering
    entities = [{"name": "Gregory Landua", "type": "PERSON"}, {"name": "they", "type": "PERSON"}]
    clean_entities = filter.filter_batch(entities)
    # clean_entities = [{"name": "Gregory Landua", "type": "PERSON"}]

    # Get statistics
    stats = filter.get_stats()
    print(f"Filtered: {stats['total_filtered']} / {stats['total_checked']}")

Author: Claude Code (adapted from an earlier extraction system)
Date: 2025-12-08
Version: 1.4.0 (FIX-013, FIX-014 - Week 6)
"""

import re
from typing import Dict, List, Set, Tuple, Optional, Pattern
from dataclasses import dataclass, field


# ============================================================================
# Built-in Entity Whitelist
# ============================================================================
# Common abbreviations and entity names that should NOT be blocked
# even though they might match pronoun/generic patterns.
# These are checked before any blocking patterns.
# ============================================================================

# ISO 3166-1 alpha-2 country codes (commonly used as entities)
COUNTRY_CODES = {
    # Major countries
    'US', 'UK', 'EU', 'CA', 'AU', 'NZ', 'FR', 'DE', 'IT',
    'JP', 'CN', 'IN', 'BR', 'MX', 'ZA', 'KR', 'ES', 'SE', 'NO',
    'DK', 'FI', 'NL', 'BE', 'AT', 'CH', 'IE', 'PT', 'GR', 'PL',
    'CZ', 'HU', 'RO', 'BG', 'HR', 'SK', 'SI', 'EE', 'LV', 'LT',
    'CY', 'MT', 'LU', 'IS', 'LI', 'MC', 'SM', 'VA', 'AD', 'AL',
    'RU', 'UA', 'BY', 'MD', 'GE', 'AM', 'AZ', 'KZ', 'UZ', 'TM',
    'KG', 'TJ', 'MN', 'PH', 'TH', 'VN', 'MY', 'SG', 'ID', 'PK',
    'BD', 'LK', 'NP', 'MM', 'KH', 'LA', 'BN', 'TW', 'HK', 'MO',
    'AE', 'SA', 'IL', 'TR', 'IR', 'IQ', 'SY', 'JO', 'LB', 'QA',
    'KW', 'BH', 'OM', 'YE', 'EG', 'LY', 'TN', 'DZ', 'MA', 'SD',
    'KE', 'NG', 'GH', 'TZ', 'ET', 'UG', 'RW', 'ZW', 'ZM', 'BW',
    'MW', 'MZ', 'AO', 'NA', 'SN', 'CI', 'CM', 'ML', 'BF', 'NE',
    'AR', 'CL', 'CO', 'PE', 'VE', 'EC', 'BO', 'PY', 'UY', 'SR',
    'GY', 'CR', 'PA', 'GT', 'HN', 'SV', 'NI', 'BZ', 'CU', 'DO',
    'HT', 'JM', 'TT', 'BB', 'BS', 'PR', 'FJ', 'PG', 'WS', 'TO',
}

# International organizations and government agencies
ORGANIZATIONS = {
    # International
    'UN', 'NATO', 'ASEAN', 'OPEC', 'WTO', 'IMF', 'WHO', 'OECD', 'G7', 'G20',
    'UNESCO', 'UNICEF', 'UNHCR', 'WFP', 'FAO', 'ILO', 'IAEA', 'ICAO', 'IMO',
    'WIPO', 'ITU', 'UNEP', 'UNDP', 'WB', 'IFC', 'ADB', 'AIIB', 'EBRD', 'IDB',
    'IPCC', 'UNFCCC', 'CBD', 'CITES', 'IUCN', 'WWF', 'WRI', 'GCF', 'GEF',

    # US Government
    'NASA', 'NOAA', 'EPA', 'FDA', 'CDC', 'NIH', 'NSF', 'USDA', 'USGS', 'NPS',
    'DARPA', 'ARPA', 'DOE', 'DOD', 'DHS', 'FBI', 'CIA', 'NSA', 'DOJ', 'DOS',
    'HUD', 'DOT', 'DOL', 'HHS', 'VA', 'FCC', 'SEC', 'FTC', 'CFTC', 'FDIC',
    'FEMA', 'SSA', 'SBA', 'GSA', 'OPM', 'OMB', 'GAO', 'CBO', 'NIST', 'NTIA',

    # Other government agencies
    'ESA', 'JAXA', 'CERN', 'ITER', 'CSIRO', 'DLR', 'CNSA', 'ISRO', 'KARI',
    'DEFRA', 'BEIS', 'FSA', 'NHS', 'BBC', 'CBC', 'ABC', 'NHK', 'RAI', 'ARD',

    # Standards organizations
    'ISO', 'IEEE', 'IETF', 'W3C', 'ANSI', 'NIST', 'IEC', 'ITU', 'ETSI', 'CEN',
    'BSI', 'DIN', 'JIS', 'GB', 'GOST', 'AS', 'NZS', 'CSA', 'UL', 'CE', 'FCC',
}

# Technology and science abbreviations
TECH_SCIENCE = {
    # Academic
    'MIT', 'UCLA', 'USC', 'NYU', 'CMU', 'UCB', 'UCSF', 'UCSD', 'UCI', 'UCD',
    'GTech', 'UMD', 'UVA', 'UNC', 'OSU', 'PSU', 'MSU', 'ASU', 'UW', 'UT',
    'Harvard', 'Stanford', 'Yale', 'Princeton', 'Cornell', 'Columbia', 'Penn',
    'CalTech', 'JHU', 'Duke', 'Northwestern', 'Vanderbilt', 'WashU', 'Rice',
    'Oxford', 'Cambridge', 'Imperial', 'UCL', 'LSE', 'KCL', 'Edinburgh', 'Bristol',
    'ETH', 'EPFL', 'TUM', 'LMU', 'KIT', 'RWTH', 'TU', 'Delft', 'KTH', 'DTU',

    # Tech companies (commonly referenced as entities)
    'IBM', 'HP', 'AMD', 'ARM', 'NVIDIA', 'TSMC', 'ASML', 'SAP', 'Oracle', 'Dell',
    'Cisco', 'Intel', 'Qualcomm', 'Broadcom', 'TI', 'Analog', 'NXP', 'STM',

    # Scientific terms commonly used as entities
    'DNA', 'RNA', 'AI', 'ML', 'NLP', 'CV', 'RL', 'DL', 'NN', 'CNN', 'RNN', 'GAN',
    'GPS', 'GIS', 'LiDAR', 'SAR', 'NDVI', 'EVI', 'LAI', 'NPP', 'GPP', 'NEE',
    'API', 'SDK', 'IDE', 'CLI', 'GUI', 'OS', 'CPU', 'GPU', 'TPU', 'RAM', 'ROM',
    'SSD', 'HDD', 'NVMe', 'SATA', 'USB', 'HDMI', 'PCIe', 'DDR', 'LPDDR', 'GDDR',
    'IoT', 'IIoT', 'M2M', 'V2X', 'LTE', '5G', '6G', 'WiFi', 'BLE', 'NFC', 'RFID',

    # Environmental/Climate science (Regen-specific)
    'GHG', 'CO2', 'CH4', 'N2O', 'HFC', 'PFC', 'SF6', 'CFC', 'HCFC', 'VOC',
    'MRV', 'ERW', 'DAC', 'BECCS', 'CCS', 'CCU', 'CDR', 'NET', 'NbS', 'NCS',
    'SOC', 'SIC', 'DOC', 'POC', 'DIC', 'TIC', 'TOC', 'TN', 'TP', 'TSS', 'TDS',
    'GWP', 'ODP', 'AP', 'EP', 'POCP', 'LCA', 'LCI', 'LCIA', 'EPD', 'CFP', 'WFP',
}

# Currency codes (ISO 4217)
CURRENCY_CODES = {
    'USD', 'EUR', 'GBP', 'JPY', 'CNY', 'INR', 'BRL', 'MXN', 'RUB', 'KRW',
    'CAD', 'AUD', 'NZD', 'CHF', 'SEK', 'NOK', 'DKK', 'PLN', 'CZK', 'HUF',
    'TRY', 'ZAR', 'SGD', 'HKD', 'TWD', 'THB', 'MYR', 'IDR', 'PHP', 'VND',
    'AED', 'SAR', 'ILS', 'EGP', 'NGN', 'KES', 'ARS', 'CLP', 'COP', 'PEN',
    'BTC', 'ETH', 'USDT', 'USDC', 'BNB', 'XRP', 'ADA', 'SOL', 'DOT', 'DOGE',
    'ATOM', 'REGEN', 'OSMO', 'JUNO', 'STARS', 'EVMOS', 'INJ', 'KAVA', 'AKT',
}

# Common person names that match verb patterns but are valid names
# These prevent false positives from sentence_like pattern matching names like "Will"
PERSON_NAMES_WHITELIST = {
    # Names that match modal verbs
    'Will', 'May', 'Can', 'Art', 'Bill', 'Rob', 'Mark', 'Grant', 'Dawn',
    'Faith', 'Hope', 'Grace', 'Joy', 'Chance', 'Chase', 'Miles', 'Hunter',
    # Common validator/forum usernames in Regen ecosystem
    'vitwit', 'swidnikk', 'ryanchristo',
}

# Combine into master whitelist
ENTITY_WHITELIST = COUNTRY_CODES | ORGANIZATIONS | TECH_SCIENCE | CURRENCY_CODES | PERSON_NAMES_WHITELIST


@dataclass
class FilterConfig:
    """Configuration for entity quality filter."""

    # Additional stop words to block (merged with defaults)
    additional_stop_words: Set[str] = field(default_factory=set)

    # Words to whitelist (never block, even if they match patterns)
    whitelist: Set[str] = field(default_factory=set)

    # Maximum entity name length (characters)
    max_name_length: int = 80

    # Maximum word count in entity name
    max_word_count: int = 8

    # Enable verbose logging
    verbose: bool = False


class EntityQualityFilter:
    """
    Filters low-quality entities from knowledge graph extraction results.

    Implements pattern and template-based checks, including:
    - Stop-word entities (pronouns, generic nouns)
    - Placeholder PERSON entities ("Public Users", "Unknown", "Anonymous")
    - FIX-003: Expanded placeholder detection for ALL types
    - FIX-003: Min-length validation (blocks single-char names)
    - JIRA/issue IDs and ERC standards (APP-776, ERC-20)
    - Boilerplate/template phrases ("Testing Instructions", "DRY Principles")
    - Numeric-only entities
    - Tautological entities (name equals type)
    - Lowercase single-word PERSON entities
    - Generic person patterns
    - Sentence-like entities
    - Length limit violations
    - Technical patterns (URLs, code identifiers)

    Attributes:
        config: FilterConfig instance with customization options
        stats: Dictionary tracking filter statistics
    """

    # ========================================================================
    # FIX-003: Min-Length Validation
    # ========================================================================
    MIN_NAME_LENGTH = 2  # Block 0-1 char names

    # ========================================================================
    # FIX-003: Expanded Placeholder Patterns (applies to ALL types)
    # ========================================================================
    PLACEHOLDER_PATTERNS: List[re.Pattern] = [
        re.compile(r'^unknown\s*\d*$', re.IGNORECASE),
        re.compile(r'^anonymous(\s+user)?$', re.IGNORECASE),
        re.compile(r'^public\s+users?$', re.IGNORECASE),
        re.compile(r'^user\s*\d+$', re.IGNORECASE),
        re.compile(r'^(tbd|todo|n/?a|none)$', re.IGNORECASE),
        re.compile(r'^placeholder\s*\d*$', re.IGNORECASE),
        re.compile(r'^(test|dummy|sample)\s*(user|data|entity)?$', re.IGNORECASE),
    ]

    # Default stop words - pronouns and generic nouns
    DEFAULT_STOP_WORDS: Set[str] = {
        # Personal pronouns
        'i', 'me', 'my', 'mine', 'myself',
        'you', 'your', 'yours', 'yourself', 'yourselves',
        'he', 'him', 'his', 'himself',
        'she', 'her', 'hers', 'herself',
        'it', 'its', 'itself',
        'we', 'us', 'our', 'ours', 'ourselves',
        'they', 'them', 'their', 'theirs', 'themselves',

        # Generic collective nouns
        'people', 'person', 'individual', 'individuals',
        'everyone', 'someone', 'anyone', 'nobody', 'somebody',
        'everybody', 'anybody', 'noone', 'no one',

        # Generic familial/social references
        'mom', 'dad', 'mother', 'father', 'parent', 'parents',
        'friend', 'friends', 'family', 'families',
        'guy', 'guys', 'girl', 'girls', 'woman', 'women', 'man', 'men',
        'kid', 'kids', 'child', 'children',

        # Generic occupational (lowercase singular)
        'farmer', 'farmers', 'teacher', 'teachers',
        'scientist', 'scientists', 'activist', 'activists',
        'developer', 'developers', 'engineer', 'engineers',

        # Regen/forum-specific generics
        'user', 'users', 'member', 'members',
        'participant', 'participants', 'attendee', 'attendees',
        'validator', 'validators', 'delegator', 'delegators',
        'contributor', 'contributors', 'stakeholder', 'stakeholders',

        # Very generic organizational terms (lowercase)
        'team', 'teams', 'group', 'groups',
        'company', 'companies', 'organization', 'organizations',
        'project', 'projects', 'initiative', 'initiatives',
        'community', 'communities',

        # Other low-value entities
        'thing', 'things', 'stuff', 'something', 'anything', 'nothing',
        'way', 'ways', 'kind', 'kinds', 'type', 'types',
        'example', 'examples', 'case', 'cases',
    }

    # Patterns for generic person/entity descriptions
    # NOTE: These must distinguish between generic descriptions and proper names
    # Generic: "the character", "a user" (determiner + lowercase generic noun)
    # Proper: "The Ministry for the Future", "The World Bank" (determiner + capitalized proper name)
    GENERIC_PERSON_PATTERNS: List[re.Pattern] = [
        # Determiner + lowercase word = generic description
        # "the character", "a user", "an expert" - but NOT "The Ministry", "A Novel"
        # Match: determiner (case-insensitive) followed by space and LOWERCASE letter
        # Note: No IGNORECASE flag so [a-z] only matches lowercase
        re.compile(r'^(?i:the|a|an|our|their|my|your|his|her|its) [a-z]'),
        # Generic group noun endings (these are almost always generic regardless of case)
        re.compile(r'(friends|teachers|officials|people|generations|characters?|speakers?|participants?|members?|users?)s?$', re.IGNORECASE),
        # Relative/demonstrative pronouns starting a phrase
        re.compile(r'^(who|which|that|those|these|some|many|few|all|most|several) ', re.IGNORECASE),
        # Indefinite references
        re.compile(r'^(someone|anyone|everyone|nobody|somebody|anybody|everybody) ', re.IGNORECASE),
        # Generic descriptive phrases
        re.compile(r'^(other|various|different|certain|specific) ', re.IGNORECASE),
    ]

    # Patterns indicating sentence-like structures
    # NOTE: These patterns must be carefully tuned to avoid false positives
    # Common false positives: version numbers (v2.0), domains (.fi), person names (Will)
    SENTENCE_PATTERNS: List[re.Pattern] = [
        # Common verbs indicating sentence structure
        # EXCLUDED from standalone match: "will" (common name), "can" (ambiguous)
        # These verbs strongly indicate a sentence when surrounded by words
        re.compile(r'\b(is|are|was|were|has|have|had|would|could|should|may|might|must)\b', re.IGNORECASE),
        # Phrase structures (these are clearly sentence-like)
        re.compile(r'\b(the most|in order to|according to|in terms of|as well as|such as|rather than)\b', re.IGNORECASE),
        # Sentence punctuation - REFINED to avoid false positives:
        # - Don't match period followed by digit (v2.0, 3.14)
        # - Don't match period in common TLDs or crypto tokens (.fi, .io, .noble, .network)
        # Only match: !, ?, ;
        re.compile(r'[!?;]'),
        # Multiple commas (likely a list, not an entity)
        re.compile(r',.*,.*,'),
        # Question patterns
        re.compile(r'^(how|what|why|when|where|who|which)\b', re.IGNORECASE),
    ]

    # Technical patterns that are not valid entities
    TECHNICAL_PATTERNS: List[re.Pattern] = [
        # URLs and URL-like strings
        re.compile(r'^https?://'),
        re.compile(r'^www\.'),
        re.compile(r'\.(com|org|net|io|network|app)$', re.IGNORECASE),  # Domain endings
        # IP addresses and ports
        re.compile(r'^localhost'),
        re.compile(r':\d{2,5}$'),  # Port numbers
        re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'),  # IP addresses
        # Blockchain addresses (Cosmos-style regen1...)
        re.compile(r'^regen1[a-z0-9]{38,}$'),
        re.compile(r'^cosmos1[a-z0-9]{38,}$'),
        re.compile(r'^0x[a-fA-F0-9]{40,}$'),  # Ethereum addresses
        # File paths
        re.compile(r'^[/\\]'),
        re.compile(r'\.(md|js|ts|py|go|json|yaml|yml|toml|txt|csv)$', re.IGNORECASE),  # Code file extensions
        # Code patterns
        # FIX-005: Removed x/[a-z]+ pattern - now allowed for MODULE type
        re.compile(r'^[a-z]+\.[a-z]+\.[a-z\-]+', re.IGNORECASE),  # Package paths
        re.compile(r'^@[a-z]+/'),  # npm-style package refs
        # Technical identifiers
        re.compile(r'^[A-Z_]{10,}$'),  # All caps with underscores (constants)
        re.compile(r'_ALGORITHM_|_UNSPECIFIED|_NONE|_DEFAULT'),  # Enum-like patterns
        re.compile(r'^[a-z]+_[a-z_]+$'),  # snake_case identifiers
        # Note: Removed CamelCase pattern - too many false positives with valid function names like MsgCreateBatch
        # Technical single words (command line tools, protocols)
        re.compile(r'^(grpcurl|protoc|curl|wget|npm|yarn|docker|kubectl|grpc|api|sdk|cli|gui|ui|ux)$', re.IGNORECASE),
        # Hash-like strings
        re.compile(r'^[a-f0-9]{32,}$', re.IGNORECASE),  # MD5/SHA hashes
    ]

    # ========================================================================
    # FIX-005: Domain Identifier Patterns
    # ========================================================================
    # Allow legitimate domain identifiers that would otherwise be blocked
    # as technical patterns. These are checked when entity type matches.
    DOMAIN_IDENTIFIER_PATTERNS: List[re.Pattern] = [
        # Cosmos SDK messages (MsgSend, MsgCreateBatch, etc.)
        re.compile(r'^Msg[A-Z][a-zA-Z]+$'),
        # Credit class IDs (C01, C02, etc.)
        re.compile(r'^C\d{2,3}$'),
        # Module paths (x/ecocredit, x/group)
        re.compile(r'^x/[a-z]+$'),
    ]

    # FIX-005: Generic domain type words that should be blocked even when
    # the LLM assigns a domain type. E.g., "validator" typed as VALIDATOR.
    GENERIC_DOMAIN_TYPE_WORDS: Dict[str, Set[str]] = {
        'VALIDATOR': {'validator', 'validators'},
        'KEEPER': {'keeper', 'keepers'},
        'MODULE': {'module', 'modules'},
        'API_MESSAGE': {'message', 'messages', 'msg'},
        'GOVERNANCE_PROPOSAL': {'proposal', 'proposals'},
        'CREDIT_CLASS': {'credit', 'credits', 'credit class', 'credit classes'},
    }

    # Patterns and blocklists for known noisy entities
    JIRA_ISSUE_PATTERN = re.compile(r'^[A-Z]+-\d+$', re.IGNORECASE)  # APP-776
    ERC_STANDARD_PATTERN = re.compile(r'^ERC-\d+$', re.IGNORECASE)  # ERC-20, ERC-721

    # ========================================================================
    # FIX-002: Git/Changelog Patterns
    # ========================================================================
    # Block these patterns ONLY when entity type is CLAIM, EVIDENCE, or QUESTION
    GIT_CHANGELOG_PATTERNS: List[re.Pattern] = [
        # Conventional commits: feat(scope): message, fix: message, etc.
        re.compile(r'^(feat|fix|chore|docs|style|refactor|test|build|ci|perf)(\([^)]*\))?:', re.IGNORECASE),
        # Git merge commits
        re.compile(r'^Merge (pull request|branch|remote)', re.IGNORECASE),
        # Version strings: v1.2.3, 1.0.0-beta, etc.
        re.compile(r'^v?\d+\.\d+(\.\d+)?(-[0-9A-Za-z.\-]+)?$', re.IGNORECASE),
        # Changelog entries: [Added] Feature, [Fixed] Bug, etc.
        re.compile(r'^\[[\w\s]+\]\s*(Added|Fixed|Changed|Removed|Updated|Deprecated)\b', re.IGNORECASE),
    ]

    # ========================================================================
    # FIX-002: AI/Software Blocklist
    # ========================================================================
    # Block these names ONLY when entity type is PERSON (they should be TECHNOLOGY)
    AI_SOFTWARE_BLOCKLIST: Set[str] = {
        # Major LLMs/chatbots
        'chatgpt', 'gpt-4', 'gpt-3', 'gpt-4o', 'gpt-3.5', 'gpt4', 'gpt3',
        'claude', 'claude-3', 'claude-2', 'copilot', 'github copilot',
        'bard', 'gemini', 'gemini-pro', 'llama', 'llama-2', 'llama-3',
        'mistral', 'mixtral', 'palm', 'palm-2',
        # AI companies (when used as the AI itself)
        'openai', 'anthropic',
        # Image/media AI
        'dall-e', 'dalle', 'midjourney', 'stable diffusion', 'stability ai',
        # Voice assistants
        'alexa', 'siri', 'cortana', 'google assistant',
        # Automation tools often mistaken for people
        'github actions', 'ci bot', 'auto-merge', 'dependabot', 'renovate',
    }

    # ========================================================================
    # FIX-002: Generic Event Terms
    # ========================================================================
    # Block these ONLY when entity type is EVENT (they need specific titles)
    # These are generic words that should not be extracted as events
    GENERIC_EVENT_TERMS: Set[str] = {
        # Single generic words
        'meeting', 'call', 'conference', 'webinar', 'workshop',
        'summit', 'session', 'panel', 'discussion', 'talk',
        'event', 'gathering', 'meetup', 'hangout', 'sync',
        'standup', 'stand-up', 'retro', 'retrospective',
        'demo', 'presentation', 'briefing', 'update',
    }

    # Patterns for compound generic event names
    GENERIC_EVENT_PATTERNS: List[re.Pattern] = [
        # "community call", "weekly meeting", "monthly sync", etc.
        re.compile(r'^(community|weekly|monthly|daily|bi-weekly|quarterly|annual|team|group|all-hands)\s+(call|meeting|sync|standup|check-in)$', re.IGNORECASE),
        # "town hall meeting", "office hours", etc.
        re.compile(r'^(town hall|office hours|open forum|q&a session|ama)(\s+meeting)?$', re.IGNORECASE),
    ]

    # ========================================================================
    # E325: FirstName-OrgName Artifact Pattern
    # ========================================================================
    # Block PERSON entities where a first name is incorrectly merged with an
    # organization name via hyphen (e.g., "Will-Regen Foundation" from text
    # like "Will said Regen Foundation...").
    #
    # Pattern: FirstName-OrgSuffix where OrgSuffix matches organization patterns.
    # Does NOT block legitimate hyphenated surnames (e.g., "Mary-Jane", "Smith-Jones").
    # ========================================================================
    FIRSTNAME_ORGNAME_PATTERN = re.compile(
        r'^[A-Z][a-z]+[-_]'  # Capitalized first name + hyphen/underscore
        r'(?:'
        r'[A-Z].*(?:Foundation|Network|DAO|Protocol|Labs?|Inc|LLC|Corp|Group|Team|Fund|Council|Committee|Association|Institute|Society|Organization|Org|Company|Co)'
        r'|'
        r'[A-Z][a-z]*flow'  # Matches -Chainflow, -Dataflow, etc.
        r'|'
        r'[A-Z][a-z]*[_-]?Network'  # Matches Meme_Network, etc.
        r')$',
        re.IGNORECASE
    )

    # ========================================================================
    # FIX-011: Blockchain Names Blocklist for LOCATION Type
    # ========================================================================
    # Block LOCATION type for known blockchain/network names that are
    # incorrectly extracted as locations. These should be TECHNOLOGY or PROJECT.
    #
    # Examples from production data:
    # - "ethereum" as LOCATION (4 occ) - should be TECHNOLOGY
    # - "polygon" as LOCATION (17 occ) - should be TECHNOLOGY (L2 chain)
    # - "solana" as LOCATION (3 occ) - should be TECHNOLOGY
    # - "arbitrum" as LOCATION (2 occ) - should be TECHNOLOGY (L2)
    # ========================================================================
    BLOCKCHAIN_NAMES: Set[str] = {
        # Major L1 blockchains
        'ethereum', 'bitcoin', 'solana', 'cardano', 'polkadot', 'avalanche',
        'tezos', 'algorand', 'near', 'fantom', 'cosmos', 'hedera',
        'flow', 'elrond', 'harmony', 'celo', 'eos', 'tron', 'stellar',
        'iota', 'vechain', 'neo', 'waves', 'zilliqa', 'icon', 'qtum',
        'ontology', 'ravencoin', 'dash', 'zcash', 'monero', 'litecoin',

        # L2 chains and scaling solutions
        'polygon', 'arbitrum', 'optimism', 'base', 'zksync', 'starknet',
        'immutable', 'loopring', 'metis', 'boba', 'mantle', 'linea',
        'scroll', 'mode', 'blast', 'manta', 'taiko', 'zkevm',

        # Cosmos ecosystem chains
        'osmosis', 'juno', 'evmos', 'injective', 'kava', 'akash',
        'secret', 'terra', 'thorchain', 'agoric', 'stride', 'neutron',
        'celestia', 'dymension', 'sei',

        # Regen-specific
        'regen', 'regen network',

        # Network terms that are often confused with locations
        'mainnet', 'testnet', 'devnet',
    }

    # ========================================================================
    # FIX-013: Code Module Names Blocklist for PROCESS Type
    # ========================================================================
    # Block PROCESS type for known code module/class names that are clearly
    # TECHNOLOGY (software components), not processes.
    #
    # Examples from production data (Week 6 report):
    # - "entityqualityfilter" as PROCESS (1 occ) - should be TECHNOLOGY
    # - "canonicalresolver" as PROCESS (1 occ) - should be TECHNOLOGY
    # - "confidencefilter" as PROCESS (1 occ) - should be TECHNOLOGY
    # ========================================================================
    CODE_MODULE_NAMES: Set[str] = {
        # KOI pipeline components
        'entityqualityfilter', 'entity quality filter',
        'canonicalresolver', 'canonical resolver',
        'confidencefilter', 'confidence filter',
        'documentleveldeduplicator', 'document level deduplicator',
        'ontologynormalizer', 'ontology normalizer',
        'listsplitter', 'list splitter',

        # Common software module patterns
        'dataloader', 'data loader',
        'configparser', 'config parser',
        'jsonparser', 'json parser',
        'xmlparser', 'xml parser',
        'requesthandler', 'request handler',
        'responsehandler', 'response handler',
        'eventlistener', 'event listener',
        'messagequeue', 'message queue',
        'taskqueue', 'task queue',
        'jobrunner', 'job runner',
        'taskrunner', 'task runner',
    }

    # Pattern to catch CamelCase class names as PROCESS
    CODE_MODULE_PATTERN = re.compile(
        r'^[A-Z][a-z]+(?:[A-Z][a-z]+)+$'  # CamelCase: EntityQualityFilter, DataLoader
    )

    # ========================================================================
    # FIX-014: Abstract Concept Names Blocklist for MATERIAL Type
    # ========================================================================
    # Block MATERIAL type for abstract environmental/ecological concepts.
    # MATERIAL should be physical substances, not abstract concepts.
    #
    # Examples from production data (Week 6 report):
    # - "biodiversity" as MATERIAL (1 occ) - should be CONCEPT
    # - "carbon sequestration" as MATERIAL (1 occ) - should be CONCEPT
    # - "ecological assets" as MATERIAL (3 occ) - should be CONCEPT
    # ========================================================================
    ABSTRACT_CONCEPT_AS_MATERIAL: Set[str] = {
        # Ecological concepts (not physical materials)
        'biodiversity', 'ecosystem', 'ecosystems', 'ecology',
        'carbon sequestration', 'carbon capture', 'carbon offset', 'carbon offsetting',
        'sequestration', 'capture', 'mitigation',
        'sustainability', 'sustainable development',
        'regeneration', 'regenerative', 'restoration',
        'conservation', 'preservation', 'protection',
        'resilience', 'adaptation', 'climate adaptation',
        'ecosystem services', 'ecological services', 'nature-based solutions',

        # Economic/financial concepts (not materials)
        'ecological assets', 'natural assets', 'natural capital',
        'ecosystem assets', 'environmental assets',
        'carbon credits', 'carbon credit', 'offset credits',
        'biodiversity credits', 'biodiversity credit',
        'nature credits', 'nature credit',
        'environmental credits', 'environmental credit',

        # Process/methodology concepts
        'verification', 'validation', 'monitoring',
        'measurement', 'reporting', 'assessment',
        'accounting', 'quantification',
        'impact', 'outcomes', 'benefits', 'co-benefits',

        # Governance/institutional concepts
        'governance', 'stewardship', 'custody',
        'ownership', 'rights', 'tenure',
        'commons', 'public goods',
    }

    # Lowercase comparison for boilerplate/template text that should be blocked
    BOILERPLATE_BLOCKLIST: Set[str] = {
        # Issue / PR templates
        "testing instructions",
        "dry principles",
        "test plan",
        "acceptance criteria",
        "definition of done",
        "success criteria",

        # Forum boilerplate
        "knowledge network expands with data ingestion",
        "strengthening collective intelligence",
        "building regenerative economies",

        # Generic filler
        "more information needed",
        "further research required",
        "additional context",
        "n/a",
        "tbd",
        "todo",
    }

    # Placeholder PERSON entities to drop
    PLACEHOLDER_PERSONS: Set[str] = {"public users", "unknown", "anonymous"}

    # Generic group terms that should not be tagged as PERSON entities
    # FIX-004: Expanded with singular forms + Cosmos SDK terms
    GENERIC_GROUP_TERMS: Set[str] = {
        # Economic actors (singular + plural)
        "buyer", "buyers", "seller", "sellers", "trader", "traders",
        "investor", "investors", "stakeholder", "stakeholders",
        "partner", "partners", "sponsor", "sponsors",
        "funder", "funders", "donor", "donors", "backer", "backers",

        # Organizational roles (singular + plural)
        "user", "users", "member", "members", "participant", "participants",
        "contributor", "contributors", "volunteer", "volunteers",
        "admin", "admins", "administrator", "administrators",
        "moderator", "moderators", "coordinator", "coordinators",
        "validator", "validators", "delegator", "delegators",
        "voter", "voters", "creator", "creators",

        # Teams/groups (singular + plural)
        "team", "teams", "group", "groups", "community", "communities",
        "committee", "committees", "council", "councils",
        "network", "networks", "coalition", "coalitions",
        "alliance", "alliances", "consortium", "consortiums",

        # Service providers (singular + plural)
        "utility", "utilities", "provider", "providers",
        "supplier", "suppliers", "vendor", "vendors",
        "contractor", "contractors", "developer", "developers",
        "builder", "builders", "consultant", "consultants",
        "advisor", "advisors", "auditor", "auditors",
        "verifier", "verifiers", "operator", "operators",

        # Governance/management (singular + plural)
        "board", "boards", "panel", "panels", "taskforce", "taskforces",
        "staff", "employee", "employees", "workforce", "personnel",

        # Cosmos SDK / blockchain terms (block as PERSON)
        "keeper", "keepers", "relayer", "relayers",
        "proposer", "proposers", "depositor", "depositors",
    }

    # ========================================================================
    # FIX-004: Multi-word Role Patterns
    # ========================================================================
    # Regex patterns to catch role phrases that should not be PERSON entities.
    # These catch multi-word patterns that aren't in GENERIC_GROUP_TERMS.
    ROLE_PATTERNS: List[re.Pattern] = [
        # Department + title patterns
        # Examples: "Partnerships Lead", "Comms Lead", "Governance Director", "Engineering Manager"
        re.compile(
            r'^(partnerships?|comms?|communications?|governance|dev|development|engineering|'
            r'product|project|program|operations|ops|marketing|growth|community|core|'
            r'research|design|finance|legal|security|data)\s+'
            r'(lead|manager|director|head|chief|officer)$',
            re.IGNORECASE,
        ),
        # Named role collectives
        re.compile(r'^(core|community)\s+contributors?$', re.IGNORECASE),
        # Team/group qualifiers (matches anywhere in string)
        re.compile(r'\b(team|group|committee|council|task\s*force|working\s*group)\b', re.IGNORECASE),
    ]

    # ========================================================================
    # FIX-016: Single-Token PERSON Guard
    # ========================================================================
    # Blocks single-token PERSON names (e.g., "Max", "Will") unless they have
    # an explicit PERSON cue prefix (Dr., CEO, Chairman, etc.).
    # ========================================================================
    PERSON_CUE_PREFIXES: Set[str] = {
        # Honorifics
        'mr', 'mr.', 'mrs', 'mrs.', 'ms', 'ms.', 'miss',
        'dr', 'dr.', 'prof', 'prof.', 'professor',
        'sir', 'dame', 'lord', 'lady', 'rev', 'rev.', 'reverend',
        # Corporate roles
        'ceo', 'cfo', 'cto', 'coo', 'cmo', 'cio',
        'president', 'vp', 'vice president',
        'chairman', 'chairwoman', 'chairperson', 'chair',
        'director', 'founder', 'co-founder', 'cofounder',
        'partner', 'managing partner',
        # Other roles
        'chief', 'head', 'lead', 'manager', 'senior', 'principal',
    }

    # Regex pattern for matching cue prefixes with optional punctuation
    # Matches: "Dr. Jane", "Dr Jane", "CEO Alice", "CEO: Alice"
    # Build pattern from unique prefix roots (without trailing periods)
    _prefix_roots = sorted(set(p.rstrip('.') for p in PERSON_CUE_PREFIXES if p), key=len, reverse=True)
    PERSON_CUE_PREFIX_PATTERN: re.Pattern = re.compile(
        r'^(' + '|'.join(re.escape(p) for p in _prefix_roots) + r')[.:]?\s+',
        re.IGNORECASE
    )

    def __init__(self, config: Optional[FilterConfig] = None):
        """
        Initialize the entity quality filter.

        Args:
            config: Optional FilterConfig for customization.
                    If None, uses default configuration.
        """
        self.config = config or FilterConfig()

        # Build effective stop word set
        self._stop_words = self.DEFAULT_STOP_WORDS.union(self.config.additional_stop_words)

        # Build effective whitelist (built-in + user config)
        # Store both original case and lowercase for case-insensitive matching
        self._whitelist = ENTITY_WHITELIST.copy()
        if self.config.whitelist:
            self._whitelist.update(self.config.whitelist)
        self._whitelist_lower = {w.lower() for w in self._whitelist}

        # Initialize statistics
        self._stats = {
            'total_checked': 0,
            'total_filtered': 0,
            'total_passed': 0,
            'reasons': {
                'stop_word': 0,
                'numeric_only': 0,
                'tautological': 0,
                'lowercase_person': 0,
                'generic_pattern': 0,
                'sentence_like': 0,
                'too_long': 0,
                'technical_pattern': 0,
                'jira_issue_id': 0,
                'erc_standard': 0,
                'boilerplate': 0,
                'placeholder_person': 0,
                'generic_group': 0,
                # FIX-002: New filter reasons
                'git_changelog': 0,
                'ai_as_person': 0,
                'generic_event': 0,
                # FIX-003: New filter reasons
                'too_short': 0,
                'placeholder': 0,
                # FIX-005: New filter reasons
                'generic_domain_type': 0,
                # E325: FirstName-OrgName artifact
                'firstname_orgname_artifact': 0,
                # FIX-011: Blockchain as LOCATION
                'blockchain_as_location': 0,
                # FIX-013: Code module as PROCESS
                'code_module_as_process': 0,
                # FIX-014: Abstract concept as MATERIAL
                'abstract_concept_as_material': 0,
                # FIX-016: Single-token PERSON
                'single_token_person': 0,
            }
        }

    def is_whitelisted(self, name: str) -> bool:
        """
        Check if entity name is in the whitelist.

        Whitelist includes country codes, organization abbreviations,
        tech terms, and currency codes that should never be blocked.

        Args:
            name: Entity name to check

        Returns:
            True if name is whitelisted (should NOT be blocked)
        """
        normalized = name.strip().lower()
        return normalized in self._whitelist_lower

    # ========================================================================
    # FIX-003: Min-Length and Placeholder Methods
    # ========================================================================

    def is_too_short(self, name: str) -> bool:
        """
        FIX-003: Block empty/single-character entity names.

        Args:
            name: Entity name to check

        Returns:
            True if name is too short (should be blocked)

        Examples:
            >>> filter.is_too_short("X")
            True
            >>> filter.is_too_short(" ")
            True
            >>> filter.is_too_short("US")
            False
            >>> filter.is_too_short("AI")
            False
        """
        stripped = name.strip()
        return len(stripped) < self.MIN_NAME_LENGTH

    def is_placeholder(self, name: str, entity_type: str = None) -> bool:
        """
        FIX-003: Check for placeholder patterns (applies to ALL types).

        Expanded from the original is_placeholder_person() to catch
        placeholder patterns regardless of entity type.

        Args:
            name: Entity name to check
            entity_type: Entity type (optional, not used - applies to all types)

        Returns:
            True if matches a placeholder pattern (should be blocked)

        Examples:
            >>> filter.is_placeholder("Unknown")
            True
            >>> filter.is_placeholder("Anonymous User")
            True
            >>> filter.is_placeholder("User 123")
            True
            >>> filter.is_placeholder("N/A")
            True
            >>> filter.is_placeholder("TBD")
            True
            >>> filter.is_placeholder("placeholder")
            True
            >>> filter.is_placeholder("test user")
            True
            >>> filter.is_placeholder("Gregory Landua")
            False
            >>> filter.is_placeholder("Regen Network")
            False
        """
        stripped = name.strip()

        # Check against placeholder patterns
        for pattern in self.PLACEHOLDER_PATTERNS:
            if pattern.match(stripped):
                return True

        return False

    def is_stop_word(self, name: str) -> bool:
        """
        Check if entity name is a stop word.

        Args:
            name: Entity name to check

        Returns:
            True if name is a stop word (should be blocked)
        """
        normalized = name.strip().lower()
        return normalized in self._stop_words

    def is_numeric_only(self, name: str) -> bool:
        """
        Check if entity is purely numeric.

        Blocks: "2030", "35", "1956"
        Allows: "Episode 120", "Project 2025", "CO2"

        Args:
            name: Entity name to check

        Returns:
            True if name is purely numeric (should be blocked)
        """
        normalized = name.strip()
        return bool(re.match(r'^\d+$', normalized))

    def is_tautological(self, name: str, entity_type: str) -> bool:
        """
        Check if entity name equals its type (tautological).

        Blocks: ("organization", "ORGANIZATION"), ("places", "PLACE")
        Allows: ("Regen Network", "ORGANIZATION")

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if name is tautological (should be blocked)
        """
        if not entity_type:
            return False

        # Normalize: lowercase, remove trailing 's', remove underscores
        name_norm = name.strip().lower().rstrip('s').replace('_', ' ')
        type_norm = entity_type.strip().lower().rstrip('s').replace('_', ' ')

        return name_norm == type_norm

    def is_lowercase_person(self, name: str, entity_type: str) -> bool:
        """
        Check if this is a generic lowercase single-word PERSON.

        Blocks: ("mom", "PERSON"), ("friend", "PERSON")
        Allows: ("Aaron", "PERSON"), ("John Smith", "PERSON")
        Allows: Usernames/handles with special chars: ("ryanchristo-Validator", "PERSON")

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if invalid lowercase person (should be blocked)
        """
        if not entity_type or entity_type.upper() != 'PERSON':
            return False

        stripped = name.strip()

        # Must be single word (no spaces)
        if ' ' in stripped:
            return False

        # Must start with lowercase
        if not stripped or not stripped[0].islower():
            return False

        # If contains special characters (-, _, numbers), it's likely a username/handle
        # Usernames should NOT be blocked even if lowercase
        if '-' in stripped or '_' in stripped or any(c.isdigit() for c in stripped):
            return False

        return True

    def matches_generic_pattern(self, name: str) -> bool:
        """
        Check if name matches generic person description patterns.

        Blocks: "the character", "our friends", "some people"
        Allows: "Dr. Jane Goodall", "Gregory Landua"

        Args:
            name: Entity name to check

        Returns:
            True if matches generic pattern (should be blocked)
        """
        stripped = name.strip()

        for pattern in self.GENERIC_PERSON_PATTERNS:
            if pattern.search(stripped):
                return True

        return False

    def is_sentence_like(self, name: str) -> bool:
        """
        Check if name looks like a sentence fragment.

        Blocks: "the most important thing is", "according to research"
        Allows: "Regenerative Agriculture", "Carbon Credit"

        Args:
            name: Entity name to check

        Returns:
            True if sentence-like (should be blocked)
        """
        stripped = name.strip()

        for pattern in self.SENTENCE_PATTERNS:
            if pattern.search(stripped):
                return True

        return False

    def is_jira_issue_id(self, name: str) -> bool:
        """Check if name looks like a JIRA/issue identifier (e.g., APP-776)."""
        return bool(self.JIRA_ISSUE_PATTERN.match(name.strip()))

    def is_erc_standard(self, name: str) -> bool:
        """Check if name matches an ERC standard identifier (e.g., ERC-20)."""
        return bool(self.ERC_STANDARD_PATTERN.match(name.strip()))

    def is_boilerplate(self, name: str) -> bool:
        """Check if name matches known boilerplate/template phrases."""
        normalized = name.strip().lower()
        return any(phrase in normalized for phrase in self.BOILERPLATE_BLOCKLIST)

    def is_placeholder_person(self, name: str, entity_type: str) -> bool:
        """Check for placeholder PERSON entities like 'Public Users'."""
        if not entity_type or entity_type.upper() != 'PERSON':
            return False
        return name.strip().lower() in self.PLACEHOLDER_PERSONS

    def matches_role_pattern(self, name: str, entity_type: str) -> bool:
        """
        FIX-004: Check if name matches multi-word role patterns.

        Only applies to PERSON/HUMANACTOR/ENTITY types.
        Catches: "Development Team", "Partnerships Lead", "Governance Committee"

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if matches a role pattern (should be blocked)

        Examples:
            >>> filter.matches_role_pattern("Partnerships Lead", "PERSON")
            True
            >>> filter.matches_role_pattern("Development Team", "PERSON")
            True
            >>> filter.matches_role_pattern("Gregory Landua", "PERSON")
            False
        """
        if not entity_type or entity_type.upper() not in ('PERSON', 'HUMANACTOR', 'ENTITY'):
            return False

        stripped = name.strip()

        # Patterns are for multi-word roles only
        if len(stripped.split()) < 2:
            return False

        for pattern in self.ROLE_PATTERNS:
            if pattern.search(stripped):
                return True

        return False

    def is_generic_group(self, name: str, entity_type: str) -> bool:
        """
        Check for generic group terms that should not be PERSON entities.

        Blocks: "Buyers", "Partners", "water utilities", "carbon credit buyers",
                "Development Team", "Partnerships Lead"
        Allows: Proper person names.

        FIX-004: Now includes regex patterns for multi-word roles.
        """
        if not entity_type or entity_type.upper() not in ('PERSON', 'HUMANACTOR', 'ENTITY'):
            return False

        normalized = name.strip().lower()

        # 1. Standalone terms (e.g., "buyers", "buyer", "partners")
        if normalized in self.GENERIC_GROUP_TERMS:
            return True

        # 2. Compound terms: check last token ("water utilities", "carbon credit buyers")
        parts = normalized.split()
        if len(parts) >= 2:
            last = parts[-1]
            if last in self.GENERIC_GROUP_TERMS:
                return True

        # 3. FIX-004: Regex patterns for multi-word roles
        if self.matches_role_pattern(name, entity_type):
            return True

        return False

    # ========================================================================
    # FIX-016: Single-Token PERSON Guard Methods
    # ========================================================================

    def has_person_cue_prefix(self, name: str) -> bool:
        """
        FIX-016: Check if name has an explicit PERSON cue prefix.

        Matches: "Dr. Jane", "Dr Jane", "CEO Alice", "CEO: Alice"

        Args:
            name: Entity name to check

        Returns:
            True if name starts with a recognized PERSON cue prefix
        """
        return bool(self.PERSON_CUE_PREFIX_PATTERN.match(name.strip()))

    def is_single_token_person(self, name: str, entity_type: str) -> bool:
        """
        FIX-016: Check if entity is a CAPITALIZED single-token PERSON without cues.

        Blocks capitalized single-token PERSON names (e.g., "Max", "Will") unless:
        1. Has explicit PERSON cue prefix (Dr., CEO, Chairman, etc.)
        2. Is multi-token (full name like "Max Semenchuk")
        3. Is hyphenated/underscored, e.g. "Mary-Jane" (treated as multi-token)

        Note: Lowercase single-token names like "bob" are handled by is_lowercase_person().

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is capitalized single-token PERSON without cues)

        Examples:
            >>> filter.is_single_token_person("Max", "PERSON")
            True  # Blocked: capitalized single-token, no cue
            >>> filter.is_single_token_person("bob", "PERSON")
            False  # Not blocked here: lowercase (handled by is_lowercase_person)
            >>> filter.is_single_token_person("Dr. Jane", "PERSON")
            False  # Allowed: has "Dr." prefix
            >>> filter.is_single_token_person("CEO: Alice", "PERSON")
            False  # Allowed: has "CEO:" prefix
            >>> filter.is_single_token_person("Max Semenchuk", "PERSON")
            False  # Allowed: multi-token (full name)
            >>> filter.is_single_token_person("Mary-Jane", "PERSON")
            False  # Allowed: hyphenated (multi-token)
            >>> filter.is_single_token_person("Max_Semenchuk", "PERSON")
            False  # Allowed: underscored (multi-token)
            >>> filter.is_single_token_person("Max", "ORGANIZATION")
            False  # Not applicable: wrong type
        """
        if not entity_type or entity_type.upper() != 'PERSON':
            return False

        stripped = name.strip()

        # Allow if has PERSON cue prefix
        if self.has_person_cue_prefix(stripped):
            return False

        # Tokenize on whitespace, hyphens, and underscores
        # This treats "Mary-Jane" and "Max_Semenchuk" as multi-token
        tokens = re.split(r'[\s_-]+', stripped)
        tokens = [t for t in tokens if t]  # Remove empty tokens

        if len(tokens) != 1:
            return False

        # If lowercase single-token, let is_lowercase_person() handle it
        # This guard only blocks CAPITALIZED single-token names like "Max", "Will"
        if stripped[0].islower():
            return False

        # Capitalized single-token PERSON without cue - block it
        return True

    # ========================================================================
    # FIX-002: New Filter Methods
    # ========================================================================

    def is_git_changelog_claim(self, name: str, entity_type: str) -> bool:
        """
        FIX-002: Check if entity looks like a git commit or changelog entry.

        Only blocks when entity type is CLAIM, EVIDENCE, or QUESTION.
        Git commits and changelog entries should not be extracted as claims.

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is git/changelog AND type is CLAIM/EVIDENCE/QUESTION)

        Examples:
            >>> filter.is_git_changelog_claim("feat(api): add endpoint", "CLAIM")
            True
            >>> filter.is_git_changelog_claim("feat(api): add endpoint", "CONCEPT")
            False  # Wrong type, don't block
            >>> filter.is_git_changelog_claim("v1.2.3", "EVIDENCE")
            True
        """
        if not entity_type:
            return False

        # Only check for CLAIM, EVIDENCE, or QUESTION types
        upper_type = entity_type.upper()
        if upper_type not in ('CLAIM', 'EVIDENCE', 'QUESTION'):
            return False

        stripped = name.strip()
        for pattern in self.GIT_CHANGELOG_PATTERNS:
            if pattern.search(stripped):
                return True

        return False

    def is_ai_mistyped_as_person(self, name: str, entity_type: str) -> bool:
        """
        FIX-002: Check if entity is an AI system mis-typed as PERSON.

        AI systems like ChatGPT, Claude, etc. should be typed as TECHNOLOGY,
        not PERSON. This blocks them only when incorrectly typed as PERSON.

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is AI system AND type is PERSON)

        Examples:
            >>> filter.is_ai_mistyped_as_person("ChatGPT", "PERSON")
            True
            >>> filter.is_ai_mistyped_as_person("ChatGPT", "TECHNOLOGY")
            False  # Correct type, don't block
            >>> filter.is_ai_mistyped_as_person("Gregory Landua", "PERSON")
            False  # Not an AI system
        """
        if not entity_type or entity_type.upper() != 'PERSON':
            return False

        normalized = name.strip().lower()
        return normalized in self.AI_SOFTWARE_BLOCKLIST

    def is_generic_event(self, name: str, entity_type: str) -> bool:
        """
        FIX-002: Check if entity is a generic event word.

        Generic event words like "meeting", "call", "community call" should
        not be extracted as EVENT entities. Only named events with specific
        titles (e.g., "Regen Gathering 2024", "COP28") should be EVENT.

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is generic event AND type is EVENT)

        Examples:
            >>> filter.is_generic_event("meeting", "EVENT")
            True
            >>> filter.is_generic_event("community call", "EVENT")
            True
            >>> filter.is_generic_event("Regen Gathering 2024", "EVENT")
            False  # Specific named event
            >>> filter.is_generic_event("meeting", "CONCEPT")
            False  # Wrong type, don't block
        """
        if not entity_type or entity_type.upper() != 'EVENT':
            return False

        normalized = name.strip().lower()

        # Check single-word generic terms
        if normalized in self.GENERIC_EVENT_TERMS:
            return True

        # Check compound generic patterns
        for pattern in self.GENERIC_EVENT_PATTERNS:
            if pattern.match(normalized):
                return True

        return False

    # ========================================================================
    # E325: FirstName-OrgName Artifact Detection
    # ========================================================================

    def is_firstname_orgname_artifact(self, name: str, entity_type: str) -> bool:
        """
        E325: Check if entity is a FirstName-OrgName extraction artifact.

        LLM extraction sometimes merges a first name with an organization name
        when they appear adjacent in text (e.g., "Will said Regen Foundation..."
        becomes "Will-Regen Foundation" as a PERSON entity).

        This blocks patterns like:
        - Will-Regen Foundation
        - Chris-Chainflow
        - Curtis-Meme_Network

        Does NOT block legitimate names:
        - Mary-Jane (hyphenated first name)
        - Smith-Jones (hyphenated surname)
        - Jean-Pierre (French name)

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is FirstName-OrgName AND type is PERSON)

        Examples:
            >>> filter.is_firstname_orgname_artifact("Will-Regen Foundation", "PERSON")
            True
            >>> filter.is_firstname_orgname_artifact("Chris-Chainflow", "PERSON")
            True
            >>> filter.is_firstname_orgname_artifact("Curtis-Meme_Network", "PERSON")
            True
            >>> filter.is_firstname_orgname_artifact("Mary-Jane", "PERSON")
            False  # Legitimate hyphenated first name
            >>> filter.is_firstname_orgname_artifact("Will-Regen Foundation", "ORGANIZATION")
            False  # Wrong type, don't block
            >>> filter.is_firstname_orgname_artifact("Gregory Landua", "PERSON")
            False  # Normal name
        """
        if not entity_type or entity_type.upper() != 'PERSON':
            return False

        stripped = name.strip()

        # Must contain hyphen or underscore
        if '-' not in stripped and '_' not in stripped:
            return False

        # Match against the FirstName-OrgName pattern
        if self.FIRSTNAME_ORGNAME_PATTERN.match(stripped):
            return True

        return False

    # ========================================================================
    # FIX-011: Blockchain as LOCATION Detection
    # ========================================================================

    def is_blockchain_as_location(self, name: str, entity_type: str) -> bool:
        """
        FIX-011: Check if entity is a blockchain name mis-typed as LOCATION.

        Blockchain names like "ethereum", "polygon", "solana" are sometimes
        extracted as LOCATION by LLMs when they appear in geographic-like contexts.
        These should always be TECHNOLOGY or PROJECT, never LOCATION.

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is blockchain name AND type is LOCATION)

        Examples:
            >>> filter.is_blockchain_as_location("Ethereum", "LOCATION")
            True
            >>> filter.is_blockchain_as_location("Ethereum", "TECHNOLOGY")
            False  # Correct type, don't block
            >>> filter.is_blockchain_as_location("Boulder", "LOCATION")
            False  # Legitimate location
            >>> filter.is_blockchain_as_location("Polygon", "LOCATION")
            True  # L2 chain, not location
            >>> filter.is_blockchain_as_location("Base", "LOCATION")
            True  # Coinbase L2, not location
        """
        if not entity_type or entity_type.upper() != 'LOCATION':
            return False

        normalized = name.strip().lower()

        # Direct match against blockchain names
        if normalized in self.BLOCKCHAIN_NAMES:
            return True

        # Also check if it starts with a known blockchain name (e.g., "ethereum mainnet")
        for blockchain in self.BLOCKCHAIN_NAMES:
            if normalized.startswith(blockchain + ' ') or normalized.endswith(' ' + blockchain):
                return True

        return False

    # ========================================================================
    # FIX-013: Code Module as PROCESS Detection
    # ========================================================================

    def is_code_module_as_process(self, name: str, entity_type: str) -> bool:
        """
        FIX-013: Check if entity is a code module name mis-typed as PROCESS.

        Code modules like EntityQualityFilter, CanonicalResolver are software
        components (TECHNOLOGY), not processes. This blocks them when incorrectly
        typed as PROCESS.

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is code module AND type is PROCESS)

        Examples:
            >>> filter.is_code_module_as_process("EntityQualityFilter", "PROCESS")
            True
            >>> filter.is_code_module_as_process("EntityQualityFilter", "TECHNOLOGY")
            False  # Correct type, don't block
            >>> filter.is_code_module_as_process("CanonicalResolver", "PROCESS")
            True
            >>> filter.is_code_module_as_process("verification", "PROCESS")
            False  # Legitimate process name
        """
        if not entity_type or entity_type.upper() != 'PROCESS':
            return False

        normalized = name.strip().lower()

        # Direct match against known code module names
        if normalized in self.CODE_MODULE_NAMES:
            return True

        # Check CamelCase pattern (e.g., DataLoader, ConfigParser)
        stripped = name.strip()
        if self.CODE_MODULE_PATTERN.match(stripped):
            return True

        return False

    # ========================================================================
    # FIX-014: Abstract Concept as MATERIAL Detection
    # ========================================================================

    def is_abstract_concept_as_material(self, name: str, entity_type: str) -> bool:
        """
        FIX-014: Check if entity is an abstract concept mis-typed as MATERIAL.

        Abstract environmental/ecological concepts like biodiversity, carbon
        sequestration should be CONCEPT, not MATERIAL. MATERIAL should be
        reserved for physical substances.

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is abstract concept AND type is MATERIAL)

        Examples:
            >>> filter.is_abstract_concept_as_material("biodiversity", "MATERIAL")
            True
            >>> filter.is_abstract_concept_as_material("biodiversity", "CONCEPT")
            False  # Correct type, don't block
            >>> filter.is_abstract_concept_as_material("carbon sequestration", "MATERIAL")
            True
            >>> filter.is_abstract_concept_as_material("wood", "MATERIAL")
            False  # Legitimate physical material
            >>> filter.is_abstract_concept_as_material("steel", "MATERIAL")
            False  # Legitimate physical material
        """
        if not entity_type or entity_type.upper() != 'MATERIAL':
            return False

        normalized = name.strip().lower()

        # Direct match against known abstract concepts
        if normalized in self.ABSTRACT_CONCEPT_AS_MATERIAL:
            return True

        return False

    # ========================================================================
    # FIX-005: Domain Identifier Methods
    # ========================================================================

    def is_allowed_domain_identifier(self, name: str, entity_type: str) -> bool:
        """
        FIX-005: Check if name is a legitimate domain identifier.

        Domain identifiers like MsgSend, C01, x/ecocredit should NOT be blocked
        as technical patterns when they have the correct entity type.

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if this is a valid domain identifier that should pass through

        Examples:
            >>> filter.is_allowed_domain_identifier("MsgSend", "API_MESSAGE")
            True
            >>> filter.is_allowed_domain_identifier("C01", "CREDIT_CLASS")
            True
            >>> filter.is_allowed_domain_identifier("x/ecocredit", "MODULE")
            True
            >>> filter.is_allowed_domain_identifier("MsgSend", "CONCEPT")
            False  # Wrong type
        """
        if not entity_type:
            return False

        upper_type = entity_type.upper()

        # Only check for relevant domain types
        if upper_type not in ('API_MESSAGE', 'MODULE', 'CREDIT_CLASS', 'KEEPER'):
            return False

        stripped = name.strip()

        for pattern in self.DOMAIN_IDENTIFIER_PATTERNS:
            if pattern.match(stripped):
                return True

        return False

    def is_generic_domain_type_word(self, name: str, entity_type: str) -> bool:
        """
        FIX-005: Check if name is a generic word mis-typed as a domain type.

        Block generic role words like "validator" even if the LLM assigns
        the VALIDATOR type. These should not be entities.

        Args:
            name: Entity name to check
            entity_type: Entity type

        Returns:
            True if should be blocked (is generic word for this domain type)

        Examples:
            >>> filter.is_generic_domain_type_word("validators", "VALIDATOR")
            True
            >>> filter.is_generic_domain_type_word("keepers", "KEEPER")
            True
            >>> filter.is_generic_domain_type_word("Chorus One", "VALIDATOR")
            False  # Specific validator name
            >>> filter.is_generic_domain_type_word("validators", "PERSON")
            False  # Handled by generic_group check
        """
        if not entity_type:
            return False

        upper_type = entity_type.upper()

        # Check if this type has generic words to block
        if upper_type not in self.GENERIC_DOMAIN_TYPE_WORDS:
            return False

        normalized = name.strip().lower()

        return normalized in self.GENERIC_DOMAIN_TYPE_WORDS[upper_type]

    def exceeds_length_limits(self, name: str) -> bool:
        """
        Check if name exceeds length limits.

        Args:
            name: Entity name to check

        Returns:
            True if too long (should be blocked)
        """
        stripped = name.strip()

        # Check character length
        if len(stripped) > self.config.max_name_length:
            return True

        # Check word count
        word_count = len(stripped.split())
        if word_count > self.config.max_word_count:
            return True

        return False

    def is_technical_pattern(self, name: str) -> bool:
        """
        Check if name matches technical patterns that shouldn't be entities.

        Blocks: URLs, IP addresses, blockchain addresses, file paths, code identifiers
        Allows: Proper entity names

        Args:
            name: Entity name to check

        Returns:
            True if matches technical pattern (should be blocked)
        """
        stripped = name.strip()

        for pattern in self.TECHNICAL_PATTERNS:
            if pattern.search(stripped):
                return True

        return False

    def filter_with_reasons(self, name: str, entity_type: str = '') -> Tuple[bool, List[str]]:
        """
        Apply all filters to an entity and return all reasons.

        Args:
            name: Entity name to check
            entity_type: Entity type (optional)

        Returns:
            Tuple of (is_valid, list_of_rejection_reasons)
            - (True, []) if entity passes all filters
            - (False, ["reason1", "reason2", ...]) if entity should be filtered
        """
        reasons = []

        # FIX-016: Single-token PERSON guard runs BEFORE whitelist
        if self.is_single_token_person(name, entity_type):
            reasons.append("single_token_person")

        # 0. Whitelist check - if whitelisted, skip all other checks
        # (but still return single_token_person if caught above)
        if self.is_whitelisted(name):
            return (len(reasons) == 0, reasons)

        # ====================================================================
        # FIX-003: Early checks for min-length and placeholder
        # ====================================================================

        # 0.1 FIX-003: Min-length check (before anything else)
        if self.is_too_short(name):
            reasons.append("too_short")

        # 0.2 FIX-003: Placeholder check (applies to ALL types)
        if self.is_placeholder(name, entity_type):
            reasons.append("placeholder")

        # 1. Stop word check
        if self.is_stop_word(name):
            reasons.append("stop_word")

        # 2. Numeric only check
        if self.is_numeric_only(name):
            reasons.append("numeric_only")

        # 3. Tautological check
        if self.is_tautological(name, entity_type):
            reasons.append("tautological")

        # 4. Lowercase single-word PERSON check
        if self.is_lowercase_person(name, entity_type):
            reasons.append("lowercase_person")

        # 5. Placeholder PERSON entities
        if self.is_placeholder_person(name, entity_type):
            reasons.append("placeholder_person")

        # 5.5 Generic group terms extracted as PERSON
        if self.is_generic_group(name, entity_type):
            reasons.append("generic_group")

        # 6. Template/ID patterns
        if self.is_erc_standard(name):
            reasons.append("erc_standard")
        elif self.is_jira_issue_id(name):
            reasons.append("jira_issue_id")

        # 7. Boilerplate/template text
        if self.is_boilerplate(name):
            reasons.append("boilerplate")

        # 8. Generic pattern check
        if self.matches_generic_pattern(name):
            reasons.append("generic_pattern")

        # 9. Sentence-like check
        if self.is_sentence_like(name):
            reasons.append("sentence_like")

        # 10. Length limits check
        if self.exceeds_length_limits(name):
            reasons.append("too_long")

        # 11. Technical pattern check
        # FIX-005: Skip for valid domain identifiers
        if self.is_technical_pattern(name):
            if not self.is_allowed_domain_identifier(name, entity_type):
                reasons.append("technical_pattern")

        # ====================================================================
        # FIX-002: New filter checks
        # ====================================================================

        # 12. Git/changelog as CLAIM/EVIDENCE/QUESTION
        if self.is_git_changelog_claim(name, entity_type):
            reasons.append("git_changelog")

        # 13. AI systems mis-typed as PERSON
        if self.is_ai_mistyped_as_person(name, entity_type):
            reasons.append("ai_as_person")

        # 14. Generic event words as EVENT
        if self.is_generic_event(name, entity_type):
            reasons.append("generic_event")

        # ====================================================================
        # FIX-005: Domain type checks
        # ====================================================================

        # 15. Generic domain type words
        if self.is_generic_domain_type_word(name, entity_type):
            reasons.append("generic_domain_type")

        # ====================================================================
        # E325: FirstName-OrgName artifact detection
        # ====================================================================

        # 16. FirstName-OrgName artifacts (e.g., "Will-Regen Foundation")
        if self.is_firstname_orgname_artifact(name, entity_type):
            reasons.append("firstname_orgname_artifact")

        # ====================================================================
        # FIX-011: Blockchain as LOCATION
        # ====================================================================

        # 17. Blockchain names mis-typed as LOCATION
        if self.is_blockchain_as_location(name, entity_type):
            reasons.append("blockchain_as_location")

        # ====================================================================
        # FIX-013: Code module as PROCESS (Week 6)
        # ====================================================================

        # 18. Code module names mis-typed as PROCESS
        if self.is_code_module_as_process(name, entity_type):
            reasons.append("code_module_as_process")

        # ====================================================================
        # FIX-014: Abstract concept as MATERIAL (Week 6)
        # ====================================================================

        # 19. Abstract concepts mis-typed as MATERIAL
        if self.is_abstract_concept_as_material(name, entity_type):
            reasons.append("abstract_concept_as_material")

        is_valid = len(reasons) == 0
        return is_valid, reasons

    def filter_entity(self, entity: Dict) -> Tuple[bool, str]:
        """
        Apply all filters to a single entity.

        Args:
            entity: Dictionary with at least 'name' key, optionally 'type'

        Returns:
            Tuple of (passes_filter, rejection_reason)
            - (True, "") if entity passes all filters
            - (False, "reason") if entity should be filtered out
        """
        name = entity.get('name', '')
        entity_type = entity.get('type', '')

        # FIX-016: Single-token PERSON guard runs BEFORE whitelist
        # This prevents whitelist from bypassing this specific guard
        if self.is_single_token_person(name, entity_type):
            return (False, "single_token_person")

        # Check whitelist (includes built-in + user config)
        if self.is_whitelisted(name):
            return (True, "")

        # ====================================================================
        # FIX-003: Early checks for min-length and placeholder
        # ====================================================================

        # 0.1 FIX-003: Min-length check (before anything else)
        if self.is_too_short(name):
            return (False, "too_short")

        # 0.2 FIX-003: Placeholder check (applies to ALL types)
        if self.is_placeholder(name, entity_type):
            return (False, "placeholder")

        # 1. Stop word check
        if self.is_stop_word(name):
            return (False, "stop_word")

        # 2. Numeric only check
        if self.is_numeric_only(name):
            return (False, "numeric_only")

        # 3. Tautological check
        if self.is_tautological(name, entity_type):
            return (False, "tautological")

        # 4. Lowercase single-word PERSON check
        if self.is_lowercase_person(name, entity_type):
            return (False, "lowercase_person")

        # 5. Placeholder PERSON entities
        if self.is_placeholder_person(name, entity_type):
            return (False, "placeholder_person")

        # 5.5 Generic group terms as PERSON
        if self.is_generic_group(name, entity_type):
            return (False, "generic_group")

        # 6. Template/ID patterns
        if self.is_erc_standard(name):
            return (False, "erc_standard")
        if self.is_jira_issue_id(name):
            return (False, "jira_issue_id")

        # 7. Boilerplate/template text
        if self.is_boilerplate(name):
            return (False, "boilerplate")

        # 8. Generic pattern check
        if self.matches_generic_pattern(name):
            return (False, "generic_pattern")

        # 9. Sentence-like check
        if self.is_sentence_like(name):
            return (False, "sentence_like")

        # 10. Length limits check
        if self.exceeds_length_limits(name):
            return (False, "too_long")

        # 11. Technical pattern check
        # FIX-005: Skip for valid domain identifiers (MsgSend, C01, x/ecocredit)
        if self.is_technical_pattern(name):
            if not self.is_allowed_domain_identifier(name, entity_type):
                return (False, "technical_pattern")

        # ====================================================================
        # FIX-002: New filter checks
        # ====================================================================

        # 12. Git/changelog as CLAIM/EVIDENCE/QUESTION
        if self.is_git_changelog_claim(name, entity_type):
            return (False, "git_changelog")

        # 13. AI systems mis-typed as PERSON
        if self.is_ai_mistyped_as_person(name, entity_type):
            return (False, "ai_as_person")

        # 14. Generic event words as EVENT
        if self.is_generic_event(name, entity_type):
            return (False, "generic_event")

        # ====================================================================
        # FIX-005: Domain type checks
        # ====================================================================

        # 15. Generic domain type words (e.g., "validator" as VALIDATOR)
        if self.is_generic_domain_type_word(name, entity_type):
            return (False, "generic_domain_type")

        # ====================================================================
        # E325: FirstName-OrgName artifact detection
        # ====================================================================

        # 16. FirstName-OrgName artifacts (e.g., "Will-Regen Foundation")
        if self.is_firstname_orgname_artifact(name, entity_type):
            return (False, "firstname_orgname_artifact")

        # ====================================================================
        # FIX-011: Blockchain as LOCATION
        # ====================================================================

        # 17. Blockchain names mis-typed as LOCATION
        if self.is_blockchain_as_location(name, entity_type):
            return (False, "blockchain_as_location")

        # ====================================================================
        # FIX-013: Code module as PROCESS (Week 6)
        # ====================================================================

        # 18. Code module names mis-typed as PROCESS
        if self.is_code_module_as_process(name, entity_type):
            return (False, "code_module_as_process")

        # ====================================================================
        # FIX-014: Abstract concept as MATERIAL (Week 6)
        # ====================================================================

        # 19. Abstract concepts mis-typed as MATERIAL
        if self.is_abstract_concept_as_material(name, entity_type):
            return (False, "abstract_concept_as_material")

        return (True, "")

    def filter_batch(self, entities: List[Dict]) -> List[Dict]:
        """
        Filter a batch of entities, returning only those that pass.

        Updates internal statistics.

        Args:
            entities: List of entity dictionaries

        Returns:
            List of entities that pass all filters
        """
        passed = []

        for entity in entities:
            self._stats['total_checked'] += 1

            passes, reason = self.filter_entity(entity)

            if passes:
                self._stats['total_passed'] += 1
                passed.append(entity)
            else:
                self._stats['total_filtered'] += 1
                if reason in self._stats['reasons']:
                    self._stats['reasons'][reason] += 1

        return passed

    def get_stats(self) -> Dict:
        """
        Get filtering statistics.

        Returns:
            Dictionary with:
            - total_checked: Total entities processed
            - total_filtered: Entities filtered out
            - total_passed: Entities that passed
            - reasons: Breakdown by rejection reason
        """
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset all statistics counters."""
        self._stats = {
            'total_checked': 0,
            'total_filtered': 0,
            'total_passed': 0,
            'reasons': {
                'stop_word': 0,
                'numeric_only': 0,
                'tautological': 0,
                'lowercase_person': 0,
                'generic_pattern': 0,
                'sentence_like': 0,
                'too_long': 0,
                'technical_pattern': 0,
                'jira_issue_id': 0,
                'erc_standard': 0,
                'boilerplate': 0,
                'placeholder_person': 0,
                'generic_group': 0,
                # FIX-002: New filter reasons
                'git_changelog': 0,
                'ai_as_person': 0,
                'generic_event': 0,
                # FIX-003: New filter reasons
                'too_short': 0,
                'placeholder': 0,
                # FIX-005: New filter reasons
                'generic_domain_type': 0,
                # E325: FirstName-OrgName artifact
                'firstname_orgname_artifact': 0,
                # FIX-011: Blockchain as LOCATION
                'blockchain_as_location': 0,
                # FIX-013: Code module as PROCESS
                'code_module_as_process': 0,
                # FIX-014: Abstract concept as MATERIAL
                'abstract_concept_as_material': 0,
                # FIX-016: Single-token PERSON
                'single_token_person': 0,
            }
        }

    def get_filtered_with_reasons(self, entities: List[Dict]) -> Tuple[List[Dict], List[Tuple[Dict, str]]]:
        """
        Filter entities and return both passed and filtered with reasons.

        Useful for debugging and analysis.

        Args:
            entities: List of entity dictionaries

        Returns:
            Tuple of (passed_entities, filtered_entities_with_reasons)
            where filtered_entities_with_reasons is List[(entity, reason)]
        """
        passed = []
        filtered = []

        for entity in entities:
            passes, reason = self.filter_entity(entity)

            if passes:
                passed.append(entity)
            else:
                filtered.append((entity, reason))

        return passed, filtered

    def generate_report(self, entities: List[Dict]) -> str:
        """
        Generate a human-readable report on entity quality.

        Args:
            entities: List of entity dictionaries to analyze

        Returns:
            Formatted string report
        """
        self.reset_stats()
        passed, filtered = self.get_filtered_with_reasons(entities)

        # Update stats
        self._stats['total_checked'] = len(entities)
        self._stats['total_passed'] = len(passed)
        self._stats['total_filtered'] = len(filtered)

        for _, reason in filtered:
            if reason in self._stats['reasons']:
                self._stats['reasons'][reason] += 1

        # Build report
        lines = [
            "=" * 60,
            "ENTITY QUALITY FILTER REPORT",
            "=" * 60,
            "",
            f"Total entities analyzed: {len(entities)}",
            f"Passed filters: {len(passed)} ({100*len(passed)/len(entities):.1f}%)" if entities else "Passed: 0",
            f"Filtered out: {len(filtered)} ({100*len(filtered)/len(entities):.1f}%)" if entities else "Filtered: 0",
            "",
            "Rejection reasons breakdown:",
        ]

        for reason, count in sorted(self._stats['reasons'].items(), key=lambda x: -x[1]):
            if count > 0:
                pct = 100 * count / len(entities) if entities else 0
                lines.append(f"  - {reason}: {count} ({pct:.1f}%)")

        if filtered:
            lines.extend([
                "",
                "Sample filtered entities (first 10):",
            ])
            for entity, reason in filtered[:10]:
                name = entity.get('name', 'N/A')[:40]
                etype = entity.get('type', 'N/A')
                lines.append(f"  - '{name}' ({etype}) -> {reason}")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)


# Convenience function for quick filtering
def filter_entities(entities: List[Dict], config: Optional[FilterConfig] = None) -> List[Dict]:
    """
    Quick utility to filter a list of entities.

    Args:
        entities: List of entity dictionaries
        config: Optional FilterConfig

    Returns:
        List of entities that pass all quality filters
    """
    filter_instance = EntityQualityFilter(config)
    return filter_instance.filter_batch(entities)
