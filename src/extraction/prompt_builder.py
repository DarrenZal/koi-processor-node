"""
Shared prompt builder for LLM-based entity extraction.

This module provides a single source of truth for extraction prompts,
ensuring consistency across all extractors (OpenAI, Ollama/Mistral, etc.).

FIX-002: Extractor/Schema Unification + Prompt Hardening
Week 12: Added CANONICAL_PREDICATES allowlist
Week 13: Moved CANONICAL_PREDICATES to predicate_guard.py for shared access
"""

from typing import Dict, Any, Optional

# Import canonical types
try:
    from core.entity_types import LLM_ALLOWED_TYPES
except ImportError:
    try:
        from src.core.entity_types import LLM_ALLOWED_TYPES
    except ImportError:
        # Fallback for testing
        LLM_ALLOWED_TYPES = {
            "PERSON", "ORGANIZATION", "PROJECT", "CONCEPT",
            "TECHNOLOGY", "CLAIM", "EVIDENCE", "QUESTION",
            "LOCATION", "EVENT"
        }

# Import canonical predicates from shared module (Week 13)
try:
    from .predicate_guard import CANONICAL_PREDICATES
except ImportError:
    try:
        from extraction.predicate_guard import CANONICAL_PREDICATES
    except ImportError:
        from src.extraction.predicate_guard import CANONICAL_PREDICATES


def build_extraction_prompt(
    content: str,
    source_type: str,
    metadata: Optional[Dict[str, Any]] = None,
    max_content_length: int = 3000
) -> str:
    """
    Build a unified extraction prompt for LLM-based entity extraction.

    This is the single source of truth for all extractors. Changes here
    affect both OpenAI and Ollama/Mistral extractors.

    Args:
        content: Text content to analyze
        source_type: Type of source (discourse, twitter, medium, github, etc.)
        metadata: Optional existing metadata from sensors
        max_content_length: Maximum content length to include (default 3000)

    Returns:
        Complete extraction prompt string

    Notes:
        - Entity types are uppercase from LLM_ALLOWED_TYPES
        - AI systems should be TECHNOLOGY, never PERSON
        - Git commits/changelog lines should NOT be extracted
        - Generic event words should NOT be extracted as EVENT
    """
    # Truncate content for context window
    content_snippet = content[:max_content_length] if len(content) > max_content_length else content

    # Build allowed types string
    allowed_types_str = ", ".join(sorted(LLM_ALLOWED_TYPES))

    prompt = f"""Extract structured data from this {source_type} content.

CONTENT:
{content_snippet}

Extract and return JSON with:
1. Metadata fields with confidence scores (0.0-1.0)
2. Entities based on Regen Network ontology
3. Relationships between entities
4. Summary

## ALLOWED ENTITY TYPES (use UPPERCASE exactly as shown)
{allowed_types_str}

## CRITICAL TYPE RULES

### AI Systems are TECHNOLOGY, never PERSON
AI systems, LLMs, and chatbots MUST be typed as TECHNOLOGY:
- ChatGPT, GPT-4, GPT-3, GPT-4o, Claude, Copilot, Bard, Gemini
- LLaMA, Mistral, OpenAI, Anthropic, Dall-E, Midjourney
- Any AI model, chatbot, or automated assistant
DO NOT type these as PERSON. They are TECHNOLOGY.

### LOCATION: Geographic places only
Use LOCATION for: Countries, cities, regions, states, addresses
Examples: "Boulder, Colorado" (LOCATION), "Colombia" (LOCATION), "United States" (LOCATION)
NOT for: Projects, organizations, or concepts

### EVENT: Only named events with specific titles
ONLY extract as EVENT if the content names a SPECIFIC event with a REAL TITLE.
Valid EVENT examples:
- "Regen Network Community Call" (specific named event)
- "Regen Gathering 2024" (specific named event)
- "COP28" (specific named event)
- "ETHDenver 2024" (specific named event)

DO NOT extract generic event words as EVENT:
- "meeting" - NOT an event
- "call" - NOT an event
- "community call" (without specific title) - NOT an event
- "weekly meeting" - NOT an event
- "workshop" - NOT an event
- "conference" - NOT an event (unless specifically named)
- "webinar" - NOT an event
- "session" - NOT an event
- "discussion" - NOT an event

Rule: If it's a generic word without a specific event name, DO NOT extract it.

## DO NOT EXTRACT (CRITICAL)

### Git commits, changelog entries, version strings
DO NOT extract these patterns as entities:
- "feat(api): add endpoint" - git commit
- "fix: resolve bug" - git commit
- "chore(deps): update dependencies" - git commit
- "Merge pull request #123" - git merge
- "v1.2.3" or "1.0.0-beta" - version strings
- "[Added] New feature" - changelog entry
- "[Fixed] Bug in module" - changelog entry

### Templates and boilerplate
- "Testing Instructions" - template text
- "Acceptance Criteria" - template text
- "Definition of Done" - template text
- "DRY Principles" - template reference
- Issue IDs like "APP-776", "JIRA-123"

### Generic nouns and pronouns
- Pronouns: we, they, it, our, their, you, he, she
- Generic groups: buyers, sellers, partners, users, members, contributors
- Generic roles: validators, participants, stakeholders, developers
- Placeholders: Unknown, Anonymous, Public Users, TBD

### Technical patterns
- URLs: https://example.com
- IP addresses: 192.168.1.1
- File paths: /path/to/file.py
- Code identifiers: snake_case_var, camelCaseVar
- Blockchain addresses: regen1..., cosmos1..., 0x...

## CONCEPT vs PROJECT
- CONCEPT: Abstract ideas, methodologies, frameworks, theories
  Examples: "regenerative agriculture", "proof of stake", "carbon sequestration"
- PROJECT: Concrete initiatives, platforms, named programs
  Examples: "Regen Ledger" (project), "Koi Project" (initiative)
Rule: Abstract idea → CONCEPT. Named implementation → PROJECT or TECHNOLOGY.

## CONCEPT RELATIONSHIPS (IMPORTANT - Week 15)

Core domain concepts MUST have relationships to related concepts. When extracting
concepts like "carbon", "carbon credits", "NCT", or other domain terms, ALWAYS
create relationships connecting them to related entities.

### Required concept predicates (use these for concepts):
- relates_to: Connect related concepts (carbon relates_to carbon markets)
- part_of: Hierarchical concept relationships (NCT part_of carbon markets)
- is_a: Type/classification relationships (NCT token is_a carbon credit)
- includes: Container relationships (carbon markets includes carbon credits)
- used_in: Application relationships (carbon used_in carbon sequestration)
- associated_with: General concept associations

### Core domain concept examples:
Input: "The NCT token represents carbon credits on the Toucan Protocol."
Extract relationships:
- (NCT token, represents, carbon credits) - 0.90
- (NCT token, associated_with, Toucan Protocol) - 0.90
- (carbon credits, part_of, carbon markets) - 0.85

Input: "Carbon sequestration is a key mechanism in regenerative agriculture for carbon removal."
Extract relationships:
- (carbon sequestration, part_of, regenerative agriculture) - 0.90
- (carbon sequestration, relates_to, carbon removal) - 0.85
- (carbon, used_in, carbon sequestration) - 0.85

### MUST extract relationships for these core concepts:
- "carbon" → connect to carbon credits, carbon markets, carbon sequestration
- "NCT", "NCT token", "Nature Carbon Tonne" → connect to Toucan, carbon credits, Regen
- "carbon credits" → connect to carbon markets, verification, retirement
- "carbon sequestration" → connect to carbon, soil, regenerative agriculture

## FIX-005: DOMAIN TYPES (Regen/Cosmos)

### CREDIT_CLASS: Regen Network credit classes
Use CREDIT_CLASS for carbon/eco credit classifications:
- "C01", "C02", "C03" (credit class IDs)
- "CarbonPlus Grasslands", "Wilmot Cattle Grazing" (credit class names)
- "Verified Carbon Standard", "Gold Standard" (certification programs)
NOT for: Organizations that issue credits (those are ORGANIZATION)

### GOVERNANCE_PROPOSAL: On-chain proposals
Use GOVERNANCE_PROPOSAL for blockchain governance proposals:
- "Proposal 47", "Signaling Proposal: Community Pool Spend"
- "Parameter Change Proposal", "Text Proposal"
NOT for: General ideas or suggestions (those are CONCEPT)

### VALIDATOR: Blockchain validators
Use VALIDATOR for blockchain validator operators:
- "Regen Validator", "Chorus One", "Figment"
- Validator node operators and their infrastructure
NOT for: The "validator" role term (blocked as generic)

### MODULE: Cosmos SDK modules
Use MODULE for Cosmos SDK/blockchain modules:
- "x/ecocredit", "x/group", "x/data", "x/staking"
- "EcocreditModule", "GroupModule"
NOT for: General software modules (those are TECHNOLOGY)

### API_MESSAGE: Protobuf message types
Use API_MESSAGE for Cosmos SDK message types:
- "MsgSend", "MsgCreateBatch", "MsgRetire"
- "MsgVote", "MsgDelegate", "MsgSubmitProposal"
NOT for: General API endpoints (those are TECHNOLOGY)

### KEEPER: Cosmos SDK keepers
Use KEEPER for SDK keeper interfaces:
- "EcocreditKeeper", "GroupKeeper", "BankKeeper"
NOT for: The "keeper" role term (blocked as generic)

## FIX-005: GENERAL TYPES

### LICENSE: Software/content licenses
Use LICENSE for licensing terms:
- "Apache 2.0", "MIT License", "GPL-3.0"
- "CC BY-SA 4.0", "Creative Commons"
NOT for: Organizations that create licenses (those are ORGANIZATION)

### STANDARD: Technical standards
Use STANDARD for specifications and standards:
- "ISO 14064", "ISO 14067", "GHG Protocol"
- "Verra VM0042", "VCS Standard"
NOT for: Organizations that publish standards (those are ORGANIZATION)

### PROCESS: Business/technical processes
Use PROCESS for named processes:
- "MRV Process", "Verification Process"
- "Credit Issuance Workflow"
NOT for: Actions or verbs (skip those)

### MATERIAL: Physical materials
Use MATERIAL for physical substances:
- "Biochar", "Biomass", "Soil Carbon"
- "Organic Matter", "Compost"
NOT for: Abstract concepts (those are CONCEPT)

## RELATIONSHIP PREDICATES FOR TECHNOLOGY/PLATFORM ENTITIES

When extracting relationships involving TECHNOLOGY, PLATFORM, or TOOL entities, use these specific predicates:

### Usage relationships
- uses: Subject actively uses the platform/tool (also for hosting)
  - "Regen Network uses Notion for documentation" → (Regen Network, uses, Notion)
  - "The team uses Slack for communication" → (team, uses, Slack)
  - "The forum is hosted on Discourse" → (forum, uses, Discourse)
  - "Project code is hosted on GitHub" → (Project, uses, GitHub)

- powered_by: System is powered by a technology
  - "Search is powered by PostgreSQL" → (Search, powered_by, PostgreSQL)

### Integration relationships
- integrates_with: Two systems connect to each other
  - "The bot integrates with Discord" → (bot, integrates_with, Discord)
  - "n8n.io connects to Notion" → (n8n.io, integrates_with, Notion)

### Documentation relationships
- documents_on: Content is stored/documented/published on a platform
  - "Meeting notes are on Notion" → (Meeting notes, documents_on, Notion)
  - "Specs are documented in Notion" → (specs, documents_on, Notion)
  - "Article was published on Medium" → (Article, documents_on, Medium)

### Communication relationships
- communicates_via: Entity uses a platform for communication
  - "Community discussions happen on Discord" → (community, communicates_via, Discord)
  - "Updates are shared on Telegram" → (updates, communicates_via, Telegram)

### Predicate normalization (Week 12)
DO NOT use these predicates (they have been normalized):
- "hosted_on" → use "uses" instead
- "published_on" → use "documents_on" instead
- "linked_to" → use "associated_with" instead
- Tense variants like "exploring", "presented" → use present tense ("discusses")
- "founder_of", "is_founder_of" → use "founded"
- "is_ceo_of" → use "leads"

### Canonical predicates (ONLY use these)
Use ONLY these predicates for relationships:
- Core: supports, uses, mentions, implements, includes, manages, enables, part_of, requires, provides, associated_with, located_in, defines, relates_to, works_with, represents, contains, addresses, hosts, validates, governs, participates_in, leads, monitors, promotes, performs, focuses_on, affects, queries, updates, aligns_with, is_a, targets, interacts_with, contributes_to, improves, operates, creates, built_on, proposes, authored, founded, discusses
- People/Org: member_of, works_at, employs, advises
- Process: executes, processes, generates, analyzes, evaluates, measures, deploys, maintains, funds, connects
- Knowledge: describes, explains, documents, announces
- Platform: documents_on, integrates_with, powered_by, communicates_via
- Regen: anchors, bridges, delegates, votes, credits, issues, retires, verifies, registers, approves, mints, burns
- Lifecycle: replaces, upgrades

If a relationship doesn't fit these predicates, use the closest match or "associated_with" as fallback.

### Avoid generic predicates
- DO NOT use "associated_with" for platform/tool relationships
- Only use "associated_with" if no better predicate applies

## PREDICATE TYPE CONSTRAINTS (must follow)

Certain predicates require specific entity types. Violating these rules will cause the relationship to be rejected.

| Predicate | Subject Type | Object Type |
|-----------|--------------|-------------|
| founded | PERSON, ORGANIZATION | ORGANIZATION, PROJECT, TECHNOLOGY |
| leads | PERSON, ORGANIZATION | ORGANIZATION, PROJECT, EVENT, PROCESS |
| works_at | PERSON | ORGANIZATION, PROJECT, VALIDATOR |
| employs | ORGANIZATION | PERSON |
| member_of | PERSON, ORGANIZATION, VALIDATOR | ORGANIZATION, PROJECT |
| located_in | (any) | LOCATION only |
| authored | PERSON, ORGANIZATION | (any) |
| validates | VALIDATOR, ORGANIZATION, TECHNOLOGY | (any) |
| delegates | (any) | VALIDATOR, PERSON, ORGANIZATION |
| votes | PERSON, ORGANIZATION, VALIDATOR | (any) |
| operates | NOT CONCEPT/EVENT | NOT CONCEPT/MATERIAL/LOCATION/EVENT |

Examples:
- ✓ "Gregory Landua (PERSON) founded Regen Network (ORGANIZATION)" - valid
- ✗ "carbon credits (CONCEPT) operates markets" - invalid (CONCEPT cannot use operates)
- ✓ "Regen Network (ORGANIZATION) located_in Delaware (LOCATION)" - valid
- ✗ "protocol operates regenerative finance (CONCEPT)" - invalid (operates cannot target CONCEPT)

## Confidence Scoring
- HIGH (0.85-1.0): Explicit named mention ("Regen Network announced...")
- MEDIUM (0.70-0.84): Implied/contextual ("the network launched...")
- LOW (<0.70): DO NOT EXTRACT (skip entirely)

## JSON Output Structure
Return ONLY valid JSON in this exact format:
{{
  "metadata": {{
    "title": {{"value": "...", "confidence": 0.9}},
    "author": {{"value": "...", "confidence": 0.8}},
    "published_date": {{"value": "ISO date", "confidence": 0.7}},
    "organization": {{"value": "...", "confidence": 0.6}},
    "tags": {{"value": ["tag1", "tag2"], "confidence": 0.8}}
  }},
  "entities": [
    {{"type": "PERSON", "name": "...", "confidence": 0.9}},
    {{"type": "ORGANIZATION", "name": "...", "confidence": 0.9}},
    {{"type": "PROJECT", "name": "...", "confidence": 0.9}},
    {{"type": "CONCEPT", "name": "...", "confidence": 0.85}},
    {{"type": "TECHNOLOGY", "name": "...", "confidence": 0.9}},
    {{"type": "LOCATION", "name": "...", "confidence": 0.9}},
    {{"type": "EVENT", "name": "...", "confidence": 0.9}},
    {{"type": "CLAIM", "name": "...", "confidence": 0.8}},
    {{"type": "EVIDENCE", "name": "...", "confidence": 0.8}},
    {{"type": "QUESTION", "name": "...", "confidence": 0.8}}
  ],
  "relationships": [
    {{"subject": "entity1", "predicate": "supports", "object": "entity2", "confidence": 0.85}}
  ],
  "summary": "one sentence summary"
}}

## Few-Shot Examples

Example 1: Governance Discussion
Input: "Gregory Landua and Sarah Bax discussed regenerative agriculture principles at the Regen Network community meeting."
Extract:
- "Gregory Landua" (PERSON, 0.95)
- "Sarah Bax" (PERSON, 0.95)
- "regenerative agriculture" (CONCEPT, 0.90)
- "Regen Network" (ORGANIZATION, 0.95)
Do NOT extract: "community", "meeting" (generic words)

Example 2: Technical Documentation
Input: "The Regen Ledger blockchain uses proof of stake consensus. ChatGPT and Claude can assist with documentation."
Extract:
- "Regen Ledger" (TECHNOLOGY, 0.95)
- "proof of stake" (CONCEPT, 0.90)
- "ChatGPT" (TECHNOLOGY, 0.95) - AI system, NOT PERSON
- "Claude" (TECHNOLOGY, 0.95) - AI system, NOT PERSON
Do NOT extract: "blockchain", "consensus", "documentation"

Example 3: Event Mention
Input: "Join us at Regen Gathering 2024 in Boulder, Colorado. We also have a weekly meeting every Tuesday."
Extract:
- "Regen Gathering 2024" (EVENT, 0.95) - specific named event
- "Boulder, Colorado" (LOCATION, 0.95)
Do NOT extract: "weekly meeting" (generic, no specific title), "Tuesday" (time reference)

Example 4: Git/Changelog (extract NOTHING)
Input: "feat(api): add endpoint for user registration. Merge pull request #456 from feature/auth"
Extract:
- NOTHING (this is git commit/changelog content)

Focus on regenerative finance, ecological, and commons-oriented content.
Return ONLY valid JSON, no additional text or markdown."""

    # Append code-docstring-specific extraction guidance
    if source_type == "code_docstring":
        prompt += """

## CODE DOCSTRING CONTEXT (source_type=code_docstring)

This content is aggregated from code comments and docstrings extracted via tree-sitter.
Each block is labeled with its entity type and name (e.g., [Function: CreateBatch]).

### PRIORITIZE extracting:
- API_MESSAGE types (MsgCreateBatch, MsgRetire, QueryBalanceRequest)
- KEEPER names (EcocreditKeeper, BasketKeeper, BankKeeper)
- MODULE references (x/ecocredit, x/group, x/data)
- STANDARD references (ISO 14064, Verra VM0042, EIP-721, ICS-20)
- TECHNOLOGY (Cosmos SDK, IBC, Tendermint, protobuf, gRPC)
- CONCEPT (credit retirement, batch issuance, basket redemption, MRV)

### DO NOT extract from code docstrings:
- Variable names, parameter names, return type names
- Generic programming words: returns, error, context, params, args, nil, None
- Implementation details: loop, index, pointer, buffer, mutex
- Test fixture names or test helper descriptions"""

    return prompt


def get_system_message() -> str:
    """
    Get the system message for LLM extraction.

    Returns:
        System message string for the LLM
    """
    return "You are a semantic extraction system that outputs only valid JSON. Extract entities with their types in UPPERCASE exactly as specified."


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    "build_extraction_prompt",
    "get_system_message",
    "LLM_ALLOWED_TYPES",
    "CANONICAL_PREDICATES",
]
