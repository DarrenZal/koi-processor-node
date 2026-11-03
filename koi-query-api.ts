#!/usr/bin/env bun
/**
 * KOI Query API Bridge
 * Provides REST API bridge to adaptive hybrid RAG functionality
 * Serves the GAIA React interface
 */

import express from 'express';
import cors from 'cors';
import fs from 'fs';
import path from 'path';
import { Pool } from "pg";

// Import the adaptive features
import {
  reciprocalRankFusion,
  calculateConfidence,
  logQuery,
  shouldTriggerExtraction,
  selectDocumentsForExtraction
} from "./adaptive-features.ts";

// Import canonical response envelope types (Session D1)
import {
  type KoiResponseEnvelope,
  type Citation,
  type KoiError,
  type WarningCode,
  type ToolTraceEntry,
  type AsOfMetadata,
  generateRequestId,
  getKoiAsOfMetadata,
  summarizeParams,
  extractCitations,
  shouldExcludeKoiResultSource,
  createSuccessEnvelope,
  createErrorEnvelope,
} from "./src/types/koi-response-envelope.ts";

// Import anchored metadata system (Session E)
import {
  AnchoredMetadataIntegration,
  createAnchoredMetadataIntegration,
  AnchoredMetadataResolver,
  createAnchoredMetadataSystem,
  type MetadataCitation,
  type AnchoredMetric,
  type ResolvedMetadata,
} from "./src/metadata/index.ts";

// =============================================================================
// Authentication Gating (metadata/document endpoints)
// =============================================================================
const KOI_INTERNAL_API_KEY = process.env.KOI_INTERNAL_API_KEY || '';

/**
 * Dual-auth middleware: accepts EITHER a valid X-Internal-API-Key header
 * (for headless services like regen-python-mcp) OR a valid OAuth session token
 * via Authorization: Bearer (for interactive MCP clients like regen-koi-mcp).
 *
 * Sets req.authMethod, req.userEmail, req.isAuthenticated for downstream use.
 * Internal key auth does NOT set isAuthenticated — private docs require session token.
 */
async function requireInternalOrSessionAuth(req: any, res: any, next: () => void) {
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);

  // Path 1: Internal API key (for headless services)
  const internalKey = req.headers['x-internal-api-key'];
  if (internalKey && KOI_INTERNAL_API_KEY && internalKey === KOI_INTERNAL_API_KEY) {
    req.authMethod = 'internal_key';
    return next();
  }

  // Path 2: OAuth session token (for interactive MCP clients)
  const authHeader = req.headers['authorization'];
  const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
  if (sessionToken) {
    const userEmail = await validateSessionToken(sessionToken);
    if (userEmail) {
      req.userEmail = userEmail;
      req.isAuthenticated = true;
      req.authMethod = 'session_token';
      return next();
    }
  }

  // Neither auth method succeeded
  const koiError: KoiError = {
    code: 'UNAUTHORIZED',
    message: 'Authentication required. Use regen_koi_authenticate or provide X-Internal-API-Key.',
    retryable: false,
  };
  return res.status(401).json(createErrorEnvelope(requestId, koiError));
}

/**
 * Optional-auth middleware: same credential recognition as
 * requireInternalOrSessionAuth, but anonymous requests pass through instead
 * of 401ing. Downstream handlers gate per-document via is_private
 * (public docs serve to anyone; private docs need req.isAuthenticated).
 *
 * Internal key auth does NOT set isAuthenticated — private docs require a
 * session token. Invalid/expired Bearer tokens are treated as anonymous.
 */
async function optionalSessionAuth(req: any, res: any, next: () => void) {
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);

  const internalKey = req.headers['x-internal-api-key'];
  if (internalKey && KOI_INTERNAL_API_KEY && internalKey === KOI_INTERNAL_API_KEY) {
    req.authMethod = 'internal_key';
    return next();
  }

  const authHeader = req.headers['authorization'];
  const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
  if (sessionToken) {
    const userEmail = await validateSessionToken(sessionToken);
    if (userEmail) {
      req.userEmail = userEmail;
      req.isAuthenticated = true;
      req.authMethod = 'session_token';
      return next();
    }
  }

  req.authMethod = 'anonymous';
  return next();
}

const app = express();
const PORT = 8301;

// Database configuration
const POSTGRES_URL = process.env.POSTGRES_URL || "postgresql://postgres:postgres@localhost:5433/eliza";

function parsePostgresUrl(url: string) {
  const match = url.match(/postgres(?:ql)?:\/\/([^:]+):([^@]+)@([^:]+):(\d+)\/(.+)/);
  if (!match) throw new Error("Invalid PostgreSQL URL");
  return {
    user: match[1],
    password: match[2],
    host: match[3],
    port: parseInt(match[4]),
    database: match[5]
  };
}

const dbConfig = parsePostgresUrl(POSTGRES_URL);
const pool = new Pool(dbConfig);

// Lazy-loaded anchored metadata system (Session E)
let _metadataIntegration: AnchoredMetadataIntegration | null = null;
let _metadataResolver: AnchoredMetadataResolver | null = null;

function getMetadataIntegration(): AnchoredMetadataIntegration {
  if (!_metadataIntegration) {
    _metadataIntegration = createAnchoredMetadataIntegration(pool);
  }
  return _metadataIntegration;
}

function getMetadataResolver(): AnchoredMetadataResolver {
  if (!_metadataResolver) {
    const { resolver } = createAnchoredMetadataSystem(pool);
    _metadataResolver = resolver;
  }
  return _metadataResolver;
}

// HNSW index provides excellent recall without tuning parameters

type CanonicalAliasEntry = {
  canonical_name: string;
  entity_type: string;
};

const canonicalAliasMap = new Map<string, CanonicalAliasEntry>();

function normalizeForCanonicalLookup(name: string, entityType?: string | null): string {
  if (!name) return '';
  let normalized = name.trim();

  // Strip leading @ (usernames)
  if (normalized.startsWith('@')) {
    normalized = normalized.slice(1);
  }

  // Strip trailing "| SOMETHING" suffix pattern
  normalized = normalized.replace(/\s*\|\s*[A-Za-z0-9\s]+$/, '');

  // Convert underscores and hyphens to spaces
  normalized = normalized.replace(/[_-]+/g, ' ');

  // Lowercase
  normalized = normalized.toLowerCase();

  // Organization/project/technology: convert dots to spaces (regen.foundation -> regen foundation)
  if (!entityType || ['ORGANIZATION', 'ORG', 'PROJECT', 'TECHNOLOGY'].includes(entityType.toUpperCase())) {
    normalized = normalized.replace(/(\w)\.(\w)/g, '$1 $2');
  }

  // Remove common corporate suffixes for matching
  normalized = normalized.replace(/,?\s*(inc|llc|ltd|corp|pbc|ag)\.?$/i, '');

  // Remove common articles at start
  normalized = normalized.replace(/^\s*(the|a|an)\s+/i, '');

  // Normalize whitespace + trailing punctuation
  normalized = normalized.replace(/\s+/g, ' ').trim();
  normalized = normalized.replace(/[.,;:!?]+$/g, '');

  return normalized.trim();
}

function loadCanonicalAliasMap() {
  const registryPath = process.env.CANONICAL_ENTITIES_PATH
    || path.join(process.cwd(), 'data', 'canonical_entities.json');

  try {
    const raw = fs.readFileSync(registryPath, 'utf-8');
    const registry = JSON.parse(raw);
    const categories = registry?.entities || {};

    for (const category of Object.values(categories)) {
      if (!category || typeof category !== 'object') continue;
      for (const entityData of Object.values(category as Record<string, any>)) {
        const canonicalName = entityData?.canonical_name;
        const entityType = entityData?.entity_type || 'ENTITY';
        if (!canonicalName) continue;

        const canonicalKey = normalizeForCanonicalLookup(canonicalName, entityType);
        if (canonicalKey && !canonicalAliasMap.has(canonicalKey)) {
          canonicalAliasMap.set(canonicalKey, { canonical_name: canonicalName, entity_type: entityType });
        }

        const aliases: string[] = entityData?.aliases || [];
        for (const alias of aliases) {
          const aliasKey = normalizeForCanonicalLookup(alias, entityType);
          if (aliasKey && !canonicalAliasMap.has(aliasKey)) {
            canonicalAliasMap.set(aliasKey, { canonical_name: canonicalName, entity_type: entityType });
          }
        }
      }
    }
  } catch (err) {
    console.warn(`[CanonicalAlias] Failed to load registry: ${registryPath}`, err);
  }
}

function resolveCanonicalAlias(label: string): CanonicalAliasEntry | null {
  const key = normalizeForCanonicalLookup(label, null);
  return canonicalAliasMap.get(key) || null;
}

function buildNormalizedLookupKeys(label: string, typeHint?: string | null): string[] {
  const base = label.trim();
  if (!base) return [];

  const variants = new Set<string>();
  variants.add(base);
  for (const variant of normalizeQueryForEntityMatch(base)) {
    variants.add(variant);
  }

  const keys = new Set<string>();
  for (const variant of variants) {
    const canonical = resolveCanonicalAlias(variant);
    if (canonical) {
      keys.add(normalizeForCanonicalLookup(canonical.canonical_name, canonical.entity_type));
    }
    keys.add(normalizeForCanonicalLookup(variant, typeHint));
  }

  return Array.from(keys).filter(Boolean);
}

loadCanonicalAliasMap();

// Helper function to fix duplicated paths in entity data
function fixEntityPaths(entity: any): any {
  if (!entity || typeof entity !== "object") return entity;
  
  // Fix file_path - handle multiple prefix patterns
  if (entity.file_path && entity.repo) {
    let path = entity.file_path;
    
    // Pattern 1: /opt/projects/regen-repos/<repo>/...
    const absPrefix = `/opt/projects/regen-repos/${entity.repo}/`;
    if (path.startsWith(absPrefix)) {
      path = path.substring(absPrefix.length);
    }
    
    // Pattern 2: regen-network/<repo>/...
    const regenPrefix = `regen-network/${entity.repo}/`;
    if (path.startsWith(regenPrefix)) {
      path = path.substring(regenPrefix.length);
    }
    
    // Pattern 3: <repo>/... (e.g., regen-ledger/x/...)
    const repoPrefix = `${entity.repo}/`;
    if (path.startsWith(repoPrefix)) {
      path = path.substring(repoPrefix.length);
    }
    
    entity.file_path = path;
  }
  
  // Reconstruct github_url with correct path
  if (entity.file_path && entity.repo) {
    const branch = entity.commit_sha && entity.commit_sha !== "None" ? entity.commit_sha : (entity.branch || "main");
    entity.github_url = `https://github.com/regen-network/${entity.repo}/blob/${branch}/${entity.file_path}${entity.line_number ? "#L" + entity.line_number : ""}`;
  }
  
  return entity;
}

// No need to set probes or other runtime parameters

// Middleware
app.use(cors());
app.use(express.json());

// Generate a random session token (UUID v4 format)
function generateSessionToken(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x40; // Version 4
  bytes[8] = (bytes[8] & 0x3f) | 0x80; // Variant 1
  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}

// Session token lifetime (1 hour - shorter than Google OAuth token)
const SESSION_TOKEN_LIFETIME_MS = 60 * 60 * 1000;

// Hash a token using SHA-256 for secure storage/lookup
async function hashToken(token: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(token);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

// Create a new session token for an authenticated user
// NOTE: This is kept for internal use only - MCP clients should use the device_code flow
// SECURITY: Plain token is returned to caller but NEVER stored in database
async function createSessionToken(userEmail: string, clientInfo?: string): Promise<string> {
  const sessionToken = generateSessionToken();
  const tokenHash = await hashToken(sessionToken);
  const expiresAt = new Date(Date.now() + SESSION_TOKEN_LIFETIME_MS);

  // Store ONLY the hash in session_tokens - plain token never touches the DB
  await pool.query(
    `INSERT INTO session_tokens (token_hash, user_email, expires_at, client_info)
     VALUES ($1, $2, $3, $4)`,
    [tokenHash, userEmail, expiresAt, clientInfo || null]
  );

  console.log(`[Auth] Created session token for ${userEmail}, expires at ${expiresAt.toISOString()}`);
  return sessionToken;  // Plain token only exists in memory, returned to caller
}

// Validate a session token - returns user email if valid, null otherwise
// SECURITY: This validates by hashing the token and comparing to stored hash
// This prevents timing attacks and protects against database leaks
async function validateSessionToken(sessionToken: string | undefined): Promise<string | null> {
  if (!sessionToken) return null;

  try {
    const tokenHash = await hashToken(sessionToken);

    const result = await pool.query(
      `SELECT user_email, expires_at FROM session_tokens
       WHERE token_hash = $1
       AND expires_at > NOW()
       AND revoked_at IS NULL`,
      [tokenHash]
    );

    if (result.rows.length === 0) {
      return null; // Token not found, expired, or revoked
    }

    const userEmail = result.rows[0].user_email;

    // Additional validation: ensure it's a @regen.network email
    if (!userEmail.endsWith('@regen.network')) {
      console.warn(`Session token found but email domain not @regen.network: ${userEmail}`);
      return null;
    }

    return userEmail;
  } catch (error) {
    console.error('Session token validation error:', error);
    return null;
  }
}

// Check if user has valid OAuth token (for creating session tokens)
async function hasValidOAuthToken(userEmail: string): Promise<boolean> {
  try {
    const result = await pool.query(
      `SELECT 1 FROM oauth_tokens
       WHERE user_email = $1 AND token_expiry > NOW()`,
      [userEmail]
    );
    return result.rows.length > 0;
  } catch (error) {
    console.error('OAuth token check error:', error);
    return false;
  }
}

// Build privacy filter clause based on authentication status
function buildPrivacyFilter(isAuthenticated: boolean, tableAlias: string = 'm'): string {
  if (isAuthenticated) {
    return ''; // Authenticated users see all data
  }
  // Unauthenticated users only see public data
  return ` AND (${tableAlias}.is_private = FALSE OR ${tableAlias}.is_private IS NULL)`;
}

// Get 1-hop neighbors from koi_relationships for graph expansion
// Returns neighbor entities connected to the matched entity names
async function get1HopNeighbors(
  matchedEntityNames: string[],  // normalized entity names (lowercased)
  maxPerEntity: number = 5,
  totalLimit: number = 15
): Promise<{ neighbor_uri: string; neighbor_name: string; neighbor_type: string; via_predicate: string; confidence: number }[]> {
  if (matchedEntityNames.length === 0) return [];

  const query = `
    WITH matched AS (
      SELECT DISTINCT ON (normalized_text) id, fuseki_uri, entity_text, entity_type
      FROM entity_registry
      WHERE normalized_text = ANY($1)
      ORDER BY normalized_text, occurrence_count DESC
    ),
    neighbors_ranked AS (
      SELECT
        er.fuseki_uri as neighbor_uri,
        er.entity_text as neighbor_name,
        er.entity_type as neighbor_type,
        r.predicate as via_predicate,
        COALESCE(r.confidence, 0) as confidence,
        r.occurrence_count,
        ROW_NUMBER() OVER (
          PARTITION BY m.id
          ORDER BY r.occurrence_count DESC, COALESCE(r.confidence, 0) DESC
        ) as rank_per_source
      FROM koi_relationships r
      JOIN matched m ON (r.subject_entity_id = m.id OR r.object_entity_id = m.id)
      JOIN entity_registry er ON (
        CASE WHEN r.subject_entity_id = m.id
             THEN r.object_entity_id
             ELSE r.subject_entity_id END = er.id
      )
      WHERE COALESCE(r.confidence, 0) >= 0.5
        AND r.occurrence_count >= 2
    )
    SELECT DISTINCT neighbor_uri, neighbor_name, neighbor_type, via_predicate, confidence
    FROM neighbors_ranked
    WHERE rank_per_source <= $2
    ORDER BY confidence DESC
    LIMIT $3
  `;
  const result = await pool.query(query, [matchedEntityNames, maxPerEntity, totalLimit]);
  return result.rows;
}

// ============================================================================
// Week 13: GraphRAG Integration
// Provides graph context for query responses
// ============================================================================

// GraphContext interface for graph-enhanced query responses
interface GraphContextEdge {
  predicate: string;
  subject_uri: string;
  subject_text: string;
  object_uri: string;
  object_text: string;
  direction: 'out' | 'in';
  confidence: number;
  occurrence_count: number;
  source_entity?: string; // Week 21: For multi-entity queries, indicates which entity contributed this edge
}

interface GraphContext {
  dominant_entity: {
    uri: string;
    text: string;
    type: string;
    occurrence_count: number;
  } | null;
  secondary_entity?: { // Week 21: For multi-entity queries
    uri: string;
    text: string;
    type: string;
    occurrence_count: number;
  } | null;
  edges: GraphContextEdge[];
  edge_count: number;
  truncated: boolean;
  query_type?: 'entity' | 'question' | 'multi_entity'; // Week 21: Query classification
  _privacy_warning?: string;
}

// ============================================================================
// Week 21: Query Type Detection and Entity Extraction for Question Queries
// ============================================================================

type QueryType = 'entity' | 'question' | 'multi_entity';

interface QueryClassification {
  type: QueryType;
  entity_candidates: string[];
  multi_entity_pattern?: 'relationship_between' | 'and' | 'vs' | null;
}

/**
 * Week 21b: Stop-phrases that indicate context rather than entity names.
 * These often follow PERSON names in multi-entity queries.
 */
const ENTITY_CONTEXT_STOP_PHRASES = [
  'leadership roles', 'leadership', 'roles', 'role',
  'background', 'backgrounds', 'bio', 'bios', 'profile', 'profiles',
  'career', 'careers', 'timeline', 'timelines',
  'involvement', 'involvements', 'contribution', 'contributions',
  'work', 'works', 'history', 'experience', 'experiences',
  'connection', 'connections', 'relationship', 'relationships',
  'comparison', 'comparisons', 'difference', 'differences',
  'collaboration', 'collaborations', 'partnership', 'partnerships',
];

/**
 * Week 21b: Clean an entity candidate by stripping trailing stop-phrases.
 * For PERSON×PERSON patterns, this helps isolate the actual name.
 *
 * Examples:
 *   "Martin Wainstein leadership roles" → "Martin Wainstein"
 *   "Gregory Landua background" → "Gregory Landua"
 */
function cleanEntityCandidate(candidate: string): string {
  let cleaned = candidate.trim();

  // Try stripping each stop-phrase from the end (case-insensitive)
  for (const phrase of ENTITY_CONTEXT_STOP_PHRASES) {
    const regex = new RegExp(`\\s+${phrase}\\s*$`, 'i');
    if (regex.test(cleaned)) {
      cleaned = cleaned.replace(regex, '').trim();
      // Continue checking - might have multiple trailing phrases
    }
  }

  // If we stripped too much, return original
  if (cleaned.length < 2) {
    return candidate.trim();
  }

  return cleaned;
}

/**
 * Week 21b: Extract a clean name from a candidate by preferring capitalized spans.
 * This helps with patterns like "Martin Wainstein leadership roles" where
 * the name is "Martin Wainstein" (capitalized) and "leadership roles" is lowercase.
 *
 * Returns the original candidate if no clear capitalized span is found.
 */
function extractCapitalizedName(candidate: string): string {
  // First apply stop-phrase cleaning
  const cleaned = cleanEntityCandidate(candidate);

  // Match sequences of capitalized words at the start
  // Pattern: One or more words starting with uppercase, followed by lowercase letters
  const nameMatch = cleaned.match(/^([A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*)/);
  if (nameMatch && nameMatch[1].length >= 3) {
    return nameMatch[1].trim();
  }

  return cleaned;
}

/**
 * Detect whether a query is entity-style, question-style, or multi-entity.
 *
 * Entity-style: Direct entity mentions (e.g., "Gregory Landua", "x/ecocredit module")
 * Question-style: Questions starting with How/What/Who/Where/Why/When
 * Multi-entity: Queries mentioning relationships between entities (e.g., "relationship between X and Y")
 */
function classifyQuery(query: string): QueryClassification {
  const q = query.trim();
  const qLower = q.toLowerCase();

  // Pattern 1: Multi-entity - "relationship between X and Y"
  const relationshipBetweenMatch = qLower.match(/relationship\s+between\s+(.+?)\s+and\s+(.+?)(?:\?|$)/i);
  if (relationshipBetweenMatch) {
    return {
      type: 'multi_entity',
      entity_candidates: [
        relationshipBetweenMatch[1].trim(),
        relationshipBetweenMatch[2].trim()
      ],
      multi_entity_pattern: 'relationship_between'
    };
  }

  // Pattern 2: Multi-entity - "X vs Y" or "X versus Y"
  // Week 21b: Use extractCapitalizedName to strip trailing context like "leadership roles"
  const vsMatch = q.match(/^(.+?)\s+(?:vs\.?|versus)\s+(.+?)(?:\?|$)/i);
  if (vsMatch) {
    const candidate1 = extractCapitalizedName(vsMatch[1]);
    const candidate2 = extractCapitalizedName(vsMatch[2]);
    return {
      type: 'multi_entity',
      entity_candidates: [candidate1, candidate2],
      multi_entity_pattern: 'vs'
    };
  }

  // Pattern 3: Multi-entity - "X and Y" at the start (without relationship keyword)
  // Only if both look like entity names (capitalized or known patterns)
  // Week 21b: Use extractCapitalizedName to strip trailing context
  const andMatch = q.match(/^(.+?)\s+and\s+(.+?)(?:\?|$)/i);
  if (andMatch && !qLower.startsWith('how') && !qLower.startsWith('what') &&
      !qLower.startsWith('who') && !qLower.startsWith('where') &&
      !qLower.startsWith('why') && !qLower.startsWith('when')) {
    const candidate1 = extractCapitalizedName(andMatch[1]);
    const candidate2 = extractCapitalizedName(andMatch[2]);
    // Check if candidates look like entity names (capitalized or short phrases)
    if (looksLikeEntityName(candidate1) && looksLikeEntityName(candidate2)) {
      return {
        type: 'multi_entity',
        entity_candidates: [candidate1, candidate2],
        multi_entity_pattern: 'and'
      };
    }
  }

  // Pattern 4: Question-style queries
  if (/^(how|what|who|where|why|when|which|is|are|does|do|can|could|would|will)\s+/i.test(qLower)) {
    const candidates = extractEntityCandidatesFromQuestion(q);
    return {
      type: 'question',
      entity_candidates: candidates,
      multi_entity_pattern: null
    };
  }

  // Default: Entity-style query
  return {
    type: 'entity',
    entity_candidates: [q],
    multi_entity_pattern: null
  };
}

/**
 * Check if a string looks like an entity name (capitalized, short phrase, known patterns)
 */
function looksLikeEntityName(text: string): boolean {
  const t = text.trim();
  if (t.length < 2 || t.length > 50) return false;

  // Known patterns that indicate entity names
  if (t.startsWith('x/') || t.startsWith('$')) return true;

  // Check for capitalization (at least first word capitalized)
  const words = t.split(/\s+/);
  if (words.length > 0 && /^[A-Z]/.test(words[0])) return true;

  // Short phrases (2-4 words) are likely entity names
  if (words.length >= 1 && words.length <= 4) return true;

  return false;
}

/**
 * Extract potential entity names from a question-style query.
 * Looks for capitalized phrases, quoted text, and known entity patterns.
 */
function extractEntityCandidatesFromQuestion(question: string): string[] {
  const candidates: string[] = [];

  // Remove question words from the start
  let cleaned = question.replace(/^(how|what|who|where|why|when|which|is|are|does|do|can|could|would|will)\s+/i, '');

  // Extract quoted phrases first
  const quotedMatches = question.match(/"([^"]+)"|'([^']+)'/g);
  if (quotedMatches) {
    for (const match of quotedMatches) {
      const content = match.replace(/['"]/g, '').trim();
      if (content.length > 0) candidates.push(content);
    }
  }

  // Extract Cosmos SDK module patterns (x/modulename)
  const moduleMatches = cleaned.match(/x\/\w+/gi);
  if (moduleMatches) {
    for (const match of moduleMatches) {
      candidates.push(match);
    }
  }

  // Extract token patterns ($TOKEN)
  const tokenMatches = cleaned.match(/\$\w+/g);
  if (tokenMatches) {
    for (const match of tokenMatches) {
      candidates.push(match.slice(1)); // Remove $ prefix for matching
    }
  }

  // Extract capitalized phrases (2+ words starting with capitals, or single proper nouns)
  // Match sequences like "Regen Network", "Gregory Landua", "NCT", "Cosmos SDK"
  const capitalizedMatches = cleaned.match(/(?:[A-Z][a-zA-Z]*(?:\s+[A-Z][a-zA-Z]*)*)|(?:[A-Z]{2,})/g);
  if (capitalizedMatches) {
    for (const match of capitalizedMatches) {
      const trimmed = match.trim();
      // Filter out common words that might be capitalized at sentence start
      const commonWords = ['The', 'A', 'An', 'This', 'That', 'It', 'They', 'How', 'What', 'Who', 'Where', 'When', 'Which', 'Is', 'Are', 'Does', 'Do', 'Can', 'Could', 'Would', 'Will'];
      if (!commonWords.includes(trimmed) && trimmed.length > 1) {
        candidates.push(trimmed);
      }
    }
  }

  // Extract known entity suffixes (ending in "module", "token", "validator", "network", etc.)
  const suffixPatterns = [
    /(\w+(?:\s+\w+)*\s+module)/gi,
    /(\w+(?:\s+\w+)*\s+token)/gi,
    /(\w+(?:\s+\w+)*\s+validator)/gi,
    /(\w+(?:\s+\w+)*\s+network)/gi,
    /(\w+(?:\s+\w+)*\s+credit\s+class)/gi,
    /(\w+(?:\s+\w+)*\s+credits?)/gi,
  ];

  for (const pattern of suffixPatterns) {
    const matches = cleaned.match(pattern);
    if (matches) {
      for (const match of matches) {
        candidates.push(match.trim());
      }
    }
  }

  // Deduplicate and filter
  const uniqueCandidates = [...new Set(candidates)]
    .filter(c => c.length >= 2)
    .sort((a, b) => b.length - a.length); // Prefer longer matches

  return uniqueCandidates;
}

/**
 * Week 21: Resolve entity candidates from a question query.
 * Tries each candidate through the entity resolver until one matches.
 */
async function resolveQuestionQueryEntity(
  candidates: string[],
  minOccurrenceCount: number = 2
): Promise<{
  entity_id: number;
  uri: string;
  text: string;
  type: string;
  occurrence_count: number;
  matched_candidate: string;
} | null> {
  for (const candidate of candidates) {
    // Try the candidate directly
    const resolved = await resolveEntityInternal(candidate, null, null);
    if (resolved && resolved.occurrence_count >= minOccurrenceCount) {
      if (process.env.DEBUG_GRAPH_EXPANSION) {
        console.log(`[GraphRAG-W21] Question query resolved '${candidate}' → ${resolved.entity_text} (${resolved.entity_type})`);
      }
      return {
        entity_id: resolved.entity_id,
        uri: resolved.uri,
        text: resolved.entity_text,
        type: resolved.entity_type,
        occurrence_count: resolved.occurrence_count,
        matched_candidate: candidate
      };
    }

    // Try normalized variants
    const variants = normalizeQueryForEntityMatch(candidate);
    for (const variant of variants) {
      if (variant === candidate.toLowerCase()) continue; // Skip if same as original
      const resolvedVariant = await resolveEntityInternal(variant, null, null);
      if (resolvedVariant && resolvedVariant.occurrence_count >= minOccurrenceCount) {
        if (process.env.DEBUG_GRAPH_EXPANSION) {
          console.log(`[GraphRAG-W21] Question query resolved '${candidate}' via variant '${variant}' → ${resolvedVariant.entity_text}`);
        }
        return {
          entity_id: resolvedVariant.entity_id,
          uri: resolvedVariant.uri,
          text: resolvedVariant.entity_text,
          type: resolvedVariant.entity_type,
          occurrence_count: resolvedVariant.occurrence_count,
          matched_candidate: candidate
        };
      }
    }
  }

  return null;
}

/**
 * Week 21: Resolve two entities for multi-entity queries.
 * Returns up to 2 resolved entities.
 */
async function resolveMultiEntityQuery(
  candidates: [string, string],
  minOccurrenceCount: number = 2
): Promise<{
  primary: { entity_id: number; uri: string; text: string; type: string; occurrence_count: number } | null;
  secondary: { entity_id: number; uri: string; text: string; type: string; occurrence_count: number } | null;
}> {
  const results: Array<{ entity_id: number; uri: string; text: string; type: string; occurrence_count: number } | null> = [];

  for (const candidate of candidates) {
    // Try direct resolution
    let resolved = await resolveEntityInternal(candidate, null, null);

    // If not found, try normalized variants
    if (!resolved || resolved.occurrence_count < minOccurrenceCount) {
      const variants = normalizeQueryForEntityMatch(candidate);
      for (const variant of variants) {
        const resolvedVariant = await resolveEntityInternal(variant, null, null);
        if (resolvedVariant && resolvedVariant.occurrence_count >= minOccurrenceCount) {
          resolved = resolvedVariant;
          break;
        }
      }
    }

    if (resolved && resolved.occurrence_count >= minOccurrenceCount) {
      results.push({
        entity_id: resolved.entity_id,
        uri: resolved.uri,
        text: resolved.entity_text,
        type: resolved.entity_type,
        occurrence_count: resolved.occurrence_count
      });
      if (process.env.DEBUG_GRAPH_EXPANSION) {
        console.log(`[GraphRAG-W21] Multi-entity resolved '${candidate}' → ${resolved.entity_text} (${resolved.entity_type})`);
      }
    } else {
      results.push(null);
      if (process.env.DEBUG_GRAPH_EXPANSION) {
        console.log(`[GraphRAG-W21] Multi-entity failed to resolve '${candidate}'`);
      }
    }
  }

  // Sort by occurrence_count to determine primary vs secondary
  const [first, second] = results;
  if (first && second) {
    if (first.occurrence_count >= second.occurrence_count) {
      return { primary: first, secondary: second };
    } else {
      return { primary: second, secondary: first };
    }
  }

  return { primary: first || second, secondary: first ? second : null };
}

/**
 * Week 21: Merge graph contexts from two entities.
 * Combines edges from both entities, marks source_entity, and limits to maxEdges.
 */
async function getMergedGraphContext(
  primaryEntityId: number,
  secondaryEntityId: number | null,
  primaryText: string,
  secondaryText: string | null,
  maxEdges: number = 20
): Promise<{ edges: GraphContextEdge[]; truncated: boolean }> {
  const allEdges: GraphContextEdge[] = [];

  // Get edges for primary entity
  const primaryContext = await getGraphContext(primaryEntityId, maxEdges);
  if (primaryContext) {
    for (const edge of primaryContext.edges) {
      allEdges.push({
        ...edge,
        source_entity: primaryText
      });
    }
  }

  // Get edges for secondary entity if present
  if (secondaryEntityId !== null && secondaryText) {
    const secondaryContext = await getGraphContext(secondaryEntityId, maxEdges);
    if (secondaryContext) {
      for (const edge of secondaryContext.edges) {
        // Avoid duplicate edges
        const isDuplicate = allEdges.some(
          e => e.subject_uri === edge.subject_uri &&
               e.object_uri === edge.object_uri &&
               e.predicate === edge.predicate
        );
        if (!isDuplicate) {
          allEdges.push({
            ...edge,
            source_entity: secondaryText
          });
        }
      }
    }
  }

  // Sort by occurrence_count DESC and limit
  allEdges.sort((a, b) => b.occurrence_count - a.occurrence_count);
  const truncated = allEdges.length > maxEdges;
  const limitedEdges = allEdges.slice(0, maxEdges);

  return { edges: limitedEdges, truncated };
}

// Week 14: Normalize query text for entity matching
// Strips common prefixes (x/, the) and suffixes (module, validator, token)
// Week 21: Added acronym/token variant handling for short names like NCT
function normalizeQueryForEntityMatch(query: string): string[] {
  const variants: string[] = [];
  const original = query.trim();
  let normalized = original.toLowerCase();

  // Strip Cosmos SDK module prefix
  if (normalized.startsWith('x/')) {
    normalized = normalized.slice(2);
  }

  // Strip common article prefix
  if (normalized.startsWith('the ')) {
    normalized = normalized.slice(4);
  }

  // Strip $ prefix (for tokens like $NCT)
  if (normalized.startsWith('$')) {
    normalized = normalized.slice(1);
  }

  variants.push(normalized);

  // Strip common type suffixes and add variants
  const suffixes = [' module', ' validator', ' token', ' project', ' network'];
  for (const suffix of suffixes) {
    if (normalized.endsWith(suffix)) {
      variants.push(normalized.slice(0, -suffix.length).trim());
    }
  }

  // Week 21: Add variants for short acronyms (2-5 chars, all letters)
  // Try: uppercase, with $ prefix, with 's' suffix
  if (normalized.length >= 2 && normalized.length <= 5 && /^[a-z]+$/.test(normalized)) {
    const upper = normalized.toUpperCase();
    variants.push(upper);                    // NCT
    variants.push('$' + normalized);         // $nct
    variants.push('$' + upper);              // $NCT
    variants.push(normalized + 's');         // ncts
    variants.push(upper + 's');              // NCTs
    variants.push(normalized + ' token');    // nct token
    variants.push(upper + ' token');         // NCT token
  }

  // Week 21: If query ends with 's', try singular form
  if (normalized.endsWith('s') && normalized.length > 3) {
    variants.push(normalized.slice(0, -1));
  }

  return [...new Set(variants)]; // Deduplicate
}

// Get dominant entity from entity search results
// Uses entity occurrence_count and relationship_count to pick the best entity
// Falls back to resolveEntityInternal if entity search doesn't provide metadata
async function getDominantEntity(
  entityResults: any[],
  queryText?: string
): Promise<{
  entity_id: number;
  uri: string;
  text: string;
  type: string;
  occurrence_count: number;
} | null> {
  // Week 14: Lowered default from 5 to 2 to include more entities
  const entityThreshold = parseInt(process.env.GRAPHRAG_ENTITY_THRESHOLD || '2');

  // Extract unique entities from entity search results
  const entityCounts: Map<string, { name: string; type: string | null; count: number }> = new Map();

  for (const result of entityResults) {
    const entities = result.metadata?.entities_matched || [];
    for (const entity of entities) {
      const normalized = entity.toLowerCase().trim();
      const existing = entityCounts.get(normalized);
      if (existing) {
        existing.count++;
      } else {
        entityCounts.set(normalized, { name: entity, type: null, count: 1 });
      }
    }
  }

  if (entityCounts.size === 0) {
    // Fallback: try to resolve entity from query text directly
    // Week 14: Try normalized variants to improve matching
    if (queryText) {
      const variants = normalizeQueryForEntityMatch(queryText);
      for (const variant of variants) {
        const resolved = await resolveEntityInternal(variant, null, null);
        if (resolved && resolved.occurrence_count >= entityThreshold) {
          if (process.env.DEBUG_GRAPH_EXPANSION) {
            console.log(`[GraphRAG] Resolved query '${queryText}' via variant '${variant}' → ${resolved.entity_text}`);
          }
          return {
            entity_id: resolved.entity_id,
            uri: resolved.uri,
            text: resolved.entity_text,
            type: resolved.entity_type,
            occurrence_count: resolved.occurrence_count,
          };
        }
      }
    }
    return null;
  }

  // Sort entities by search match count (descending)
  const sortedEntities = Array.from(entityCounts.entries())
    .sort((a, b) => b[1].count - a[1].count)
    .map(([_, data]) => data.name);

  if (process.env.DEBUG_GRAPH_EXPANSION) {
    console.log(`[GraphRAG] Candidates from search: ${sortedEntities.slice(0, 5).join(', ')}`);
  }

  // Pass 1: Try direct matches first (prefer exact matches)
  for (const candidateName of sortedEntities) {
    const resolved = await resolveEntityInternal(candidateName, null, null);
    if (resolved && resolved.occurrence_count >= entityThreshold) {
      if (process.env.DEBUG_GRAPH_EXPANSION) {
        console.log(`[GraphRAG] Resolved '${candidateName}' → ${resolved.entity_text} (occ=${resolved.occurrence_count})`);
      }
      return {
        entity_id: resolved.entity_id,
        uri: resolved.uri,
        text: resolved.entity_text,
        type: resolved.entity_type,
        occurrence_count: resolved.occurrence_count,
      };
    }
  }

  // Pass 2: Try plural variants as fallback
  for (const candidateName of sortedEntities) {
    const pluralVariants = [candidateName + 's', candidateName.replace(/s$/, '')];
    for (const variant of pluralVariants) {
      if (variant === candidateName) continue; // Skip if same as original
      const resolvedVariant = await resolveEntityInternal(variant, null, null);
      if (resolvedVariant && resolvedVariant.occurrence_count >= entityThreshold) {
        if (process.env.DEBUG_GRAPH_EXPANSION) {
          console.log(`[GraphRAG] Resolved '${candidateName}' via plural variant '${variant}'`);
        }
        return {
          entity_id: resolvedVariant.entity_id,
          uri: resolvedVariant.uri,
          text: resolvedVariant.entity_text,
          type: resolvedVariant.entity_type,
          occurrence_count: resolvedVariant.occurrence_count,
        };
      }
    }
  }

  return null;
}

// Get graph context (neighborhood edges) for an entity
async function getGraphContext(
  entityId: number,
  maxEdges: number = 20
): Promise<GraphContext | null> {
  const query = `
    WITH edges AS (
      SELECT
        r.predicate,
        COALESCE(r.confidence, 0.5) as confidence,
        r.occurrence_count,
        CASE WHEN r.subject_entity_id = $1 THEN 'out' ELSE 'in' END as direction,
        subj.fuseki_uri as subject_uri,
        subj.entity_text as subject_text,
        obj.fuseki_uri as object_uri,
        obj.entity_text as object_text
      FROM koi_relationships r
      JOIN entity_registry subj ON r.subject_entity_id = subj.id
      JOIN entity_registry obj ON r.object_entity_id = obj.id
      WHERE (r.subject_entity_id = $1 OR r.object_entity_id = $1)
        AND COALESCE(r.confidence, 0) >= 0.5
        AND r.occurrence_count >= 1
      ORDER BY r.occurrence_count DESC, COALESCE(r.confidence, 0) DESC NULLS LAST
      LIMIT $2
    )
    SELECT * FROM edges
  `;

  try {
    const result = await pool.query(query, [entityId, maxEdges]);

    const edges: GraphContextEdge[] = result.rows.map(row => ({
      predicate: row.predicate,
      subject_uri: row.subject_uri,
      subject_text: row.subject_text,
      object_uri: row.object_uri,
      object_text: row.object_text,
      direction: row.direction as 'out' | 'in',
      confidence: parseFloat(row.confidence) || 0.5,
      occurrence_count: parseInt(row.occurrence_count) || 1,
    }));

    return {
      dominant_entity: null, // Will be set by caller
      edges,
      edge_count: edges.length,
      truncated: edges.length >= maxEdges,
      _privacy_warning: 'graph_context is not privacy-filtered',
    };
  } catch (error) {
    console.error('[GraphRAG] Error fetching graph context:', error);
    return null;
  }
}

// Entity-based graph search using koi_entity_chunk_links
// Detects entities in query and returns memories where those entities appear
async function performEntitySearch(query: string, topK: number = 20, privacyFilter: string = '') {
  try {
    // Extract potential entity names from query (words and phrases of 2-4 words)
    const words = query.toLowerCase()
      .replace(/[^\w\s]/g, ' ')
      .split(/\s+/)
      .filter(w => w.length >= 3);

    if (words.length === 0) {
      return [];
    }

    // Build patterns to match: individual words and adjacent word pairs/triples
    const patterns: string[] = [...words];
    for (let i = 0; i < words.length - 1; i++) {
      patterns.push(`${words[i]} ${words[i+1]}`);
      if (i < words.length - 2) {
        patterns.push(`${words[i]} ${words[i+1]} ${words[i+2]}`);
      }
    }

    // FIX-018: Expand patterns with canonical aliases (e.g., "BCT" → "Base Carbon Tonne")
    const expandedPatterns = new Set(patterns);
    const canonicalExpansions: string[] = [];
    for (const pattern of patterns) {
      const canonical = resolveCanonicalAlias(pattern);
      if (canonical && canonical.canonical_name) {
        // Add the canonical name (normalized to lowercase)
        const canonicalLower = canonical.canonical_name.toLowerCase();
        if (!expandedPatterns.has(canonicalLower)) {
          expandedPatterns.add(canonicalLower);
          canonicalExpansions.push(`${pattern} → ${canonical.canonical_name}`);
        }
        // Also add common variations of multi-word names
        const canonicalWords = canonicalLower.split(/\s+/);
        if (canonicalWords.length > 1) {
          expandedPatterns.add(canonicalWords.join(' '));
        }
      }
    }
    const finalPatterns = Array.from(expandedPatterns);

    // Log canonical expansions for debugging
    if (canonicalExpansions.length > 0) {
      console.log(`[EntitySearch] Canonical expansions: ${canonicalExpansions.join(', ')}`);
    }

    // Query for entities matching these patterns
    // Uses source-diversity sampling to prevent any single source from dominating
    const entityQuery = `
      WITH matched_entities AS (
        SELECT DISTINCT entity_name_lower, entity_name, entity_type,
          LENGTH(entity_name_lower) as entity_length
        FROM koi_entity_chunk_links
        WHERE entity_name_lower = ANY($1)
        LIMIT 50
      ),
      entity_memories AS (
        SELECT
          l.chunk_rid,
          l.document_rid,
          array_agg(DISTINCT l.entity_name) as entities_matched,
          COUNT(DISTINCT l.entity_name_lower) as entity_count,
          MAX(me.entity_length) as max_entity_length
        FROM koi_entity_chunk_links l
        JOIN matched_entities me ON l.entity_name_lower = me.entity_name_lower
        GROUP BY l.chunk_rid, l.document_rid
      ),
      with_source AS (
        SELECT
          m.rid,
          m.content->>'text' as content,
          m.metadata->>'source' as source,
          m.metadata->>'url' as url,
          em.entities_matched,
          em.entity_count,
          em.max_entity_length,
          m.published_at,
          CASE
            WHEN m.rid LIKE 'orn:web.page:%' THEN 'web'
            WHEN m.rid LIKE 'regen.github:%' THEN 'github'
            WHEN m.rid LIKE 'regen.gitlab:%' THEN 'gitlab'
            ELSE 'other'
          END as source_type,
          CASE
            WHEN m.rid LIKE 'orn:web.page:regen.network/%' THEN 'main'
            WHEN m.rid LIKE 'orn:web.page:forum.regen.network/%' THEN 'forum'
            WHEN m.rid LIKE 'orn:web.page:registry.regen.network/%' THEN 'registry'
            WHEN m.rid LIKE 'orn:web.page:guides.regen.network/%' THEN 'guides'
            ELSE NULL
          END as web_domain
        FROM entity_memories em
        JOIN koi_memories m ON m.id::text = em.chunk_rid
        WHERE m.superseded_at IS NULL
          AND m.content->>'text' IS NOT NULL
          ${privacyFilter}
      ),
      non_web_ranked AS (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY source_type ORDER BY max_entity_length DESC, entity_count DESC
        ) as source_rank
        FROM with_source WHERE source_type != 'web'
      ),
      web_domain_ranked AS (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY web_domain ORDER BY max_entity_length DESC, entity_count DESC
        ) as domain_rank
        FROM with_source WHERE source_type = 'web'
      ),
      web_diverse AS (
        SELECT *, ROW_NUMBER() OVER (ORDER BY domain_rank, web_domain) as source_rank
        FROM web_domain_ranked WHERE domain_rank <= 10
      ),
      combined AS (
        SELECT rid, content, source, url, entities_matched, entity_count,
               max_entity_length, published_at
        FROM non_web_ranked WHERE source_rank <= 25
        UNION ALL
        SELECT rid, content, source, url, entities_matched, entity_count,
               max_entity_length, published_at
        FROM web_diverse WHERE source_rank <= 50
      ),
      -- Deduplicate by content hash to remove duplicates from multiple sensor runs
      deduplicated AS (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY md5(content)
            ORDER BY published_at DESC NULLS LAST, entity_count DESC
          ) as content_rank
        FROM combined
      )
      SELECT rid, content, source, url, entities_matched, entity_count,
             max_entity_length, published_at
      FROM deduplicated
      WHERE content_rank = 1
      ORDER BY max_entity_length DESC
      LIMIT $2
    `;

    const results = await pool.query(entityQuery, [finalPatterns, topK]);

    // Calculate scores based on entity count (normalized)
    const maxCount = results.rows.length > 0
      ? Math.max(...results.rows.map(r => parseInt(r.entity_count)))
      : 1;

    // Log-only expansion analysis - only runs when DEBUG_GRAPH_EXPANSION is set (zero overhead otherwise)
    if (process.env.DEBUG_GRAPH_EXPANSION) {
      // Extract matched entity names from results, filtering to multi-token (>= 2 words or >= 8 chars)
      // to avoid single-token seeds like "gregory" that explode to 1000+ docs
      const matchedEntityNames = [...new Set(
        results.rows.flatMap(r => (r.entities_matched || []).map((e: string) => e.toLowerCase()))
      )].filter(name => name.includes(' ') || name.length >= 8).slice(0, 50);

      if (matchedEntityNames.length > 0) {
        const neighbors = await get1HopNeighbors(matchedEntityNames, 5, 15);

        if (neighbors.length > 0) {
          console.log(`[GraphExpansion] Query: "${query}"`);
          console.log(`[GraphExpansion] Matched ${matchedEntityNames.length} entities: ${matchedEntityNames.slice(0, 3).join(', ')}`);
          console.log(`[GraphExpansion] Expanded to ${neighbors.length}: ${neighbors.slice(0, 3).map(n => `${n.neighbor_name} (${n.neighbor_type})`).join(', ')}`);
          console.log(`[GraphExpansion] Predicates: ${[...new Set(neighbors.map(n => n.via_predicate))].join(', ')}`);

          // Guard: skip count query if too many neighbors (likely to be expensive)
          if (neighbors.length > 10) {
            console.log(`[GraphExpansion] Skipping doc count (${neighbors.length} neighbors > 10 limit)`);
          } else {
            // Get RIDs already in direct results for comparison (cap at 100)
            const directRids = results.rows.map(r => r.rid).slice(0, 100);

            // Optimized query: counts + small sample, lookup by entity name (lowercased)
            const expansionQuery = `
              WITH expansion_docs AS (
                SELECT DISTINCT m.rid
                FROM koi_entity_chunk_links ecl
                JOIN koi_memories m ON m.id::text = ecl.chunk_rid
                WHERE ecl.entity_name_lower = ANY($1)
                  AND m.superseded_at IS NULL
                  AND m.content->>'text' IS NOT NULL
                  ${privacyFilter}
              )
              SELECT
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE NOT (rid = ANY($2))) AS new_count,
                (SELECT array_agg(rid) FROM (SELECT rid FROM expansion_docs LIMIT 5) s) AS sample_rids
              FROM expansion_docs
            `;
            const expansionResult = await pool.query(expansionQuery, [
              neighbors.map(n => n.neighbor_name.toLowerCase()),
              directRids
            ]);

            const { total_count, new_count, sample_rids } = expansionResult.rows[0] || {};

            console.log(`[GraphExpansion] Would add ${new_count || 0}/${total_count || 0} new docs (${directRids.length} direct)`);
            if (sample_rids?.length) {
              console.log(`[GraphExpansion] Sample RIDs: ${sample_rids.slice(0, 3).join(', ')}`);
            }
          }
        }
      }
    }

    return results.rows.map(row => ({
      id: row.rid,
      content: row.content?.substring(0, 200) + "...",
      similarity: parseInt(row.entity_count) / maxCount,
      score: parseInt(row.entity_count) / maxCount,
      source: 'graph' as const,
      metadata: {
        rid: row.rid,
        source: row.source,
        url: typeof row.url === 'string' ? row.url.trim() : row.url,
        entities_matched: row.entities_matched,
        entity_count: parseInt(row.entity_count),
        published_at: row.published_at || null
      },
      rid: row.rid
    }));

  } catch (error) {
    console.error('[EntitySearch] Error:', error);
    return [];
  }
}

// Semantic search using BGE embeddings
async function performSemanticSearch(query: string, topK: number = 10, filters?: any, privacyFilter: string = '') {
  try {
    // Generate embedding for the query using BGE API
    const bgeResponse = await fetch('http://localhost:8090/encode', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ text: query })
    });

    if (!bgeResponse.ok) {
      console.log('BGE API not available, falling back to ILIKE search');
      // Fallback to simple text search when BGE API is not available
      const dateClauses: string[] = [];
      const params: any[] = [`%${query}%`, `%${query.replace(/\s+/g, '%')}%`];
      if (filters?.date_range?.start) {
        dateClauses.push(`m.published_at >= $${params.length + 1}::timestamptz`);
        params.push(filters.date_range.start);
      }
      if (filters?.date_range?.end) {
        dateClauses.push(`m.published_at <= $${params.length + 1}::timestamptz`);
        params.push(filters.date_range.end);
      }
      const whereDate = dateClauses.length
        ? ` AND (${dateClauses.join(' AND ')}${filters?.include_undated ? ' OR m.published_at IS NULL' : ''})`
        : '';
      const fallbackQuery = `
        SELECT
          m.rid,
          m.content->>'text' as content,
          m.metadata->>'source' as source,
          m.metadata->>'url' as url,
          0.5 as similarity,
          m.published_at
        FROM koi_memories m
        WHERE
          m.content->>'text' IS NOT NULL
          AND LENGTH(m.content->>'text') > 50
        AND m.superseded_at IS NULL
          AND (
            m.content->>'text' ILIKE $1
            OR m.content->>'text' ILIKE $2
          )
          ${whereDate}
          ${privacyFilter}
        LIMIT $${params.length + 1}
      `;
      params.push(topK);
      const results = await pool.query(fallbackQuery, params);
      
      return results.rows.map(row => ({
        id: row.rid,
        content: row.content.substring(0, 200) + "...",
        similarity: parseFloat(row.similarity),
        score: parseFloat(row.similarity),
        source: 'vector' as const,
        metadata: {
          rid: row.rid,
          source: row.source,
          url: typeof row.url === 'string' ? row.url.trim() : row.url,
          similarity: parseFloat(row.similarity),
          published_at: row.published_at || null
        },
        rid: row.rid
      }));
    }

    const bgeData = await bgeResponse.json();
    const queryEmbedding = bgeData.embedding;

    // Convert array to PostgreSQL vector format
    const vectorString = `[${queryEmbedding.join(',')}]`;

    // Optional date filters
    const dateClauses: string[] = [];
    const params: any[] = [vectorString];
    if (filters?.date_range?.start) {
      dateClauses.push(`m.published_at >= $${params.length + 1}::timestamptz`);
      params.push(filters.date_range.start);
    }
    if (filters?.date_range?.end) {
      dateClauses.push(`m.published_at <= $${params.length + 1}::timestamptz`);
      params.push(filters.date_range.end);
    }
    const andDate = dateClauses.length
      ? ` AND (${dateClauses.join(' AND ')}${filters?.include_undated ? ' OR m.published_at IS NULL' : ''})`
      : '';

    const searchQuery = `
      WITH vector_results AS (
        SELECT
          m.rid,
          m.content->>'text' as content,
          m.metadata->>'source' as source,
          m.metadata->>'url' as url,
          e.dim_1024 <=> $1::vector as distance,
          1 - (e.dim_1024 <=> $1::vector) as similarity,
          m.published_at
        FROM koi_memories m
        JOIN koi_embeddings e ON e.memory_id = m.id
        WHERE
          e.dim_1024 IS NOT NULL
          AND m.content->>'text' IS NOT NULL
          AND LENGTH(m.content->>'text') > 50
          AND m.superseded_at IS NULL
          ${andDate}
          ${privacyFilter}
        ORDER BY e.dim_1024 <=> $1::vector
        LIMIT $${params.length + 1} * 3
      ),
      -- Deduplicate by content hash to remove duplicates from multiple sensor runs
      deduplicated AS (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY md5(content)
            ORDER BY similarity DESC
          ) as content_rank
        FROM vector_results
      )
      SELECT rid, content, source, url, distance, similarity, published_at
      FROM deduplicated
      WHERE content_rank = 1
      ORDER BY similarity DESC
      LIMIT $${params.length + 1}
    `;

    params.push(topK);
    const results = await pool.query(searchQuery, params);

    return results.rows.map(row => ({
      id: row.rid,
      content: row.content.substring(0, 200) + "...",
      similarity: parseFloat(row.similarity),
      score: parseFloat(row.similarity),
      source: 'vector' as const,
      metadata: {
        rid: row.rid,
        source: row.source,
        url: typeof row.url === 'string' ? row.url.trim() : row.url,
        similarity: parseFloat(row.similarity),
        published_at: row.published_at || null
      },
      rid: row.rid
    }));
  } catch (error) {
    console.error('Semantic search error:', error);
    return [];
  }
}

// Trigger adaptive extraction using Python extractor
async function triggerAdaptiveExtraction(
  query: string, 
  searchResults: any[], 
  userId: string = 'web-user', 
  agentId: string = 'koi-interface'
) {
  try {
    const extractionPayload = {
      query,
      search_results: searchResults,
      user_id: userId,
      agent_id: agentId
    };

    // Call Python adaptive extraction service
    const response = await fetch('http://localhost:8350/process-koi-event', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(extractionPayload)
    });

    if (!response.ok) {
      throw new Error(`Extraction service error: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Failed to trigger adaptive extraction:', error);
    throw error;
  }
}


// Hybrid RAG Query Endpoint
// Full-text search using PostgreSQL's ts_rank (BM25-like ranking)
async function performKeywordSearch(query: string, topK: number = 10, filters?: any, privacyFilter: string = '') {
  try {
    // Split query into words and clean them
    const words = query.trim().split(/\s+/)
      .map(word => word.replace(/[^a-zA-Z0-9]/g, ''))
      .filter(w => w.length > 0);

    if (words.length === 0) {
      return [];
    }

    // Create both AND and OR queries for better name matching
    // For names like "greg landua", we want to match "Gregory Landua" too
    const andQuery = words.join(' & ');
    const orQuery = words.join(' | ');
    const prefixAndQuery = words.map(w => `${w}:*`).join(' & ');

    // Try AND first, then fall back to OR for broader matching
    const dateClauses: string[] = [];
    const params: any[] = [andQuery, orQuery, prefixAndQuery];
    if (filters?.date_range?.start) {
      dateClauses.push(`published_at >= $${params.length + 1}::timestamptz`);
      params.push(filters.date_range.start);
    }
    if (filters?.date_range?.end) {
      dateClauses.push(`published_at <= $${params.length + 1}::timestamptz`);
      params.push(filters.date_range.end);
    }
    const whereCombined = dateClauses.length
      ? `WHERE (${dateClauses.join(' AND ')}${filters?.include_undated ? ' OR published_at IS NULL' : ''})`
      : '';

    const searchQuery = `
      WITH combined AS (
        SELECT
          m.rid,
          COALESCE(m.content->>'text', m.content->>'title') as content,
          m.metadata->>'source' as source,
          m.metadata->>'url' as url,
          ts_rank_cd(m.content_tsv, to_tsquery('english', $1)) as rank,
          'strict' as match_type,
          m.published_at
        FROM koi_memories m
        WHERE
          m.content_tsv @@ to_tsquery('english', $1)
          AND (
            (m.content->>'text' IS NOT NULL AND LENGTH(m.content->>'text') > 50)
            OR (m.content->>'title' IS NOT NULL AND LENGTH(m.content->>'title') > 5)
          )
          AND m.superseded_at IS NULL
          ${privacyFilter}

        UNION ALL

        SELECT
          m.rid,
          COALESCE(m.content->>'text', m.content->>'title') as content,
          m.metadata->>'source' as source,
          m.metadata->>'url' as url,
          ts_rank_cd(m.content_tsv, to_tsquery('english', $2)) as rank,
          'relaxed' as match_type,
          m.published_at
        FROM koi_memories m
        WHERE
          m.content_tsv @@ to_tsquery('english', $2)
          AND m.content_tsv @@ to_tsquery('english', $3)
          AND (
            (m.content->>'text' IS NOT NULL AND LENGTH(m.content->>'text') > 50)
            OR (m.content->>'title' IS NOT NULL AND LENGTH(m.content->>'title') > 5)
          )
          AND m.superseded_at IS NULL
          ${privacyFilter}
          AND NOT EXISTS (
            SELECT 1 FROM koi_memories m2
            WHERE m2.rid = m.rid
            AND m2.content_tsv @@ to_tsquery('english', $1)
          )
      ),
      -- Deduplicate by content hash to remove duplicates from multiple sensor runs
      deduplicated AS (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY md5(content)
            ORDER BY rank DESC, match_type
          ) as content_rank
        FROM combined
        ${whereCombined}
      )
      SELECT rid, content, source, url, rank, match_type, published_at
      FROM deduplicated
      WHERE content_rank = 1
      ORDER BY
        CASE WHEN match_type = 'strict' THEN 0 ELSE 1 END,  -- strict first
        rank DESC
      LIMIT $${params.length + 1}
    `;

    // Increased multiplier to ensure OR results are well represented
    // With AND getting ~10 results and OR getting ~10, we need more total results
    params.push(Math.max(topK * 3, 50));
    const results = await pool.query(searchQuery, params);

    const rows = results.rows;

    // Recompute maxRank after filtering to avoid score compression from filtered-out high-rank results
    const maxRank = rows.length > 0
      ? Math.max(...rows.map(r => parseFloat(r.rank)))
      : 1;

    if (process.env.DEBUG_KEYWORD_SEARCH) {
      const strictCount = rows.filter(r => r.match_type === 'strict').length;
      console.log(`[Keyword Search] Query: "${query}", Found: ${rows.length}, Strict: ${strictCount}, Max rank: ${maxRank.toFixed(3)}`);
    }

    return rows.slice(0, topK).map(row => {
      const rawRank = parseFloat(row.rank);

      // Logarithmic scaling for better score discrimination
      const normalizedScore = Math.log(1 + rawRank) / Math.log(1 + maxRank);

      // Boost for exact phrase matches (case-insensitive)
      const queryLower = query.toLowerCase();
      const contentLower = row.content.toLowerCase();
      const hasExactMatch = contentLower.includes(queryLower);

      // Boost strict matches (AND query) over relaxed matches (OR query)
      const matchTypeBoost = row.match_type === 'strict' ? 1.0 : 0.9;
      const finalScore = (hasExactMatch ? normalizedScore * 1.2 : normalizedScore) * matchTypeBoost;

      return {
        id: row.rid,
        content: (row.content?.substring(0, 200) || '') + (row.content?.length > 200 ? "..." : ""),
        similarity: finalScore,
        score: finalScore,
        source: 'keyword' as const,
        metadata: {
          rid: row.rid,
          source: row.source,
          url: typeof row.url === 'string' ? row.url.trim() : row.url,
          published_at: row.published_at || null,
          fts_rank: rawRank,
          normalized_score: normalizedScore,
          exact_match_boost: hasExactMatch,
          match_type: row.match_type
        },
        rid: row.rid
      };
    });
  } catch (error) {
    console.error('[Keyword Search] FTS FAILED, falling back to ILIKE:', error);
    // Fallback to ILIKE if FTS fails
    const dateClauses: string[] = [];
    const params: any[] = [`%${query}%`, `%${query}%`];
    if (filters?.date_range?.start) {
      dateClauses.push(`m.published_at >= $${params.length + 1}::timestamptz`);
      params.push(filters.date_range.start);
    }
    if (filters?.date_range?.end) {
      dateClauses.push(`m.published_at <= $${params.length + 1}::timestamptz`);
      params.push(filters.date_range.end);
    }
    const andDate = dateClauses.length
      ? ` AND (${dateClauses.join(' AND ')}${filters?.include_undated ? ' OR m.published_at IS NULL' : ''})`
      : '';

    const fallbackQuery = `
      SELECT
        m.rid,
        COALESCE(m.content->>'text', m.content->>'title') as content,
        m.metadata->>'source' as source,
        m.metadata->>'url' as url,
        0.5 as rank,
        m.published_at
      FROM koi_memories m
      WHERE
        COALESCE(m.content->>'text', m.content->>'title') ILIKE $1
        AND (
          (m.content->>'text' IS NOT NULL AND LENGTH(m.content->>'text') > 50)
          OR (m.content->>'title' IS NOT NULL AND LENGTH(m.content->>'title') > 5)
        )
        AND m.superseded_at IS NULL
        ${andDate}
        ${privacyFilter}
      ORDER BY CASE
        WHEN COALESCE(m.content->>'text', m.content->>'title') ILIKE $2 THEN 3  -- Exact phrase match
        WHEN COALESCE(m.content->>'text', m.content->>'title') ILIKE $1 THEN 2  -- Contains all words
        ELSE 1
      END DESC
      LIMIT $${params.length + 1}
    `;

    params.push(topK);
    const results = await pool.query(fallbackQuery, params);

    return results.rows.map(row => ({
      id: row.rid,
      content: (row.content?.substring(0, 200) || '') + (row.content?.length > 200 ? "..." : ""),
      similarity: parseFloat(row.rank),
      score: parseFloat(row.rank),
      source: 'keyword' as const,
      metadata: {
        rid: row.rid,
        source: row.source,
        url: typeof row.url === 'string' ? row.url.trim() : row.url,
        published_at: row.published_at || null
      },
      rid: row.rid
    }));
  }
}
app.post('/api/koi/query', async (req, res) => {
  // Session D1: Generate request_id for traceability
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);

  try {
    const {
      question: questionParam,
      query: queryParam,  // Accept both 'question' and 'query' for compatibility
      user_id = 'web-user',
      agent_id = 'koi-interface',
      limit = 10,
      filters = {},
      graph_context: requestGraphContext = false  // Week 13: GraphRAG body field
    } = req.body;

    // Accept either 'question' or 'query' parameter (question takes precedence)
    const question = questionParam || queryParam;

    // Also accept query param for backward compatibility
    const queryParamGraphContext = req.query.graph_context === 'true';

    if (!question) {
      const envelope = createErrorEnvelope(requestId, {
        code: 'MISSING_PARAMETER',
        message: 'Either "question" or "query" parameter is required',
        retryable: false,
      });
      return res.status(400).json(envelope);
    }

    const startTime = Date.now();

    // Session D1: Initialize tool trace and warnings
    const toolTrace: ToolTraceEntry[] = [];
    const warnings: WarningCode[] = [];

    // Extract session token from Authorization header and validate
    // Format: "Bearer <session_token>" - NOT the Google OAuth token
    const authHeader = req.headers['authorization'] as string | undefined;
    const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;

    // Validate session token and get authenticated user email (if any)
    const authenticatedEmail = await validateSessionToken(sessionToken);
    const isAuthenticated = !!authenticatedEmail;
    const privacyFilter = buildPrivacyFilter(isAuthenticated);

    // Log auth status for debugging (gated to avoid log volume/PII)
    if (process.env.DEBUG_AUTH) {
      const logEmail = authenticatedEmail || req.headers['x-user-email'] as string | undefined;
      if (logEmail || sessionToken) {
        console.log(`[Query] User: ${logEmail || 'unknown'}, Authenticated: ${isAuthenticated}${sessionToken ? ' (session token provided)' : ''}`);
      }
    }

    // Perform hybrid search with RRF
    // Includes vector (semantic), entity (graph), and keyword search
    // Higher limits to capture diverse results before fusion
    let vectorResults: any[] = [];
    let entityResults: any[] = [];
    let keywordResults: any[] = [];

    try {
      [vectorResults, entityResults, keywordResults] = await Promise.all([
        performSemanticSearch(question, 20, filters, privacyFilter),
        performEntitySearch(question, 100, privacyFilter).catch(e => {
          console.error('[EntitySearch] Error:', e);
          return [];
        }),
        performKeywordSearch(question, 20, filters, privacyFilter)
      ]);
    } catch (err) {
      console.error('[Search] Error in parallel search:', err);
    }

    // Log search results counts (gated to avoid log volume)
    if (process.env.DEBUG_FUSION) {
      console.log(`[Search] Results - Vector: ${vectorResults.length}, Entity: ${entityResults.length}, Keyword: ${keywordResults.length}`);
    }

    // Apply Reciprocal Rank Fusion with entity/graph results
    let fusedResults = reciprocalRankFusion(vectorResults, entityResults, keywordResults);

    // Week 17: Polysemy-aware reranking (optional)
    let resolvedEntity: PolysemyResolution | null = null;
    const enablePolysemyRerank = String(process.env.ENABLE_POLYSEMY_RERANK).toLowerCase() === 'true';
    const debugPolysemy = String(process.env.DEBUG_POLYSEMY_RERANK).toLowerCase() === 'true';
    let polysemyDebugInfo: any = null;

    // Always log entry point when debug is enabled
    if (debugPolysemy) {
      console.log(`[PolysemyRerank] Entry: enabled=${enablePolysemyRerank}, fusedResults=${fusedResults.length}, query="${question}"`);
    }

    if (enablePolysemyRerank && fusedResults.length > 0) {
      try {
        if (debugPolysemy) {
          console.log(`[PolysemyRerank] Calling resolveQueryPolysemy...`);
        }
        resolvedEntity = await resolveQueryPolysemy(question);

        if (resolvedEntity) {
          const { results: rerankedResults, boosted_count } = applyPolysemyRerank(fusedResults, resolvedEntity);
          fusedResults = rerankedResults;

          polysemyDebugInfo = {
            enabled: true,
            resolved: true,
            entity_text: resolvedEntity.entity_text,
            entity_type: resolvedEntity.entity_type,
            occurrence_count: resolvedEntity.occurrence_count,
            is_polysemous: resolvedEntity.is_polysemous,
            variant_count: resolvedEntity.variant_count,
            resolution_method: resolvedEntity.resolution_method,
            boosted_count,
            total_results: fusedResults.length,
          };

          if (debugPolysemy) {
            console.log(`[PolysemyRerank] Resolved to: ${resolvedEntity.entity_text} (${resolvedEntity.entity_type})`);
            console.log(`[PolysemyRerank] Boosted ${boosted_count}/${fusedResults.length} results`);
          }
        } else {
          polysemyDebugInfo = {
            enabled: true,
            resolved: false,
            reason: 'no_entity_match',
            query: question,
          };
          if (debugPolysemy) {
            console.log(`[PolysemyRerank] No entity resolved for query: "${question}"`);
          }
        }
      } catch (err) {
        polysemyDebugInfo = {
          enabled: true,
          resolved: false,
          reason: 'error',
          error: String(err),
        };
        console.error('[PolysemyRerank] Error:', err);
      }
    } else {
      polysemyDebugInfo = {
        enabled: enablePolysemyRerank,
        resolved: false,
        reason: enablePolysemyRerank ? 'no_fused_results' : 'feature_disabled',
      };
    }

    // Calculate confidence
    const confidence = calculateConfidence(fusedResults);
    
    // Check if extraction should be triggered
    const triggeredExtraction = process.env.ENABLE_ADAPTIVE_EXTRACTION === 'true' && shouldTriggerExtraction(confidence);
    
    // Select documents for extraction if needed
    let selectedDocs = [];
    let extractionResult = null;
    if (triggeredExtraction) {
      selectedDocs = selectDocumentsForExtraction(fusedResults, 3);
      
      // Trigger Python adaptive extraction
      try {
        if (process.env.DEBUG_EXTRACTION) {
          console.log(`🔧 Triggering adaptive extraction for query: "${question}" (confidence: ${confidence.toFixed(3)})`);
        }
        extractionResult = await triggerAdaptiveExtraction(question, fusedResults, user_id, agent_id);
        if (process.env.DEBUG_EXTRACTION) {
          console.log(`✅ Extraction completed with ${extractionResult?.extracted_facts?.length || 0} facts extracted`);
        }
      } catch (error) {
        console.error('❌ Adaptive extraction failed:', error);
        // Continue without failing the main query
      }
    }

    const responseTime = Date.now() - startTime;

    // Week 13: GraphRAG context retrieval
    // Week 21: Enhanced with question-query and multi-entity resolution
    let graphContext: GraphContext | null = null;
    const enableGraphRAG = process.env.ENABLE_GRAPHRAG_CONTEXT === 'true' || requestGraphContext || queryParamGraphContext;

    if (enableGraphRAG) {
      try {
        // Week 21: Classify the query type
        const queryClassification = classifyQuery(question);
        if (process.env.DEBUG_GRAPH_EXPANSION) {
          console.log(`[GraphRAG-W21] Query classified as: ${queryClassification.type}, candidates: ${queryClassification.entity_candidates.join(', ')}`);
        }

        // Try different resolution strategies based on query type
        if (queryClassification.type === 'multi_entity' && queryClassification.entity_candidates.length >= 2) {
          // Week 21: Multi-entity resolution
          const multiResult = await resolveMultiEntityQuery(
            queryClassification.entity_candidates.slice(0, 2) as [string, string],
            2 // minOccurrenceCount
          );

          if (multiResult.primary) {
            // Get merged graph context
            const merged = await getMergedGraphContext(
              multiResult.primary.entity_id,
              multiResult.secondary?.entity_id || null,
              multiResult.primary.text,
              multiResult.secondary?.text || null,
              20 // maxEdges
            );

            graphContext = {
              dominant_entity: {
                uri: multiResult.primary.uri,
                text: multiResult.primary.text,
                type: multiResult.primary.type,
                occurrence_count: multiResult.primary.occurrence_count,
              },
              secondary_entity: multiResult.secondary ? {
                uri: multiResult.secondary.uri,
                text: multiResult.secondary.text,
                type: multiResult.secondary.type,
                occurrence_count: multiResult.secondary.occurrence_count,
              } : null,
              edges: merged.edges,
              edge_count: merged.edges.length,
              truncated: merged.truncated,
              query_type: 'multi_entity',
              _privacy_warning: 'graph_context is not privacy-filtered',
            };

            if (process.env.DEBUG_GRAPH_EXPANSION) {
              console.log(`[GraphRAG-W21] Multi-entity context: primary='${multiResult.primary.text}', secondary='${multiResult.secondary?.text || 'none'}', edges: ${graphContext.edge_count}`);
            }
          }
        } else if (queryClassification.type === 'question' && queryClassification.entity_candidates.length > 0) {
          // Week 21: Question-query resolution
          // First try to get dominant entity from entity search (existing behavior)
          let dominant = entityResults.length > 0 ? await getDominantEntity(entityResults, question) : null;

          // If entity search didn't find anything, try extracting from question
          if (!dominant) {
            const questionResolved = await resolveQuestionQueryEntity(queryClassification.entity_candidates, 2);
            if (questionResolved) {
              dominant = {
                entity_id: questionResolved.entity_id,
                uri: questionResolved.uri,
                text: questionResolved.text,
                type: questionResolved.type,
                occurrence_count: questionResolved.occurrence_count,
              };
              if (process.env.DEBUG_GRAPH_EXPANSION) {
                console.log(`[GraphRAG-W21] Question resolved via candidate '${questionResolved.matched_candidate}' → ${dominant.text}`);
              }
            }
          }

          if (dominant) {
            graphContext = await getGraphContext(dominant.entity_id, 20);
            if (graphContext) {
              graphContext.dominant_entity = {
                uri: dominant.uri,
                text: dominant.text,
                type: dominant.type,
                occurrence_count: dominant.occurrence_count,
              };
              graphContext.query_type = 'question';
            }

            if (process.env.DEBUG_GRAPH_EXPANSION) {
              console.log(`[GraphRAG-W21] Question context for '${dominant.text}' (${dominant.type}), edges: ${graphContext?.edge_count || 0}`);
            }
          }
        } else {
          // Entity-style query: use existing getDominantEntity
          const dominant = entityResults.length > 0 ? await getDominantEntity(entityResults, question) : null;

          if (dominant) {
            graphContext = await getGraphContext(dominant.entity_id, 20);
            if (graphContext) {
              graphContext.dominant_entity = {
                uri: dominant.uri,
                text: dominant.text,
                type: dominant.type,
                occurrence_count: dominant.occurrence_count,
              };
              graphContext.query_type = 'entity';
            }

            if (process.env.DEBUG_GRAPH_EXPANSION) {
              console.log(`[GraphRAG] Context retrieved for entity '${dominant.text}' (${dominant.type}), edges: ${graphContext?.edge_count || 0}`);
            }
          } else if (process.env.DEBUG_GRAPH_EXPANSION) {
            console.log(`[GraphRAG] No dominant entity found for query: "${question}"`);
          }
        }
      } catch (err) {
        console.error('[GraphRAG] Error retrieving graph context:', err);
      }
    }

    // Log the query
    try {
      await logQuery(pool, {
        query_text: question,
        user_id,
        agent_id,
        confidence_score: confidence,
        triggered_extraction: triggeredExtraction,
        response_time_ms: responseTime,
        results: fusedResults.slice(0, 5)
      });
    } catch (error) {
      console.error('Query logging failed:', error);
    }

    // Session D1: Add tool trace entries for search operations
    toolTrace.push({
      tool: 'hybrid_search',
      params_summary: summarizeParams({ question, limit, filters }),
      timestamp: new Date(startTime).toISOString(),
      data_source: 'koi-derived',
      duration_ms: responseTime,
    });

    // Session D1: Add warnings for triggered extraction
    if (triggeredExtraction) {
      warnings.push('extraction_triggered');
    }

    // Session D1: Add privacy filter warning if applicable
    if (!isAuthenticated) {
      warnings.push('privacy_filtered');
    }

    // Filter out derived artifacts (e.g., crawl dumps committed to GitHub) to avoid double-indexing + dead links
    const userResults = fusedResults.filter(r => !shouldExcludeKoiResultSource(r.rid, r.metadata?.url));

    // Format response data
    const responseData: any = {
      question,
      total_results: userResults.length,
      confidence: confidence,
      execution_time: responseTime / 1000,
      triggered_extraction: triggeredExtraction,
      results: userResults.slice(0, limit).map(r => ({
        title: `Document ${r.rid}`,
        content: r.content,
        score: r.score,
        source: r.source,
        rid: r.rid,
        metadata: r.metadata || {}
      }))
    };

    // Add graph context if available
    if (graphContext) {
      responseData.graph_context = graphContext;
      toolTrace.push({
        tool: 'graph_context',
        params_summary: `entity=${graphContext.dominant_entity?.text || 'unknown'}`,
        timestamp: new Date().toISOString(),
        data_source: 'graph',
      });
    }

    // Week 17: Add resolved entity info if polysemy rerank was enabled
    if (resolvedEntity) {
      responseData.resolved_entity = {
        entity_text: resolvedEntity.entity_text,
        entity_type: resolvedEntity.entity_type,
        uri: resolvedEntity.uri,
        occurrence_count: resolvedEntity.occurrence_count,
        is_polysemous: resolvedEntity.is_polysemous,
        variant_count: resolvedEntity.variant_count,
        resolution_method: resolvedEntity.resolution_method,
        alternatives: resolvedEntity.alternatives,
      };
    }

    // Always include polysemy debug info when debug is enabled
    if (debugPolysemy && polysemyDebugInfo) {
      responseData.polysemy_debug = polysemyDebugInfo;
    }

    // Session D1: Extract citations from results
    const citations = extractCitations(userResults.slice(0, limit));

    // Session D1: Create envelope response
    const envelope = createSuccessEnvelope(requestId, responseData, {
      citations,
      warnings,
      tool_trace: toolTrace,
    });

    res.json(envelope);

  } catch (error) {
    console.error('Query error:', error);
    const koiError: KoiError = {
      code: 'QUERY_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: error instanceof Error &&
        (error.message.includes('timeout') || error.message.includes('connection')),
    };
    if (koiError.retryable) {
      koiError.retry_after_ms = 1000;
    }
    const envelope = createErrorEnvelope(requestId, koiError);
    res.status(500).json(envelope);
  }
});

// Podcast chatbot endpoint with LLM synthesis
app.post('/api/podcast/chat', async (req, res) => {
  try {
    const { question, limit = 5 } = req.body;

    if (!question) {
      return res.status(400).json({ error: 'Question is required' });
    }

    // Call the existing hybrid RAG API with larger limit to get podcast results after filtering
    const ragResponse = await fetch('http://localhost:8301/api/koi/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        question,
        limit: 50  // Request more results so we can filter to podcasts and still have enough
      })
    });

    if (!ragResponse.ok) {
      return res.status(500).json({ error: 'Search failed', synthesized: false });
    }

    const ragData = await ragResponse.json();
    const allResults = ragData.results || [];

    // Filter to only podcast results, then limit to requested amount
    const filteredResults = allResults.filter((r: any) => {
      const rid = r.rid || '';
      return rid.includes('podcast');
    });
    const results = filteredResults.slice(0, limit || 5);

    console.log(`[Podcast Chat] Filtered ${filteredResults.length} podcast results from ${allResults.length} total, using top ${results.length}`);
    if (results.length > 0) {
      console.log(`[Podcast Chat] Sample RID: ${results[0].rid}`);
    }

    if (results.length === 0) {
      return res.json({
        question,
        answer: 'No relevant information found about that in the podcast.',
        synthesized: false
      });
    }

    // Load podcast metadata
    let podcastData: any = null;
    try {
      const fs = await import('fs');
      const podcastJson = fs.readFileSync('/opt/projects/koi-processor/static/podcast/podcast_map_3d.json', 'utf8');
      podcastData = JSON.parse(podcastJson);
      console.log(`[Podcast Chat] Loaded ${podcastData.episodes?.length || 0} episodes from metadata`);
    } catch (error) {
      console.error('[Podcast Chat] Error loading podcast metadata:', error);
    }

    // Extract episodes and build context for LLM
    const episodesInfo: Record<string, {url: string, episode_int_id: number}> = {};
    const contextParts: string[] = [];

    results.forEach((r: any, idx: number) => {
      const content = r.content || '';
      contextParts.push(`[${idx + 1}] ${content}`);

      // Try to extract episode metadata from RID
      // Format: regen.podcast:podcast_716040283#chunk34
      const rid = r.rid || '';
      const match = rid.match(/:podcast_(\d+)/);
      if (match) {
        const episodeIdStr = `podcast_${match[1]}`;
        console.log(`[Episode Extract] RID: ${rid} -> episode_id: ${episodeIdStr}`);

        if (podcastData && podcastData.episodes) {
          // Find episode in podcast data by episode_id field
          const episode = podcastData.episodes.find((ep: any) => ep.episode_id === episodeIdStr);
          console.log(`[Episode Extract] Found episode:`, episode ? episode.title : 'NOT FOUND');

          if (episode) {
            const episodeTitle = episode.title || episodeIdStr;
            const episodeUrl = episode.url || `https://regen.gaiaai.xyz/podcast`;
            if (!(episodeTitle in episodesInfo)) {
              episodesInfo[episodeTitle] = {
                url: episodeUrl,
                episode_int_id: episode.episode_int_id
              };
              console.log(`[Episode Extract] Added: ${episodeTitle}`);
            }
          }
        }
      } else {
        console.log(`[Episode Extract] No match for RID: ${rid}`);
      }
    });

    console.log(`[Episode Extract] Total episodes found: ${Object.keys(episodesInfo).length}`);

    const context = contextParts.join('\n\n');

    // LLM synthesis with OpenAI
    const openaiKey = process.env.OPENAI_API_KEY;
    if (!openaiKey) {
      return res.json({
        question,
        results: results.slice(0, 5),
        synthesized: false,
        error: 'OpenAI not configured'
      });
    }

    try {
      const systemPrompt = `You are a helpful AI assistant for the Planetary Regeneration Podcast.

Answer questions based ONLY on the provided context from podcast transcripts. Be concise and direct in your response.

Do NOT include any citations, source numbers, or episode lists in your answer. Just provide a clear, informative response to the question.`;

      const userPrompt = `Question: ${question}

Context from podcast:
${context}

Answer the question using the context above.`;

      const openaiResponse = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${openaiKey}`
        },
        body: JSON.stringify({
          model: 'gpt-4o-mini',
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userPrompt }
          ],
          temperature: 0.7,
          max_tokens: 600
        })
      });

      if (!openaiResponse.ok) {
        throw new Error(`OpenAI API error: ${openaiResponse.status}`);
      }

      const openaiData = await openaiResponse.json();
      const answer = openaiData.choices[0].message.content;

      return res.json({
        question,
        answer,
        episodes: episodesInfo,
        synthesized: true,
        total_results: results.length
      });

    } catch (error) {
      console.error('LLM synthesis error:', error);
      return res.json({
        question,
        results: results.slice(0, 5),
        synthesized: false,
        error: error instanceof Error ? error.message : 'LLM error'
      });
    }

  } catch (error) {
    console.error('Podcast chat error:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Graph Query Endpoint - Direct Apache AGE access

// =============================================================================
// Chat Endpoint - Single-turn RAG chat for bizdev dashboards
// =============================================================================

const CLIENT_CONTEXTS: Record<string, string> = {
  landbanking: 'The client is Landbanking Group, a Munich-based natural capital fintech. They have the Nature Equity Asset (Landler) platform measuring carbon, biodiversity, soil, water, and social dimensions. Key partner: Ritter Sport. Focus on carbon credit registration via existing C-class.',
  renew: 'The client is Renew/RePlanet, a UK-based nature data & intelligence company. They use the Wallacea Trust v2.1 biodiversity scoring methodology across 5 taxa. Focus areas: BT01 biodiversity credits and carbon stacking opportunities.',
};

const CHAT_SYSTEM_PROMPT = `You are a Regen Network registry expert. Answer questions using ONLY the provided context from the knowledge graph. Be concise and cite sources by number [1], [2], etc.

Key facts:
- Regen Registry has 78 credit batches, 58 projects, 13 credit classes
- Credit types: Carbon (C01-C09), Biodiversity (BT01), plus KSH01, MBS01, USS01
- BT01 uses conservation-weighted biodiversity scoring per 10m²

If the context doesn't contain enough information, say so honestly.`;

app.post('/api/koi/chat', async (req, res) => {
  try {
    const { query, client, limit = 5 } = req.body;

    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }

    // Call internal RAG search
    const ragResponse = await fetch('http://localhost:8301/api/koi/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: query, limit: limit * 2 })
    });

    if (!ragResponse.ok) {
      return res.status(500).json({ error: 'Search failed' });
    }

    const ragData = await ragResponse.json();
    const results = (ragData.data?.results || ragData.results || []).slice(0, limit);

    if (results.length === 0) {
      return res.json({
        answer: 'No relevant information found in the knowledge graph for that query.',
        sources: [],
        model: 'none'
      });
    }

    // Build numbered citation context
    const contextParts: string[] = [];
    const sources = results.map((r: any, idx: number) => {
      const content = r.content || r.text || '';
      const title = r.title || r.rid || 'Unknown';
      contextParts.push(`[${idx + 1}] ${title}: ${content}`);
      return {
        rid: r.rid || null,
        title,
        excerpt: content.slice(0, 200),
        score: r.score || r.confidence || null,
        source: r.source || null,
        url: r.url || null,
      };
    });

    const context = contextParts.join('\n\n');

    // Build system prompt with optional client context
    let systemPrompt = CHAT_SYSTEM_PROMPT;
    if (client && CLIENT_CONTEXTS[client]) {
      systemPrompt += `\n\nClient context: ${CLIENT_CONTEXTS[client]}`;
    }

    // Call OpenAI
    const openaiKey = process.env.OPENAI_API_KEY;
    if (!openaiKey) {
      return res.json({
        answer: 'AI synthesis not available (API key not configured). Here are the raw search results.',
        sources,
        model: 'none'
      });
    }

    const openaiResponse = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${openaiKey}`
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: `Question: ${query}\n\nContext:\n${context}\n\nAnswer using the context above, citing sources by number.` }
        ],
        temperature: 0.7,
        max_tokens: 600
      })
    });

    if (!openaiResponse.ok) {
      throw new Error(`OpenAI API error: ${openaiResponse.status}`);
    }

    const openaiData = await openaiResponse.json();
    const answer = openaiData.choices[0].message.content;

    return res.json({
      answer,
      sources,
      model: 'gpt-4o-mini'
    });

  } catch (error) {
    console.error('[KOI Chat] Error:', error);
    res.status(500).json({
      error: 'Chat failed',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Accepts 'query_type' for specific graph operations
// Also accepts 'query' as a natural language description (for OpenAPI compatibility)
app.post('/api/koi/graph', async (req, res) => {
  // Session D1: Generate request_id for traceability
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);
  const startTime = Date.now();

  try {
    const { query_type, query, ...params } = req.body;

    // List of supported query types for helpful error messages
    const supportedQueryTypes = [
      'list_repos', 'find_by_type', 'search_entities', 'list_modules',
      'get_module', 'keeper_for_msg', 'msgs_for_keeper', 'related_entities',
      'list_entity_types', 'get_entity_stats', 'list_concepts', 'explain_concept',
      'find_concept_for_query', 'find_callers', 'find_callees', 'find_call_graph',
      'search_modules', 'module_entities', 'module_for_entity',
      'code_impact', 'list_communities', 'community_members', 'community_for_entity',
      'list_flows', 'flow_steps', 'flows_for_entity', 'check_staleness'
    ];

    // If only 'query' is provided (OpenAPI compatibility), provide guidance
    if (!query_type && query) {
      const envelope = createErrorEnvelope(requestId, {
        code: 'MISSING_QUERY_TYPE',
        message: 'query_type is required for graph queries. The "query" field is for natural language description.',
        retryable: false,
      });
      // Add helpful info to the error response
      (envelope.data as any) = {
        hint: 'Use "query_type" to specify the operation.',
        supported_query_types: supportedQueryTypes,
        examples: [
          { query_type: 'list_repos', description: 'List all indexed repositories' },
          { query_type: 'find_by_type', params: { entity_type: 'Function', limit: 10 }, description: 'Find entities by type' },
          { query_type: 'search_entities', params: { entity_name: 'credit' }, description: 'Search entities by name' },
          { query_type: 'related_entities', params: { entity_name: 'MsgCreateBatch' }, description: 'Find related entities' },
        ]
      };
      return res.status(400).json(envelope);
    }

    if (!query_type) {
      const envelope = createErrorEnvelope(requestId, {
        code: 'MISSING_QUERY_TYPE',
        message: 'query_type is required',
        retryable: false,
      });
      (envelope.data as any) = { supported_query_types: supportedQueryTypes };
      return res.status(400).json(envelope);
    }

    // Validate query_type
    if (!supportedQueryTypes.includes(query_type)) {
      const envelope = createErrorEnvelope(requestId, {
        code: 'INVALID_QUERY_TYPE',
        message: `Invalid query_type: ${query_type}`,
        retryable: false,
      });
      (envelope.data as any) = { supported_query_types: supportedQueryTypes };
      return res.status(400).json(envelope);
    }

    // Create a client for this request to manage AGE setup
    const client = await pool.connect();

    try {
      // Load AGE extension and set search path
      await client.query("LOAD 'age'");
      await client.query("SET search_path = ag_catalog, '$user', public");

      let cypherQuery = '';
      let queryParams: any[] = [];

      // Build cypher query based on query_type
      switch (query_type) {
        case 'list_repos':
          // Get all unique repository names from entities
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n) WHERE n.repo IS NOT NULL
            WITH DISTINCT n.repo as repo_name, count(*) as entity_count
            RETURN {name: repo_name} as repo, entity_count
            ORDER BY repo_name
          $$) as (repo agtype, entity_count agtype)`;
          break;

        case 'find_by_type':
          const entityType = params.entity_type || 'Function';
          const limit = params.limit || 10;
          const repoFilter = params.repo_name ? `WHERE r.name = '${params.repo_name}'` : '';

          // Match entity type directly by graph label
          const typeMatch = `MATCH (n:${entityType})`;

          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            ${typeMatch}
            ${repoFilter ? `MATCH (n)-[:DEFINED_IN]->(f:File)-[:IN_REPO]->(r:Repository) ${repoFilter}` : ''}
            RETURN properties(n) as entity
            LIMIT ${limit}
          $$) as (entity agtype)`;
          break;

        case 'search_entities':
          const searchTerm = params.entity_name || '';
          const searchLimit = params.limit || 10;
          const searchRepoFilter = params.repo_name ? `WHERE n.repo = '${params.repo_name}' AND` : 'WHERE';

          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n)
            ${searchRepoFilter} n.name =~ '(?i).*${searchTerm}.*'
            RETURN labels(n)[0] as type, properties(n) as entity
            LIMIT ${searchLimit}
          $$) as (type agtype, entity agtype)`;
          break;

        case 'list_modules':
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (m:Module)
            RETURN properties(m) as module
            ORDER BY m.name
          $$) as (module agtype)`;
          break;

        case 'get_module':
          const moduleName = params.module_name || '';
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (m:Module {name: '${moduleName}'})
            OPTIONAL MATCH (m)<-[:IN_MODULE]-(e)
            RETURN properties(m) as module, labels(e)[0] as entity_type, properties(e) as entity
          $$) as (module agtype, entity_type agtype, entity agtype)`;
          break;

        case 'keeper_for_msg':
          const msgName = params.entity_name || '';
          // Updated to query generic Struct with domain_type='keeper'
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (msg {name: '${msgName}'})-[:HANDLED_BY]->(k:Struct)
            WHERE k.domain_type = 'keeper'
            RETURN properties(k) as keeper
          $$) as (keeper agtype)`;
          break;

        case 'msgs_for_keeper':
          const keeperName = params.entity_name || '';
          // Updated to query generic Struct with domain_type='keeper' and domain_type='message'
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (k:Struct {name: '${keeperName}'})<-[:HANDLED_BY]-(msg:Struct)
            WHERE k.domain_type = 'keeper' AND msg.domain_type = 'message'
            RETURN properties(msg) as message
          $$) as (message agtype)`;
          break;

        case 'related_entities':
          const entityName = params.entity_name || '';
          const relatedLimit = params.limit || 10;
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n {name: '${entityName}'})-[r]-(related)
            RETURN labels(related)[0] as type, properties(related) as entity, type(r) as relationship
            LIMIT ${relatedLimit}
          $$) as (type agtype, entity agtype, relationship agtype)`;
          break;

        case 'list_entity_types':
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n)
            WITH labels(n)[0] as entity_type, count(*) as count
            WHERE entity_type IS NOT NULL
            RETURN entity_type, count
            ORDER BY count DESC
          $$) as (entity_type agtype, count agtype)`;
          break;

        case 'get_entity_stats':
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n)
            WITH labels(n)[0] as entity_type,
                 count(*) as count,
                 collect(DISTINCT n.language)[0..5] as languages,
                 collect(DISTINCT n.repo)[0..5] as repos
            WHERE entity_type IS NOT NULL
            RETURN entity_type, count, languages, repos
            ORDER BY count DESC
          $$) as (entity_type agtype, count agtype, languages agtype, repos agtype)`;
          break;

        case 'list_concepts':
          // List all concepts with their descriptions
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (c:Concept)
            RETURN properties(c) as concept
            ORDER BY c.name
          $$) as (concept agtype)`;
          break;

        case 'explain_concept':
          // Get a concept and its related entities via EXPLAINS edges
          const conceptName = params.concept_name || '';
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (c:Concept {name: '${conceptName}'})
            OPTIONAL MATCH (c)-[:EXPLAINS]->(e)
            RETURN properties(c) as concept, 
                   labels(e)[0] as entity_type,
                   properties(e) as entity
          $$) as (concept agtype, entity_type agtype, entity agtype)`;
          break;

        case 'find_concept_for_query':
          // Find concepts matching a natural language query via keywords
          const userQuery = params.query || '';
          const queryLower = userQuery.toLowerCase();
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (c:Concept)
            WHERE c.keywords CONTAINS '${queryLower}'
               OR toLower(c.name) CONTAINS '${queryLower}'
               OR toLower(c.description) CONTAINS '${queryLower}'
            RETURN properties(c) as concept
            LIMIT 5
          $$) as (concept agtype)`;
          break;
        case 'find_callers':
          // Find all functions/methods that call a given entity
          const targetName = params.entity_name || '';
          const callersLimit = params.limit || 50;
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (caller)-[:CALLS]->(callee)
            WHERE callee.name = '${targetName}'
            RETURN properties(caller) as caller,
                   properties(callee) as callee
            LIMIT ${callersLimit}
          $$) as (caller agtype, callee agtype)`;
          break;

        case 'find_callees':
          // Find all functions/methods called by a given entity
          const callerName = params.entity_name || '';
          const calleesLimit = params.limit || 50;
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (caller {name: '${callerName}'})-[:CALLS]->(callee)
            RETURN properties(caller) as caller,
                   properties(callee) as callee
            LIMIT ${calleesLimit}
          $$) as (caller agtype, callee agtype)`;
          break;

        case 'find_call_graph':
          // Return the local call graph (1-2 hops) around an entity
          const centerEntity = params.entity_name || '';
          const hops = params.hops || 1;
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (center {name: '${centerEntity}'})
            OPTIONAL MATCH (caller)-[:CALLS*1..${hops}]->(center)
            OPTIONAL MATCH (center)-[:CALLS*1..${hops}]->(callee)
            RETURN properties(center) as center,
                   collect(DISTINCT properties(caller)) as callers,
                   collect(DISTINCT properties(callee)) as callees
          $$) as (center agtype, callers agtype, callees agtype)`;
          break;

        case 'find_orphaned_code':
          // Find entities with no incoming or outgoing CALLS edges
          const orphanType = params.entity_type || 'Function';
          const orphanLimit = params.limit || 100;
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n)
            WHERE (n.entity_type = '${orphanType}' OR labels(n)[0] = '${orphanType}')
            OPTIONAL MATCH (n)-[r_out:CALLS]->()
            OPTIONAL MATCH (n)<-[r_in:CALLS]-()
            WHERE r_out IS NULL AND r_in IS NULL
            RETURN properties(n) as orphan
            LIMIT ${orphanLimit}
          $$) as (orphan agtype)`;
          break;

        case 'trace_call_chain':
          // Find the full call path from entity A to entity B
          const fromName = params.from || '';
          const toName = params.to || '';
          const maxDepth = params.max_depth || 5;
          const chainLimit = params.limit || 10;
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH path = (source {name: '${fromName}'})-[:CALLS*1..${maxDepth}]->(target {name: '${toName}'})
            RETURN nodes(path) as chain
            LIMIT ${chainLimit}
          $$) as (chain agtype)`;
          break;

        case 'code_impact': {
          // Impact analysis: BFS traversal to find affected code at each depth
          const impactTarget = params.entity_name || '';
          const impactDirection = params.direction || 'upstream'; // upstream = who calls me, downstream = what I call
          const impactMaxDepth = Math.min(params.max_depth || 3, 5);
          const includeTests = params.include_tests || false;
          const relationTypes = params.relation_types || ['CALLS'];

          const testPatterns = ['/test/', '/tests/', '.test.', '.spec.', '_test.go', '_test.py', 'test_', '__tests__/'];

          // Find target entity
          const targetResult = await client.query(`SELECT * FROM cypher('regen_graph', $$
            MATCH (n)
            WHERE n.name = '${impactTarget}'
            RETURN n.entity_id, n.name, n.entity_type, n.file_path, n.line_start, n.repo
            LIMIT 1
          $$) as (entity_id agtype, name agtype, entity_type agtype, file_path agtype, line_start agtype, repo agtype)`);

          if (targetResult.rows.length === 0) {
            const envelope = createSuccessEnvelope(requestId, {
              query_type: 'code_impact',
              total_results: 0,
              results: [],
              target: null,
              error: `Entity '${impactTarget}' not found`
            });
            return res.json(envelope);
          }

          const target = {
            entity_id: JSON.parse(targetResult.rows[0].entity_id),
            name: JSON.parse(targetResult.rows[0].name),
            entity_type: JSON.parse(targetResult.rows[0].entity_type),
            file_path: JSON.parse(targetResult.rows[0].file_path),
            line_start: JSON.parse(targetResult.rows[0].line_start),
            repo: JSON.parse(targetResult.rows[0].repo),
          };

          // BFS: iterative depth queries
          const visited = new Set<string>([target.entity_id]);
          const byDepth: Record<number, any[]> = {};
          let frontier = [target.entity_id];

          for (let depth = 1; depth <= impactMaxDepth; depth++) {
            if (frontier.length === 0) break;

            const depthResults: any[] = [];
            for (const relType of relationTypes) {
              // Build match pattern based on direction
              // upstream: find who calls the frontier → (found)-[:CALLS]->(frontier)
              // downstream: find what the frontier calls → (frontier)-[:CALLS]->(found)
              const whereClause = frontier.map(id => `'${id}'`).join(', ');

              const depthQuery = impactDirection === 'upstream'
                ? `SELECT * FROM cypher('regen_graph', $$
                    MATCH (found)-[:${relType}]->(target)
                    WHERE target.entity_id IN [${whereClause}]
                    RETURN found.entity_id, found.name, found.entity_type, found.file_path, found.line_start, found.repo
                  $$) as (entity_id agtype, name agtype, entity_type agtype, file_path agtype, line_start agtype, repo agtype)`
                : `SELECT * FROM cypher('regen_graph', $$
                    MATCH (source)-[:${relType}]->(found)
                    WHERE source.entity_id IN [${whereClause}]
                    RETURN found.entity_id, found.name, found.entity_type, found.file_path, found.line_start, found.repo
                  $$) as (entity_id agtype, name agtype, entity_type agtype, file_path agtype, line_start agtype, repo agtype)`;

              try {
                const result = await client.query(depthQuery);
                for (const row of result.rows) {
                  const entityId = JSON.parse(row.entity_id);
                  if (visited.has(entityId)) continue;

                  const filePath = JSON.parse(row.file_path) || '';
                  // Filter test files unless included
                  if (!includeTests && testPatterns.some(p => filePath.includes(p))) continue;

                  visited.add(entityId);
                  const entity = {
                    entity_id: entityId,
                    name: JSON.parse(row.name),
                    entity_type: JSON.parse(row.entity_type),
                    file_path: filePath,
                    line_start: JSON.parse(row.line_start),
                    repo: JSON.parse(row.repo),
                    relationType: relType,
                  };
                  depthResults.push(entity);
                }
              } catch (e) {
                console.error(`Impact analysis depth ${depth} error:`, e);
              }
            }

            if (depthResults.length > 0) {
              byDepth[depth] = depthResults;
              frontier = depthResults.map(e => e.entity_id);
            } else {
              frontier = [];
            }
          }

          // Calculate risk
          const directCount = (byDepth[1] || []).length;
          const totalCount = Object.values(byDepth).reduce((sum, arr) => sum + arr.length, 0);
          const affectedModules = new Set(Object.values(byDepth).flat().map(e => {
            const parts = (e.file_path || '').split('/');
            return parts.length > 1 ? parts[0] : 'root';
          }));

          let risk = 'LOW';
          if (directCount >= 30 || affectedModules.size >= 5) risk = 'CRITICAL';
          else if (directCount >= 15 || affectedModules.size >= 3) risk = 'HIGH';
          else if (directCount >= 5) risk = 'MEDIUM';

          const depthLabels: Record<number, string> = {
            1: 'WILL BREAK',
            2: 'LIKELY AFFECTED',
            3: 'MAY NEED TESTING',
          };

          const impactResults = Object.entries(byDepth).map(([depth, entities]) => ({
            depth: parseInt(depth),
            label: depthLabels[parseInt(depth)] || `DEPTH ${depth}`,
            entities,
          }));

          const responseTime = Date.now() - startTime;
          const envelope = createSuccessEnvelope(requestId, {
            query_type: 'code_impact',
            total_results: totalCount,
            results: impactResults,
            target,
            direction: impactDirection,
            risk,
            summary: { direct: directCount, total: totalCount, modules: affectedModules.size },
            byDepth,
          }, {
            tool_trace: [{
              tool: 'graph_query',
              params_summary: summarizeParams({ query_type, ...params }),
              timestamp: new Date(startTime).toISOString(),
              data_source: 'graph',
              duration_ms: responseTime,
            }],
          });
          // Don't release client here — the finally block handles it
          return res.json(envelope);
        }

        case 'list_communities': {
          const commRepo = params.repo_name || '';
          const commLimit = params.limit || 50;
          const repoWhere = commRepo ? `WHERE c.repo = '${commRepo}'` : '';

          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (c:Community)
            ${repoWhere}
            RETURN properties(c) as community
            ORDER BY c.symbol_count DESC
            LIMIT ${commLimit}
          $$) as (community agtype)`;
          break;
        }

        case 'community_members': {
          const commId = params.community_id || params.community_name || '';
          const memberLimit = params.limit || 100;
          // Match by community_id or name
          const commMatch = commId.startsWith('comm_')
            ? `{community_id: '${commId}'}`
            : `{name: '${commId}'}`;

          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n)-[:MEMBER_OF]->(c:Community ${commMatch})
            RETURN properties(n) as entity, properties(c) as community
            LIMIT ${memberLimit}
          $$) as (entity agtype, community agtype)`;
          break;
        }

        case 'community_for_entity': {
          const entityForComm = params.entity_name || '';
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n {name: '${entityForComm}'})-[:MEMBER_OF]->(c:Community)
            RETURN properties(n) as entity, properties(c) as community
          $$) as (entity agtype, community agtype)`;
          break;
        }

        case 'list_flows': {
          const flowRepo = params.repo_name || '';
          const flowLimit = params.limit || 50;
          const flowRepoWhere = flowRepo ? `WHERE p.repo = '${flowRepo}'` : '';

          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (p:Process)
            ${flowRepoWhere}
            RETURN properties(p) as process
            ORDER BY p.step_count DESC
            LIMIT ${flowLimit}
          $$) as (process agtype)`;
          break;
        }

        case 'flow_steps': {
          const processId = params.process_id || params.process_name || '';
          const procMatch = processId.startsWith('proc_')
            ? `{process_id: '${processId}'}`
            : `{name: '${processId}'}`;

          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n)-[s:STEP_IN_PROCESS]->(p:Process ${procMatch})
            RETURN properties(n) as entity, s.step as step, properties(p) as process
            ORDER BY s.step
          $$) as (entity agtype, step agtype, process agtype)`;
          break;
        }

        case 'flows_for_entity': {
          const flowEntity = params.entity_name || '';
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n {name: '${flowEntity}'})-[s:STEP_IN_PROCESS]->(p:Process)
            RETURN properties(p) as process, s.step as step
            ORDER BY p.step_count DESC
          $$) as (process agtype, step agtype)`;
          break;
        }

        case 'check_staleness': {
          // Check which repos have stale graph data by comparing extraction timestamps
          cypherQuery = `SELECT * FROM cypher('regen_graph', $$
            MATCH (n)
            WHERE n.repo IS NOT NULL
            WITH DISTINCT n.repo as repo, n.extraction_run_id as run_id, count(*) as entity_count
            RETURN repo, run_id, entity_count
            ORDER BY repo
          $$) as (repo agtype, run_id agtype, entity_count agtype)`;
          break;
        }

        default:
          return res.status(400).json({ error: `Unknown query_type: ${query_type}` });
      }

      // Execute the cypher query
      const result = await client.query(cypherQuery, queryParams);

      // Convert agtype results to plain JSON
      const rows = result.rows.map(row => {
        const converted: any = {};
        for (const key in row) {
          const value = row[key];
          // AGE returns values as JSON strings, parse them
          if (value === null || value === undefined) {
            converted[key] = null;
          } else if (typeof value === 'string') {
            try {
              // Try to parse as JSON (AGE agtype format)
              const parsed = JSON.parse(value);
              converted[key] = parsed;
            } catch {
              // If not JSON, keep as string
              converted[key] = value;
            }
          } else {
            converted[key] = value;
          }
        }
        return converted;
      });


      // Fix duplicated paths in entity data
      const fixedRows = rows.map(row => {
        const fixed: any = { ...row };
        // Fix entity objects
        if (fixed.entity) fixed.entity = fixEntityPaths(fixed.entity);
        if (fixed.caller) fixed.caller = fixEntityPaths(fixed.caller);
        if (fixed.callee) fixed.callee = fixEntityPaths(fixed.callee);
        if (fixed.orphan) fixed.orphan = fixEntityPaths(fixed.orphan);
        // For chain results (array of entities)
        if (Array.isArray(fixed.chain)) {
          fixed.chain = fixed.chain.map((e: any) => fixEntityPaths(e));
        }
        return fixed;
      });

      const responseTime = Date.now() - startTime;

      // Session D1: Create tool trace for graph query
      const toolTrace: ToolTraceEntry[] = [{
        tool: 'graph_query',
        params_summary: summarizeParams({ query_type, ...params }),
        timestamp: new Date(startTime).toISOString(),
        data_source: 'graph',
        duration_ms: responseTime,
      }];

      // Session D1: Create envelope response
      const responseData = {
        query_type,
        total_results: fixedRows.length,
        results: fixedRows
      };

      const envelope = createSuccessEnvelope(requestId, responseData, {
        tool_trace: toolTrace,
      });

      res.json(envelope);

    } finally {
      client.release();
    }

  } catch (error) {
    console.error('Graph query error:', error);
    const koiError: KoiError = {
      code: 'GRAPH_QUERY_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: error instanceof Error &&
        (error.message.includes('timeout') || error.message.includes('connection')),
    };
    if (koiError.retryable) {
      koiError.retry_after_ms = 1000;
    }
    const envelope = createErrorEnvelope(requestId, koiError);
    res.status(500).json(envelope);
  }
});

// Proxy middleware for KOI services
const proxyToService = (serviceUrl: string) => {
  return async (req: express.Request, res: express.Response) => {
    try {
      const targetUrl = `${serviceUrl}${req.originalUrl}`;
      const response = await fetch(targetUrl, {
        method: req.method,
        headers: {
          'Content-Type': 'application/json',
          ...req.headers
        },
        body: req.method !== 'GET' ? JSON.stringify(req.body) : undefined
      });

      const data = await response.text();
      res.status(response.status);

      // Set content type based on response
      const contentType = response.headers.get('content-type');
      if (contentType) {
        res.set('Content-Type', contentType);
      }

      res.send(data);
    } catch (error) {
      console.error(`Proxy error for ${req.originalUrl}:`, error);
      res.status(503).json({
        error: 'Service unavailable',
        service: serviceUrl,
        message: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  };
};

// KOI Service Proxies (Development API Gateway)
// Note: /api/koi/graph is handled directly above, not proxied
app.use('/api/koi/coordinator/', proxyToService('http://localhost:8005'));
app.use('/api/koi/event-bridge/', proxyToService('http://localhost:8100'));
app.use('/api/koi/bge/', proxyToService('http://localhost:8090'));
app.use('/api/koi/transformations', proxyToService('http://localhost:8002'));
app.use('/api/koi/rids', proxyToService('http://localhost:8002'));
app.use('/api/koi/rid-lookup', proxyToService('http://localhost:8002'));

// Auth status check - for MCP server to validate user authentication
// SECURITY: Uses device_code binding to prevent IDOR attacks
// Session tokens are returned ONCE only to the client that initiated the auth
app.get('/api/koi/auth/status', async (req, res) => {
  try {
    // Method 1: validate existing session_token from Authorization header
    const authHeader = req.headers['authorization'] as string | undefined;
    const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;

    // Also support token in query param for easier testing
    const queryToken = req.query.session_token as string | undefined;
    const tokenToValidate = sessionToken || queryToken;

    if (tokenToValidate) {
      // Validate the provided session token (uses hash comparison)
      const authenticatedEmail = await validateSessionToken(tokenToValidate);

      if (!authenticatedEmail) {
        return res.json({
          authenticated: false,
          reason: 'Invalid or expired session token'
        });
      }

      return res.json({
        authenticated: true,
        user_email: authenticatedEmail,
        timestamp: new Date().toISOString()
      });
    }

    // Method 2: Poll using device_code (SECURE - prevents IDOR)
    // Only the client that initiated the auth can retrieve the session token
    const deviceCode = req.query.device_code as string | undefined;

    if (!deviceCode || deviceCode.length < 32) {
      return res.json({
        authenticated: false,
        reason: 'No session token or device_code provided. Use device_code to poll for auth status.'
      });
    }

    // Look up auth request by device_code
    // SECURITY: session_token is stored temporarily here (not in long-lived session_tokens)
    const authRequest = await pool.query(
      `SELECT id, user_email, status, session_token, expires_at
       FROM auth_requests
       WHERE device_code = $1`,
      [deviceCode]
    );

    if (authRequest.rows.length === 0) {
      return res.json({
        status: 'not_found',
        authenticated: false,
        reason: 'Invalid or expired auth request'
      });
    }

    const row = authRequest.rows[0];

    if (new Date(row.expires_at).getTime() < Date.now()) {
      return res.json({
        status: 'expired',
        authenticated: false,
        reason: 'Auth request expired'
      });
    }

    if (row.status === 'pending') {
      return res.json({
        status: 'pending',
        authenticated: false,
        reason: 'User has not completed authentication yet'
      });
    }

    if (row.status === 'rejected') {
      return res.json({
        status: 'rejected',
        authenticated: false,
        reason: 'Email domain not allowed'
      });
    }

    if (row.status === 'used') {
      // Token was already retrieved - don't return it again
      return res.json({
        status: 'already_retrieved',
        authenticated: true,
        user_email: row.user_email,
        reason: 'Session token was already retrieved. Use the token you received earlier.'
      });
    }

    if (row.status === 'authenticated') {
      // First time polling after auth - get plain token from auth_requests
      // SECURITY: Plain token is stored temporarily in auth_requests (short-lived)
      // NOT in session_tokens (long-lived, stores only hashes)
      const plainToken = row.session_token;

      if (!plainToken) {
        return res.json({
          status: 'error',
          authenticated: false,
          reason: 'Session token not found or already retrieved'
        });
      }

      // Mark as used and NULL out the plain token (security: don't keep it around)
      await pool.query(
        `UPDATE auth_requests
         SET status = 'used', used_at = CURRENT_TIMESTAMP, session_token = NULL
         WHERE device_code = $1`,
        [deviceCode]
      );

      console.log(`[Auth] Session token retrieved for ${row.user_email} via device_code ${deviceCode.substring(0, 8)}...`);

      const expiresAt = new Date(Date.now() + SESSION_TOKEN_LIFETIME_MS);

      return res.json({
        status: 'authenticated',
        authenticated: true,
        user_email: row.user_email,
        session_token: plainToken,  // Plain token, returned ONCE then NULLed from DB
        token_expiry: expiresAt.toISOString(),
        timestamp: new Date().toISOString()
      });
    }

    // Unknown status
    return res.json({
      status: row.status,
      authenticated: false
    });

  } catch (error) {
    console.error('Auth status check error:', error);
    res.status(500).json({
      authenticated: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// =============================================================================
// User Profile Endpoints (Teaching Mode)
// Stores user experience level and preferences for personalized responses
// =============================================================================

const FUSEKI_URL = process.env.FUSEKI_URL || 'http://localhost:3030';
const FUSEKI_DATASET = process.env.FUSEKI_DATASET || 'koi';

// Default preferences by experience level
const DEFAULT_PREFERENCES_BY_LEVEL: Record<string, object> = {
  junior: { explain_before_code: true, verbosity: 'detailed', show_examples: true },
  mid: { explain_before_code: true, verbosity: 'balanced', show_examples: true },
  senior: { explain_before_code: false, verbosity: 'concise', show_examples: false },
  staff: { explain_before_code: false, verbosity: 'concise', show_examples: false },
  principal: { explain_before_code: false, verbosity: 'concise', show_examples: false },
};

// Helper to query SPARQL endpoint
async function sparqlQuery(query: string): Promise<any> {
  try {
    const response = await fetch(`${FUSEKI_URL}/${FUSEKI_DATASET}/sparql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/sparql-query',
        'Accept': 'application/sparql-results+json',
      },
      body: query,
    });
    if (!response.ok) {
      console.error(`[SPARQL] Query failed: ${response.status} ${response.statusText}`);
      return null;
    }
    return await response.json();
  } catch (error) {
    console.error('[SPARQL] Query error:', error);
    return null;
  }
}

// Helper to insert triples via SPARQL UPDATE
async function sparqlUpdate(updateQuery: string): Promise<boolean> {
  try {
    const response = await fetch(`${FUSEKI_URL}/${FUSEKI_DATASET}/update`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/sparql-update',
      },
      body: updateQuery,
    });
    if (!response.ok) {
      console.error(`[SPARQL] Update failed: ${response.status} ${response.statusText}`);
      return false;
    }
    return true;
  } catch (error) {
    console.error('[SPARQL] Update error:', error);
    return false;
  }
}

// Helper to get Jena relationships for a user
async function getJenaRelationships(email: string): Promise<{
  manages: string[];
  works_on: string[];
  learning: string[];
}> {
  const personUri = `<https://regen.network/person/${encodeURIComponent(email)}>`;

  const query = `
    PREFIX regen: <https://regen.network/ontology/>

    SELECT ?predicate ?object WHERE {
      ${personUri} ?predicate ?object .
      FILTER(?predicate IN (regen:manages, regen:works_on, regen:learning))
    }
  `;

  const result = await sparqlQuery(query);
  const relationships = { manages: [] as string[], works_on: [] as string[], learning: [] as string[] };

  if (result?.results?.bindings) {
    for (const binding of result.results.bindings) {
      const pred = binding.predicate?.value || '';
      const obj = binding.object?.value || '';

      if (pred.endsWith('manages')) {
        relationships.manages.push(obj);
      } else if (pred.endsWith('works_on')) {
        relationships.works_on.push(obj);
      } else if (pred.endsWith('learning')) {
        relationships.learning.push(obj);
      }
    }
  }

  return relationships;
}

// Helper to sync profile relationships to Jena
async function syncProfileToJena(email: string, managedBy?: string): Promise<boolean> {
  const personUri = `<https://regen.network/person/${encodeURIComponent(email)}>`;

  // Build update query - delete old :manages relationships and insert new ones
  let updateQuery = `
    PREFIX regen: <https://regen.network/ontology/>

    DELETE WHERE {
      ?manager regen:manages ${personUri} .
    }
  `;

  if (managedBy) {
    const managerUri = `<https://regen.network/person/${encodeURIComponent(managedBy)}>`;
    updateQuery += `
    ;
    INSERT DATA {
      ${managerUri} regen:manages ${personUri} .
    }
    `;
  }

  return await sparqlUpdate(updateQuery);
}

// GET /api/koi/user/profile - Get current user's profile
app.get('/api/koi/user/profile', async (req, res) => {
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);

  try {
    // Authenticate user via session token
    const authHeader = req.headers['authorization'] as string | undefined;
    const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
    const userEmail = await validateSessionToken(sessionToken);

    if (!userEmail) {
      const koiError: KoiError = {
        code: 'UNAUTHORIZED',
        message: 'Authentication required. Please authenticate first.',
        retryable: false,
      };
      return res.status(401).json(createErrorEnvelope(requestId, koiError));
    }

    // Get profile from PostgreSQL
    const result = await pool.query(
      `SELECT id, email, experience_level, role, preferences, managed_by, created_at, updated_at
       FROM user_profiles
       WHERE email = $1`,
      [userEmail]
    );

    let profile: any;

    if (result.rows.length === 0) {
      // Return default profile for new users
      const defaultLevel = 'mid';
      profile = {
        email: userEmail,
        experience_level: defaultLevel,
        role: null,
        preferences: DEFAULT_PREFERENCES_BY_LEVEL[defaultLevel],
        managed_by: null,
        is_new: true,
      };
    } else {
      profile = result.rows[0];
      profile.is_new = false;
    }

    // Enrich with Jena relationships
    const relationships = await getJenaRelationships(userEmail);
    profile.relationships = relationships;

    console.log(`[UserProfile] Retrieved profile for ${userEmail}`);

    return res.json(createSuccessEnvelope(requestId, profile));
  } catch (error) {
    console.error('[UserProfile] Error getting profile:', error);
    const koiError: KoiError = {
      code: 'INTERNAL_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: true,
    };
    return res.status(500).json(createErrorEnvelope(requestId, koiError));
  }
});

// PUT /api/koi/user/profile - Update user's profile
app.put('/api/koi/user/profile', async (req, res) => {
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);

  try {
    // Authenticate user via session token
    const authHeader = req.headers['authorization'] as string | undefined;
    const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
    const userEmail = await validateSessionToken(sessionToken);

    if (!userEmail) {
      const koiError: KoiError = {
        code: 'UNAUTHORIZED',
        message: 'Authentication required. Please authenticate first.',
        retryable: false,
      };
      return res.status(401).json(createErrorEnvelope(requestId, koiError));
    }

    const { experience_level, role, preferences, managed_by } = req.body;

    // Validate experience_level if provided
    const validLevels = ['junior', 'mid', 'senior', 'staff', 'principal'];
    if (experience_level && !validLevels.includes(experience_level)) {
      const koiError: KoiError = {
        code: 'INVALID_INPUT',
        message: `Invalid experience_level. Must be one of: ${validLevels.join(', ')}`,
        retryable: false,
      };
      return res.status(400).json(createErrorEnvelope(requestId, koiError));
    }

    // Merge preferences with defaults based on experience level
    const level = experience_level || 'mid';
    const mergedPreferences = {
      ...DEFAULT_PREFERENCES_BY_LEVEL[level],
      ...(preferences || {}),
    };

    // Upsert profile in PostgreSQL
    const result = await pool.query(
      `INSERT INTO user_profiles (email, experience_level, role, preferences, managed_by)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (email) DO UPDATE SET
         experience_level = COALESCE(EXCLUDED.experience_level, user_profiles.experience_level),
         role = COALESCE(EXCLUDED.role, user_profiles.role),
         preferences = COALESCE(EXCLUDED.preferences, user_profiles.preferences),
         managed_by = EXCLUDED.managed_by,
         updated_at = NOW()
       RETURNING id, email, experience_level, role, preferences, managed_by, created_at, updated_at`,
      [userEmail, level, role || null, JSON.stringify(mergedPreferences), managed_by || null]
    );

    const profile = result.rows[0];

    // Sync to Jena (async, don't wait)
    syncProfileToJena(userEmail, managed_by).catch(err => {
      console.error('[UserProfile] Failed to sync to Jena:', err);
    });

    // Enrich with Jena relationships
    const relationships = await getJenaRelationships(userEmail);
    profile.relationships = relationships;

    console.log(`[UserProfile] Updated profile for ${userEmail}: level=${level}`);

    return res.json(createSuccessEnvelope(requestId, profile));
  } catch (error) {
    console.error('[UserProfile] Error updating profile:', error);
    const koiError: KoiError = {
      code: 'INTERNAL_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: true,
    };
    return res.status(500).json(createErrorEnvelope(requestId, koiError));
  }
});

// Default type priority (higher = more preferred) - matches Python polysemy_resolver.py
const DEFAULT_TYPE_PRIORITY: Record<string, number> = {
  'TECHNOLOGY': 100,
  'PROJECT': 90,
  'ORGANIZATION': 80,
  'CONCEPT': 70,
  'STANDARD': 60,
  'PERSON': 50,
  'PROCESS': 40,
  'MATERIAL': 35,
  'MODULE': 30,
  'LOCATION': 25,
  'EVENT': 20,
  'VALIDATOR': 15,
  'CREDIT_CLASS': 10,
  'GOVERNANCE_PROPOSAL': 5,
  'EVIDENCE': 4,
  'CLAIM': 3,
  'QUESTION': 2,
  'API_MESSAGE': 1,
  'LICENSE': 0,
  'KEEPER': 0,
};

// Polysemy-aware entity resolution endpoint with ledger entity support
// GET /api/koi/entity/resolve?label=...&type_hint=...&limit=5
// Supports: normalized_text match, ledger_id exact match, aliases JSONB containment
app.get('/api/koi/entity/resolve', async (req, res) => {
  try {
    const label = (req.query.label as string || '').trim();
    const typeHint = (req.query.type_hint as string || '').trim().toUpperCase() || null;
    const limit = Math.min(Math.max(parseInt(req.query.limit as string) || 5, 1), 20);

    if (!label) {
      return res.status(400).json({ error: 'label parameter is required' });
    }

    const lookupKeys = buildNormalizedLookupKeys(label, typeHint);
    if (lookupKeys.length === 0) {
      return res.status(400).json({ error: 'label parameter is required' });
    }

    // Prepare alias lookup key (for JSONB containment)
    const aliasLookupKey = JSON.stringify([label.toLowerCase()]);

    // Query for entity variants matching:
    // 1. ledger_id exact match (e.g., "C02", "C02-003") - highest priority for ledger entities
    // 2. normalized_text exact match - standard KOI resolution
    // 3. aliases JSONB containment - fuzzy alias matching
    const query = `
      WITH entity_matches AS (
        SELECT
          id,
          entity_text,
          entity_type,
          normalized_text,
          occurrence_count,
          fuseki_uri,
          ledger_id,
          metadata_iri,
          admin_address,
          aliases,
          jurisdiction,
          class_id,
          source,
          CASE
            WHEN UPPER(ledger_id) = UPPER($2) THEN 'ledger_id'
            WHEN LOWER(TRIM(normalized_text)) = ANY($1) THEN 'normalized_text'
            WHEN aliases @> $3::jsonb THEN 'alias'
            ELSE 'unknown'
          END as match_type
        FROM entity_registry
        WHERE UPPER(ledger_id) = UPPER($2)
           OR LOWER(TRIM(normalized_text)) = ANY($1)
           OR (aliases IS NOT NULL AND aliases @> $3::jsonb)
      ),
      rel_counts AS (
        SELECT
          e.id,
          COALESCE(subj.subj_count, 0) + COALESCE(obj.obj_count, 0) as relationship_count
        FROM entity_matches e
        LEFT JOIN (
          SELECT subject_entity_id, COUNT(*) as subj_count
          FROM koi_relationships
          GROUP BY subject_entity_id
        ) subj ON e.id = subj.subject_entity_id
        LEFT JOIN (
          SELECT object_entity_id, COUNT(*) as obj_count
          FROM koi_relationships
          GROUP BY object_entity_id
        ) obj ON e.id = obj.object_entity_id
      )
      SELECT
        e.id,
        e.entity_text,
        e.entity_type,
        e.normalized_text,
        e.occurrence_count,
        e.fuseki_uri,
        e.ledger_id,
        e.metadata_iri,
        e.admin_address,
        e.aliases,
        e.jurisdiction,
        e.class_id,
        e.source,
        e.match_type,
        r.relationship_count,
        csm.source_url as canonical_source_url
      FROM entity_matches e
      JOIN rel_counts r ON e.id = r.id
      LEFT JOIN LATERAL (
        SELECT source_url FROM canonical_source_mappings
        WHERE entity_uri = e.fuseki_uri AND is_canonical = true
        ORDER BY priority DESC, updated_at DESC, source_url ASC
        LIMIT 1
      ) csm ON true
      ORDER BY
        CASE e.match_type
          WHEN 'ledger_id' THEN 1
          WHEN 'normalized_text' THEN 2
          WHEN 'alias' THEN 3
          ELSE 4
        END,
        e.occurrence_count DESC,
        r.relationship_count DESC
    `;

    const result = await pool.query(query, [lookupKeys, label, aliasLookupKey]);
    const variants = result.rows;

    if (variants.length === 0) {
      return res.json({
        query_label: label,
        type_hint: typeHint,
        variant_count: 0,
        winner: null,
        alternatives: [],
        is_polysemy: false,
        resolution_method: 'no_match'
      });
    }

    // Compute scores for all variants (ledger entities get priority)
    const scoredVariants = variants.map(v => {
      const occScore = parseInt(v.occurrence_count) * 1000;
      const relScore = parseInt(v.relationship_count) * 100;
      const typePriority = DEFAULT_TYPE_PRIORITY[v.entity_type] || 0;
      const typeScore = typePriority * 10;

      let totalScore = occScore + relScore + typeScore;
      const reasons: string[] = [
        `occ=${v.occurrence_count}`,
        `rels=${v.relationship_count}`,
        `type_pri=${typePriority}`
      ];

      // Boost for match type (ledger_id is highest priority)
      if (v.match_type === 'ledger_id') {
        totalScore += 100000;
        reasons.push('ledger_id_match=+100k');
      } else if (v.match_type === 'alias') {
        totalScore += 25000;
        reasons.push('alias_match=+25k');
      }

      // Boost if matches type hint
      if (typeHint && v.entity_type.toUpperCase() === typeHint) {
        totalScore += 50000;
        reasons.push('type_hint_match=+50k');
      }

      return {
        uri: v.fuseki_uri,
        entity_text: v.entity_text,
        entity_type: v.entity_type,
        occurrence_count: parseInt(v.occurrence_count),
        relationship_count: parseInt(v.relationship_count),
        match_type: v.match_type,
        // Include ledger-specific fields when present
        ledger_id: v.ledger_id || null,
        metadata_iri: v.metadata_iri || null,
        admin_address: v.admin_address || null,
        aliases: v.aliases || null,
        jurisdiction: v.jurisdiction || null,
        class_id: v.class_id || null,
        source: v.source || null,
        canonical_source_url: v.canonical_source_url || null,
        score: totalScore,
        score_breakdown: reasons.join(', ')
      };
    });

    // Sort by score descending
    scoredVariants.sort((a, b) => b.score - a.score);

    // Determine winner and alternatives
    const winner = scoredVariants[0];
    const alternatives = scoredVariants.slice(1, limit);

    // Determine if polysemy exists
    const uniqueTypes = new Set(scoredVariants.map(v => v.entity_type));
    const isPolysemy = uniqueTypes.size > 1;

    // Determine resolution method
    let resolutionMethod = 'highest_combined_score';
    if (winner.match_type === 'ledger_id') {
      resolutionMethod = 'ledger_id_exact_match';
    } else if (winner.match_type === 'alias') {
      resolutionMethod = 'alias_match';
    } else if (typeHint && winner.entity_type.toUpperCase() === typeHint) {
      resolutionMethod = 'type_hint_match';
    } else {
      const totalOcc = scoredVariants.reduce((sum, v) => sum + v.occurrence_count, 0);
      const totalRels = scoredVariants.reduce((sum, v) => sum + v.relationship_count, 0);

      if (winner.occurrence_count > totalOcc * 0.5) {
        resolutionMethod = 'dominant_occurrence';
      } else if (winner.relationship_count > totalRels * 0.5) {
        resolutionMethod = 'dominant_connectivity';
      }
    }

    res.json({
      query_label: label,
      type_hint: typeHint,
      variant_count: scoredVariants.length,
      winner: winner,
      alternatives: alternatives,
      is_polysemy: isPolysemy,
      resolution_method: resolutionMethod
    });

  } catch (error) {
    console.error('Entity resolve error:', error);
    res.status(500).json({
      error: 'Entity resolution failed',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Internal helper: resolve entity by label or URI
// Returns { entity_id, uri, entity_text, entity_type } or null
async function resolveEntityInternal(
  label: string | null,
  uri: string | null,
  typeHint: string | null
): Promise<{
  entity_id: number;
  uri: string;
  entity_text: string;
  entity_type: string;
  occurrence_count: number;
} | null> {
  if (uri) {
    // Direct URI lookup
    const result = await pool.query(
      `SELECT id, fuseki_uri, entity_text, entity_type, occurrence_count
       FROM entity_registry
       WHERE fuseki_uri = $1`,
      [uri]
    );
    if (result.rows.length === 0) return null;
    const row = result.rows[0];
    return {
      entity_id: row.id,
      uri: row.fuseki_uri,
      entity_text: row.entity_text,
      entity_type: row.entity_type,
      occurrence_count: parseInt(row.occurrence_count),
    };
  }

  if (!label) return null;

  const lookupKeys = buildNormalizedLookupKeys(label, typeHint);
  if (lookupKeys.length === 0) return null;

  // Use the same logic as /api/koi/entity/resolve
  const query = `
    WITH entity_matches AS (
      SELECT
        id,
        entity_text,
        entity_type,
        normalized_text,
        occurrence_count,
        fuseki_uri
      FROM entity_registry
      WHERE LOWER(TRIM(normalized_text)) = ANY($1)
    ),
    rel_counts AS (
      SELECT
        e.id,
        COALESCE(subj.subj_count, 0) + COALESCE(obj.obj_count, 0) as relationship_count
      FROM entity_matches e
      LEFT JOIN (
        SELECT subject_entity_id, COUNT(*) as subj_count
        FROM koi_relationships
        GROUP BY subject_entity_id
      ) subj ON e.id = subj.subject_entity_id
      LEFT JOIN (
        SELECT object_entity_id, COUNT(*) as obj_count
        FROM koi_relationships
        GROUP BY object_entity_id
      ) obj ON e.id = obj.object_entity_id
    )
    SELECT
      e.id,
      e.entity_text,
      e.entity_type,
      e.normalized_text,
      e.occurrence_count,
      e.fuseki_uri,
      r.relationship_count
    FROM entity_matches e
    JOIN rel_counts r ON e.id = r.id
    ORDER BY e.occurrence_count DESC, r.relationship_count DESC
  `;

  const result = await pool.query(query, [lookupKeys]);
  if (result.rows.length === 0) return null;

  // Score and find winner (same logic as /api/koi/entity/resolve)
  const variants = result.rows.map(v => {
    const occScore = parseInt(v.occurrence_count) * 1000;
    const relScore = parseInt(v.relationship_count) * 100;
    const typePriority = DEFAULT_TYPE_PRIORITY[v.entity_type] || 0;
    const typeScore = typePriority * 10;

    let totalScore = occScore + relScore + typeScore;

    // Boost if matches type hint
    if (typeHint && v.entity_type.toUpperCase() === typeHint.toUpperCase()) {
      totalScore += 50000;
    }

    return { ...v, score: totalScore };
  });

  variants.sort((a, b) => b.score - a.score);
  const winner = variants[0];

  return {
    entity_id: winner.id,
    uri: winner.fuseki_uri,
    entity_text: winner.entity_text,
    entity_type: winner.entity_type,
    occurrence_count: parseInt(winner.occurrence_count),
  };
}

// ============================================================================
// Week 17: Polysemy-Aware Entity Resolution for Query Reranking
// Returns the dominant entity for a query with polysemy metadata
// ============================================================================

interface PolysemyResolution {
  entity_id: number;
  uri: string;
  entity_text: string;
  entity_type: string;
  occurrence_count: number;
  is_polysemous: boolean;
  variant_count: number;
  alternatives: Array<{
    entity_type: string;
    occurrence_count: number;
    score: number;
  }>;
  resolution_method: string;
  score: number;
}

/**
 * Resolve query text to a dominant entity with polysemy awareness.
 * Returns the highest-scoring entity variant plus metadata about alternatives.
 *
 * @param queryText - The query string to resolve
 * @param typeHint - Optional type to prefer (e.g., "TECHNOLOGY")
 * @returns PolysemyResolution or null if no entity found
 */
async function resolveQueryPolysemy(
  queryText: string,
  typeHint?: string
): Promise<PolysemyResolution | null> {
  // Try normalized variants of the query
  const variants = normalizeQueryForEntityMatch(queryText);

  for (const variant of variants) {
    const query = `
      WITH entity_matches AS (
        SELECT
          id,
          entity_text,
          entity_type,
          normalized_text,
          occurrence_count,
          fuseki_uri
        FROM entity_registry
        WHERE LOWER(TRIM(normalized_text)) = LOWER(TRIM($1))
      ),
      rel_counts AS (
        SELECT
          e.id,
          COALESCE(subj.subj_count, 0) + COALESCE(obj.obj_count, 0) as relationship_count
        FROM entity_matches e
        LEFT JOIN (
          SELECT subject_entity_id, COUNT(*) as subj_count
          FROM koi_relationships
          GROUP BY subject_entity_id
        ) subj ON e.id = subj.subject_entity_id
        LEFT JOIN (
          SELECT object_entity_id, COUNT(*) as obj_count
          FROM koi_relationships
          GROUP BY object_entity_id
        ) obj ON e.id = obj.object_entity_id
      )
      SELECT
        e.id,
        e.entity_text,
        e.entity_type,
        e.normalized_text,
        e.occurrence_count,
        e.fuseki_uri,
        r.relationship_count,
        csm.source_url as canonical_source_url
      FROM entity_matches e
      JOIN rel_counts r ON e.id = r.id
      LEFT JOIN LATERAL (
        SELECT source_url FROM canonical_source_mappings
        WHERE entity_uri = e.fuseki_uri AND is_canonical = true
        ORDER BY priority DESC, updated_at DESC, source_url ASC
        LIMIT 1
      ) csm ON true
      ORDER BY e.occurrence_count DESC, r.relationship_count DESC
    `;

    const result = await pool.query(query, [variant]);
    if (result.rows.length === 0) continue;

    // Score all variants
    const scoredVariants = result.rows.map(v => {
      const occScore = parseInt(v.occurrence_count) * 1000;
      const relScore = parseInt(v.relationship_count) * 100;
      const typePriority = DEFAULT_TYPE_PRIORITY[v.entity_type] || 0;
      const typeScore = typePriority * 10;

      let totalScore = occScore + relScore + typeScore;

      // Boost if matches type hint
      if (typeHint && v.entity_type.toUpperCase() === typeHint.toUpperCase()) {
        totalScore += 50000;
      }

      return { ...v, score: totalScore };
    });

    scoredVariants.sort((a, b) => b.score - a.score);
    const winner = scoredVariants[0];

    // Check for polysemy (multiple distinct types)
    const uniqueTypes = new Set(scoredVariants.map(v => v.entity_type));
    const isPolysemous = uniqueTypes.size > 1;

    // Determine resolution method
    let resolutionMethod = 'highest_combined_score';
    if (typeHint && winner.entity_type.toUpperCase() === typeHint.toUpperCase()) {
      resolutionMethod = 'type_hint_match';
    } else if (winner.occurrence_count > scoredVariants.reduce((sum, v) => sum + parseInt(v.occurrence_count), 0) * 0.5) {
      resolutionMethod = 'dominant_occurrence';
    }

    // Build alternatives list (other type variants, not duplicates of winner)
    const alternatives = scoredVariants
      .filter(v => v.entity_type !== winner.entity_type)
      .slice(0, 5)
      .map(v => ({
        entity_type: v.entity_type,
        occurrence_count: parseInt(v.occurrence_count),
        score: v.score,
      }));

    return {
      entity_id: winner.id,
      uri: winner.fuseki_uri,
      entity_text: winner.entity_text,
      entity_type: winner.entity_type,
      occurrence_count: parseInt(winner.occurrence_count),
      is_polysemous: isPolysemous,
      variant_count: scoredVariants.length,
      alternatives,
      resolution_method: resolutionMethod,
      score: winner.score,
    };
  }

  return null;
}

/**
 * Apply polysemy-based score boost to fused results.
 * Boosts results that contain the resolved entity name.
 *
 * @param fusedResults - Results from RRF fusion
 * @param resolved - Polysemy resolution result
 * @param boostFactor - Score multiplier for matching results (default 1.15)
 * @returns Modified results with boost applied
 */
function applyPolysemyRerank(
  fusedResults: any[],
  resolved: PolysemyResolution,
  boostFactor: number = 1.15
): { results: any[]; boosted_count: number } {
  const resolvedNameLower = resolved.entity_text.toLowerCase();
  let boostedCount = 0;

  const rerankResults = fusedResults.map(r => {
    // Check if this result contains the resolved entity
    const entitiesMatched = r.metadata?.entities_matched || [];
    const hasResolvedEntity = entitiesMatched.some(
      (e: string) => e.toLowerCase() === resolvedNameLower
    );

    if (hasResolvedEntity) {
      boostedCount++;
      return {
        ...r,
        score: r.score * boostFactor,
        metadata: {
          ...r.metadata,
          polysemy_boost: boostFactor,
          polysemy_matched_entity: resolved.entity_text,
        },
      };
    }
    return r;
  });

  // Re-sort by boosted score
  rerankResults.sort((a, b) => (b.score || 0) - (a.score || 0));

  return { results: rerankResults, boosted_count: boostedCount };
}

// Graph neighborhood endpoint - query local graph structure
// GET /api/koi/entity/neighborhood?label=...&uri=...&type_hint=...&limit=50&direction=both
app.get('/api/koi/entity/neighborhood', async (req, res) => {
  try {
    const label = (req.query.label as string || '').trim() || null;
    const uri = (req.query.uri as string || '').trim() || null;
    const typeHint = (req.query.type_hint as string || '').trim().toUpperCase() || null;
    const limit = Math.min(Math.max(parseInt(req.query.limit as string) || 50, 1), 200);
    const direction = (req.query.direction as string || 'both').toLowerCase();

    if (!label && !uri) {
      return res.status(400).json({ error: 'Either label or uri parameter is required' });
    }

    if (!['out', 'in', 'both'].includes(direction)) {
      return res.status(400).json({ error: 'direction must be one of: out, in, both' });
    }

    // Resolve entity
    const resolved = await resolveEntityInternal(label, uri, typeHint);

    if (!resolved) {
      return res.json({
        query_label: label,
        query_uri: uri,
        type_hint: typeHint,
        resolved_uri: null,
        resolved_entity_id: null,
        nodes: [],
        edges: [],
        truncated: false,
        error: 'Entity not found'
      });
    }

    // Build direction clause
    let directionClause = '';
    if (direction === 'out') {
      directionClause = 'AND r.subject_entity_id = $1';
    } else if (direction === 'in') {
      directionClause = 'AND r.object_entity_id = $1';
    } else {
      directionClause = 'AND (r.subject_entity_id = $1 OR r.object_entity_id = $1)';
    }

    // Query relationships with entity info
    const relQuery = `
      WITH edges AS (
        SELECT
          r.id as rel_id,
          r.predicate,
          r.confidence,
          r.occurrence_count as rel_occurrence_count,
          r.subject_entity_id,
          r.object_entity_id,
          CASE
            WHEN r.subject_entity_id = $1 THEN 'out'
            ELSE 'in'
          END as direction
        FROM koi_relationships r
        WHERE 1=1 ${directionClause}
        ORDER BY r.occurrence_count DESC, r.confidence DESC NULLS LAST
        LIMIT $2
      ),
      all_entity_ids AS (
        SELECT DISTINCT subject_entity_id as entity_id FROM edges
        UNION
        SELECT DISTINCT object_entity_id as entity_id FROM edges
      ),
      nodes AS (
        SELECT
          e.id,
          e.fuseki_uri as uri,
          e.entity_text as text,
          e.entity_type as type,
          e.occurrence_count,
          COALESCE(subj.subj_count, 0) + COALESCE(obj.obj_count, 0) as relationship_count
        FROM entity_registry e
        JOIN all_entity_ids a ON e.id = a.entity_id
        LEFT JOIN (
          SELECT subject_entity_id, COUNT(*) as subj_count
          FROM koi_relationships
          GROUP BY subject_entity_id
        ) subj ON e.id = subj.subject_entity_id
        LEFT JOIN (
          SELECT object_entity_id, COUNT(*) as obj_count
          FROM koi_relationships
          GROUP BY object_entity_id
        ) obj ON e.id = obj.object_entity_id
      )
      SELECT
        'edge' as result_type,
        ed.rel_id,
        ed.predicate,
        ed.confidence,
        ed.rel_occurrence_count,
        ed.direction,
        subj.fuseki_uri as subject_uri,
        obj.fuseki_uri as object_uri,
        NULL as node_id,
        NULL as node_uri,
        NULL as node_text,
        NULL as node_type,
        NULL as node_occurrence_count,
        NULL as node_relationship_count
      FROM edges ed
      JOIN entity_registry subj ON ed.subject_entity_id = subj.id
      JOIN entity_registry obj ON ed.object_entity_id = obj.id

      UNION ALL

      SELECT
        'node' as result_type,
        NULL as rel_id,
        NULL as predicate,
        NULL as confidence,
        NULL as rel_occurrence_count,
        NULL as direction,
        NULL as subject_uri,
        NULL as object_uri,
        n.id as node_id,
        n.uri as node_uri,
        n.text as node_text,
        n.type as node_type,
        n.occurrence_count as node_occurrence_count,
        n.relationship_count as node_relationship_count
      FROM nodes n
    `;

    const results = await pool.query(relQuery, [resolved.entity_id, limit]);

    // Separate nodes and edges
    const nodes: any[] = [];
    const edges: any[] = [];
    const nodeSet = new Set<number>();

    for (const row of results.rows) {
      if (row.result_type === 'node') {
        if (!nodeSet.has(row.node_id)) {
          nodeSet.add(row.node_id);
          nodes.push({
            id: row.node_id,
            uri: row.node_uri,
            text: row.node_text,
            type: row.node_type,
            occurrence_count: parseInt(row.node_occurrence_count),
            relationship_count: parseInt(row.node_relationship_count),
          });
        }
      } else if (row.result_type === 'edge') {
        edges.push({
          predicate: row.predicate,
          subject_uri: row.subject_uri,
          object_uri: row.object_uri,
          direction: row.direction,
          confidence: row.confidence,
          occurrence_count: parseInt(row.rel_occurrence_count),
        });
      }
    }

    // Check if results were truncated
    const truncated = edges.length >= limit;

    res.json({
      query_label: label,
      query_uri: uri,
      type_hint: typeHint,
      resolved_uri: resolved.uri,
      resolved_entity_id: resolved.entity_id,
      resolved_entity_text: resolved.entity_text,
      resolved_entity_type: resolved.entity_type,
      nodes: nodes,
      edges: edges,
      node_count: nodes.length,
      edge_count: edges.length,
      truncated: truncated,
    });

  } catch (error) {
    console.error('Neighborhood query error:', error);
    res.status(500).json({
      error: 'Neighborhood query failed',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Entity documents endpoint - find documents where an entity appears
// GET /api/koi/entity/documents?label=...&uri=...&type_hint=...&limit=20
app.get('/api/koi/entity/documents', async (req, res) => {
  try {
    const label = (req.query.label as string || '').trim() || null;
    const uri = (req.query.uri as string || '').trim() || null;
    const typeHint = (req.query.type_hint as string || '').trim().toUpperCase() || null;
    const limit = Math.min(Math.max(parseInt(req.query.limit as string) || 20, 1), 50);

    if (!label && !uri) {
      return res.status(400).json({ error: 'Either label or uri parameter is required' });
    }

    // Extract session token and validate for privacy filter
    const authHeader = req.headers['authorization'] as string | undefined;
    const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
    const authenticatedEmail = await validateSessionToken(sessionToken);
    const isAuthenticated = !!authenticatedEmail;
    const privacyFilter = buildPrivacyFilter(isAuthenticated);

    // Resolve entity
    const resolved = await resolveEntityInternal(label, uri, typeHint);

    if (!resolved) {
      return res.json({
        query_label: label,
        query_uri: uri,
        type_hint: typeHint,
        resolved_uri: null,
        resolved_entity_id: null,
        documents: [],
        error: 'Entity not found'
      });
    }

    // Query documents via koi_entity_chunk_links
    // Join on document_rid to match koi_memories.rid
    // NOTE: Privacy filter is applied early (in entity_docs CTE) to ensure LIMIT
    // doesn't exclude all public docs when private docs come first in index order
    const docQuery = `
      WITH entity_docs AS (
        SELECT DISTINCT
          l.document_rid,
          l.entity_name,
          MAX(l.confidence) as link_confidence
        FROM koi_entity_chunk_links l
        JOIN koi_memories m ON m.rid = l.document_rid
        WHERE (l.entity_uri = $1
           OR LOWER(TRIM(l.entity_name)) = LOWER(TRIM($2)))
          AND m.superseded_at IS NULL
          AND m.content->>'text' IS NOT NULL
          ${privacyFilter}
        GROUP BY l.document_rid, l.entity_name
        LIMIT $3 * 3
      ),
      docs_with_content AS (
        SELECT
          ed.document_rid,
          ed.entity_name,
          ed.link_confidence,
          m.content->>'text' as snippet,
          m.metadata->>'source' as source,
          m.metadata->>'url' as url,
          m.published_at,
          m.rid
        FROM entity_docs ed
        JOIN koi_memories m ON m.rid = ed.document_rid
        WHERE m.superseded_at IS NULL
          AND m.content->>'text' IS NOT NULL
      ),
      deduplicated AS (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY md5(snippet)
            ORDER BY published_at DESC NULLS LAST
          ) as content_rank
        FROM docs_with_content
      )
      SELECT
        rid,
        document_rid,
        url,
        source,
        SUBSTRING(snippet, 1, 500) as snippet,
        published_at,
        entity_name,
        link_confidence
      FROM deduplicated
      WHERE content_rank = 1
      ORDER BY published_at DESC NULLS LAST, link_confidence DESC NULLS LAST
      LIMIT $3
    `;

    const normalizedLabel = label || resolved.entity_text;
    const results = await pool.query(docQuery, [resolved.uri, normalizedLabel, limit]);

    const documents = results.rows.map(row => ({
      rid: row.rid,
      document_rid: row.document_rid,
      url: typeof row.url === 'string' ? row.url.trim() : row.url,
      source: row.source,
      snippet: row.snippet,
      published_at: row.published_at,
      entity_matched: row.entity_name,
      confidence: row.link_confidence,
    }));

    res.json({
      query_label: label,
      query_uri: uri,
      type_hint: typeHint,
      resolved_uri: resolved.uri,
      resolved_entity_id: resolved.entity_id,
      resolved_entity_text: resolved.entity_text,
      resolved_entity_type: resolved.entity_type,
      document_count: documents.length,
      documents: documents,
    });

  } catch (error) {
    console.error('Entity documents query error:', error);
    res.status(500).json({
      error: 'Entity documents query failed',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Consolidated entity endpoint for GPT (POST /api/koi/entity)
// Supports query_type: resolve, neighborhood, documents
// This consolidates the three GET endpoints into one for GPT's 30-operation limit
app.post('/api/koi/entity', async (req, res) => {
  // Session D1: Generate request_id for traceability
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);
  const startTime = Date.now();
  const toolTrace: ToolTraceEntry[] = [];
  const warnings: WarningCode[] = [];

  try {
    const {
      query_type,
      label,
      uri,
      type_hint,
      limit: requestLimit,
      direction = 'both'
    } = req.body;

    if (!query_type) {
      const envelope = createErrorEnvelope(requestId, {
        code: 'MISSING_QUERY_TYPE',
        message: 'query_type is required',
        retryable: false,
      });
      (envelope.data as any) = { valid_types: ['resolve', 'neighborhood', 'documents'] };
      return res.status(400).json(envelope);
    }

    if (!['resolve', 'neighborhood', 'documents'].includes(query_type)) {
      const envelope = createErrorEnvelope(requestId, {
        code: 'INVALID_QUERY_TYPE',
        message: `Invalid query_type: ${query_type}`,
        retryable: false,
      });
      (envelope.data as any) = { valid_types: ['resolve', 'neighborhood', 'documents'] };
      return res.status(400).json(envelope);
    }

    // Handle resolve query type
    if (query_type === 'resolve') {
      const labelValue = (label || '').trim();
      const typeHintValue = (type_hint || '').trim().toUpperCase() || null;
      const limit = Math.min(Math.max(parseInt(requestLimit) || 5, 1), 20);

      if (!labelValue) {
        const envelope = createErrorEnvelope(requestId, {
          code: 'MISSING_PARAMETER',
          message: 'label parameter is required for resolve query',
          retryable: false,
        });
        return res.status(400).json(envelope);
      }

      const lookupKeys = buildNormalizedLookupKeys(labelValue, typeHintValue);
      if (lookupKeys.length === 0) {
        const envelope = createErrorEnvelope(requestId, {
          code: 'MISSING_PARAMETER',
          message: 'label parameter is required for resolve query',
          retryable: false,
        });
        return res.status(400).json(envelope);
      }

      const query = `
        WITH entity_matches AS (
          SELECT
            id,
            entity_text,
            entity_type,
            normalized_text,
            occurrence_count,
            fuseki_uri
          FROM entity_registry
          WHERE LOWER(TRIM(normalized_text)) = ANY($1)
        ),
        rel_counts AS (
          SELECT
            e.id,
            COALESCE(subj.subj_count, 0) + COALESCE(obj.obj_count, 0) as relationship_count
          FROM entity_matches e
          LEFT JOIN (
            SELECT subject_entity_id, COUNT(*) as subj_count
            FROM koi_relationships
            GROUP BY subject_entity_id
          ) subj ON e.id = subj.subject_entity_id
          LEFT JOIN (
            SELECT object_entity_id, COUNT(*) as obj_count
            FROM koi_relationships
            GROUP BY object_entity_id
          ) obj ON e.id = obj.object_entity_id
        )
        SELECT
          e.id,
          e.entity_text,
          e.entity_type,
          e.normalized_text,
          e.occurrence_count,
          e.fuseki_uri,
          r.relationship_count
        FROM entity_matches e
        JOIN rel_counts r ON e.id = r.id
        ORDER BY e.occurrence_count DESC, r.relationship_count DESC
      `;

      const result = await pool.query(query, [lookupKeys]);
      const variants = result.rows;

      if (variants.length === 0) {
        // Session D1: Add tool trace for resolve query
        toolTrace.push({
          tool: 'entity_resolve',
          params_summary: summarizeParams({ label: labelValue, type_hint: typeHintValue }),
          timestamp: new Date(startTime).toISOString(),
          data_source: 'koi-derived',
          duration_ms: Date.now() - startTime,
        });

        const responseData = {
          query_type: 'resolve',
          query_label: labelValue,
          type_hint: typeHintValue,
          variant_count: 0,
          winner: null,
          alternatives: [],
          is_polysemy: false,
          resolution_method: 'no_match'
        };
        return res.json(createSuccessEnvelope(requestId, responseData, { tool_trace: toolTrace }));
      }

      const scoredVariants = variants.map(v => {
        const occScore = parseInt(v.occurrence_count) * 1000;
        const relScore = parseInt(v.relationship_count) * 100;
        const typePriority = DEFAULT_TYPE_PRIORITY[v.entity_type] || 0;
        const typeScore = typePriority * 10;

        let totalScore = occScore + relScore + typeScore;
        const reasons: string[] = [
          `occ=${v.occurrence_count}`,
          `rels=${v.relationship_count}`,
          `type_pri=${typePriority}`
        ];

        if (typeHintValue && v.entity_type.toUpperCase() === typeHintValue) {
          totalScore += 50000;
          reasons.push('type_hint_match=+50k');
        }

        return {
          uri: v.fuseki_uri,
          entity_text: v.entity_text,
          entity_type: v.entity_type,
          occurrence_count: parseInt(v.occurrence_count),
          relationship_count: parseInt(v.relationship_count),
          score: totalScore,
          score_breakdown: reasons.join(', ')
        };
      });

      scoredVariants.sort((a, b) => b.score - a.score);
      const winner = scoredVariants[0];
      const alternatives = scoredVariants.slice(1, limit);
      const uniqueTypes = new Set(scoredVariants.map(v => v.entity_type));
      const isPolysemy = uniqueTypes.size > 1;

      let resolutionMethod = 'highest_combined_score';
      if (typeHintValue && winner.entity_type.toUpperCase() === typeHintValue) {
        resolutionMethod = 'type_hint_match';
      } else {
        const totalOcc = scoredVariants.reduce((sum, v) => sum + v.occurrence_count, 0);
        const totalRels = scoredVariants.reduce((sum, v) => sum + v.relationship_count, 0);

        if (winner.occurrence_count > totalOcc * 0.5) {
          resolutionMethod = 'dominant_occurrence';
        } else if (winner.relationship_count > totalRels * 0.5) {
          resolutionMethod = 'dominant_connectivity';
        }
      }

      // Session D1: Add tool trace for resolve query
      toolTrace.push({
        tool: 'entity_resolve',
        params_summary: summarizeParams({ label: labelValue, type_hint: typeHintValue, limit }),
        timestamp: new Date(startTime).toISOString(),
        data_source: 'koi-derived',
        duration_ms: Date.now() - startTime,
      });

      const responseData = {
        query_type: 'resolve',
        query_label: labelValue,
        type_hint: typeHintValue,
        variant_count: scoredVariants.length,
        winner: winner,
        alternatives: alternatives,
        is_polysemy: isPolysemy,
        resolution_method: resolutionMethod
      };
      return res.json(createSuccessEnvelope(requestId, responseData, { tool_trace: toolTrace }));
    }

    // Handle neighborhood query type
    if (query_type === 'neighborhood') {
      const labelValue = (label || '').trim() || null;
      const uriValue = (uri || '').trim() || null;
      const typeHintValue = (type_hint || '').trim().toUpperCase() || null;
      const limit = Math.min(Math.max(parseInt(requestLimit) || 50, 1), 200);
      const directionValue = (direction || 'both').toLowerCase();

      if (!labelValue && !uriValue) {
        const envelope = createErrorEnvelope(requestId, {
          code: 'MISSING_PARAMETER',
          message: 'Either label or uri parameter is required for neighborhood query',
          retryable: false,
        });
        return res.status(400).json(envelope);
      }

      if (!['out', 'in', 'both'].includes(directionValue)) {
        const envelope = createErrorEnvelope(requestId, {
          code: 'INVALID_PARAMETER',
          message: 'direction must be one of: out, in, both',
          retryable: false,
        });
        return res.status(400).json(envelope);
      }

      const resolved = await resolveEntityInternal(labelValue, uriValue, typeHintValue);

      if (!resolved) {
        toolTrace.push({
          tool: 'entity_neighborhood',
          params_summary: summarizeParams({ label: labelValue, uri: uriValue, type_hint: typeHintValue }),
          timestamp: new Date(startTime).toISOString(),
          data_source: 'koi-derived',
          duration_ms: Date.now() - startTime,
        });

        const responseData = {
          query_type: 'neighborhood',
          query_label: labelValue,
          query_uri: uriValue,
          type_hint: typeHintValue,
          resolved_uri: null,
          resolved_entity_id: null,
          nodes: [],
          edges: [],
          truncated: false,
          error: 'Entity not found'
        };
        return res.json(createSuccessEnvelope(requestId, responseData, { tool_trace: toolTrace }));
      }

      let directionClause = '';
      if (directionValue === 'out') {
        directionClause = 'AND r.subject_entity_id = $1';
      } else if (directionValue === 'in') {
        directionClause = 'AND r.object_entity_id = $1';
      } else {
        directionClause = 'AND (r.subject_entity_id = $1 OR r.object_entity_id = $1)';
      }

      const relQuery = `
        WITH edges AS (
          SELECT
            r.id as rel_id,
            r.predicate,
            r.confidence,
            r.occurrence_count as rel_occurrence_count,
            r.subject_entity_id,
            r.object_entity_id,
            CASE
              WHEN r.subject_entity_id = $1 THEN 'out'
              ELSE 'in'
            END as direction
          FROM koi_relationships r
          WHERE 1=1 ${directionClause}
          ORDER BY r.occurrence_count DESC, r.confidence DESC NULLS LAST
          LIMIT $2
        ),
        all_entity_ids AS (
          SELECT DISTINCT subject_entity_id as entity_id FROM edges
          UNION
          SELECT DISTINCT object_entity_id as entity_id FROM edges
        ),
        nodes AS (
          SELECT
            e.id,
            e.fuseki_uri as uri,
            e.entity_text as text,
            e.entity_type as type,
            e.occurrence_count,
            COALESCE(subj.subj_count, 0) + COALESCE(obj.obj_count, 0) as relationship_count
          FROM entity_registry e
          JOIN all_entity_ids a ON e.id = a.entity_id
          LEFT JOIN (
            SELECT subject_entity_id, COUNT(*) as subj_count
            FROM koi_relationships
            GROUP BY subject_entity_id
          ) subj ON e.id = subj.subject_entity_id
          LEFT JOIN (
            SELECT object_entity_id, COUNT(*) as obj_count
            FROM koi_relationships
            GROUP BY object_entity_id
          ) obj ON e.id = obj.object_entity_id
        )
        SELECT
          'edge' as result_type,
          ed.rel_id,
          ed.predicate,
          ed.confidence,
          ed.rel_occurrence_count,
          ed.direction,
          subj.fuseki_uri as subject_uri,
          obj.fuseki_uri as object_uri,
          NULL as node_id,
          NULL as node_uri,
          NULL as node_text,
          NULL as node_type,
          NULL as node_occurrence_count,
          NULL as node_relationship_count
        FROM edges ed
        JOIN entity_registry subj ON ed.subject_entity_id = subj.id
        JOIN entity_registry obj ON ed.object_entity_id = obj.id

        UNION ALL

        SELECT
          'node' as result_type,
          NULL as rel_id,
          NULL as predicate,
          NULL as confidence,
          NULL as rel_occurrence_count,
          NULL as direction,
          NULL as subject_uri,
          NULL as object_uri,
          n.id as node_id,
          n.uri as node_uri,
          n.text as node_text,
          n.type as node_type,
          n.occurrence_count as node_occurrence_count,
          n.relationship_count as node_relationship_count
        FROM nodes n
      `;

      const results = await pool.query(relQuery, [resolved.entity_id, limit]);

      const nodes: any[] = [];
      const edges: any[] = [];
      const nodeSet = new Set<number>();

      for (const row of results.rows) {
        if (row.result_type === 'node') {
          if (!nodeSet.has(row.node_id)) {
            nodeSet.add(row.node_id);
            nodes.push({
              id: row.node_id,
              uri: row.node_uri,
              text: row.node_text,
              type: row.node_type,
              occurrence_count: parseInt(row.node_occurrence_count),
              relationship_count: parseInt(row.node_relationship_count),
            });
          }
        } else if (row.result_type === 'edge') {
          edges.push({
            predicate: row.predicate,
            subject_uri: row.subject_uri,
            object_uri: row.object_uri,
            direction: row.direction,
            confidence: row.confidence,
            occurrence_count: parseInt(row.rel_occurrence_count),
          });
        }
      }

      const truncated = edges.length >= limit;

      // Session D1: Add tool trace for neighborhood query
      toolTrace.push({
        tool: 'entity_neighborhood',
        params_summary: summarizeParams({ label: labelValue, uri: uriValue, direction: directionValue, limit }),
        timestamp: new Date(startTime).toISOString(),
        data_source: 'graph',
        duration_ms: Date.now() - startTime,
      });

      if (truncated) {
        warnings.push('pagination_not_exhausted');
      }

      const responseData = {
        query_type: 'neighborhood',
        query_label: labelValue,
        query_uri: uriValue,
        type_hint: typeHintValue,
        resolved_uri: resolved.uri,
        resolved_entity_id: resolved.entity_id,
        resolved_entity_text: resolved.entity_text,
        resolved_entity_type: resolved.entity_type,
        nodes: nodes,
        edges: edges,
        node_count: nodes.length,
        edge_count: edges.length,
        truncated: truncated,
      };
      return res.json(createSuccessEnvelope(requestId, responseData, { tool_trace: toolTrace, warnings }));
    }

    // Handle documents query type
    if (query_type === 'documents') {
      const labelValue = (label || '').trim() || null;
      const uriValue = (uri || '').trim() || null;
      const typeHintValue = (type_hint || '').trim().toUpperCase() || null;
      const limit = Math.min(Math.max(parseInt(requestLimit) || 20, 1), 50);

      if (!labelValue && !uriValue) {
        const envelope = createErrorEnvelope(requestId, {
          code: 'MISSING_PARAMETER',
          message: 'Either label or uri parameter is required for documents query',
          retryable: false,
        });
        return res.status(400).json(envelope);
      }

      // Extract session token and validate for privacy filter
      const authHeader = req.headers['authorization'] as string | undefined;
      const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
      const authenticatedEmail = await validateSessionToken(sessionToken);
      const isAuthenticated = !!authenticatedEmail;
      const privacyFilter = buildPrivacyFilter(isAuthenticated);

      const resolved = await resolveEntityInternal(labelValue, uriValue, typeHintValue);

      if (!resolved) {
        toolTrace.push({
          tool: 'entity_documents',
          params_summary: summarizeParams({ label: labelValue, uri: uriValue, type_hint: typeHintValue }),
          timestamp: new Date(startTime).toISOString(),
          data_source: 'koi-derived',
          duration_ms: Date.now() - startTime,
        });

        const responseData = {
          query_type: 'documents',
          query_label: labelValue,
          query_uri: uriValue,
          type_hint: typeHintValue,
          resolved_uri: null,
          resolved_entity_id: null,
          documents: [],
          error: 'Entity not found'
        };
        return res.json(createSuccessEnvelope(requestId, responseData, { tool_trace: toolTrace }));
      }

      const docQuery = `
        WITH entity_docs AS (
          SELECT DISTINCT
            l.document_rid,
            l.entity_name,
            MAX(l.confidence) as link_confidence
          FROM koi_entity_chunk_links l
          JOIN koi_memories m ON m.rid = l.document_rid
          WHERE (l.entity_uri = $1
             OR LOWER(TRIM(l.entity_name)) = LOWER(TRIM($2)))
            AND m.superseded_at IS NULL
            AND m.content->>'text' IS NOT NULL
            ${privacyFilter}
          GROUP BY l.document_rid, l.entity_name
          LIMIT $3 * 3
        ),
        docs_with_content AS (
          SELECT
            ed.document_rid,
            ed.entity_name,
            ed.link_confidence,
            m.content->>'text' as snippet,
            m.metadata->>'source' as source,
            m.metadata->>'url' as url,
            m.published_at,
            m.rid
          FROM entity_docs ed
          JOIN koi_memories m ON m.rid = ed.document_rid
          WHERE m.superseded_at IS NULL
            AND m.content->>'text' IS NOT NULL
        ),
        deduplicated AS (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY md5(snippet)
              ORDER BY published_at DESC NULLS LAST
            ) as content_rank
          FROM docs_with_content
        )
        SELECT
          rid,
          document_rid,
          url,
          source,
          SUBSTRING(snippet, 1, 500) as snippet,
          published_at,
          entity_name,
          link_confidence
        FROM deduplicated
        WHERE content_rank = 1
        ORDER BY published_at DESC NULLS LAST, link_confidence DESC NULLS LAST
        LIMIT $3
      `;

      const normalizedLabel = labelValue || resolved.entity_text;
      const results = await pool.query(docQuery, [resolved.uri, normalizedLabel, limit]);

      const documents = results.rows.map(row => ({
        rid: row.rid,
        document_rid: row.document_rid,
        url: typeof row.url === 'string' ? row.url.trim() : row.url,
        source: row.source,
        snippet: row.snippet,
        published_at: row.published_at,
        entity_matched: row.entity_name,
        confidence: row.link_confidence,
      })).filter(d => !shouldExcludeKoiResultSource(d.document_rid, d.url));

      // Session D1: Add tool trace for documents query
      toolTrace.push({
        tool: 'entity_documents',
        params_summary: summarizeParams({ label: labelValue, uri: uriValue, limit }),
        timestamp: new Date(startTime).toISOString(),
        data_source: 'koi-derived',
        duration_ms: Date.now() - startTime,
      });

      // Session D1: Add privacy warning if applicable
      if (!isAuthenticated) {
        warnings.push('privacy_filtered');
      }

      // Session D1: Extract citations from documents
      const citations = extractCitations(documents.map(d => ({
        rid: d.document_rid,
        metadata: { url: typeof d.url === 'string' ? d.url.trim() : d.url },
        content: d.snippet,
      })));

      const responseData = {
        query_type: 'documents',
        query_label: labelValue,
        query_uri: uriValue,
        type_hint: typeHintValue,
        resolved_uri: resolved.uri,
        resolved_entity_id: resolved.entity_id,
        resolved_entity_text: resolved.entity_text,
        resolved_entity_type: resolved.entity_type,
        document_count: documents.length,
        documents: documents,
      };
      return res.json(createSuccessEnvelope(requestId, responseData, { citations, tool_trace: toolTrace, warnings }));
    }

  } catch (error) {
    console.error('Entity query error:', error);
    const koiError: KoiError = {
      code: 'ENTITY_QUERY_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: error instanceof Error &&
        (error.message.includes('timeout') || error.message.includes('connection')),
    };
    if (koiError.retryable) {
      koiError.retry_after_ms = 1000;
    }
    const envelope = createErrorEnvelope(requestId, koiError);
    res.status(500).json(envelope);
  }
});

// Health check
app.get('/api/koi/health', async (req, res) => {
  try {
    const dbCheck = await pool.query("SELECT COUNT(*) FROM koi_memories");
    const embeddingCheck = await pool.query("SELECT COUNT(*) FROM koi_embeddings WHERE dim_1024 IS NOT NULL");
    
    res.json({
      status: 'healthy',
      database: 'connected',
      memories: dbCheck.rows[0].count,
      embeddings: embeddingCheck.rows[0].count,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    res.status(500).json({
      status: 'unhealthy',
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Start server

// Statistics endpoint
app.get('/api/koi/stats', async (req, res) => {
  try {
    const authHeader = req.headers['authorization'] as string | undefined;
    const sessionToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : undefined;
    const userEmail = await validateSessionToken(sessionToken);
    const privacyFilter = buildPrivacyFilter(!!userEmail, 'm');

    // Total documents
    const totalResult = await pool.query(
      `SELECT COUNT(*) as total FROM koi_memories m WHERE 1=1 ${privacyFilter}`
    );
    const total = parseInt(totalResult.rows[0].total);

    // By source
    const bySourceResult = await pool.query(`
      SELECT
        m.metadata->>'source' as source,
        COUNT(*) as count
      FROM koi_memories m
      WHERE m.metadata->>'source' IS NOT NULL ${privacyFilter}
      GROUP BY source
      ORDER BY count DESC
    `);

    // Recent activity (last 7 days)
    const recentResult = await pool.query(`
      SELECT COUNT(*) as recent
      FROM koi_memories m
      WHERE m.created_at > NOW() - INTERVAL '7 days' ${privacyFilter}
    `);
    const recent = parseInt(recentResult.rows[0].recent);

    // Format by_source as object
    const by_source = {};
    bySourceResult.rows.forEach(row => {
      by_source[row.source] = parseInt(row.count);
    });

    res.json({
      total_documents: total,
      recent_7_days: recent,
      by_source: by_source
    });
  } catch (error) {
    console.error('Stats error:', error);
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});
// Weekly digest endpoint - simplified version using search
app.get('/api/koi/weekly-digest', async (req, res) => {
  // Session D1: Generate request_id for traceability
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);
  const startTime = Date.now();
  const toolTrace: ToolTraceEntry[] = [];
  const warnings: WarningCode[] = [];

  try {
    const { start_date, end_date, format = 'markdown' } = req.query;

    // Calculate dates
    const now = new Date();
    const endDateStr = end_date || now.toISOString().split('T')[0];

    let startDateStr = start_date;
    if (!startDateStr) {
      const weekAgo = new Date(now);
      weekAgo.setDate(now.getDate() - 7);
      startDateStr = weekAgo.toISOString().split('T')[0];
    }

    console.log(`Generating weekly digest from ${startDateStr} to ${endDateStr}`);

    // Query for recent Regen Network activity
    const searchQuery = 'Regen Network activity updates discussions governance proposals';
    const topK = 50;

    // Perform semantic search with date filters
    const bgeResponse = await fetch('http://localhost:8090/encode', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: searchQuery })
    });

    let results = [];
    
    if (bgeResponse.ok) {
      const { embedding } = await bgeResponse.json();
      
      // Vector search with date range - use parameterized query
      const vectorQuery = `
        SELECT
          m.rid,
          m.content->>'text' as content,
          m.metadata->>'source' as source,
          m.metadata->>'url' as url,
          1 - (e.dim_1024 <=> $1::vector) as similarity,
          m.published_at
        FROM koi_memories m
        JOIN koi_embeddings e ON m.id = e.memory_id
        WHERE
          m.content->>'text' IS NOT NULL
          AND LENGTH(m.content->>'text') > 50
        AND m.superseded_at IS NULL
          AND e.dim_1024 IS NOT NULL
          AND m.published_at >= $2::timestamptz
          AND m.published_at <= $3::timestamptz
        ORDER BY e.dim_1024 <=> $1::vector
        LIMIT $4
      `;
      
      const queryResults = await pool.query(vectorQuery, [
        JSON.stringify(embedding),
        startDateStr,
        endDateStr,
        topK
      ]);
      
      results = queryResults.rows;
    } else {
      // Fallback to keyword search
      warnings.push('fallback_used');
      const fallbackQuery = `
        SELECT
          m.rid,
          m.content->>'text' as content,
          m.metadata->>'source' as source,
          m.metadata->>'url' as url,
          0.5 as similarity,
          m.published_at
        FROM koi_memories m
        WHERE
          m.content->>'text' IS NOT NULL
          AND LENGTH(m.content->>'text') > 50
        AND m.superseded_at IS NULL
          AND m.published_at >= $1::timestamptz
          AND m.published_at <= $2::timestamptz
        ORDER BY m.published_at DESC
        LIMIT $3
      `;

      const queryResults = await pool.query(fallbackQuery, [startDateStr, endDateStr, topK]);
      results = queryResults.rows;
    }

    // Generate markdown digest
    let markdownContent = `# Regen Network Weekly Digest\\n\\n`;
    markdownContent += `**Period:** ${startDateStr} to ${endDateStr}\\n\\n`;
    markdownContent += `## Summary\\n\\n`;
    markdownContent += `This digest contains ${results.length} recent documents and discussions from the Regen Network community.\\n\\n`;
    markdownContent += `## Recent Activity\\n\\n`;

    // Group by date
    const byDate = {};
    results.forEach(r => {
      const date = r.published_at ? r.published_at.toISOString().split('T')[0] : 'undated';
      if (!byDate[date]) byDate[date] = [];
      byDate[date].push(r);
    });

    // Output results by date
    Object.keys(byDate).sort().reverse().forEach(date => {
      if (date !== 'undated') {
        markdownContent += `### ${date}\\n\\n`;
      }
      byDate[date].forEach(item => {
        const preview = item.content.substring(0, 200);
        markdownContent += `**Source:** ${item.source}\\n`;
        if (item.url) {
          markdownContent += `**URL:** ${item.url}\\n`;
        }
        markdownContent += `${preview}...\\n\\n---\\n\\n`;
      });
    });

    const wordCount = markdownContent.split(/\\s+/).length;

    // Session D1: Add tool trace
    toolTrace.push({
      tool: 'weekly_digest',
      params_summary: summarizeParams({ start_date: startDateStr, end_date: endDateStr, format }),
      timestamp: new Date(startTime).toISOString(),
      data_source: 'koi-derived',
      duration_ms: Date.now() - startTime,
    });

    // Session D1: Extract citations from results
    const citations = extractCitations(results.map(r => ({
      rid: r.rid,
      metadata: { url: typeof r.url === 'string' ? r.url.trim() : r.url, title: r.source },
      content: r.content,
    })));

    let responseData: any;
    if (format === 'json') {
      responseData = {
        success: true,
        week_start: startDateStr,
        week_end: endDateStr,
        total_items: results.length,
        content: markdownContent,
        metadata: {
          word_count: wordCount,
          source_count: results.length
        }
      };
    } else {
      responseData = {
        success: true,
        format: 'markdown',
        content: markdownContent,
        metadata: {
          week_start: startDateStr,
          week_end: endDateStr,
          total_items: results.length,
          word_count: wordCount
        }
      };
    }

    const envelope = createSuccessEnvelope(requestId, responseData, {
      citations,
      warnings,
      tool_trace: toolTrace,
    });
    res.json(envelope);
  } catch (error) {
    console.error('Weekly digest error:', error);
    const koiError: KoiError = {
      code: 'WEEKLY_DIGEST_ERROR',
      message: error instanceof Error ? error.message : 'Failed to generate weekly digest',
      retryable: error instanceof Error &&
        (error.message.includes('timeout') || error.message.includes('connection')),
    };
    if (koiError.retryable) {
      koiError.retry_after_ms = 1000;
    }
    const envelope = createErrorEnvelope(requestId, koiError);
    res.status(500).json(envelope);
  }
});

// =============================================================================
// Anchored Metadata Endpoints (Session E: MCP-only tools)
// INTERNAL ONLY: Requires X-Internal-API-Key header
// =============================================================================

/**
 * POST /api/koi/metadata/resolve
 * Resolve a Regen metadata IRI via the allowlisted resolver
 *
 * INTERNAL ONLY: Requires X-Internal-API-Key header
 *
 * This endpoint ONLY resolves and caches metadata. It does NOT require
 * hectares derivation to succeed. Use derive_offchain_hectares for metrics.
 *
 * Request headers:
 *   X-Internal-API-Key: <KOI_INTERNAL_API_KEY>
 *
 * Request body:
 *   { iri: string, force_refresh?: boolean }
 *
 * Response (success):
 *   { iri, resolver_url, content_hash, rid, resolved_at, from_cache }
 */
app.post('/api/koi/metadata/resolve', requireInternalOrSessionAuth, async (req, res) => {
  const requestId = res.getHeader('X-Request-ID') as string || generateRequestId();
  const startTime = Date.now();
  console.log(`[Metadata] POST /metadata/resolve auth=${req.authMethod}${req.userEmail ? ` user=${req.userEmail}` : ''}`);

  try {
    const { iri, force_refresh = false } = req.body || {};

    if (!iri || typeof iri !== 'string') {
      const koiError: KoiError = {
        code: 'INVALID_REQUEST',
        message: 'Missing or invalid "iri" parameter',
        retryable: false,
      };
      return res.status(400).json(createErrorEnvelope(requestId, koiError));
    }

    console.log(`[Metadata] Resolving IRI: ${iri} (force_refresh=${force_refresh})`);

    // Use resolver directly - no hectares derivation required
    const resolver = getMetadataResolver();
    const result = await resolver.resolveMetadataIri(iri, force_refresh);

    if (!result.success || !result.metadata) {
      const koiError: KoiError = {
        code: result.error?.code || 'RESOLUTION_FAILED',
        message: result.error?.message || 'Failed to resolve metadata IRI',
        retryable: result.error?.retryable ?? false,
      };
      console.log(`[Metadata] Resolution failed: ${koiError.code} - ${koiError.message}`);
      return res.status(404).json(createErrorEnvelope(requestId, koiError));
    }

    // Success - return resolution details (no citations for pure resolution)
    const metadata = result.metadata as ResolvedMetadata;
    const responseData = {
      iri: metadata.iri,
      resolver_url: metadata.resolver_url,
      content_hash: metadata.content_hash,
      rid: metadata.rid,
      resolved_at: metadata.resolved_at.toISOString(),
      from_cache: metadata.from_cache,
    };

    console.log(`[Metadata] Resolved successfully in ${Date.now() - startTime}ms (from_cache=${metadata.from_cache})`);

    // Create envelope with appropriate data_source
    const envelope = createSuccessEnvelope(requestId, responseData);
    envelope.data_source = 'metadata';
    return res.json(envelope);

  } catch (error) {
    console.error('[Metadata] Resolve error:', error);
    const koiError: KoiError = {
      code: 'INTERNAL_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: true,
    };
    return res.status(500).json(createErrorEnvelope(requestId, koiError));
  }
});

/**
 * POST /api/koi/metadata/hectares
 * Derive hectares from a resolved metadata IRI with full citation
 *
 * INTERNAL ONLY: Requires X-Internal-API-Key header
 *
 * Enforces "no citation, no metric" policy - returns blocked=true if
 * derivation is not possible.
 *
 * Request headers:
 *   X-Internal-API-Key: <KOI_INTERNAL_API_KEY>
 *
 * Request body:
 *   { iri: string, force_refresh?: boolean }
 *
 * Response (success):
 *   { hectares, unit, derivation: { iri, rid, resolver_url, content_hash, json_pointer, expected_unit }, citations[] }
 *
 * Response (error with blocked=true):
 *   { blocked: true, code, message } - metric should NOT be reported
 */
app.post('/api/koi/metadata/hectares', requireInternalOrSessionAuth, async (req, res) => {
  const requestId = res.getHeader('X-Request-ID') as string || generateRequestId();
  const startTime = Date.now();
  console.log(`[Metadata] POST /metadata/hectares auth=${req.authMethod}${req.userEmail ? ` user=${req.userEmail}` : ''}`);

  try {
    const { iri, force_refresh = false } = req.body || {};

    if (!iri || typeof iri !== 'string') {
      const koiError: KoiError = {
        code: 'INVALID_REQUEST',
        message: 'Missing or invalid "iri" parameter',
        retryable: false,
      };
      return res.status(400).json(createErrorEnvelope(requestId, koiError));
    }

    console.log(`[Metadata] Deriving hectares for IRI: ${iri} (force_refresh=${force_refresh})`);

    const integration = getMetadataIntegration();
    const result = await integration.extractHectaresWithCitation(iri, force_refresh);

    if (!result.success || !result.metric || !result.citation) {
      // Derivation failed - return blocked error (no metric should be reported)
      const koiError: KoiError = {
        code: result.error?.code || 'DERIVATION_FAILED',
        message: result.error?.message || 'Failed to derive hectares from metadata',
        retryable: false,
      };
      console.log(`[Metadata] Derivation blocked: ${koiError.code} - ${koiError.message}`);
      return res.status(404).json({
        ...createErrorEnvelope(requestId, koiError),
        blocked: true, // Critical: no citation, no metric
      });
    }

    // Success - return hectares with full derivation provenance
    const metric = result.metric as AnchoredMetric;
    const citation = result.citation as MetadataCitation;

    const responseData = {
      hectares: metric.value,
      unit: metric.unit,
      derivation: {
        iri,
        rid: citation.rid,
        resolver_url: citation.resolver_url,
        content_hash: citation.content_hash,
        json_pointer: citation.json_pointer,
        expected_unit: 'unit:HA',
      },
      citations: [{
        rid: citation.rid,
        url: typeof citation.url === 'string' ? citation.url.trim() : citation.url,
        title: citation.title,
        excerpt: citation.excerpt,
        content_hash: citation.content_hash,
        json_pointer: citation.json_pointer,
        resolver_url: citation.resolver_url,
        resolved_at: citation.resolved_at,
        citation_type: citation.citation_type,
      }],
    };

    console.log(`[Metadata] Derived hectares: ${metric.value} ${metric.unit} in ${Date.now() - startTime}ms`);

    // Create envelope with appropriate data_source and citations
    const envelope = createSuccessEnvelope(requestId, responseData);
    envelope.data_source = 'koi-derived';
    envelope.citations = responseData.citations;
    return res.json(envelope);

  } catch (error) {
    console.error('[Metadata] Hectares derivation error:', error);
    const koiError: KoiError = {
      code: 'INTERNAL_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: true,
    };
    return res.status(500).json(createErrorEnvelope(requestId, koiError));
  }
});

/**
 * GET /api/koi/metadata/stats
 * Get statistics about anchored metadata records
 *
 * INTERNAL ONLY: Requires X-Internal-API-Key header
 */
app.get('/api/koi/metadata/stats', requireInternalOrSessionAuth, async (req, res) => {
  const requestId = res.getHeader('X-Request-ID') as string || generateRequestId();
  console.log(`[Metadata] GET /metadata/stats auth=${req.authMethod}${req.userEmail ? ` user=${req.userEmail}` : ''}`);

  try {
    const integration = getMetadataIntegration();
    const stats = await integration.getAnchoredMetadataStats();

    const envelope = createSuccessEnvelope(requestId, stats);
    envelope.data_source = 'metadata';
    return res.json(envelope);
  } catch (error) {
    console.error('[Metadata] Stats error:', error);
    const koiError: KoiError = {
      code: 'INTERNAL_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: true,
    };
    return res.status(500).json(createErrorEnvelope(requestId, koiError));
  }
});

// =============================================================================
// Document Retrieval Endpoint (MCP-only)
// Retrieves full document content by RID
// =============================================================================

// Minimum text length to consider direct content valid (avoid triggering fallback for stubs)
const MIN_DIRECT_TEXT_LENGTH = 50;

/**
 * GET /api/koi/document/full
 * Retrieve full document content by RID
 *
 * Auth is optional: public documents (is_private = false/null) are served to
 * anonymous callers, matching /query and /entity/documents behavior.
 * Private documents require a valid Bearer session token (internal API key
 * alone does NOT unlock private docs).
 *
 * Query parameters:
 *   rid: Document RID (can be base doc or chunk RID)
 *
 * Response (success):
 *   {
 *     document: { rid, title, url, source, published_at, content_source },
 *     content: "Full document text...",
 *     char_count, word_count,
 *     missing_chunk_indices: [2, 4],  // if gaps detected
 *     fallback_reason: "empty_text" | "short_text" | null,
 *     warnings: ["fallback_used", "partial_results"]
 *   }
 */
app.get('/api/koi/document/full', optionalSessionAuth, async (req, res) => {
  const requestId = res.getHeader('X-Request-ID') as string || generateRequestId();
  const startTime = Date.now();
  console.log(`[Document] GET /document/full auth=${req.authMethod}${req.userEmail ? ` user=${req.userEmail}` : ''}`);

  try {
    const rid = req.query.rid as string;

    if (!rid || typeof rid !== 'string') {
      const koiError: KoiError = {
        code: 'INVALID_RID',
        message: 'Missing or invalid "rid" query parameter',
        retryable: false,
      };
      return res.status(400).json(createErrorEnvelope(requestId, koiError));
    }

    console.log(`[Document] Fetching full document for RID: ${rid}`);

    // Private doc access requires session token auth (internal key alone → public docs only)
    const isAuthenticated = !!req.isAuthenticated;

    // Step 1: Normalize RID - if it's a chunk RID, look up the document RID
    let documentRid = rid;
    const chunkSuffixMatch = rid.match(/(:chunk_\d+|#chunk\d+)$/);

    if (chunkSuffixMatch) {
      // Try to lookup document_rid from koi_memory_chunks first
      const chunkLookupResult = await pool.query(
        `SELECT document_rid FROM koi_memory_chunks WHERE chunk_rid = $1`,
        [rid]
      );

      if (chunkLookupResult.rows.length > 0) {
        documentRid = chunkLookupResult.rows[0].document_rid;
        console.log(`[Document] Resolved chunk RID to document: ${documentRid}`);
      } else {
        // Fallback: strip the chunk suffix
        documentRid = rid.replace(/(:chunk_\d+|#chunk\d+)$/, '');
        console.log(`[Document] Stripped chunk suffix: ${documentRid}`);
      }
    }

    // Step 2: Query primary content from koi_memories
    const primaryQuery = `
      SELECT rid,
             content->>'text' as text,
             COALESCE(content->>'title', metadata->>'title') as title,
             COALESCE(metadata->>'url', content->>'url') as url,
             COALESCE(metadata->>'source', content->>'source_type') as source,
             is_private,
             published_at
      FROM koi_memories
      WHERE rid = $1 AND superseded_at IS NULL
    `;
    const primaryResult = await pool.query(primaryQuery, [documentRid]);

    let documentRow = primaryResult.rows[0];
    let content: string | null = null;
    let contentSource: 'direct' | 'chunks' | 'legacy_chunks' = 'direct';
    let fallbackReason: string | null = null;
    const warnings: WarningCode[] = [];
    let missingChunkIndices: number[] = [];

    // Step 3: Privacy check
    if (documentRow && documentRow.is_private === true && !isAuthenticated) {
      // Return 404 to avoid leaking existence of private documents
      const koiError: KoiError = {
        code: 'DOCUMENT_NOT_FOUND',
        message: 'No document found for the specified RID',
        retryable: false,
      };
      return res.status(404).json(createErrorEnvelope(requestId, koiError));
    }

    // Step 4: Check if direct text is usable
    if (documentRow) {
      const directText = documentRow.text;
      if (directText && directText.trim().length >= MIN_DIRECT_TEXT_LENGTH) {
        content = directText;
        contentSource = 'direct';
      } else if (directText === null) {
        fallbackReason = 'null_text';
      } else if (directText.trim() === '') {
        fallbackReason = 'empty_text';
      } else {
        fallbackReason = 'short_text';
      }
    }

    // Step 5: Fallback to chunk reassembly from koi_memory_chunks
    if (!content) {
      console.log(`[Document] Direct text not usable (${fallbackReason}), trying chunk reassembly`);

      const privacyJoinFilter = isAuthenticated
        ? ''
        : 'AND (m.is_private IS NULL OR m.is_private = false)';

      const chunkQuery = `
        SELECT c.chunk_rid, c.chunk_index, c.content->>'text' as text,
               c.total_chunks, c.metadata as chunk_metadata
        FROM koi_memory_chunks c
        JOIN koi_memories m ON m.rid = c.document_rid
        WHERE c.document_rid = $1
          AND m.superseded_at IS NULL
          ${privacyJoinFilter}
        ORDER BY c.chunk_index ASC
      `;
      const chunkResult = await pool.query(chunkQuery, [documentRid]);

      if (chunkResult.rows.length > 0) {
        // Reassemble chunks
        const chunks = chunkResult.rows;
        const chunkTexts = chunks.map(c => c.text).filter(Boolean);
        content = chunkTexts.join('\n\n');
        contentSource = 'chunks';
        warnings.push('fallback_used' as WarningCode);

        // Check for gaps in chunk indices
        const totalChunks = chunks[0]?.total_chunks || (Math.max(...chunks.map(c => c.chunk_index)) + 1);
        const presentIndices = new Set(chunks.map(c => c.chunk_index));
        for (let i = 0; i < totalChunks; i++) {
          if (!presentIndices.has(i)) {
            missingChunkIndices.push(i);
          }
        }
        if (missingChunkIndices.length > 0) {
          warnings.push('partial_results' as WarningCode);
          console.log(`[Document] Missing chunk indices: ${missingChunkIndices.join(', ')}`);
        }

        // If no documentRow exists, infer metadata from first chunk
        if (!documentRow && chunks[0]?.chunk_metadata) {
          const meta = chunks[0].chunk_metadata;
          documentRow = {
            rid: documentRid,
            title: meta.title || null,
            url: meta.url || null,
            source: meta.source || null,
            published_at: meta.published_at || null,
            is_private: false,
          };
        }

        console.log(`[Document] Reassembled ${chunks.length} chunks`);
      }
    }

    // Step 6: Fallback to legacy chunks in koi_memories
    if (!content) {
      console.log(`[Document] No chunks found, trying legacy chunk format`);

      const privacyFilter = isAuthenticated
        ? ''
        : 'AND (is_private IS NULL OR is_private = false)';

      // Escape _ and % in documentRid for LIKE pattern
      const escapedRid = documentRid.replace(/_/g, '\\_').replace(/%/g, '\\%');

      const legacyQuery = `
        SELECT rid, content->>'text' as text,
               (metadata->>'chunk_index')::int as chunk_index,
               metadata, content
        FROM koi_memories
        WHERE (rid LIKE $1 || ':chunk\\_%' ESCAPE '\\'
               OR rid LIKE $1 || '#chunk%' ESCAPE '\\')
          AND superseded_at IS NULL
          ${privacyFilter}
        ORDER BY chunk_index ASC NULLS LAST
      `;
      const legacyResult = await pool.query(legacyQuery, [escapedRid]);

      if (legacyResult.rows.length > 0) {
        const legacyChunks = legacyResult.rows;
        const chunkTexts = legacyChunks.map(c => c.text).filter(Boolean);
        content = chunkTexts.join('\n\n');
        contentSource = 'legacy_chunks';
        warnings.push('fallback_used' as WarningCode);

        // Check for gaps
        const indicesPresent = legacyChunks.map(c => c.chunk_index).filter(i => i !== null);
        if (indicesPresent.length > 0) {
          const maxIndex = Math.max(...indicesPresent);
          const presentSet = new Set(indicesPresent);
          for (let i = 0; i <= maxIndex; i++) {
            if (!presentSet.has(i)) {
              missingChunkIndices.push(i);
            }
          }
          if (missingChunkIndices.length > 0) {
            warnings.push('partial_results' as WarningCode);
          }
        }

        // Infer metadata from first chunk if no documentRow
        if (!documentRow && legacyChunks[0]) {
          const firstChunk = legacyChunks[0];
          const meta = firstChunk.metadata || {};
          const cont = firstChunk.content || {};
          documentRow = {
            rid: documentRid,
            title: meta.title || cont.title || null,
            url: meta.url || cont.url || null,
            source: meta.source || cont.source_type || null,
            published_at: meta.published_at || null,
            is_private: false,
          };
        }

        console.log(`[Document] Reassembled ${legacyChunks.length} legacy chunks`);
      }
    }

    // Step 7: No content found at all
    if (!content) {
      const koiError: KoiError = {
        code: 'DOCUMENT_NOT_FOUND',
        message: 'No document found for the specified RID',
        retryable: false,
      };
      return res.status(404).json(createErrorEnvelope(requestId, koiError));
    }

    // Build response
    const charCount = content.length;
    const wordCount = content.split(/\s+/).filter(Boolean).length;

    const responseData = {
      document: {
        rid: documentRow?.rid || documentRid,
        title: documentRow?.title || null,
        url: documentRow?.url || null,
        source: documentRow?.source || null,
        published_at: documentRow?.published_at || null,
        content_source: contentSource,
      },
      content,
      char_count: charCount,
      word_count: wordCount,
      missing_chunk_indices: missingChunkIndices.length > 0 ? missingChunkIndices : undefined,
      fallback_reason: fallbackReason,
      warnings: warnings.length > 0 ? warnings : undefined,
    };

    console.log(`[Document] Retrieved ${wordCount} words via ${contentSource} in ${Date.now() - startTime}ms`);

    const envelope = createSuccessEnvelope(requestId, responseData, {
      warnings: warnings.length > 0 ? warnings : undefined,
    });
    envelope.data_source = 'koi-memories';
    return res.json(envelope);

  } catch (error) {
    console.error('[Document] Full document retrieval error:', error);
    const koiError: KoiError = {
      code: 'INTERNAL_ERROR',
      message: error instanceof Error ? error.message : 'Unknown error',
      retryable: true,
    };
    return res.status(500).json(createErrorEnvelope(requestId, koiError));
  }
});

// =============================================================================
// Feedback Endpoint - For MCP submit_feedback tool
// =============================================================================
app.post('/api/koi/feedback', async (req, res) => {
  const requestId = generateRequestId();
  res.setHeader('X-Request-ID', requestId);

  const startTime = Date.now();
  console.log('[Feedback] Received feedback submission');

  try {
    const { rating, category, task_description, notes, session_context, user_email, client_version } = req.body;

    // Validate required fields
    if (typeof rating !== 'number' || rating < 1 || rating > 5) {
      const koiError: KoiError = {
        code: 'VALIDATION_ERROR',
        message: 'Rating must be a number between 1 and 5',
        retryable: false,
      };
      return res.status(400).json(createErrorEnvelope(requestId, koiError));
    }

    const validCategories = ['success', 'partial', 'bug', 'suggestion', 'question', 'other'];
    if (!category || !validCategories.includes(category)) {
      const koiError: KoiError = {
        code: 'VALIDATION_ERROR',
        message: `Category must be one of: ${validCategories.join(', ')}`,
        retryable: false,
      };
      return res.status(400).json(createErrorEnvelope(requestId, koiError));
    }

    if (!notes || typeof notes !== 'string' || notes.trim().length === 0) {
      const koiError: KoiError = {
        code: 'VALIDATION_ERROR',
        message: 'Notes field is required and cannot be empty',
        retryable: false,
      };
      return res.status(400).json(createErrorEnvelope(requestId, koiError));
    }

    // Insert feedback into database
    const result = await pool.query(
      `INSERT INTO user_feedback (rating, category, task_description, notes, session_context, user_email, client_version)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING id, created_at`,
      [
        rating,
        category,
        task_description || null,
        notes.trim(),
        session_context ? JSON.stringify(session_context) : '{}',
        user_email || 'anonymous',
        client_version || null
      ]
    );

    const feedbackId = result.rows[0].id;
    const createdAt = result.rows[0].created_at;

    console.log(`[Feedback] Stored feedback #${feedbackId}: rating=${rating}, category=${category}`);

    const responseData = {
      feedback_id: feedbackId,
      rating,
      category,
      created_at: createdAt,
      message: 'Thank you for your feedback!',
    };

    const envelope = createSuccessEnvelope(requestId, responseData, {});
    envelope.data_source = 'feedback';
    return res.status(201).json(envelope);

  } catch (error) {
    console.error('[Feedback] Error storing feedback:', error);
    const koiError: KoiError = {
      code: 'INTERNAL_ERROR',
      message: error instanceof Error ? error.message : 'Failed to store feedback',
      retryable: true,
    };
    return res.status(500).json(createErrorEnvelope(requestId, koiError));
  }
});

// Debug ping endpoint
app.get('/api/koi/ping', (req, res) => {
  console.log('[Ping] Request received');
  res.json({ pong: true, version: 3, time: new Date().toISOString() });
});

// Debug endpoint for entity search testing
app.get('/api/koi/debug-entity', async (req, res) => {
  const query = (req.query.q as string) || "What is the Regen Network?";
  console.log(`[Debug] Testing entity search for: "${query}"`);

  try {
    const entityResults = await performEntitySearch(query, 10, '');
    res.json({
      query,
      entity_count: entityResults.length,
      entities: entityResults.slice(0, 5).map((r: any) => ({
        rid: r.rid?.substring(0, 50),
        score: r.score,
        entities_matched: r.metadata?.entities_matched
      }))
    });
  } catch (err: any) {
    console.error('[Debug] Error:', err);
    res.status(500).json({ error: err.message, stack: err.stack });
  }
});

console.log('[Init] Debug endpoints registered');

app.listen(PORT, () => {
  console.log(`🚀 KOI Query API running on http://localhost:${PORT}`);
  console.log(`📊 Health check: http://localhost:${PORT}/api/koi/health`);
  console.log(`🔍 Query endpoint: POST http://localhost:${PORT}/api/koi/query`);
  console.log(`🔧 Debug: GET http://localhost:${PORT}/api/koi/ping`);
  console.log(`🧬 Polysemy rerank: ENABLE=${process.env.ENABLE_POLYSEMY_RERANK}, DEBUG=${process.env.DEBUG_POLYSEMY_RERANK}`);
});

export default app;
