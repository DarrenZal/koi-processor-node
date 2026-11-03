/**
 * Anchored Metadata Resolver
 * Session E: Resolves Regen metadata IRIs via the allowlisted Regen resolver API
 *
 * Security constraints (per KOI_MCP_Review_Reflection_Plan.md):
 * - Only allowlisted resolver URL (api.regen.network)
 * - Only regen: IRIs (validated format)
 * - URL-encode IRI in path
 * - Strict timeout (10s default)
 * - Max response size (1MB default)
 * - No redirects followed
 * - Content hash for integrity
 */

import { Pool } from 'pg';
import { createHash } from 'crypto';

// =============================================================================
// Configuration
// =============================================================================

// Allowlisted resolver base URL - the ONLY external URL we will fetch
const RESOLVER_BASE_URL = 'https://api.regen.network/data/v2/metadata-graph';

// Security limits
const DEFAULT_TIMEOUT_MS = 10_000;        // 10 second timeout
const MAX_RESPONSE_SIZE_BYTES = 1_048_576; // 1MB max payload
const MAX_IRI_LENGTH = 256;               // Max IRI length
const IRI_PREFIX = 'regen:';              // Required IRI prefix

// Cache policy
const CACHE_TTL_DAYS = 30;                // How long before considering refresh
const REFRESH_BACKOFF_HOURS = 1;          // Minimum time between refresh attempts

// =============================================================================
// Types
// =============================================================================

export interface ResolvedMetadata {
  rid: string;                    // Internal record ID
  iri: string;                    // The IRI that was resolved
  resolver_url: string;           // Full resolver URL used
  resolved_at: Date;              // When it was resolved
  content_hash: string;           // SHA-256 of payload
  payload: Record<string, any>;   // The JSON-LD payload
  from_cache: boolean;            // Whether this came from cache
}

export interface MetadataResolutionResult {
  success: boolean;
  metadata?: ResolvedMetadata;
  error?: {
    code: string;
    message: string;
    retryable: boolean;
  };
}

export interface DerivationResult {
  metric_id: string;
  metric_label: string;
  numeric_value?: number;
  string_value?: string;
  unit?: string;
  json_pointer: string;
  is_valid: boolean;
  validation_errors?: string[];
}

// =============================================================================
// IRI Validation
// =============================================================================

/**
 * Validate a Regen IRI format
 * Per spec: regen:{base58check(...)}.{extension}
 */
export function validateIri(iri: string): { valid: boolean; error?: string } {
  if (!iri) {
    return { valid: false, error: 'IRI is required' };
  }

  if (iri.length > MAX_IRI_LENGTH) {
    return { valid: false, error: `IRI exceeds maximum length of ${MAX_IRI_LENGTH}` };
  }

  if (!iri.startsWith(IRI_PREFIX)) {
    return { valid: false, error: `IRI must start with '${IRI_PREFIX}'` };
  }

  // Basic format check: regen:<base58>.rdf or similar
  // Pattern: regen: followed by alphanumeric/base58 chars, dot, extension
  const iriPattern = /^regen:[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]+\.[a-z]+$/;
  if (!iriPattern.test(iri)) {
    return { valid: false, error: 'IRI format invalid: expected regen:<base58hash>.<extension>' };
  }

  return { valid: true };
}

// =============================================================================
// Content Hash
// =============================================================================

/**
 * Compute SHA-256 hash of content for integrity checking
 */
export function computeContentHash(content: string): string {
  return createHash('sha256').update(content, 'utf8').digest('hex');
}

// =============================================================================
// Resolver Client
// =============================================================================

export class AnchoredMetadataResolver {
  private pool: Pool;
  private timeoutMs: number;
  private maxResponseSize: number;

  constructor(
    pool: Pool,
    options?: {
      timeoutMs?: number;
      maxResponseSize?: number;
    }
  ) {
    this.pool = pool;
    this.timeoutMs = options?.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.maxResponseSize = options?.maxResponseSize ?? MAX_RESPONSE_SIZE_BYTES;
  }

  /**
   * Resolve a metadata IRI, using cache if available
   *
   * @param iri - The Regen IRI to resolve (e.g., regen:13toVg...rdf)
   * @param forceRefresh - If true, bypass cache and fetch fresh
   * @returns Resolution result with metadata or error
   */
  async resolveMetadataIri(
    iri: string,
    forceRefresh: boolean = false
  ): Promise<MetadataResolutionResult> {
    // Step 1: Validate IRI format
    const validation = validateIri(iri);
    if (!validation.valid) {
      return {
        success: false,
        error: {
          code: 'INVALID_IRI',
          message: validation.error || 'Invalid IRI',
          retryable: false
        }
      };
    }

    // Step 2: Check cache unless force refresh
    if (!forceRefresh) {
      const cached = await this.getCachedMetadata(iri);
      if (cached) {
        return {
          success: true,
          metadata: {
            ...cached,
            from_cache: true
          }
        };
      }
    }

    // Step 3: Fetch from resolver
    const fetchResult = await this.fetchFromResolver(iri);
    if (!fetchResult.success) {
      return fetchResult;
    }

    // Step 4: Store in cache
    const stored = await this.storeMetadata(
      iri,
      fetchResult.payload!,
      fetchResult.content_hash!,
      fetchResult.resolution_time_ms!,
      fetchResult.resolver_url!
    );

    return {
      success: true,
      metadata: {
        rid: stored.id.toString(),
        iri,
        resolver_url: fetchResult.resolver_url!,
        resolved_at: new Date(),
        content_hash: fetchResult.content_hash!,
        payload: fetchResult.payload!,
        from_cache: false
      }
    };
  }

  /**
   * Check cache for existing resolved metadata
   */
  private async getCachedMetadata(iri: string): Promise<ResolvedMetadata | null> {
    try {
      const result = await this.pool.query(
        `SELECT id, iri, resolver_url, resolved_at, content_hash, payload_jsonb
         FROM anchored_metadata_records
         WHERE iri = $1 AND http_status = 200`,
        [iri]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const row = result.rows[0];
      return {
        rid: row.id.toString(),
        iri: row.iri,
        resolver_url: row.resolver_url,
        resolved_at: row.resolved_at,
        content_hash: row.content_hash,
        payload: row.payload_jsonb,
        from_cache: true
      };
    } catch (error) {
      console.error('[AnchoredMetadata] Cache lookup error:', error);
      return null;
    }
  }

  /**
   * Fetch metadata from the Regen resolver API
   *
   * Security protections:
   * - Only allowlisted base URL
   * - URL-encoded IRI
   * - Timeout enforcement
   * - Response size limit
   * - No redirect following
   */
  private async fetchFromResolver(iri: string): Promise<{
    success: boolean;
    payload?: Record<string, any>;
    content_hash?: string;
    resolution_time_ms?: number;
    resolver_url?: string;
    error?: { code: string; message: string; retryable: boolean };
  }> {
    // Build the resolver URL with URL-encoded IRI
    const encodedIri = encodeURIComponent(iri);
    const resolverUrl = `${RESOLVER_BASE_URL}/${encodedIri}`;

    const startTime = Date.now();

    try {
      // Use AbortController for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeoutMs);

      const response = await fetch(resolverUrl, {
        method: 'GET',
        headers: {
          'Accept': 'application/ld+json, application/json',
          'User-Agent': 'KOI-AnchoredMetadataResolver/1.0'
        },
        signal: controller.signal,
        redirect: 'error' // Reject redirects for security
      });

      clearTimeout(timeoutId);

      const resolutionTimeMs = Date.now() - startTime;

      // Check response status
      if (!response.ok) {
        return {
          success: false,
          resolver_url: resolverUrl,
          resolution_time_ms: resolutionTimeMs,
          error: {
            code: `HTTP_${response.status}`,
            message: `Resolver returned ${response.status}: ${response.statusText}`,
            retryable: response.status >= 500 || response.status === 429
          }
        };
      }

      // Check content length before reading
      const contentLength = response.headers.get('content-length');
      if (contentLength && parseInt(contentLength) > this.maxResponseSize) {
        return {
          success: false,
          resolver_url: resolverUrl,
          error: {
            code: 'RESPONSE_TOO_LARGE',
            message: `Response size ${contentLength} exceeds max ${this.maxResponseSize}`,
            retryable: false
          }
        };
      }

      // Read response with size limit
      const text = await this.readResponseWithLimit(response);
      const contentHash = computeContentHash(text);

      // Parse JSON
      let payload: Record<string, any>;
      try {
        payload = JSON.parse(text);
      } catch (e) {
        return {
          success: false,
          resolver_url: resolverUrl,
          error: {
            code: 'INVALID_JSON',
            message: 'Resolver returned invalid JSON',
            retryable: false
          }
        };
      }

      return {
        success: true,
        payload,
        content_hash: contentHash,
        resolution_time_ms: resolutionTimeMs,
        resolver_url: resolverUrl
      };

    } catch (error: any) {
      const resolutionTimeMs = Date.now() - startTime;

      if (error.name === 'AbortError') {
        return {
          success: false,
          resolver_url: resolverUrl,
          resolution_time_ms: resolutionTimeMs,
          error: {
            code: 'TIMEOUT',
            message: `Request timed out after ${this.timeoutMs}ms`,
            retryable: true
          }
        };
      }

      return {
        success: false,
        resolver_url: resolverUrl,
        error: {
          code: 'FETCH_ERROR',
          message: error.message || 'Failed to fetch from resolver',
          retryable: true
        }
      };
    }
  }

  /**
   * Read response body with size limit enforcement
   */
  private async readResponseWithLimit(response: Response): Promise<string> {
    const reader = response.body?.getReader();
    if (!reader) {
      throw new Error('No response body');
    }

    const chunks: Uint8Array[] = [];
    let totalSize = 0;

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        totalSize += value.length;
        if (totalSize > this.maxResponseSize) {
          reader.cancel();
          throw new Error(`Response exceeded max size of ${this.maxResponseSize} bytes`);
        }

        chunks.push(value);
      }
    } finally {
      reader.releaseLock();
    }

    // Combine chunks and decode
    const combined = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunks) {
      combined.set(chunk, offset);
      offset += chunk.length;
    }

    return new TextDecoder().decode(combined);
  }

  /**
   * Store resolved metadata in the database
   */
  private async storeMetadata(
    iri: string,
    payload: Record<string, any>,
    contentHash: string,
    resolutionTimeMs: number,
    resolverUrl: string
  ): Promise<{ id: number }> {
    const payloadJson = JSON.stringify(payload);
    const payloadSize = Buffer.byteLength(payloadJson, 'utf8');

    const result = await this.pool.query(
      `INSERT INTO anchored_metadata_records
       (iri, resolver_url, content_hash, payload_size_bytes, payload_jsonb, http_status, resolution_time_ms)
       VALUES ($1, $2, $3, $4, $5, 200, $6)
       ON CONFLICT (iri) DO UPDATE SET
         resolver_url = EXCLUDED.resolver_url,
         resolved_at = NOW(),
         content_hash = EXCLUDED.content_hash,
         payload_size_bytes = EXCLUDED.payload_size_bytes,
         payload_jsonb = EXCLUDED.payload_jsonb,
         http_status = EXCLUDED.http_status,
         resolution_time_ms = EXCLUDED.resolution_time_ms,
         last_refresh_at = NOW(),
         refresh_count = anchored_metadata_records.refresh_count + 1
       RETURNING id`,
      [iri, resolverUrl, contentHash, payloadSize, payload, resolutionTimeMs]
    );

    return { id: result.rows[0].id };
  }

  /**
   * Get the record ID for an IRI (for derivation linking)
   */
  async getRecordIdByIri(iri: string): Promise<number | null> {
    const result = await this.pool.query(
      `SELECT id FROM anchored_metadata_records WHERE iri = $1`,
      [iri]
    );
    return result.rows.length > 0 ? result.rows[0].id : null;
  }

  /**
   * Extract value from payload using JSON pointer
   * Supports paths like '/regen:projectSize/qudt:numericValue'
   */
  extractValueByPointer(
    payload: Record<string, any>,
    pointer: string
  ): { value: any; found: boolean } {
    if (!pointer || !payload) {
      return { value: undefined, found: false };
    }

    // Parse JSON pointer (split by / and handle escapes)
    const parts = pointer.split('/').filter(p => p !== '');

    let current: any = payload;
    for (const part of parts) {
      // Handle JSON pointer escapes
      const unescaped = part.replace(/~1/g, '/').replace(/~0/g, '~');

      if (current === null || current === undefined) {
        return { value: undefined, found: false };
      }

      // Handle arrays
      if (Array.isArray(current)) {
        const index = parseInt(unescaped);
        if (isNaN(index) || index < 0 || index >= current.length) {
          return { value: undefined, found: false };
        }
        current = current[index];
      } else if (typeof current === 'object') {
        if (!(unescaped in current)) {
          return { value: undefined, found: false };
        }
        current = current[unescaped];
      } else {
        return { value: undefined, found: false };
      }
    }

    return { value: current, found: true };
  }
}

// =============================================================================
// Derivation Engine
// =============================================================================

export class MetadataDerivationEngine {
  private pool: Pool;
  private resolver: AnchoredMetadataResolver;

  constructor(pool: Pool, resolver: AnchoredMetadataResolver) {
    this.pool = pool;
    this.resolver = resolver;
  }

  /**
   * Get all active derivation rules from the allowlist
   */
  async getActiveDerivationRules(): Promise<Array<{
    metric_id: string;
    metric_label: string;
    json_pointer: string;
    unit_pointer: string | null;
    expected_unit: string | null;
    value_type: string;
    min_value: number | null;
    max_value: number | null;
  }>> {
    const result = await this.pool.query(
      `SELECT metric_id, metric_label, json_pointer, unit_pointer, expected_unit,
              value_type, min_value, max_value
       FROM derivation_allowlist
       WHERE is_active = TRUE`
    );
    return result.rows;
  }

  /**
   * Derive all metrics for a resolved metadata record
   * Only extracts metrics defined in the allowlist
   */
  async deriveMetrics(
    iri: string,
    payload: Record<string, any>
  ): Promise<DerivationResult[]> {
    const rules = await this.getActiveDerivationRules();
    const results: DerivationResult[] = [];

    // Get the record ID
    const recordId = await this.resolver.getRecordIdByIri(iri);
    if (!recordId) {
      console.warn(`[Derivation] No record found for IRI: ${iri}`);
      return results;
    }

    for (const rule of rules) {
      const derivation = await this.deriveMetric(recordId, payload, rule);
      if (derivation) {
        results.push(derivation);
      }
    }

    return results;
  }

  /**
   * Derive a single metric based on a rule
   */
  private async deriveMetric(
    recordId: number,
    payload: Record<string, any>,
    rule: {
      metric_id: string;
      metric_label: string;
      json_pointer: string;
      unit_pointer: string | null;
      expected_unit: string | null;
      value_type: string;
      min_value: number | null;
      max_value: number | null;
    }
  ): Promise<DerivationResult | null> {
    const validationErrors: string[] = [];

    // Extract the value
    const { value, found } = this.resolver.extractValueByPointer(payload, rule.json_pointer);

    if (!found) {
      // Value not found - not an error, just no derivation available
      return null;
    }

    // Extract unit if specified
    let unit: string | undefined;
    if (rule.unit_pointer) {
      const unitResult = this.resolver.extractValueByPointer(payload, rule.unit_pointer);
      if (unitResult.found) {
        unit = String(unitResult.value);
      }
    }

    // Validate unit if expected
    let isValid = true;
    if (rule.expected_unit && unit !== rule.expected_unit) {
      validationErrors.push(`Expected unit ${rule.expected_unit}, got ${unit || 'none'}`);
      isValid = false;
    }

    // Convert and validate value
    let numericValue: number | undefined;
    let stringValue: string | undefined;

    if (rule.value_type === 'numeric') {
      numericValue = parseFloat(String(value));
      if (isNaN(numericValue)) {
        validationErrors.push(`Value '${value}' is not a valid number`);
        isValid = false;
      } else {
        // Range validation
        if (rule.min_value !== null && numericValue < rule.min_value) {
          validationErrors.push(`Value ${numericValue} below minimum ${rule.min_value}`);
          isValid = false;
        }
        if (rule.max_value !== null && numericValue > rule.max_value) {
          validationErrors.push(`Value ${numericValue} above maximum ${rule.max_value}`);
          isValid = false;
        }
      }
    } else {
      stringValue = String(value);
    }

    // Store the derivation
    await this.storeDerivation(recordId, rule, numericValue, stringValue, unit, value, isValid, validationErrors);

    return {
      metric_id: rule.metric_id,
      metric_label: rule.metric_label,
      numeric_value: numericValue,
      string_value: stringValue,
      unit,
      json_pointer: rule.json_pointer,
      is_valid: isValid,
      validation_errors: validationErrors.length > 0 ? validationErrors : undefined
    };
  }

  /**
   * Store a derivation result in the database
   */
  private async storeDerivation(
    recordId: number,
    rule: { metric_id: string; json_pointer: string; unit_pointer: string | null },
    numericValue: number | undefined,
    stringValue: string | undefined,
    unit: string | undefined,
    rawValue: any,
    isValid: boolean,
    validationErrors: string[]
  ): Promise<void> {
    await this.pool.query(
      `INSERT INTO metadata_derivations
       (metadata_record_id, metric_id, json_pointer, numeric_value, string_value, unit, raw_value, unit_source, is_valid, validation_errors)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
       ON CONFLICT (metadata_record_id, metric_id) DO UPDATE SET
         json_pointer = EXCLUDED.json_pointer,
         numeric_value = EXCLUDED.numeric_value,
         string_value = EXCLUDED.string_value,
         unit = EXCLUDED.unit,
         raw_value = EXCLUDED.raw_value,
         unit_source = EXCLUDED.unit_source,
         is_valid = EXCLUDED.is_valid,
         validation_errors = EXCLUDED.validation_errors,
         derived_at = NOW()`,
      [
        recordId,
        rule.metric_id,
        rule.json_pointer,
        numericValue ?? null,
        stringValue ?? null,
        unit ?? null,
        String(rawValue),
        rule.unit_pointer,
        isValid,
        validationErrors.length > 0 ? JSON.stringify(validationErrors) : null
      ]
    );
  }

  /**
   * Get hectares for a given IRI (convenience method)
   * Returns null if not found or not valid
   */
  async getHectares(iri: string): Promise<{
    value: number;
    unit: string;
    iri: string;
    rid: string;
    resolver_url: string;
    content_hash: string;
    json_pointer: string;
  } | null> {
    const result = await this.pool.query(
      `SELECT
         d.numeric_value,
         d.unit,
         d.json_pointer,
         m.id as rid,
         m.iri,
         m.resolver_url,
         m.content_hash
       FROM metadata_derivations d
       JOIN anchored_metadata_records m ON d.metadata_record_id = m.id
       WHERE m.iri = $1
         AND d.metric_id = 'hectares'
         AND d.is_valid = TRUE`,
      [iri]
    );

    if (result.rows.length === 0) {
      return null;
    }

    const row = result.rows[0];
    return {
      value: parseFloat(row.numeric_value),
      unit: row.unit,
      iri: row.iri,
      rid: row.rid.toString(),
      resolver_url: row.resolver_url,
      content_hash: row.content_hash,
      json_pointer: row.json_pointer
    };
  }
}

// =============================================================================
// Export factory function
// =============================================================================

export function createAnchoredMetadataSystem(pool: Pool, options?: {
  timeoutMs?: number;
  maxResponseSize?: number;
}): {
  resolver: AnchoredMetadataResolver;
  derivationEngine: MetadataDerivationEngine;
} {
  const resolver = new AnchoredMetadataResolver(pool, options);
  const derivationEngine = new MetadataDerivationEngine(pool, resolver);
  return { resolver, derivationEngine };
}
