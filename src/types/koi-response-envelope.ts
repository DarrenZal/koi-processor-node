/**
 * KOI Canonical Response Envelope Types
 * Session D1: Standardized response format for all KOI endpoints
 *
 * Endpoints covered:
 * - /api/koi/query
 * - /api/koi/graph
 * - /api/koi/entity
 * - /api/koi/weekly-digest
 */

/**
 * Citation object for human-verifiable evidence
 * Used in KOI search results to provide source references
 */
export interface Citation {
  /** Unique Resource Identifier for the document */
  rid: string;
  /** Canonical URL for verification */
  url?: string;
  /** Document title if available */
  title?: string;
  /** Relevant excerpt/quote from the document */
  excerpt?: string;
  /** Whether the link has been verified (optional) */
  verified?: boolean;
  /** When the link was last checked (ISO 8601) */
  checked_at?: string;
}

/**
 * Error object with retryability information
 */
export interface KoiError {
  /** Error code for programmatic handling */
  code: string;
  /** Human-readable error message */
  message: string;
  /** Whether the error is transient and can be retried */
  retryable: boolean;
  /** Suggested retry delay in milliseconds (if retryable) */
  retry_after_ms?: number;
}

/**
 * Warning codes for common issues
 */
export type WarningCode =
  | 'pagination_not_exhausted'
  | 'partial_results'
  | 'fallback_used'
  | 'stale_data'
  | 'extraction_triggered'
  | 'privacy_filtered'
  | 'graph_context_unavailable';

/**
 * Tool trace entry for provenance tracking
 * Public-safe: no secrets, internal IPs, or raw user text
 */
export interface ToolTraceEntry {
  /** Tool/operation name */
  tool: string;
  /** Redacted/summarized params (e.g., "query_terms=3,limit=10") */
  params_summary: string;
  /** When the tool was called (ISO 8601) */
  timestamp: string;
  /** Source of data: 'on-chain' | 'koi-derived' | 'cached' */
  data_source: 'on-chain' | 'koi-derived' | 'cached' | 'graph';
  /** Execution time in milliseconds (optional) */
  duration_ms?: number;
}

export type PrimaryDataSource = 'koi-derived' | 'cached' | 'graph' | 'metadata';

/**
 * As-of metadata for data freshness
 * Split by source per the remediation plan
 */
export interface AsOfMetadata {
  /** KOI corpus/index metadata */
  koi: {
    /** Corpus version identifier (e.g., date string or commit hash) */
    corpus_version: string;
    /** When the corpus was last indexed */
    indexed_at: string;
  };
  /** On-chain metadata (optional, for ledger queries) */
  on_chain?: {
    /** Chain identifier */
    chain_id: string;
    /** Block height at query time */
    block_height: number;
    /** Block timestamp */
    block_time: string;
  };
  /** Resolved metadata IRI (optional, for off-chain claims) */
  metadata?: {
    /** Metadata IRI */
    iri: string;
    /** When the metadata was resolved */
    resolved_at: string;
  };
}

/**
 * Canonical response envelope for all KOI endpoints
 * @template T The type of the data payload
 */
export interface KoiResponseEnvelope<T = unknown> {
  /** The actual response data */
  data: T;

  /** UUID for request correlation and debugging */
  request_id: string;

  /** Primary data source for this response (high-level label) */
  data_source: PrimaryDataSource;

  /** Citations for verifiable evidence (KOI search results) */
  citations: Citation[];

  /** Warnings about data quality or partial results */
  warnings: WarningCode[];

  /** Errors encountered during processing */
  errors: KoiError[];

  /** Data freshness metadata */
  as_of: AsOfMetadata;

  /**
   * Public-safe tool trace for provenance
   * Empty array if no downstream calls were made
   */
  tool_trace: ToolTraceEntry[];
}

/**
 * Generate a UUID v4 request ID
 */
export function generateRequestId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[6] = (bytes[6] & 0x0f) | 0x40; // Version 4
  bytes[8] = (bytes[8] & 0x3f) | 0x80; // Variant 1
  const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`;
}

/**
 * Get current KOI as-of metadata
 * Uses environment variables for corpus versioning
 */
export function getKoiAsOfMetadata(): AsOfMetadata {
  const now = new Date().toISOString();
  return {
    koi: {
      // Use environment variable or date-based version
      corpus_version: process.env.KOI_CORPUS_VERSION || now.split('T')[0],
      indexed_at: process.env.KOI_LAST_INDEXED || now,
    }
  };
}

/**
 * Create a summarized params string from query parameters
 * Public-safe: only includes counts/lengths, no raw text
 */
export function summarizeParams(params: Record<string, unknown>): string {
  const parts: string[] = [];

  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null) continue;

    if (typeof value === 'string') {
      // For strings, only report length or term count
      if (key === 'query' || key === 'question') {
        const termCount = value.split(/\s+/).filter(Boolean).length;
        parts.push(`${key}_terms=${termCount}`);
      } else {
        parts.push(`${key}=${value.length > 20 ? `len=${value.length}` : value}`);
      }
    } else if (typeof value === 'number' || typeof value === 'boolean') {
      parts.push(`${key}=${value}`);
    } else if (Array.isArray(value)) {
      parts.push(`${key}_count=${value.length}`);
    }
  }

  return parts.join(',') || 'none';
}

/**
 * Extract citations from search results
 * Converts result items to Citation objects
 */
export function shouldExcludeKoiResultSource(rid?: string, url?: string): boolean {
  const safeRid = (rid || "").toLowerCase();
  const safeUrl = (url || "").toLowerCase();

  // Exclude derived crawl artifacts that can cause double-indexing and often become unreachable (404) later.
  // Example: koi-sensors discourse crawl dumps committed to GitHub under indexing/discourse/storage/*.json
  if (
    safeUrl.includes("github.com/gaiaaiagent/koi-sensors/blob/") &&
    safeUrl.includes("/indexing/") &&
    safeUrl.includes("/storage/") &&
    safeUrl.endsWith(".json")
  ) {
    return true;
  }

  if (safeRid.includes("_indexing_discourse_storage_") && safeRid.endsWith(".json")) {
    return true;
  }
  if (safeRid.includes("forum_crawl_") && safeRid.endsWith(".json")) {
    return true;
  }

  return false;
}

export function extractCitations(results: Array<{
  rid?: string;
  metadata?: { url?: string; title?: string };
  content?: string;
  score?: number;
}>): Citation[] {
  return results
    .filter(r => r.rid) // Only include results with RIDs
    .filter(r => !shouldExcludeKoiResultSource(r.rid, r.metadata?.url))
    .slice(0, 10) // Limit to top 10 citations
    .map(r => {
      const citation: Citation = {
        rid: r.rid!,
      };

      if (r.metadata?.url) {
        const trimmed = r.metadata.url.trim();
        if (trimmed) citation.url = trimmed;
      }

      if (r.metadata?.title) {
        citation.title = r.metadata.title;
      }

      // Extract first 200 chars as excerpt
      if (r.content && typeof r.content === 'string') {
        citation.excerpt = r.content.slice(0, 200) + (r.content.length > 200 ? '...' : '');
      }

      return citation;
    });
}

/**
 * Create an error response envelope
 */
export function createErrorEnvelope(
  requestId: string,
  error: KoiError
): KoiResponseEnvelope<null> {
  return {
    data: null,
    request_id: requestId,
    data_source: 'koi-derived',
    citations: [],
    warnings: [],
    errors: [error],
    as_of: getKoiAsOfMetadata(),
    tool_trace: [],
  };
}

/**
 * Create a success response envelope
 */
export function createSuccessEnvelope<T>(
  requestId: string,
  data: T,
  options: {
    data_source?: PrimaryDataSource;
    citations?: Citation[];
    warnings?: WarningCode[];
    tool_trace?: ToolTraceEntry[];
    as_of?: AsOfMetadata;
  } = {}
): KoiResponseEnvelope<T> {
  return {
    data,
    request_id: requestId,
    data_source: options.data_source || 'koi-derived',
    citations: options.citations || [],
    warnings: options.warnings || [],
    errors: [],
    as_of: options.as_of || getKoiAsOfMetadata(),
    tool_trace: options.tool_trace || [],
  };
}

/**
 * Middleware-style wrapper for Express endpoints
 * Automatically adds request_id and envelope structure
 */
export function wrapWithEnvelope<T>(
  handler: (req: any, res: any, requestId: string) => Promise<{
    data: T;
    citations?: Citation[];
    warnings?: WarningCode[];
    tool_trace?: ToolTraceEntry[];
    as_of?: AsOfMetadata;
    statusCode?: number;
  }>
) {
  return async (req: any, res: any) => {
    const requestId = generateRequestId();

    // Add request_id to response header for log correlation
    res.setHeader('X-Request-ID', requestId);

    try {
      const result = await handler(req, res, requestId);
      const envelope = createSuccessEnvelope(requestId, result.data, {
        citations: result.citations,
        warnings: result.warnings,
        tool_trace: result.tool_trace,
        as_of: result.as_of,
      });

      res.status(result.statusCode || 200).json(envelope);
    } catch (error) {
      const koiError: KoiError = {
        code: 'INTERNAL_ERROR',
        message: error instanceof Error ? error.message : 'Unknown error',
        retryable: false,
      };

      // Check if it's a retryable error
      if (error instanceof Error) {
        const msg = error.message.toLowerCase();
        if (msg.includes('timeout') || msg.includes('connection') || msg.includes('unavailable')) {
          koiError.retryable = true;
          koiError.retry_after_ms = 1000;
        }
      }

      const envelope = createErrorEnvelope(requestId, koiError);
      res.status(500).json(envelope);
    }
  };
}
