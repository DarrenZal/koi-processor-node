/**
 * Metadata Integration Layer
 * Session E: Connects anchored metadata resolver to KOI response system
 *
 * Provides:
 * - Citation generation for off-chain metrics
 * - "No citation, no metric" enforcement
 * - as_of.metadata enrichment
 */

import { Pool } from 'pg';
import {
  AnchoredMetadataResolver,
  MetadataDerivationEngine,
  createAnchoredMetadataSystem,
  validateIri,
  type ResolvedMetadata,
  type DerivationResult
} from './anchored-metadata-resolver.ts';
import type { Citation, AsOfMetadata } from '../types/koi-response-envelope.ts';

// =============================================================================
// Types
// =============================================================================

export interface AnchoredMetric {
  metric_id: string;
  metric_label: string;
  value: number;
  unit: string;
  citation: MetadataCitation;
}

export interface MetadataCitation extends Citation {
  /** Content hash for integrity verification */
  content_hash: string;
  /** JSON pointer path to the derived value */
  json_pointer: string;
  /** The resolver URL used */
  resolver_url: string;
  /** When the metadata was resolved */
  resolved_at: string;
  /** Type of citation */
  citation_type: 'anchored-metadata';
}

export interface MetricExtractionResult {
  success: boolean;
  metric?: AnchoredMetric;
  citation?: MetadataCitation;
  as_of_metadata?: AsOfMetadata['metadata'];
  error?: {
    code: string;
    message: string;
    blocked: boolean; // true = metric should not be reported
  };
}

// =============================================================================
// Integration Class
// =============================================================================

export class AnchoredMetadataIntegration {
  private pool: Pool;
  private resolver: AnchoredMetadataResolver;
  private derivationEngine: MetadataDerivationEngine;

  constructor(pool: Pool) {
    this.pool = pool;
    const { resolver, derivationEngine } = createAnchoredMetadataSystem(pool);
    this.resolver = resolver;
    this.derivationEngine = derivationEngine;
  }

  /**
   * Extract hectares from a metadata IRI with full citation
   *
   * Enforces "no citation, no metric" policy:
   * - If IRI is invalid, returns blocked error
   * - If resolution fails, returns blocked error
   * - If derivation fails, returns blocked error
   * - Only returns metric when fully citeable
   *
   * @param iri - The Regen metadata IRI
   * @param forceRefresh - Whether to bypass cache
   */
  async extractHectaresWithCitation(
    iri: string,
    forceRefresh: boolean = false
  ): Promise<MetricExtractionResult> {
    // Step 1: Validate IRI
    const validation = validateIri(iri);
    if (!validation.valid) {
      return {
        success: false,
        error: {
          code: 'INVALID_IRI',
          message: validation.error || 'Invalid metadata IRI',
          blocked: true
        }
      };
    }

    // Step 2: Resolve metadata
    const resolution = await this.resolver.resolveMetadataIri(iri, forceRefresh);
    if (!resolution.success || !resolution.metadata) {
      return {
        success: false,
        error: {
          code: resolution.error?.code || 'RESOLUTION_FAILED',
          message: resolution.error?.message || 'Failed to resolve metadata IRI',
          blocked: true
        }
      };
    }

    const metadata = resolution.metadata;

    // Step 3: Derive metrics
    const derivations = await this.derivationEngine.deriveMetrics(iri, metadata.payload);

    // Step 4: Find hectares derivation
    const hectaresDerivation = derivations.find(
      d => d.metric_id === 'hectares' && d.is_valid && d.numeric_value !== undefined
    );

    if (!hectaresDerivation) {
      return {
        success: false,
        error: {
          code: 'DERIVATION_UNAVAILABLE',
          message: 'Hectares derivation not available or invalid for this metadata',
          blocked: true
        }
      };
    }

    // Step 5: Build citation
    const citation: MetadataCitation = {
      rid: metadata.rid,
      url: metadata.resolver_url,
      title: `Regen Metadata: ${iri.slice(0, 20)}...`,
      excerpt: `Project size: ${hectaresDerivation.numeric_value} ${hectaresDerivation.unit || 'hectares'}`,
      content_hash: metadata.content_hash,
      json_pointer: hectaresDerivation.json_pointer,
      resolver_url: metadata.resolver_url,
      resolved_at: metadata.resolved_at.toISOString(),
      citation_type: 'anchored-metadata'
    };

    // Step 6: Build metric
    const metric: AnchoredMetric = {
      metric_id: hectaresDerivation.metric_id,
      metric_label: hectaresDerivation.metric_label,
      value: hectaresDerivation.numeric_value!,
      unit: hectaresDerivation.unit || 'ha',
      citation
    };

    return {
      success: true,
      metric,
      citation,
      as_of_metadata: {
        iri,
        resolved_at: metadata.resolved_at.toISOString()
      }
    };
  }

  /**
   * Get anchored metrics for multiple IRIs
   * Returns only those with valid citations (enforces "no citation, no metric")
   */
  async getAnchoredMetricsForIris(
    iris: string[]
  ): Promise<{
    metrics: AnchoredMetric[];
    citations: MetadataCitation[];
    blocked: Array<{ iri: string; reason: string }>;
  }> {
    const metrics: AnchoredMetric[] = [];
    const citations: MetadataCitation[] = [];
    const blocked: Array<{ iri: string; reason: string }> = [];

    for (const iri of iris) {
      const result = await this.extractHectaresWithCitation(iri);

      if (result.success && result.metric && result.citation) {
        metrics.push(result.metric);
        citations.push(result.citation);
      } else if (result.error) {
        blocked.push({
          iri,
          reason: result.error.message
        });
      }
    }

    return { metrics, citations, blocked };
  }

  /**
   * Check if a metric is available for an IRI (without fetching)
   */
  async hasAnchoredHectares(iri: string): Promise<boolean> {
    const hectares = await this.derivationEngine.getHectares(iri);
    return hectares !== null;
  }

  /**
   * Get summary statistics for anchored metadata
   */
  async getAnchoredMetadataStats(): Promise<{
    total_records: number;
    successful_resolutions: number;
    total_hectares_derivations: number;
    valid_hectares_derivations: number;
    total_hectares_sum: number | null;
  }> {
    const result = await this.pool.query(`
      SELECT
        (SELECT COUNT(*) FROM anchored_metadata_records) as total_records,
        (SELECT COUNT(*) FROM anchored_metadata_records WHERE http_status = 200) as successful_resolutions,
        (SELECT COUNT(*) FROM metadata_derivations WHERE metric_id = 'hectares') as total_hectares_derivations,
        (SELECT COUNT(*) FROM metadata_derivations WHERE metric_id = 'hectares' AND is_valid = TRUE) as valid_hectares_derivations,
        (SELECT SUM(numeric_value) FROM metadata_derivations WHERE metric_id = 'hectares' AND is_valid = TRUE) as total_hectares_sum
    `);

    const row = result.rows[0];
    return {
      total_records: parseInt(row.total_records) || 0,
      successful_resolutions: parseInt(row.successful_resolutions) || 0,
      total_hectares_derivations: parseInt(row.total_hectares_derivations) || 0,
      valid_hectares_derivations: parseInt(row.valid_hectares_derivations) || 0,
      total_hectares_sum: row.total_hectares_sum ? parseFloat(row.total_hectares_sum) : null
    };
  }

  /**
   * Resolve a batch of IRIs (with rate limiting)
   * Used for scheduled ingestion
   */
  async resolveBatch(
    iris: string[],
    options?: {
      delayMs?: number;
      forceRefresh?: boolean;
    }
  ): Promise<{
    resolved: number;
    failed: number;
    errors: Array<{ iri: string; error: string }>;
  }> {
    const delayMs = options?.delayMs ?? 500; // Default 500ms between requests
    const forceRefresh = options?.forceRefresh ?? false;

    let resolved = 0;
    let failed = 0;
    const errors: Array<{ iri: string; error: string }> = [];

    for (const iri of iris) {
      const result = await this.resolver.resolveMetadataIri(iri, forceRefresh);

      if (result.success) {
        // Also derive metrics
        await this.derivationEngine.deriveMetrics(iri, result.metadata!.payload);
        resolved++;
      } else {
        failed++;
        errors.push({
          iri,
          error: result.error?.message || 'Unknown error'
        });
      }

      // Rate limit
      if (iris.indexOf(iri) < iris.length - 1) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }

    return { resolved, failed, errors };
  }
}

// =============================================================================
// Factory function
// =============================================================================

export function createAnchoredMetadataIntegration(pool: Pool): AnchoredMetadataIntegration {
  return new AnchoredMetadataIntegration(pool);
}

// =============================================================================
// Helper: Merge anchored metadata into response envelope
// =============================================================================

/**
 * Merge anchored metadata citation into existing as_of metadata
 */
export function mergeAsOfMetadata(
  existing: AsOfMetadata,
  metadataIri: string,
  resolvedAt: string
): AsOfMetadata {
  return {
    ...existing,
    metadata: {
      iri: metadataIri,
      resolved_at: resolvedAt
    }
  };
}

/**
 * Add anchored metadata citations to existing citations array
 */
export function addMetadataCitations(
  existingCitations: Citation[],
  metadataCitations: MetadataCitation[]
): Citation[] {
  // Dedupe by rid
  const existingRids = new Set(existingCitations.map(c => c.rid));
  const newCitations = metadataCitations.filter(c => !existingRids.has(c.rid));
  return [...existingCitations, ...newCitations];
}
