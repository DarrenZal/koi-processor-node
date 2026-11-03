/**
 * Anchored Metadata Module
 * Session E: Off-chain Metadata Resolver + KOI Caching + Derivations
 *
 * Exports:
 * - AnchoredMetadataResolver: Resolves Regen IRIs via allowlisted API
 * - MetadataDerivationEngine: Extracts metrics based on allowlist rules
 * - AnchoredMetadataIntegration: High-level integration with citations
 */

// Core resolver and derivation
export {
  AnchoredMetadataResolver,
  MetadataDerivationEngine,
  createAnchoredMetadataSystem,
  validateIri,
  computeContentHash,
  type ResolvedMetadata,
  type MetadataResolutionResult,
  type DerivationResult
} from './anchored-metadata-resolver.ts';

// Integration layer
export {
  AnchoredMetadataIntegration,
  createAnchoredMetadataIntegration,
  mergeAsOfMetadata,
  addMetadataCitations,
  type AnchoredMetric,
  type MetadataCitation,
  type MetricExtractionResult
} from './metadata-integration.ts';
