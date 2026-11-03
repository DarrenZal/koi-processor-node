#!/usr/bin/env bun
/**
 * Test Script: Anchored Metadata Resolver + Derivations
 * Validates the metadata resolution and derivation system
 *
 * Without --apply it prints what it would do and loads nothing: no database, no network.
 * --apply runs the suite: two offline tests, then it connects to POSTGRES_URL, resolves the
 * IRI through the Regen metadata resolver (network) and caches the result in
 * anchored_metadata_records, and with --iri or TEST_METADATA_IRI derives metrics into
 * metadata_derivations.
 *
 * Usage:
 *   bun run scripts/test-anchored-metadata.ts                                   # dry run
 *   bun run scripts/test-anchored-metadata.ts --apply [--iri <iri>] [--force-refresh]
 *
 * Environment:
 *   POSTGRES_URL - Database connection string (default: eliza on localhost:5433)
 */

import type { Pool } from 'pg';

// Loaded only with --apply (loadModules), so --help and the dry run import nothing.
let validateIri: any;
let computeContentHash: any;
let createAnchoredMetadataSystem: any;
let createAnchoredMetadataIntegration: any;
let PgPool: any;

async function loadModules(): Promise<void> {
  const resolver = await import('../src/metadata/anchored-metadata-resolver.ts');
  validateIri = resolver.validateIri;
  computeContentHash = resolver.computeContentHash;
  createAnchoredMetadataSystem = resolver.createAnchoredMetadataSystem;
  ({ createAnchoredMetadataIntegration } = await import('../src/metadata/metadata-integration.ts'));
  const pg: any = await import('pg');
  PgPool = pg.Pool ?? pg.default?.Pool;
}

const USAGE = `Usage: bun run scripts/test-anchored-metadata.ts [--apply] [--iri <iri>] [--force-refresh] [--dry-run]

  (default)        dry run: print the plan; load, read and write nothing
  --apply          run the suite against POSTGRES_URL and the Regen metadata resolver,
                   caching what it resolves in anchored_metadata_records
  --iri <iri>      IRI to resolve (TEST_METADATA_IRI overrides it)
  --force-refresh  re-fetch even when a cached record exists
  --dry-run        the default; wins over --apply`;

function maskUrl(url: string): string {
  return url.replace(/(:\/\/[^:/@]*):[^@]*@/, '$1:***@');
}

// =============================================================================
// Configuration
// =============================================================================

const POSTGRES_URL = process.env.POSTGRES_URL || 'postgresql://postgres:postgres@localhost:5433/eliza';

// Sample IRIs for testing (from Regen docs/examples)
// These are example IRIs - they may or may not exist on mainnet
const SAMPLE_IRIS = [
  'regen:13toVgEywwgJLpuVGZSmugHKEMPDL1HUVdaYEaLLnE6EJJwNnqVsG9i.rdf',
  'regen:113gdjFKcVCt13Za6vN7TtbgMM6LMSjRnu89BMCxeuHdkJ1hWUmy.rdf'
];

// =============================================================================
// Parse arguments
// =============================================================================

const args = process.argv.slice(2);
let customIri: string | null = null;
let forceRefresh = false;
let apply = false;
let dryRun = false;
let showHelp = false;
const envTestIri = process.env.TEST_METADATA_IRI || null;

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--iri' && args[i + 1]) {
    customIri = args[i + 1];
    i++;
  } else if (args[i] === '--force-refresh') {
    forceRefresh = true;
  } else if (args[i] === '--apply') {
    apply = true;
  } else if (args[i] === '--dry-run') {
    dryRun = true;
  } else if (args[i] === '--help' || args[i] === '-h') {
    showHelp = true;
  }
}

// =============================================================================
// Test Functions
// =============================================================================

async function testIriValidation(): Promise<{ passed: boolean; details: string[] }> {
  console.log('\n--- Test 1: IRI Validation ---\n');
  const details: string[] = [];
  let passed = true;

  const testCases = [
    // Valid IRIs
    { iri: 'regen:113gdjFKcVCt13Za6vN7TtbgMM6LMSjRnu89BMCxeuHdkJ1hWUmy.rdf', expected: true, desc: 'Valid graph IRI' },
    { iri: 'regen:13toVgEywwgJLpuVGZSmugHKEMPDL1HUVdaYEaLLnE6EJJwNnqVsG9i.rdf', expected: true, desc: 'Valid graph IRI 2' },

    // Invalid IRIs
    { iri: '', expected: false, desc: 'Empty string' },
    { iri: 'ipfs:QmTest', expected: false, desc: 'Wrong prefix (ipfs)' },
    { iri: 'http://example.com', expected: false, desc: 'HTTP URL instead of IRI' },
    { iri: 'regen:invalid!chars.rdf', expected: false, desc: 'Invalid characters' },
    { iri: 'regen:', expected: false, desc: 'Missing hash' },
    { iri: 'a'.repeat(300), expected: false, desc: 'Too long' },
  ];

  for (const tc of testCases) {
    const result = validateIri(tc.iri);
    const passed_tc = result.valid === tc.expected;

    if (!passed_tc) {
      passed = false;
    }

    const status = passed_tc ? '✓' : '✗';
    const detail = `${status} ${tc.desc}: valid=${result.valid} (expected=${tc.expected})${result.error ? ' - ' + result.error : ''}`;
    details.push(detail);
    console.log(detail);
  }

  return { passed, details };
}

async function testContentHash(): Promise<{ passed: boolean; details: string[] }> {
  console.log('\n--- Test 2: Content Hash ---\n');
  const details: string[] = [];

  const testContent = '{"@context": "https://schema.org", "name": "Test"}';
  const hash = computeContentHash(testContent);

  const isValidSha256 = /^[a-f0-9]{64}$/.test(hash);
  const status = isValidSha256 ? '✓' : '✗';
  const detail = `${status} SHA-256 hash: ${hash.slice(0, 16)}...`;
  details.push(detail);
  console.log(detail);

  // Same content should produce same hash
  const hash2 = computeContentHash(testContent);
  const sameHash = hash === hash2;
  const status2 = sameHash ? '✓' : '✗';
  const detail2 = `${status2} Deterministic hash: ${sameHash}`;
  details.push(detail2);
  console.log(detail2);

  // Different content should produce different hash
  const hash3 = computeContentHash(testContent + ' ');
  const diffHash = hash !== hash3;
  const status3 = diffHash ? '✓' : '✗';
  const detail3 = `${status3} Different content = different hash: ${diffHash}`;
  details.push(detail3);
  console.log(detail3);

  return { passed: isValidSha256 && sameHash && diffHash, details };
}

async function testDatabaseConnection(pool: Pool): Promise<{ passed: boolean; details: string[] }> {
  console.log('\n--- Test 3: Database Connection ---\n');
  const details: string[] = [];

  try {
    // Check connection
    const result = await pool.query('SELECT NOW() as now');
    const detail1 = `✓ Database connected: ${result.rows[0].now}`;
    details.push(detail1);
    console.log(detail1);

    // Check tables exist
    const tableCheck = await pool.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_name IN ('anchored_metadata_records', 'metadata_derivations', 'derivation_allowlist')
    `);

    const tables = tableCheck.rows.map(r => r.table_name);
    const hasTables = tables.length === 3;
    const status = hasTables ? '✓' : '✗';
    const detail2 = `${status} Required tables exist: ${tables.join(', ') || 'NONE'}`;
    details.push(detail2);
    console.log(detail2);

    if (!hasTables) {
      console.log('\n  ⚠ Run the migration first:');
      console.log('    psql $POSTGRES_URL -f migrations/026_anchored_metadata_records.sql\n');
      return { passed: false, details };
    }

    // Check allowlist seed
    const allowlistCheck = await pool.query(
      `SELECT metric_id FROM derivation_allowlist WHERE is_active = TRUE`
    );
    const activeMetrics = allowlistCheck.rows.map(r => r.metric_id);
    const detail3 = `✓ Active derivation metrics: ${activeMetrics.join(', ') || 'none'}`;
    details.push(detail3);
    console.log(detail3);

    return { passed: true, details };
  } catch (error: any) {
    const detail = `✗ Database error: ${error.message}`;
    details.push(detail);
    console.log(detail);
    return { passed: false, details };
  }
}

async function testResolver(pool: Pool, iri: string, forceRefresh: boolean): Promise<{ passed: boolean; details: string[] }> {
  console.log(`\n--- Test 4: Metadata Resolution ---\n`);
  console.log(`IRI: ${iri}`);
  console.log(`Force refresh: ${forceRefresh}\n`);

  const details: string[] = [];
  const { resolver, derivationEngine } = createAnchoredMetadataSystem(pool);

  try {
    const startTime = Date.now();
    const result = await resolver.resolveMetadataIri(iri, forceRefresh);
    const duration = Date.now() - startTime;

    if (!result.success) {
      const detail = `✗ Resolution failed: ${result.error?.code} - ${result.error?.message}`;
      details.push(detail);
      console.log(detail);
      console.log(`  Retryable: ${result.error?.retryable}`);

      // This is expected if the IRI doesn't exist on mainnet
      if (result.error?.code === 'HTTP_404') {
        console.log('\n  ℹ IRI not found on Regen resolver.');

        // If the user did NOT provide an explicit IRI, treat this as a skip (not a failure),
        // because sample IRIs may not exist on mainnet.
        const explicitIriProvided = !!(customIri || envTestIri);
        if (!explicitIriProvided) {
          console.log('  ℹ No explicit TEST_METADATA_IRI/--iri provided; skipping resolution-dependent tests.');
          details.push('✓ Skipped: no explicit TEST_METADATA_IRI/--iri provided and sample IRI returned 404');
          return { passed: true, details };
        }
      }

      return { passed: false, details };
    }

    const metadata = result.metadata!;
    details.push(`✓ Resolution successful in ${duration}ms`);
    console.log(`✓ Resolution successful in ${duration}ms`);

    details.push(`  From cache: ${metadata.from_cache}`);
    console.log(`  From cache: ${metadata.from_cache}`);

    details.push(`  Record ID: ${metadata.rid}`);
    console.log(`  Record ID: ${metadata.rid}`);

    details.push(`  Content hash: ${metadata.content_hash.slice(0, 16)}...`);
    console.log(`  Content hash: ${metadata.content_hash.slice(0, 16)}...`);

    details.push(`  Resolved at: ${metadata.resolved_at.toISOString()}`);
    console.log(`  Resolved at: ${metadata.resolved_at.toISOString()}`);

    // Show payload summary
    const payloadKeys = Object.keys(metadata.payload);
    details.push(`  Payload keys: ${payloadKeys.slice(0, 5).join(', ')}${payloadKeys.length > 5 ? '...' : ''}`);
    console.log(`  Payload keys: ${payloadKeys.slice(0, 5).join(', ')}${payloadKeys.length > 5 ? '...' : ''}`);

    return { passed: true, details };
  } catch (error: any) {
    const detail = `✗ Unexpected error: ${error.message}`;
    details.push(detail);
    console.log(detail);
    return { passed: false, details };
  }
}

async function testDerivation(pool: Pool, iri: string): Promise<{ passed: boolean; details: string[] }> {
  console.log(`\n--- Test 5: Metric Derivation ---\n`);
  const details: string[] = [];

  const integration = createAnchoredMetadataIntegration(pool);

  try {
    const result = await integration.extractHectaresWithCitation(iri);

    if (!result.success) {
      const detail = `✗ Derivation failed: ${result.error?.code} - ${result.error?.message}`;
      details.push(detail);
      console.log(detail);

      if (result.error?.blocked) {
        console.log('  ⚠ Metric BLOCKED (no citation, no metric)');
      }

      // Check if it's because the IRI wasn't resolved
      const recordExists = await pool.query(
        `SELECT 1 FROM anchored_metadata_records WHERE iri = $1`,
        [iri]
      );
      if (recordExists.rows.length === 0) {
        console.log('  ℹ No cached record found - run with a valid IRI first');
      }

      return { passed: false, details };
    }

    const metric = result.metric!;
    const citation = result.citation!;

    details.push(`✓ Derivation successful`);
    console.log(`✓ Derivation successful`);

    details.push(`  Metric: ${metric.metric_label}`);
    console.log(`  Metric: ${metric.metric_label}`);

    details.push(`  Value: ${metric.value} ${metric.unit}`);
    console.log(`  Value: ${metric.value} ${metric.unit}`);

    details.push(`  Citation RID: ${citation.rid}`);
    console.log(`  Citation RID: ${citation.rid}`);

    details.push(`  Content hash: ${citation.content_hash.slice(0, 16)}...`);
    console.log(`  Content hash: ${citation.content_hash.slice(0, 16)}...`);

    details.push(`  JSON pointer: ${citation.json_pointer}`);
    console.log(`  JSON pointer: ${citation.json_pointer}`);

    details.push(`  Resolver URL: ${citation.resolver_url.slice(0, 50)}...`);
    console.log(`  Resolver URL: ${citation.resolver_url.slice(0, 50)}...`);

    // Show as_of metadata
    if (result.as_of_metadata) {
      details.push(`  as_of.metadata.iri: ${result.as_of_metadata.iri.slice(0, 30)}...`);
      console.log(`  as_of.metadata.iri: ${result.as_of_metadata.iri.slice(0, 30)}...`);
      details.push(`  as_of.metadata.resolved_at: ${result.as_of_metadata.resolved_at}`);
      console.log(`  as_of.metadata.resolved_at: ${result.as_of_metadata.resolved_at}`);
    }

    return { passed: true, details };
  } catch (error: any) {
    const detail = `✗ Unexpected error: ${error.message}`;
    details.push(detail);
    console.log(detail);
    return { passed: false, details };
  }
}

async function testCaching(pool: Pool, iri: string): Promise<{ passed: boolean; details: string[] }> {
  console.log(`\n--- Test 6: Cache Behavior ---\n`);
  const details: string[] = [];
  const { resolver } = createAnchoredMetadataSystem(pool);

  try {
    // First request
    const start1 = Date.now();
    const result1 = await resolver.resolveMetadataIri(iri, false);
    const duration1 = Date.now() - start1;

    if (!result1.success) {
      details.push(`✗ First request failed (skipping cache test)`);
      console.log(`✗ First request failed (skipping cache test)`);
      return { passed: false, details };
    }

    // Second request (should hit cache)
    const start2 = Date.now();
    const result2 = await resolver.resolveMetadataIri(iri, false);
    const duration2 = Date.now() - start2;

    const detail1 = `First request: ${duration1}ms (from_cache: ${result1.metadata!.from_cache})`;
    details.push(detail1);
    console.log(detail1);

    const detail2 = `Second request: ${duration2}ms (from_cache: ${result2.metadata!.from_cache})`;
    details.push(detail2);
    console.log(detail2);

    // Cache hit should be faster
    const cacheHit = result2.metadata!.from_cache;
    const fasterWithCache = duration2 <= duration1;

    const status = cacheHit && fasterWithCache ? '✓' : '⚠';
    const detail3 = `${status} Cache behavior: ${cacheHit ? 'HIT' : 'MISS'}`;
    details.push(detail3);
    console.log(detail3);

    // Content should be the same
    const sameContent = result1.metadata!.content_hash === result2.metadata!.content_hash;
    const status2 = sameContent ? '✓' : '✗';
    const detail4 = `${status2} Same content hash: ${sameContent}`;
    details.push(detail4);
    console.log(detail4);

    return { passed: cacheHit && sameContent, details };
  } catch (error: any) {
    const detail = `✗ Unexpected error: ${error.message}`;
    details.push(detail);
    console.log(detail);
    return { passed: false, details };
  }
}

async function showStats(pool: Pool): Promise<void> {
  console.log(`\n--- Statistics ---\n`);

  const integration = createAnchoredMetadataIntegration(pool);
  const stats = await integration.getAnchoredMetadataStats();

  console.log(`Total cached records: ${stats.total_records}`);
  console.log(`Successful resolutions: ${stats.successful_resolutions}`);
  console.log(`Hectares derivations: ${stats.valid_hectares_derivations} valid / ${stats.total_hectares_derivations} total`);
  if (stats.total_hectares_sum !== null) {
    console.log(`Total hectares sum: ${stats.total_hectares_sum.toLocaleString()} ha`);
  }
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  if (showHelp) {
    console.log(USAGE);
    process.exit(0);
  }
  if (!apply || dryRun) {
    console.log('DRY RUN: nothing is loaded, read or written. Pass --apply to run the suite.');
    console.log(`  Database: ${maskUrl(POSTGRES_URL)}`);
    console.log(`  IRI: ${envTestIri || customIri || SAMPLE_IRIS[0]}${forceRefresh ? ' (force refresh)' : ''}`);
    console.log('  Would run: IRI validation and content hash (offline); then connect to the database,');
    console.log('  resolve the IRI through the Regen metadata resolver and cache it in');
    console.log('  anchored_metadata_records; with --iri or TEST_METADATA_IRI, also derive metrics');
    console.log('  into metadata_derivations');
    process.exit(0);
  }
  await loadModules();

  console.log('='.repeat(60));
  console.log('Anchored Metadata Resolver Test Suite');
  console.log('Session E: Off-chain Metadata Pipeline');
  console.log('='.repeat(60));
  console.log(`Database: ${maskUrl(POSTGRES_URL)}`);

  // Parse postgres URL
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

  const pool = new PgPool(parsePostgresUrl(POSTGRES_URL));

  const results: Array<{ name: string; passed: boolean }> = [];

  // Test 1: IRI Validation
  const t1 = await testIriValidation();
  results.push({ name: 'IRI Validation', passed: t1.passed });

  // Test 2: Content Hash
  const t2 = await testContentHash();
  results.push({ name: 'Content Hash', passed: t2.passed });

  // Test 3: Database Connection
  const t3 = await testDatabaseConnection(pool);
  results.push({ name: 'Database Connection', passed: t3.passed });

  // Only continue with resolution tests if DB is ready
  if (t3.passed) {
    const testIri = envTestIri || customIri || SAMPLE_IRIS[0];

    // Test 4: Resolution
    const t4 = await testResolver(pool, testIri, forceRefresh);
    results.push({ name: 'Metadata Resolution', passed: t4.passed });

    // Only test derivation if resolution succeeded (not just "skipped")
    if (t4.passed && (envTestIri || customIri)) {
      // Test 5: Derivation
      const t5 = await testDerivation(pool, testIri);
      results.push({ name: 'Metric Derivation', passed: t5.passed });

      // Test 6: Caching
      const t6 = await testCaching(pool, testIri);
      results.push({ name: 'Cache Behavior', passed: t6.passed });
    }

    // Show stats
    await showStats(pool);
  }

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('Test Summary');
  console.log('='.repeat(60) + '\n');

  let allPassed = true;
  for (const r of results) {
    const status = r.passed ? '✓ PASS' : '✗ FAIL';
    console.log(`  ${status}: ${r.name}`);
    if (!r.passed) allPassed = false;
  }

  console.log('\n' + '-'.repeat(60));
  console.log(`Overall: ${allPassed ? '✓ ALL TESTS PASSED' : '✗ SOME TESTS FAILED'}`);
  console.log('-'.repeat(60) + '\n');

  await pool.end();
  process.exit(allPassed ? 0 : 1);
}

if (import.meta.main) {
  main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
