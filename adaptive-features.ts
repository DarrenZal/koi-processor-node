/**
 * Adaptive Knowledge Features for MCP Server
 * Implements RRF, confidence monitoring, and query logging
 */

import { Pool } from "pg";

/**
 * Normalize rid by stripping chunk suffix for fusion merging
 * Keyword search returns base docs (UUID), entity search returns chunks (UUID#chunk14)
 * This ensures they merge correctly in fusion
 */
export function normalizeRidForFusion(rid: string): string {
  // Strip #chunk\d+ or #chunkN suffix
  return rid.replace(/#chunk\d+$/, '');
}

// Interfaces
interface SearchResult {
  id: string;
  content: string;
  similarity?: number;
  score?: number;
  source: 'vector' | 'sparql' | 'keyword' | 'hybrid';
  metadata?: any;
  rid?: string;
}

interface QueryLogEntry {
  query_text: string;
  query_embedding?: number[];
  user_id?: string;
  agent_id?: string;
  confidence_score?: number;
  triggered_extraction?: boolean;
  extraction_receipt_rid?: string;
  response_time_ms?: number;
  results?: SearchResult[];
}

/**
 * Weighted Average Fusion Implementation
 * Combines vector, entity/graph, and keyword search using weighted averaging
 * Provides better score discrimination than RRF for hybrid search
 *
 * Weights: 0.6 vector + 0.2 entity/graph + 0.2 keyword
 * Entity/graph results get a boost to surface documents with matching entities
 */
export function weightedAverageFusion(
  vectorResults: SearchResult[],
  entityResults: SearchResult[],
  keywordResults?: SearchResult[]
): SearchResult[] {
  const VECTOR_WEIGHT = 0.6;
  const ENTITY_WEIGHT = 0.2;
  const KEYWORD_WEIGHT = 0.2;
  const ENTITY_BOOST = 0.15;  // Bonus for entity matches

  // Merge results by document ID
  const merged = new Map<string, {
    vectorScore: number;
    entityScore: number;
    keywordScore: number;
    hasEntityMatch: boolean;
    result: SearchResult;
    entitiesMatched?: string[];
  }>();

  // Process vector results
  // Use normalized rid (strip chunk suffix) for merge key so base docs and chunks combine
  vectorResults.forEach(result => {
    const rawId = result.rid || result.id;
    const mergeKey = normalizeRidForFusion(rawId);
    merged.set(mergeKey, {
      vectorScore: result.similarity || result.score || 0,
      entityScore: 0,
      keywordScore: 0,
      hasEntityMatch: false,
      result
    });
  });

  // Process entity/graph results (from koi_entity_chunk_links)
  // Entity search returns chunks (UUID#chunk14), normalize to match base docs
  entityResults?.forEach(result => {
    const rawId = result.rid || result.id;
    const mergeKey = normalizeRidForFusion(rawId);
    const existing = merged.get(mergeKey);
    const entityScore = result.similarity || result.score || 0;

    if (existing) {
      existing.entityScore = entityScore;
      existing.hasEntityMatch = true;
      existing.entitiesMatched = result.metadata?.entities_matched;
    } else {
      // Document only in entity results
      merged.set(mergeKey, {
        vectorScore: 0,
        entityScore,
        keywordScore: 0,
        hasEntityMatch: true,
        result,
        entitiesMatched: result.metadata?.entities_matched
      });
    }
  });

  // Process keyword results
  // Keyword search may return base docs or chunks, normalize for consistent merging
  let keywordMergeCount = 0;
  keywordResults?.forEach(result => {
    const rawId = result.rid || result.id;
    const mergeKey = normalizeRidForFusion(rawId);
    const existing = merged.get(mergeKey);
    const keywordScore = result.similarity || result.score || 0;

    if (existing) {
      existing.keywordScore = keywordScore;
      keywordMergeCount++;
    } else {
      // Document only in keyword results
      merged.set(mergeKey, {
        vectorScore: 0,
        entityScore: 0,
        keywordScore,
        hasEntityMatch: false,
        result
      });
    }
  });

  // Log keyword merge stats (gated to avoid PII/log volume in production)
  if (process.env.DEBUG_FUSION) {
    console.log(`[Fusion] Keyword results: ${keywordResults?.length || 0}, merged with existing: ${keywordMergeCount}`);

    // Log sample merge keys for debugging (if few merges relative to inputs)
    if (keywordResults && keywordResults.length > 3 && keywordMergeCount < keywordResults.length / 2) {
      const sampleKeyword = keywordResults.slice(0, 3).map(r => {
        const raw = r.rid || r.id;
        return { raw: raw.substring(0, 60), normalized: normalizeRidForFusion(raw).substring(0, 60) };
      });
      const existingKeys = Array.from(merged.keys()).slice(0, 5).map(k => k.substring(0, 60));
      console.log(`[Fusion] Sample keyword keys:`, JSON.stringify(sampleKeyword));
      console.log(`[Fusion] Sample existing keys:`, JSON.stringify(existingKeys));
    }
  }

  // Calculate weighted average scores with entity boost
  const allResults = Array.from(merged.entries())
    .map(([id, data]) => {
      let score = (data.vectorScore * VECTOR_WEIGHT) +
                  (data.entityScore * ENTITY_WEIGHT) +
                  (data.keywordScore * KEYWORD_WEIGHT);

      // Apply entity boost for documents that match query entities
      if (data.hasEntityMatch) {
        score += ENTITY_BOOST;
      }

      return {
        ...data.result,
        id,
        score,
        source: 'hybrid' as const,
        metadata: {
          ...data.result.metadata,
          base_rid: id,  // Normalized RID (chunk suffix stripped) used for merging
          vector_score: data.vectorScore,
          entity_score: data.entityScore,
          keyword_score: data.keywordScore,
          entity_boost: data.hasEntityMatch ? ENTITY_BOOST : 0,
          entities_matched: data.entitiesMatched,
          weighted_score: score
        }
      };
    });

  // Sort all results by score (entity boost already applied to overlapping results)
  // Vector determines relevance, entity provides boost - no artificial interleaving
  const fusedResults = allResults.sort((a, b) => (b.score || 0) - (a.score || 0));

  // Log fusion stats for debugging
  const entityBoosted = fusedResults.filter(r => r.metadata?.entity_boost > 0).length;
  const entityOnly = fusedResults.filter(r => r.metadata?.entity_score > 0 && r.metadata?.vector_score === 0).length;
  const keywordMatched = fusedResults.filter(r => r.metadata?.keyword_score > 0).length;

  if (process.env.DEBUG_FUSION) {
    console.log(`[Fusion] Input: ${vectorResults.length} vector, ${entityResults?.length || 0} entity, ${keywordResults?.length || 0} keyword`);
    console.log(`[Fusion] Merged into ${fusedResults.length} unique documents (after rid normalization)`);
    console.log(`[Fusion] ${keywordMatched} results have keyword_score > 0`);
    console.log(`[Fusion] ${entityBoosted} results have entity boost applied`);
    if (fusedResults.length > 0) {
      const sample = fusedResults.slice(0, 3).map(r => ({
        rid: r.id.substring(0, 50),
        vector: r.metadata?.vector_score?.toFixed(3),
        entity: r.metadata?.entity_score?.toFixed(3),
        keyword: r.metadata?.keyword_score?.toFixed(3)
      }));
      console.log('[Fusion] Top 3 sample:', JSON.stringify(sample));
    }
  }

  return fusedResults;
}

/**
 * Legacy RRF function - kept for backwards compatibility
 * Use weightedAverageFusion for better score discrimination
 */
export function reciprocalRankFusion(
  vectorResults: SearchResult[],
  sparqlResults: SearchResult[],
  keywordResults?: SearchResult[]
): SearchResult[] {
  // Redirect to weighted average fusion
  return weightedAverageFusion(vectorResults, sparqlResults, keywordResults);
}

/**
 * Calculate confidence score for retrieval results
 * Used to trigger adaptive extraction when confidence is low
 */
export function calculateConfidence(results: SearchResult[]): number {
  if (!results || results.length === 0) return 0;

  // Multiple factors for confidence calculation
  const factors = {
    // Top result similarity score
    topScore: results[0]?.similarity || results[0]?.score || 0,
    
    // Gap between top results (larger gap = higher confidence)
    scoreGap: results.length > 1 
      ? (results[0]?.similarity || results[0]?.score || 0) - 
        (results[1]?.similarity || results[1]?.score || 0)
      : 0.5,
    
    // Number of results (normalized)
    resultCount: Math.min(results.length / 10, 1),
    
    // Average score of top 5 results
    averageScore: results
      .slice(0, 5)
      .reduce((sum, r) => sum + (r.similarity || r.score || 0), 0) / 
      Math.min(5, results.length),
    
    // Score distribution variance (lower variance = higher confidence)
    scoreVariance: calculateScoreVariance(results.slice(0, 10))
  };

  // Weighted confidence score
  const confidence = 
    factors.topScore * 0.35 +
    factors.scoreGap * 0.20 +
    factors.resultCount * 0.15 +
    factors.averageScore * 0.20 +
    (1 - factors.scoreVariance) * 0.10;

  return Math.min(1, Math.max(0, confidence));
}

/**
 * Calculate score variance for confidence assessment
 */
function calculateScoreVariance(results: SearchResult[]): number {
  if (results.length < 2) return 0;
  
  const scores = results.map(r => r.similarity || r.score || 0);
  const mean = scores.reduce((a, b) => a + b, 0) / scores.length;
  const variance = scores.reduce((sum, score) => 
    sum + Math.pow(score - mean, 2), 0) / scores.length;
  
  // Normalize to 0-1 range
  return Math.min(1, variance);
}

/**
 * Log query for analysis and learning
 */
export async function logQuery(
  pool: Pool,
  entry: QueryLogEntry
): Promise<void> {
  try {
    const query = `
      INSERT INTO koi_query_log (
        query_text,
        query_embedding,
        user_id,
        agent_id,
        confidence_score,
        triggered_extraction,
        extraction_receipt_rid,
        response_time_ms
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id
    `;

    const embedding = entry.query_embedding 
      ? `[${entry.query_embedding.join(',')}]` 
      : null;

    await pool.query(query, [
      entry.query_text,
      embedding,
      entry.user_id || null,
      entry.agent_id || null,
      entry.confidence_score || null,
      entry.triggered_extraction || false,
      entry.extraction_receipt_rid || null,
      entry.response_time_ms || null
    ]);
  } catch (error) {
    console.error("[Query Log] Failed to log query:", error);
    // Don't throw - logging should not break the main flow
  }
}

/**
 * Get query patterns for active learning
 */
export async function getQueryPatterns(
  pool: Pool,
  minConfidence: number = 0.7
): Promise<any[]> {
  try {
    const query = `
      SELECT 
        query_text,
        COUNT(*) as frequency,
        AVG(confidence_score) as avg_confidence,
        COUNT(CASE WHEN triggered_extraction THEN 1 END) as extraction_count,
        MIN(confidence_score) as min_confidence,
        MAX(confidence_score) as max_confidence
      FROM koi_query_log
      WHERE timestamp > NOW() - INTERVAL '7 days'
      GROUP BY query_text
      HAVING AVG(confidence_score) < $1
      ORDER BY frequency DESC, avg_confidence ASC
      LIMIT 100
    `;

    const result = await pool.query(query, [minConfidence]);
    return result.rows;
  } catch (error) {
    console.error("[Query Patterns] Failed to get patterns:", error);
    return [];
  }
}

/**
 * Check if extraction should be triggered based on confidence
 */
export function shouldTriggerExtraction(
  confidence: number,
  threshold: number = 0.7
): boolean {
  return confidence < threshold;
}

/**
 * Calculate IDDS score for active learning document selection
 * Balances informativeness (representativeness) and diversity
 */
export function calculateIDDSScore(
  doc: SearchResult,
  unlabeledPool: SearchResult[],
  selectedPool: SearchResult[],
  alpha: number = 0.5
): number {
  // Informativeness: How similar to other unlabeled docs (representative)
  const informativeness = unlabeledPool
    .filter(d => d.id !== doc.id)
    .reduce((sum, other) => {
      const similarity = cosineSimilarity(
        doc.metadata?.embedding || [],
        other.metadata?.embedding || []
      );
      return sum + similarity;
    }, 0) / Math.max(1, unlabeledPool.length - 1);

  // Diversity: How different from already processed docs (novel)
  let diversity = 1.0;
  if (selectedPool.length > 0) {
    const maxSimilarity = selectedPool.reduce((max, selected) => {
      const similarity = cosineSimilarity(
        doc.metadata?.embedding || [],
        selected.metadata?.embedding || []
      );
      return Math.max(max, similarity);
    }, 0);
    diversity = 1 - maxSimilarity;
  }

  // Combined score with balance parameter
  return alpha * informativeness + (1 - alpha) * diversity;
}

/**
 * Select documents for extraction using active learning
 */
export function selectDocumentsForExtraction(
  retrievedDocs: SearchResult[],
  budget: number = 5,
  alpha: number = 0.5
): SearchResult[] {
  const selected: SearchResult[] = [];
  const unlabeled = [...retrievedDocs];

  for (let i = 0; i < Math.min(budget, retrievedDocs.length); i++) {
    // Calculate IDDS for remaining documents
    const scores = unlabeled.map(doc => ({
      doc,
      score: calculateIDDSScore(doc, unlabeled, selected, alpha)
    }));

    // Select highest scoring document
    const best = scores.reduce((max, current) => 
      current.score > max.score ? current : max
    );

    selected.push(best.doc);
    const index = unlabeled.indexOf(best.doc);
    if (index > -1) {
      unlabeled.splice(index, 1);
    }
  }

  return selected;
}

/**
 * Simple cosine similarity for embeddings
 */
function cosineSimilarity(a: number[], b: number[]): number {
  if (!a.length || !b.length || a.length !== b.length) return 0;
  
  let dotProduct = 0;
  let normA = 0;
  let normB = 0;
  
  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i] * b[i];
    normA += a[i] * a[i];
    normB += b[i] * b[i];
  }
  
  const denominator = Math.sqrt(normA) * Math.sqrt(normB);
  return denominator === 0 ? 0 : dotProduct / denominator;
}

/**
 * Create database schema for query logging
 */
export const CREATE_QUERY_LOG_SCHEMA = `
CREATE TABLE IF NOT EXISTS koi_query_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_text TEXT NOT NULL,
    query_embedding vector(1024),
    user_id UUID,
    agent_id UUID,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    confidence_score FLOAT,
    triggered_extraction BOOLEAN DEFAULT FALSE,
    extraction_receipt_rid TEXT,
    response_time_ms INTEGER,
    feedback_provided BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_query_confidence ON koi_query_log(confidence_score);
CREATE INDEX IF NOT EXISTS idx_query_timestamp ON koi_query_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_query_extraction ON koi_query_log(triggered_extraction) 
  WHERE triggered_extraction = TRUE;
CREATE INDEX IF NOT EXISTS idx_query_feedback ON koi_query_log(feedback_provided) 
  WHERE feedback_provided = FALSE;
`;

// Export all functions
export default {
  weightedAverageFusion,
  reciprocalRankFusion,
  calculateConfidence,
  logQuery,
  getQueryPatterns,
  shouldTriggerExtraction,
  calculateIDDSScore,
  selectDocumentsForExtraction,
  CREATE_QUERY_LOG_SCHEMA
};
