"""
Adaptive Knowledge Extractor for KOI Pipeline
Implements query-driven extraction with confidence monitoring
"""

import asyncio
import hashlib
import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
from dataclasses import dataclass
import asyncpg
import aiohttp
from loguru import logger

from src.core.create_cat_receipt import create_cat_receipt
from src.extraction.predicate_guard import filter_relationships


@dataclass
class QueryContext:
    """Context for a user query"""
    query_id: str
    query_text: str
    user_id: Optional[str]
    agent_id: Optional[str]
    confidence: float
    top_documents: List[Dict[str, Any]]
    timestamp: datetime


@dataclass
class ExtractionResult:
    """Result of adaptive extraction"""
    receipt_rid: str
    extracted_facts: List[Dict[str, Any]]
    extracted_entities: List[Dict[str, Any]]
    extracted_relationships: List[Dict[str, Any]]
    confidence_improvement: float
    cost_usd: float


class AdaptiveExtractor:
    """
    Adaptive knowledge extraction system that triggers extraction
    based on confidence scores and user feedback
    """
    
    CONFIDENCE_THRESHOLD = 0.7  # Below this triggers extraction
    EXTRACTION_BUDGET = 5  # Max documents to extract per query
    
    def __init__(
        self,
        db_pool: asyncpg.Pool,
        bge_api_url: str = "http://localhost:8090/encode",
        llm_api_url: str = "https://api.openai.com/v1/chat/completions",
        llm_api_key: str = None
    ):
        self.db_pool = db_pool
        self.bge_api_url = bge_api_url
        self.llm_api_url = llm_api_url
        self.llm_api_key = llm_api_key
        
    async def process_query(
        self,
        query: str,
        search_results: List[Dict[str, Any]],
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[ExtractionResult]]:
        """
        Process a query and trigger extraction if confidence is low
        
        Returns:
            - Enhanced search results
            - Extraction result if extraction was triggered
        """
        start_time = datetime.utcnow()
        
        # Calculate confidence score
        confidence = self.calculate_confidence(search_results)
        logger.info(f"Query confidence: {confidence:.3f} for '{query[:50]}...'")
        
        # Create query context
        context = QueryContext(
            query_id=self._generate_query_id(query, user_id),
            query_text=query,
            user_id=user_id,
            agent_id=agent_id,
            confidence=confidence,
            top_documents=search_results[:10],
            timestamp=start_time
        )
        
        # Log query
        await self._log_query(context, search_results)
        
        # Check if extraction should be triggered
        extraction_result = None
        if self.should_trigger_extraction(confidence):
            logger.info(f"Triggering adaptive extraction (confidence {confidence:.3f} < {self.CONFIDENCE_THRESHOLD})")
            extraction_result = await self.extract_knowledge(context)
            
            # Re-run search with enhanced knowledge
            if extraction_result and extraction_result.confidence_improvement > 0:
                search_results = await self._enhanced_search(query, extraction_result)
                
        # Calculate response time
        response_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        await self._update_query_log(context.query_id, response_time_ms, extraction_result)
        
        return search_results, extraction_result
    
    def calculate_confidence(self, results: List[Dict[str, Any]]) -> float:
        """Calculate confidence score for search results"""
        if not results:
            return 0.0
            
        # Extract scores
        scores = [r.get('similarity', r.get('score', 0)) for r in results[:10]]
        
        if not scores:
            return 0.0
            
        # Multiple factors for confidence
        factors = {
            'top_score': scores[0] if scores else 0,
            'score_gap': (scores[0] - scores[1]) if len(scores) > 1 else 0.5,
            'result_count': min(len(results) / 10, 1),
            'avg_score': np.mean(scores[:5]) if len(scores) >= 5 else np.mean(scores),
            'score_variance': 1 - np.var(scores) if len(scores) > 1 else 0.5
        }
        
        # Weighted confidence score
        confidence = (
            factors['top_score'] * 0.35 +
            factors['score_gap'] * 0.20 +
            factors['result_count'] * 0.15 +
            factors['avg_score'] * 0.20 +
            factors['score_variance'] * 0.10
        )
        
        return min(1.0, max(0.0, confidence))
    
    def should_trigger_extraction(self, confidence: float) -> bool:
        """Determine if extraction should be triggered"""
        return confidence < self.CONFIDENCE_THRESHOLD
    
    async def extract_knowledge(self, context: QueryContext) -> Optional[ExtractionResult]:
        """
        Extract knowledge from top documents using LLM
        """
        # Select documents for extraction using IDDS scoring
        selected_docs = self._select_documents_for_extraction(
            context.top_documents,
            budget=self.EXTRACTION_BUDGET
        )
        
        if not selected_docs:
            logger.warning("No documents selected for extraction")
            return None
            
        # Create CAT receipt for extraction
        receipt_metadata = {
            "trigger": "low_confidence",
            "confidence_score": context.confidence,
            "documents_processed": len(selected_docs),
            "query": context.query_text,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Generate receipt ID as proper UUID
        import uuid
        receipt_content = f"adaptive_extraction:{context.query_id}:{datetime.utcnow().isoformat()}"
        receipt_hash = hashlib.sha256(receipt_content.encode()).digest()[:16]
        receipt_rid = str(uuid.UUID(bytes=receipt_hash))
        
        # Extract from each document
        all_facts = []
        all_entities = []
        all_relationships = []
        total_cost = 0.0
        
        for doc in selected_docs:
            try:
                extraction = await self._extract_from_document(doc, context.query_text)
                all_facts.extend(extraction.get('facts', []))
                all_entities.extend(extraction.get('entities', []))
                all_relationships.extend(extraction.get('relationships', []))
                total_cost += extraction.get('cost', 0)
            except Exception as e:
                logger.error(f"Extraction failed for document {doc.get('rid')}: {e}")
                
        # Store extracted knowledge
        await self._store_extracted_knowledge(
            receipt_rid,
            all_facts,
            all_entities,
            all_relationships
        )
        
        # Calculate confidence improvement
        confidence_after = await self._measure_confidence_improvement(context.query_text)
        confidence_improvement = confidence_after - context.confidence
        
        # Log extraction
        await self._log_extraction(
            context.query_id,
            receipt_rid,
            len(selected_docs),
            total_cost,
            confidence_improvement
        )
        
        return ExtractionResult(
            receipt_rid=receipt_rid,
            extracted_facts=all_facts,
            extracted_entities=all_entities,
            extracted_relationships=all_relationships,
            confidence_improvement=confidence_improvement,
            cost_usd=total_cost
        )
    
    def _select_documents_for_extraction(
        self,
        documents: List[Dict[str, Any]],
        budget: int
    ) -> List[Dict[str, Any]]:
        """
        Select documents using IDDS (Informativeness, Diversity, and Density Sampling)
        """
        if len(documents) <= budget:
            return documents
            
        selected = []
        remaining = documents.copy()
        
        for _ in range(budget):
            if not remaining:
                break
                
            # Calculate IDDS scores
            scores = []
            for doc in remaining:
                score = self._calculate_idds_score(doc, remaining, selected)
                scores.append((doc, score))
                
            # Select highest scoring document
            scores.sort(key=lambda x: x[1], reverse=True)
            best_doc = scores[0][0]
            
            selected.append(best_doc)
            remaining.remove(best_doc)
            
        return selected
    
    def _calculate_idds_score(
        self,
        doc: Dict[str, Any],
        unlabeled_pool: List[Dict[str, Any]],
        selected_pool: List[Dict[str, Any]],
        alpha: float = 0.5
    ) -> float:
        """Calculate IDDS score for a document"""
        # Simplified scoring based on similarity and diversity
        # In production, this would use actual embeddings
        
        # Informativeness: relevance score
        informativeness = doc.get('similarity', doc.get('score', 0))
        
        # Diversity: how different from already selected
        diversity = 1.0
        if selected_pool:
            # Simple diversity based on content overlap
            for selected in selected_pool:
                if doc.get('content') == selected.get('content'):
                    diversity *= 0.5
                    
        return alpha * informativeness + (1 - alpha) * diversity
    
    async def _extract_from_document(
        self,
        document: Dict[str, Any],
        query: str
    ) -> Dict[str, Any]:
        """Extract structured knowledge from a document using LLM"""
        
        prompt = f"""
        Given the following document content and user query, extract relevant knowledge:
        
        Query: {query}
        
        Document: {document.get('content', '')[:2000]}
        
        Extract:
        1. Key facts relevant to the query
        2. Important entities (people, organizations, concepts)
        3. Relationships between entities
        
        Return as JSON with keys: facts, entities, relationships
        """
        
        # Call LLM API (simplified - in production use proper client)
        async with aiohttp.ClientSession() as session:
            headers = {
                "Authorization": f"Bearer {self.llm_api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
                "response_format": {"type": "json_object"}
            }
            
            async with session.post(
                self.llm_api_url,
                headers=headers,
                json=payload
            ) as response:
                result = await response.json()
                
                # Parse response
                content = result['choices'][0]['message']['content']
                extracted = json.loads(content)
                
                # Calculate cost (approximate)
                tokens = len(prompt.split()) + len(content.split())
                cost = tokens * 0.000001  # Rough estimate
                
                extracted['cost'] = cost
                return extracted
    
    async def _store_extracted_knowledge(
        self,
        receipt_rid: str,
        facts: List[Dict],
        entities: List[Dict],
        relationships: List[Dict]
    ):
        """Store extracted knowledge in database and graph"""
        # Week 16 FIX-015: Apply predicate guard to validate relationships
        validate_types = os.getenv("PREDICATE_GUARD_VALIDATE_TYPES", "false").lower() == "true"
        strict_types = os.getenv("PREDICATE_GUARD_STRICT_TYPES", "false").lower() == "true"

        if validate_types:
            # Build entity name -> type lookup from entities list
            entity_type_lookup = {}
            for ent in entities:
                name = ent.get("name", "").lower().strip()
                etype = ent.get("type", "").upper()
                if name and etype:
                    entity_type_lookup[name] = etype

            # Enrich relationships with type info for validation
            # LLM uses entity1/entity2 OR source/target, handle both
            for rel in relationships:
                source = (rel.get("source") or rel.get("entity1") or "").lower().strip()
                target = (rel.get("target") or rel.get("entity2") or "").lower().strip()
                if source in entity_type_lookup:
                    rel["source_type"] = entity_type_lookup[source]
                if target in entity_type_lookup:
                    rel["target_type"] = entity_type_lookup[target]
                # LLM uses various field names for the predicate: 'predicate', 'relationship', or 'relation'
                # Normalize to 'predicate' field
                actual_rel = rel.get("relationship") or rel.get("relation")
                current_pred = rel.get("predicate")
                if actual_rel:
                    # If predicate is missing or generic, use the actual relationship
                    if not current_pred or current_pred == "associated_with":
                        rel["predicate"] = actual_rel

            # Debug: log relationships before filtering
            for rel in relationships:
                logger.debug(f"[PredicateTypeGuard] Checking: {rel.get('source_type')} → {rel.get('predicate')} → {rel.get('target_type')}")

            original_count = len(relationships)
            relationships = filter_relationships(
                relationships,
                strict=False,  # Don't reject non-canonical predicates
                validate_types=True,
                strict_types=strict_types
            )
            filtered_count = original_count - len(relationships)
            if filtered_count > 0:
                logger.warning(f"[PredicateTypeGuard] Filtered {filtered_count} invalid relationships (strict={strict_types})")
            else:
                logger.info(f"[PredicateTypeGuard] All {original_count} relationships passed validation")

        # Store in PostgreSQL
        async with self.db_pool.acquire() as conn:
            # Store extraction record
            await conn.execute("""
                INSERT INTO koi_adaptive_extractions 
                (cat_receipt_rid, extracted_content, triples_generated, 
                 entities_extracted, relationships_extracted)
                VALUES ($1, $2, $3, $4, $5)
            """, receipt_rid, json.dumps({
                'facts': facts,
                'entities': entities,
                'relationships': relationships
            }), len(facts), len(entities), len(relationships))
            
        # TODO: Store in Apache Jena Fuseki as RDF triples
        
    async def _log_query(
        self,
        context: QueryContext,
        results: List[Dict[str, Any]]
    ):
        """Log query to database"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO koi_query_log
                (id, query_text, user_id, agent_id, confidence_score, 
                 result_count, top_result_score)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, context.query_id, context.query_text, context.user_id,
            context.agent_id, context.confidence, len(results),
            results[0].get('similarity', 0) if results else 0)
            
    async def _update_query_log(
        self,
        query_id: str,
        response_time_ms: int,
        extraction_result: Optional[ExtractionResult]
    ):
        """Update query log with final results"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE koi_query_log
                SET response_time_ms = $1,
                    triggered_extraction = $2,
                    extraction_receipt_rid = $3
                WHERE id = $4
            """, response_time_ms, 
            extraction_result is not None,
            extraction_result.receipt_rid if extraction_result else None,
            query_id)
            
    async def _log_extraction(
        self,
        query_id: str,
        receipt_rid: str,
        doc_count: int,
        cost: float,
        improvement: float
    ):
        """Log extraction details"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE koi_adaptive_extractions
                SET query_log_id = $1,
                    extraction_cost_usd = $2,
                    confidence_improvement = $3
                WHERE cat_receipt_rid = $4
            """, query_id, cost, improvement, receipt_rid)
            
    async def _enhanced_search(
        self,
        query: str,
        extraction_result: ExtractionResult
    ) -> List[Dict[str, Any]]:
        """Re-run search with enhanced knowledge"""
        # This would re-run the search with the newly extracted knowledge
        # For now, return empty list as placeholder
        return []
        
    async def _measure_confidence_improvement(self, query: str) -> float:
        """Measure confidence after extraction"""
        # This would re-run the search and calculate new confidence
        # For now, return slight improvement
        return min(0.85, self.CONFIDENCE_THRESHOLD + 0.15)
        
    def _generate_query_id(self, query: str, user_id: Optional[str]) -> str:
        """Generate unique query ID as full UUID format"""
        import uuid
        content = f"{query}{user_id}{datetime.utcnow().isoformat()}"
        # Generate deterministic UUID from hash
        hash_bytes = hashlib.sha256(content.encode()).digest()[:16]
        return str(uuid.UUID(bytes=hash_bytes))


# Export for use in other modules
__all__ = ['AdaptiveExtractor', 'QueryContext', 'ExtractionResult']