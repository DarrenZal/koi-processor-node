"""
Intelligent Context-Aware Chunking System
Chunks content based on semantic boundaries and source type
"""

import re
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class ChunkStrategy:
    """Configuration for chunking strategy"""
    min_size: int = 500
    max_size: int = 1500
    target_size: int = 1000
    overlap: int = 200
    preserve_entities: bool = True
    respect_structure: bool = True


class SmartChunker:
    """
    Intelligent chunking that respects content structure and semantics
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.strategies = {
            "discourse": ChunkStrategy(min_size=800, max_size=2000, target_size=1200),
            "twitter": ChunkStrategy(min_size=280, max_size=560, target_size=280, overlap=0),
            "medium": ChunkStrategy(min_size=600, max_size=1500, target_size=1000),
            "github": ChunkStrategy(min_size=400, max_size=1200, target_size=800),
            "website": ChunkStrategy(min_size=500, max_size=1500, target_size=1000),
            "default": ChunkStrategy()
        }

    def chunk_content(
        self,
        content: str,
        source_type: str,
        extracted_entities: List[Dict[str, Any]] = None,
        metadata: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Chunk content intelligently based on source type and structure

        Args:
            content: Text content to chunk
            source_type: Type of source (discourse, twitter, etc.)
            extracted_entities: Entities found in content
            metadata: Additional metadata about content

        Returns:
            List of chunks with metadata
        """
        if not content:
            return []

        strategy = self.strategies.get(source_type, self.strategies["default"])
        
        # Choose chunking method based on source type
        if source_type == "twitter":
            chunks = self._chunk_tweets(content, metadata)
        elif source_type == "github":
            chunks = self._chunk_code(content, strategy)
        elif source_type == "discourse" or source_type == "medium":
            chunks = self._chunk_article(content, strategy, extracted_entities)
        else:
            chunks = self._chunk_generic(content, strategy, extracted_entities)

        # Add chunk metadata
        return self._enrich_chunks(chunks, source_type, metadata)

    def _chunk_tweets(self, content: str, metadata: Dict[str, Any]) -> List[str]:
        """Chunk Twitter content by tweet boundaries"""
        chunks = []
        
        # Split by common tweet separators
        tweet_patterns = [
            r'\n{2,}',  # Multiple newlines
            r'\d+\.\s',  # Numbered tweets
            r'\[\d+/\d+\]',  # Thread markers
        ]
        
        # Try to identify individual tweets
        parts = re.split('|'.join(tweet_patterns), content)
        
        current_chunk = ""
        for part in parts:
            part = part.strip()
            if not part:
                continue
                
            # Each tweet should be its own chunk
            if len(part) <= 560:  # Max 2 tweets together
                if current_chunk and len(current_chunk) + len(part) > 560:
                    chunks.append(current_chunk)
                    current_chunk = part
                else:
                    current_chunk = f"{current_chunk}\n\n{part}" if current_chunk else part
            else:
                # Long content - break at sentence boundaries
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = ""
                chunks.extend(self._split_long_text(part, 280))
        
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks

    def _chunk_code(self, content: str, strategy: ChunkStrategy) -> List[str]:
        """Chunk code content by functions/classes"""
        chunks = []
        
        # Patterns for code structure
        function_pattern = r'(def\s+\w+.*?(?=\ndef\s|\nclass\s|\Z))'  # Python
        class_pattern = r'(class\s+\w+.*?(?=\nclass\s|\Z))'
        
        # Try to split by functions/classes
        code_blocks = re.findall(f'{function_pattern}|{class_pattern}', content, re.DOTALL)
        
        if code_blocks:
            current_chunk = ""
            for block in code_blocks:
                block_text = block[0] if isinstance(block, tuple) else block
                if len(block_text) > strategy.max_size:
                    # Block too large - split it
                    if current_chunk:
                        chunks.append(current_chunk)
                        current_chunk = ""
                    chunks.extend(self._split_long_text(block_text, strategy.target_size))
                elif len(current_chunk) + len(block_text) > strategy.max_size:
                    chunks.append(current_chunk)
                    current_chunk = block_text
                else:
                    current_chunk = f"{current_chunk}\n\n{block_text}" if current_chunk else block_text
            
            if current_chunk:
                chunks.append(current_chunk)
        else:
            # No clear structure - use generic chunking
            chunks = self._chunk_generic(content, strategy, None)
        
        return chunks

    def _chunk_article(self, content: str, strategy: ChunkStrategy, entities: List[Dict[str, Any]]) -> List[str]:
        """Chunk article/forum content by paragraphs and semantic boundaries"""
        chunks = []
        
        # Split by paragraphs
        paragraphs = re.split(r'\n{2,}', content)
        
        # Build entity location map
        entity_locations = self._find_entity_locations(content, entities) if entities else {}
        
        current_chunk = ""
        current_size = 0
        
        for i, para in enumerate(paragraphs):
            para = para.strip()
            if not para:
                continue
            
            para_size = len(para)
            
            # Check if paragraph contains important entities
            para_has_entities = any(
                start <= sum(len(p) + 2 for p in paragraphs[:i]) <= end
                for start, end in entity_locations.values()
            ) if entity_locations else False
            
            # Decide whether to add to current chunk or start new
            if current_size + para_size > strategy.max_size:
                # Would exceed max size
                if current_size >= strategy.min_size:
                    # Current chunk is big enough - save it
                    chunks.append(current_chunk)
                    current_chunk = para
                    current_size = para_size
                else:
                    # Current chunk too small - force add
                    current_chunk = f"{current_chunk}\n\n{para}" if current_chunk else para
                    current_size += para_size + 2
            elif current_size + para_size >= strategy.target_size and not para_has_entities:
                # Reached target size and no important entities - good break point
                current_chunk = f"{current_chunk}\n\n{para}" if current_chunk else para
                chunks.append(current_chunk)
                current_chunk = ""
                current_size = 0
            else:
                # Add to current chunk
                current_chunk = f"{current_chunk}\n\n{para}" if current_chunk else para
                current_size += para_size + 2
        
        if current_chunk:
            chunks.append(current_chunk)
        
        # Apply overlap for continuity
        if strategy.overlap > 0:
            chunks = self._add_overlap(chunks, strategy.overlap)
        
        return chunks

    def _chunk_generic(self, content: str, strategy: ChunkStrategy, entities: List[Dict[str, Any]]) -> List[str]:
        """Generic chunking with sentence boundaries"""
        chunks = []
        
        # Split into sentences
        sentences = re.split(r'(?<=[.!?])\s+', content)
        
        current_chunk = ""
        current_size = 0
        
        for sentence in sentences:
            sentence = sentence.strip()
            if not sentence:
                continue
            
            sent_size = len(sentence)
            
            if current_size + sent_size > strategy.max_size:
                if current_size >= strategy.min_size:
                    chunks.append(current_chunk)
                    current_chunk = sentence
                    current_size = sent_size
                else:
                    # Force add to meet min size
                    current_chunk = f"{current_chunk} {sentence}" if current_chunk else sentence
                    current_size += sent_size + 1
                    if current_size >= strategy.min_size:
                        chunks.append(current_chunk)
                        current_chunk = ""
                        current_size = 0
            else:
                current_chunk = f"{current_chunk} {sentence}" if current_chunk else sentence
                current_size += sent_size + 1
                
                if current_size >= strategy.target_size:
                    # Check if next sentence starts new thought
                    if sentences.index(sentence) < len(sentences) - 1:
                        next_sent = sentences[sentences.index(sentence) + 1]
                        if self._is_new_thought(next_sent):
                            chunks.append(current_chunk)
                            current_chunk = ""
                            current_size = 0
        
        if current_chunk:
            chunks.append(current_chunk)
        
        # Apply overlap
        if strategy.overlap > 0:
            chunks = self._add_overlap(chunks, strategy.overlap)
        
        return chunks

    def _find_entity_locations(self, content: str, entities: List[Dict[str, Any]]) -> Dict[str, tuple]:
        """Find where entities appear in content"""
        locations = {}
        
        if not entities:
            return locations
        
        for entity in entities:
            name = entity.get("name", "")
            if name and len(name) > 3:  # Skip very short names
                # Find all occurrences
                pattern = re.compile(re.escape(name), re.IGNORECASE)
                for match in pattern.finditer(content):
                    locations[f"{name}_{match.start()}"] = (match.start(), match.end())
        
        return locations

    def _split_long_text(self, text: str, max_size: int) -> List[str]:
        """Split long text into smaller chunks"""
        chunks = []
        words = text.split()
        current_chunk = []
        current_size = 0
        
        for word in words:
            word_size = len(word) + 1
            if current_size + word_size > max_size:
                chunks.append(' '.join(current_chunk))
                current_chunk = [word]
                current_size = word_size
            else:
                current_chunk.append(word)
                current_size += word_size
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks

    def _is_new_thought(self, text: str) -> bool:
        """Check if text starts a new thought/section"""
        new_thought_indicators = [
            r'^However',
            r'^Therefore',
            r'^In conclusion',
            r'^Additionally',
            r'^Furthermore',
            r'^\d+\.',  # Numbered list
            r'^[A-Z][a-z]+ \d+:',  # Date/Chapter
            r'^#{1,6} ',  # Markdown header
        ]
        
        for pattern in new_thought_indicators:
            if re.match(pattern, text):
                return True
        
        return False

    def _add_overlap(self, chunks: List[str], overlap_size: int) -> List[str]:
        """Add overlap between chunks for context continuity"""
        if len(chunks) <= 1:
            return chunks
        
        overlapped = []
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                # First chunk - add suffix from next
                if i + 1 < len(chunks):
                    next_chunk = chunks[i + 1]
                    suffix = ' '.join(next_chunk.split()[:overlap_size // 10])
                    overlapped.append(f"{chunk}\n[...continues: {suffix}]")
                else:
                    overlapped.append(chunk)
            elif i == len(chunks) - 1:
                # Last chunk - add prefix from previous
                prev_chunk = chunks[i - 1]
                prefix = ' '.join(prev_chunk.split()[-(overlap_size // 10):])
                overlapped.append(f"[...continued from: {prefix}]\n{chunk}")
            else:
                # Middle chunk - add both
                prev_chunk = chunks[i - 1]
                next_chunk = chunks[i + 1]
                prefix = ' '.join(prev_chunk.split()[-(overlap_size // 20):])
                suffix = ' '.join(next_chunk.split()[:overlap_size // 20])
                overlapped.append(f"[...from: {prefix}]\n{chunk}\n[...to: {suffix}]")
        
        return overlapped

    def _enrich_chunks(self, chunks: List[str], source_type: str, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Add metadata to chunks"""
        enriched = []
        
        for i, chunk_text in enumerate(chunks):
            enriched.append({
                "text": chunk_text,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "source_type": source_type,
                "chunk_strategy": self.strategies.get(source_type, self.strategies["default"]).__dict__,
                "size": len(chunk_text),
                "metadata": metadata or {}
            })
        
        return enriched