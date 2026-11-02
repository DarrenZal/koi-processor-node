"""
Text Chunker for Document Ingestion

Splits document content into overlapping chunks for embedding and RAG.
Ported from RegenAI koi-sensors/sensors/email/chunker.py.
"""

import logging
import re
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def _token_spans(text: str) -> List[tuple]:
    """Character spans of whitespace-separated tokens: [(start, end), ...].

    Chunk SIZING stays token-based, which is what chunk_size has always meant.
    Chunk TEXT is then sliced out of the original string between the first and
    last token span, so everything between the tokens -- newlines, indentation,
    column alignment -- survives verbatim.

    Why this exists (2026-08-14). The previous implementation did
    `tokens = text.split()` and rebuilt each chunk as `' '.join(tokens)`. That is
    lossy in a way nothing downstream can detect or recover: every run of
    whitespace collapses to a single space, so a document's line and column
    structure is destroyed while the text still reads fine.

    It was found via The Working with Stories Sourcebook, where a two-ended scale
    is printed as two columns on one line:

        Every day                          Very rarely

    The source markdown on disk has 30 such paired-pole lines and 4,230
    two-column lines overall. After ingest, ACROSS ALL 911 CHUNKS of the two
    Kurtz documents, the number retaining any multi-space column run was ZERO,
    and the number keeping those two poles adjacent on one line was ZERO -- while
    38 chunks still contained both phrases, now unpaired and unpairable.

    The text survived; the pairing did not. Nothing downstream can then tell
    which two of those lines are the endpoints of one scale, which is why a
    knowledge-base query returned almost nothing useful from a book full of
    scales, and why an analysis of that corpus had to be retracted for
    generalising from the handful of scales that happened to be written inline.

    The loss is silent -- no error, no warning, and a chunk count that looks
    healthy. mediawiki and gmail chunks in the same database DO retain column
    runs, which is what localised the defect to this chunker rather than to
    ingest generally.
    """
    return [(m.start(), m.end()) for m in re.finditer(r"\S+", text)]


class TextChunker:
    """
    Split text into chunks suitable for embedding.
    Tokens are approximated as whitespace-separated words.

    Chunk boundaries are token-based; chunk text is sliced from the original
    string so that whitespace structure is preserved. See _token_spans().
    """

    def __init__(
        self,
        chunk_size: int = 500,
        chunk_overlap: int = 50,
        min_chunk_size: int = 100,
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size

    def chunk_text(self, text: str) -> List[Dict[str, Any]]:
        """Split text into overlapping chunks."""
        if not text or not text.strip():
            return []

        spans = _token_spans(text)
        tokens = [text[a:b] for a, b in spans]
        total_tokens = len(tokens)

        if total_tokens <= self.chunk_size:
            return [{
                'text': text.strip(),
                'index': 0,
                'start_token': 0,
                'end_token': total_tokens,
                'total_chunks': 1,
            }]

        chunks = []
        start = 0
        chunk_index = 0

        while start < total_tokens:
            end = min(start + self.chunk_size, total_tokens)
            chunk_tokens = tokens[start:end]
            # slice the ORIGINAL text between the first and last token of this
            # chunk, rather than rejoining tokens with single spaces
            chunk_text = text[spans[start][0]:spans[end - 1][1]]

            if len(chunk_tokens) >= self.min_chunk_size or start == 0:
                chunks.append({
                    'text': chunk_text,
                    'index': chunk_index,
                    'start_token': start,
                    'end_token': end,
                })
                chunk_index += 1

            next_start = end - self.chunk_overlap
            if next_start <= start:
                # Didn't advance — force forward to avoid infinite loop
                next_start = end
            start = next_start

        for chunk in chunks:
            chunk['total_chunks'] = len(chunks)

        return chunks


class SentenceAwareChunker(TextChunker):
    """Chunk text while respecting sentence boundaries."""

    def chunk_text(self, text: str) -> List[Dict[str, Any]]:
        if not text or not text.strip():
            return []

        spans = _token_spans(text)
        tokens = [text[a:b] for a, b in spans]
        total_tokens = len(tokens)

        if total_tokens <= self.chunk_size:
            return [{
                'text': text.strip(),
                'index': 0,
                'start_token': 0,
                'end_token': total_tokens,
                'total_chunks': 1,
            }]

        chunks = []
        start = 0
        chunk_index = 0

        while start < total_tokens:
            target_end = min(start + self.chunk_size, total_tokens)
            end = self._find_sentence_boundary(tokens, start, target_end)
            chunk_tokens = tokens[start:end]
            chunk_text = text[spans[start][0]:spans[end - 1][1]]

            if len(chunk_tokens) >= self.min_chunk_size or start == 0:
                chunks.append({
                    'text': chunk_text,
                    'index': chunk_index,
                    'start_token': start,
                    'end_token': end,
                })
                chunk_index += 1

            start = max(end - self.chunk_overlap, start + 1)

        for chunk in chunks:
            chunk['total_chunks'] = len(chunks)

        return chunks

    def _find_sentence_boundary(self, tokens, start, target_end):
        """Find a sentence boundary near target_end."""
        search_start = max(start, target_end - self.chunk_size // 5)
        for i in range(target_end - 1, search_start - 1, -1):
            token = tokens[i]
            if token.endswith('.') or token.endswith('!') or token.endswith('?'):
                return i + 1
        return target_end
