"""The chunker must not flatten whitespace.

Regression test for a silent data-loss defect found 2026-08-14. `TextChunker`
and `SentenceAwareChunker` both did `tokens = text.split()` and rebuilt each
chunk as `' '.join(tokens)`. Text survived; every run of whitespace collapsed to
a single space, so line and column structure was destroyed on ingest with no
error, no warning, and a healthy-looking chunk count.

The corpus that exposed it: The Working with Stories Sourcebook prints a
two-ended scale as two columns on one line ("Every day        Very rarely").
Across all 911 chunks of the two ingested Kurtz documents, the number retaining
any multi-space column run was ZERO, while 38 chunks still contained both poles
as separate unpairable lines. A downstream analysis had to be retracted because
only the scales that happened to be written inline were visible.

These tests are written to FAIL against the old implementation -- verified: the
pre-fix code scores 0 on every preservation assertion below.
"""

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api.chunker import SentenceAwareChunker, TextChunker  # noqa: E402

COLUMN_RUN = re.compile(r"[A-Za-z]{3,}[ ]{4,}[A-Za-z]{3,}")
PAIRED_POLES = re.compile(r"Every day[ ]{2,}Very rarely")

# A page shaped like the Sourcebook's: prose, then column-encoded scales.
SCALE_PAGE = """How often does something like this happen where you live?

   Every day                          Very rarely

   I don't know

And how much does it matter to the people involved?

   Not at all                         Completely

   I don't know
"""


def _long(text: str) -> str:
    """Pad past chunk_size so the multi-chunk path is exercised, not the
    single-chunk early return (which was always faithful and would hide this)."""
    return text + "\n\n" + ("filler " * 3000)


@pytest.mark.parametrize("cls", [TextChunker, SentenceAwareChunker])
def test_column_alignment_survives_chunking(cls):
    text = _long(SCALE_PAGE)
    chunks = cls(chunk_size=200, chunk_overlap=20, min_chunk_size=10).chunk_text(text)
    joined = "\n".join(c["text"] for c in chunks)

    assert PAIRED_POLES.search(joined), (
        "the two poles of a scale are no longer on one line; the pairing is lost "
        "and nothing downstream can recover which two lines belong together"
    )
    assert COLUMN_RUN.search(joined), "all multi-space column runs were collapsed"


@pytest.mark.parametrize("cls", [TextChunker, SentenceAwareChunker])
def test_newlines_survive_chunking(cls):
    text = _long(SCALE_PAGE)
    chunks = cls(chunk_size=200, chunk_overlap=20, min_chunk_size=10).chunk_text(text)
    assert any("\n" in c["text"] for c in chunks), (
        "no chunk contains a newline; the document was flattened to one line"
    )


@pytest.mark.parametrize("cls", [TextChunker, SentenceAwareChunker])
def test_chunk_text_is_a_verbatim_slice_of_the_source(cls):
    """The strongest form: every chunk must appear in the original, exactly."""
    text = _long(SCALE_PAGE)
    for c in cls(chunk_size=200, chunk_overlap=20, min_chunk_size=10).chunk_text(text):
        assert c["text"] in text, (
            "a chunk is not a substring of the input, so it was rewritten rather "
            "than sliced"
        )


@pytest.mark.parametrize("cls", [TextChunker, SentenceAwareChunker])
def test_sizing_behaviour_is_unchanged(cls):
    """The fix must change chunk CONTENT only, never chunk boundaries."""
    text = _long(SCALE_PAGE)
    chunks = cls(chunk_size=200, chunk_overlap=20, min_chunk_size=10).chunk_text(text)
    assert chunks, "no chunks produced"
    assert all(c["end_token"] > c["start_token"] for c in chunks)
    assert all(c["end_token"] - c["start_token"] <= 200 for c in chunks), (
        "a chunk exceeded chunk_size in tokens"
    )
    assert [c["index"] for c in chunks] == list(range(len(chunks)))
    assert all(c["total_chunks"] == len(chunks) for c in chunks)


def test_empty_and_short_inputs_still_behave():
    ch = TextChunker(chunk_size=200, chunk_overlap=20, min_chunk_size=10)
    assert ch.chunk_text("") == []
    assert ch.chunk_text("   \n  ") == []
    short = ch.chunk_text("just a few words here")
    assert len(short) == 1 and short[0]["total_chunks"] == 1
