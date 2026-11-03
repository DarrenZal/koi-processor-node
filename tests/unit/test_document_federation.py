"""Unit tests for the `document` federation domain.

Two things are guarded here, because neither had any coverage before:

1. **The policy layer** (`api/document_federation.py`) — the flag, the
   containment allowlist, the slug→author derivation and the URL sanitizer.
   Pure functions, no DB, no network.
2. **The privacy columns on the shared sink** (`upsert_document_memory`) —
   `is_private` / `access_source`, which the function never wrote before. The
   whole point of the change is that "caller did not say" must not be coerced
   into `false`, so both directions are asserted: an unspecified caller
   preserves an existing `true`, and an explicit caller writes what it says.

Measured facts these tests encode (see `api/document_federation.py` docstrings):
`document.author` is null in 448/448 coordinator bundles, so author is derived;
`document.url` is a canonical post URL in only 336/448, with the other 112
carrying a tracking pixel whose token contains the subscriber's email address.

DB tests run inside a transaction that is rolled back at teardown.
"""

import json

import asyncpg
import pytest

from api import document_federation as df
from scripts.ingest_document import upsert_document_memory
from tests import koi_test_targets as targets


NATE_RID = "regen.newsletter:newsletter_nate-jones-substack_00a42375cc30ed45"
OTHER_RID = "regen.newsletter:newsletter_someone-else-substack_0123456789abcdef"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    yield _conn
    await tx.rollback()
    await _conn.close()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Each test starts from "operator has set nothing"."""
    monkeypatch.delenv("KOI_FEDERATE_DOCUMENTS", raising=False)
    monkeypatch.delenv("KOI_FEDERATE_DOCUMENTS_RID_ALLOW", raising=False)
    monkeypatch.delenv("KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS", raising=False)


def _bundle(rid=NATE_RID, slug="nate-jones-substack", url=None, content="body text"):
    return {
        "document": {
            "id": rid.split(":", 1)[1],
            "source": f"newsletters:{slug}",
            "source_type": "newsletter",
            "url": url,
            "title": "A Title",
            "content": content,
            "author": None,  # null in all 448 measured bundles
        },
        "metadata": {
            "title": "A Title",
            "url": url,
            "newsletter_slug": slug,
            "is_private": True,
            "access_source": "substack-example-author-paid",
        },
    }


# ── the flag ────────────────────────────────────────────────────────────────

def test_flag_defaults_off():
    """Unset means off. `regen-prod` deploys to more than one node."""
    assert df.document_federation_enabled() is False


@pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes", "on", " true "])
def test_flag_accepts_the_usual_truthy_spellings(monkeypatch, value):
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS", value)
    assert df.document_federation_enabled() is True


@pytest.mark.parametrize("value", ["false", "0", "no", "off", ""])
def test_flag_rejects_falsy_spellings(monkeypatch, value):
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS", value)
    assert df.document_federation_enabled() is False


def test_dispatch_predicate_is_false_while_the_flag_is_off():
    """The poller must not even import the handler when the flag is off."""
    assert df.should_dispatch_as_document(NATE_RID, _bundle()) is False


def test_dispatch_predicate_is_true_once_enabled(monkeypatch):
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS", "true")
    assert df.should_dispatch_as_document(NATE_RID, _bundle()) is True


# ── containment ─────────────────────────────────────────────────────────────

def test_allowlist_defaults_to_nate_jones_only():
    assert df.rid_allowed(NATE_RID) is True
    assert df.rid_allowed(OTHER_RID) is False


def test_empty_allowlist_allows_nothing(monkeypatch):
    """Fail closed. "Allow everything" must have no representation.

    An operator who clears the variable expecting "no restriction" gets the
    safe reading instead of an open gate.
    """
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS_RID_ALLOW", "  ")
    assert df.rid_allowlist() == ()
    assert df.rid_allowed(NATE_RID) is False


def test_allowlist_is_widenable(monkeypatch):
    monkeypatch.setenv(
        "KOI_FEDERATE_DOCUMENTS_RID_ALLOW",
        "newsletter_nate-jones-substack_, newsletter_someone-else-substack_")
    assert df.rid_allowed(NATE_RID) is True
    assert df.rid_allowed(OTHER_RID) is True


def test_empty_rid_is_never_allowed():
    assert df.rid_allowed("") is False


# ── author derivation ───────────────────────────────────────────────────────

def test_author_is_derived_from_slug_not_read_from_the_bundle():
    b = _bundle()
    assert b["document"]["author"] is None       # as it is in all 448 bundles
    assert df.resolve_author(df.newsletter_slug(b)) == "Nate B. Jones"


def test_slug_falls_back_to_document_source():
    b = _bundle()
    del b["metadata"]["newsletter_slug"]
    assert df.newsletter_slug(b) == "nate-jones-substack"


def test_unmapped_slug_raises_rather_than_writing_a_null_author():
    """A null author is what lets a Jones question be answered by Hagens."""
    with pytest.raises(df.UnmappedNewsletterSlug):
        df.resolve_author("someone-else-substack")


def test_missing_slug_raises():
    with pytest.raises(df.UnmappedNewsletterSlug):
        df.resolve_author(None)


# ── URL sanitizing ──────────────────────────────────────────────────────────

CANONICAL = "https://natesnewsletter.substack.com/p/executive-briefing-the-two-class"
TRACKING = (
    # Synthetic token in the real shape: base64url JSON {"m": message-id, "r": recipient}.
    "https://eotrx.substackcdn.com/o/a1b2c3d4e5f60718/p.gif?token="
    "eyJtIjoiPHhAbWcxLnN1YnN0YWNrLmNv"
    "bT4iLCJyIjoieEBleGFtcGxlLmNvbSJ9"
)


def test_canonical_url_passes_through():
    b = _bundle(url=CANONICAL)
    assert df.canonical_url("nate-jones-substack", b) == CANONICAL


def test_tracking_pixel_url_is_refused():
    """112/448 bundles carry this in BOTH url fields; 224 values decode to
    the subscriber's email address. source_url is copied onto every chunk and
    surfaces in search results, so storing one would publish PII."""
    b = _bundle(url=TRACKING)
    assert df.canonical_url("nate-jones-substack", b) is None


def test_url_from_an_unknown_publication_is_refused():
    b = _bundle(url=CANONICAL)
    assert df.canonical_url("unknown-slug", b) is None


def test_absent_url_yields_none():
    assert df.canonical_url("nate-jones-substack", _bundle(url=None)) is None


# ── payload shape ───────────────────────────────────────────────────────────

def test_a_bundle_without_a_body_is_not_a_document_payload():
    """A parent row with no text looks ingested and returns nothing."""
    assert df.is_document_payload(_bundle(content="")) is False


@pytest.mark.parametrize("bad", [None, "string", 42, [], {}, {"document": None}])
def test_non_bundle_shapes_are_rejected(bad):
    assert df.is_document_payload(bad) is False


# ── the sink's privacy columns ──────────────────────────────────────────────

@pytest.mark.anyio
async def test_unspecified_privacy_matches_the_pre_change_behaviour(conn):
    """A caller that does not model privacy gets exactly what it got before:
    is_private=false (the schema default) and access_source NULL."""
    rid = "document:test-unspecified-privacy"
    await upsert_document_memory(conn, rid, "text", {"slug": "s"})
    row = await conn.fetchrow(
        "SELECT is_private, access_source, source_sensor FROM koi_memories WHERE rid=$1",
        rid)
    assert row["is_private"] is False
    assert row["access_source"] is None
    assert row["source_sensor"] == "document-ingest"


@pytest.mark.anyio
async def test_private_document_is_not_landed_as_public(conn):
    """The defect this change exists to fix: koi_memories.is_private defaults
    to false, so a federated private document previously landed PUBLIC."""
    rid = "document:test-private"
    await upsert_document_memory(conn, rid, "text", {
        "slug": "s", "is_private": True, "access_source": "substack-example-author-paid",
        "author": "Nate B. Jones", "source_sensor": "koi-net-federation",
    })
    row = await conn.fetchrow(
        "SELECT is_private, access_source, source_sensor, metadata FROM koi_memories "
        "WHERE rid=$1", rid)
    assert row["is_private"] is True
    assert row["access_source"] == "substack-example-author-paid"
    assert row["source_sensor"] == "koi-net-federation"
    assert json.loads(row["metadata"])["author"] == "Nate B. Jones"


@pytest.mark.anyio
async def test_a_privacy_unaware_caller_cannot_downgrade_an_existing_private_row(conn):
    """Re-ingesting through a caller that does not pass is_private must PRESERVE
    true, not coerce it back to the column default."""
    rid = "document:test-no-downgrade"
    await upsert_document_memory(conn, rid, "text", {
        "slug": "s", "is_private": True, "access_source": "substack-example-author-paid"})
    await upsert_document_memory(conn, rid, "text v2", {"slug": "s"})
    row = await conn.fetchrow(
        "SELECT is_private, access_source FROM koi_memories WHERE rid=$1", rid)
    assert row["is_private"] is True
    assert row["access_source"] == "substack-example-author-paid"


@pytest.mark.anyio
async def test_an_explicit_false_does_downgrade(conn):
    """Preserving on unspecified must not become "can never be unset"."""
    rid = "document:test-explicit-downgrade"
    await upsert_document_memory(conn, rid, "text", {"slug": "s", "is_private": True})
    await upsert_document_memory(conn, rid, "text", {"slug": "s", "is_private": False})
    row = await conn.fetchrow("SELECT is_private FROM koi_memories WHERE rid=$1", rid)
    assert row["is_private"] is False


# ── the handler end-to-end (stub embedder; no OpenAI credits needed) ─────────

class _RaisingEmbedder:
    """Stands in for an exhausted / unreachable embedding provider."""

    async def embed(self, text):
        raise RuntimeError("credit_balance_exhausted")


class _FakeEmbedder:
    async def embed(self, text):
        return [0.0] * 3072


def _install_embedder(monkeypatch, embedder):
    from api import domain_event_handlers as deh
    from api.chunker import TextChunker
    monkeypatch.setattr(
        deh, "_document_embedder_and_chunker",
        lambda: (embedder, TextChunker(chunk_size=500, chunk_overlap=50)))


@pytest.mark.anyio
async def test_a_failed_embed_leaves_no_trace(conn, monkeypatch):
    """The landing is transactional.

    Chunks written with a null embedding are invisible to retrieval but count
    as healthy rows under count(*) — so a partial landing must roll back
    entirely, not persist and redeliver. Reproduced live 2026-08-21 when the
    OpenAI account hit credit_balance_exhausted mid-run.
    """
    from api.domain_event_handlers import _apply_document, FederationDeferred
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS", "true")
    _install_embedder(monkeypatch, _RaisingEmbedder())

    before = await conn.fetchval(
        "SELECT count(*) FROM koi_memories WHERE rid=$1", NATE_RID)
    with pytest.raises(FederationDeferred):
        await _apply_document(conn, NATE_RID, "NEW", _bundle(url=CANONICAL), "peer")
    after = await conn.fetchval(
        "SELECT count(*) FROM koi_memories WHERE rid=$1", NATE_RID)
    chunks = await conn.fetchval(
        "SELECT count(*) FROM koi_memory_chunks WHERE document_rid=$1", NATE_RID)
    assert (after, chunks) == (before, 0)


@pytest.mark.anyio
async def test_a_successful_embed_does_land(conn, monkeypatch):
    """Positive control for the test above — without this, a handler that
    silently did nothing at all would also pass the rollback assertion."""
    from api.domain_event_handlers import _apply_document
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS", "true")
    _install_embedder(monkeypatch, _FakeEmbedder())

    await _apply_document(conn, NATE_RID, "NEW", _bundle(url=CANONICAL), "peer")
    row = await conn.fetchrow(
        """SELECT m.is_private, m.access_source, m.metadata->>'author' AS author,
                  m.metadata->>'source_url' AS url,
                  count(c.*) AS chunks, count(c.embedding_3072) AS embedded
           FROM koi_memories m LEFT JOIN koi_memory_chunks c ON c.document_rid = m.rid
           WHERE m.rid=$1 GROUP BY 1,2,3,4""", NATE_RID)
    assert row["is_private"] is True
    assert row["access_source"] == "substack-example-author-paid"
    assert row["author"] == "Nate B. Jones"
    assert row["url"] == CANONICAL
    assert row["chunks"] > 0
    assert row["chunks"] == row["embedded"]


@pytest.mark.anyio
async def test_containment_rejects_a_disallowed_rid(conn, monkeypatch):
    """AC10. An over-broad edge must not be able to land unrelated content."""
    from api.domain_event_handlers import _apply_document
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS", "true")
    _install_embedder(monkeypatch, _FakeEmbedder())

    await _apply_document(
        conn, OTHER_RID, "NEW",
        _bundle(rid=OTHER_RID, slug="someone-else-substack"), "peer")
    assert await conn.fetchval(
        "SELECT count(*) FROM koi_memories WHERE rid=$1", OTHER_RID) == 0


@pytest.mark.anyio
async def test_handler_is_inert_while_the_flag_is_off(conn, monkeypatch):
    """A peer may run an older copy of this code, so the flag-off path must stay inert."""
    from api.domain_event_handlers import _apply_document
    _install_embedder(monkeypatch, _FakeEmbedder())
    await _apply_document(conn, NATE_RID, "NEW", _bundle(url=CANONICAL), "peer")
    assert await conn.fetchval(
        "SELECT count(*) FROM koi_memories WHERE rid=$1", NATE_RID) == 0


@pytest.mark.anyio
async def test_forget_removes_the_document_and_its_chunks(conn, monkeypatch):
    from api.domain_event_handlers import _apply_document, apply_domain_event
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS", "true")
    _install_embedder(monkeypatch, _FakeEmbedder())

    await _apply_document(conn, NATE_RID, "NEW", _bundle(url=CANONICAL), "peer")
    assert await conn.fetchval(
        "SELECT count(*) FROM koi_memory_chunks WHERE document_rid=$1", NATE_RID) > 0

    await apply_domain_event(conn, df.DOCUMENT_DOMAIN, NATE_RID, "FORGET", {}, "peer")
    assert await conn.fetchval(
        "SELECT count(*) FROM koi_memories WHERE rid=$1", NATE_RID) == 0
    assert await conn.fetchval(
        "SELECT count(*) FROM koi_memory_chunks WHERE document_rid=$1", NATE_RID) == 0


# ── subscriber-PII redaction ────────────────────────────────────────────────

SUB = "reader@example.com"
# A base64url token whose decoded payload contains SUB (built, not pasted, so
# the fixture cannot drift from what the decoder actually sees).
import base64 as _b64  # noqa: E402
_TOKEN = _b64.urlsafe_b64encode(
    b'{"m":"<x@mg1.substack.com>","r":"reader@example.com"}').decode().rstrip("=")


def test_redaction_is_a_no_op_when_unconfigured():
    """Unset must not silently mangle content — and must not silently protect
    either: the handler logs a warning once, asserted by the returned count."""
    text, n = df.redact_subscriber_pii(f"mail {SUB} here")
    assert (text, n) == (f"mail {SUB} here", 0)


def test_plaintext_address_is_redacted_case_insensitively(monkeypatch):
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS", SUB)
    text, n = df.redact_subscriber_pii(f"to {SUB} and {SUB.upper()} ok")
    assert n == 2
    assert SUB not in text.lower()
    assert text.count(df.REDACTED_EMAIL) == 2


def test_address_hidden_inside_a_base64_token_is_redacted(monkeypatch):
    """The rule is "the address must not survive", and a decodable token
    satisfies it no less than plaintext. 324 of the 368 corpus occurrences are
    of this form."""
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS", SUB)
    body = f"click https://eotrx.substackcdn.com/o/abc/p.gif?token={_TOKEN} now"
    text, n = df.redact_subscriber_pii(body)
    assert n == 1
    assert _TOKEN not in text
    assert df.REDACTED_TOKEN in text
    assert "https://eotrx.substackcdn.com" in text  # URL structure preserved


def test_an_unrelated_token_is_left_alone(monkeypatch):
    """Positive control: without this, a function that redacted every token
    would also pass the test above."""
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS", SUB)
    other = _b64.urlsafe_b64encode(b'{"r":"someone.else@example.org"}').decode().rstrip("=")
    text, n = df.redact_subscriber_pii(f"link ?token={other}")
    assert n == 0
    assert other in text


def test_third_party_addresses_are_not_redacted(monkeypatch):
    """Scoped to the CONFIGURED addresses. The corpus also carries the
    publisher's own contact addresses and reader addresses he quotes; those are
    content, and a blanket email-shaped redaction would destroy them."""
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS", SUB)
    text, n = df.redact_subscriber_pii("write to chef@natebjones.com instead")
    assert (text, n) == ("write to chef@natebjones.com instead", 0)


def test_a_malformed_token_does_not_raise(monkeypatch):
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS", SUB)
    text, n = df.redact_subscriber_pii("?token=eyJ" + "!" * 30 + "notbase64!!!")
    assert n == 0


@pytest.mark.anyio
async def test_redacted_body_is_what_gets_indexed(conn, monkeypatch):
    """End-to-end: the address must not reach koi_memories or the chunks, and
    the redaction count must be recorded on the document row."""
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS", "true")
    monkeypatch.setenv("KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS", SUB)
    _install_embedder(monkeypatch, _FakeEmbedder())
    from api.domain_event_handlers import _apply_document

    body = f"Nate writes at length. You are receiving this at {SUB}. More text here."
    await _apply_document(
        conn, NATE_RID, "NEW", _bundle(url=CANONICAL, content=body), "peer")

    row = await conn.fetchrow(
        "SELECT content->>'text' AS text, metadata->>'redactions_applied' AS n "
        "FROM koi_memories WHERE rid=$1", NATE_RID)
    assert SUB not in row["text"]
    assert df.REDACTED_EMAIL in row["text"]
    assert row["n"] == "1"
    chunk_text = await conn.fetchval(
        "SELECT string_agg(content->>'text', ' ') FROM koi_memory_chunks "
        "WHERE document_rid=$1", NATE_RID)
    assert SUB not in chunk_text
