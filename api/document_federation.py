"""Policy for landing federated DOCUMENTS into local RAG storage.

This module holds the three decisions that gate the `document` federation
domain, deliberately separated from the handler itself (`domain_event_handlers.
_apply_document`) so each is testable without a database:

1. **Is the feature on at all?** `KOI_FEDERATE_DOCUMENTS`, default **off**.
   A node that deploys this code may not have opted in to receiving a new
   dispatcher + document sink. Default-off keeps such a node byte-identical in
   behaviour until it sets the flag itself.
2. **Is this RID allowed to land?** Server-side `rid_types` edge scoping is
   verified ineffective for `regen.newsletter:` RIDs — `extract_rid_type()`
   returns None for them and the poll filter only skips when a type *is*
   extracted. Containment therefore has to be enforced here, on the receiving
   side, or an over-broad edge silently lands unrelated coordinator content.
3. **Who wrote it?** Coordinator newsletter bundles carry a `document.author`
   key whose value is **null in all 448 bundles measured** (2026-08-21). The
   key existing is not the value existing. Author is therefore DERIVED from
   `newsletter_slug` via an explicit table, and an unmapped slug is a loud
   failure — a null author is exactly what lets "what does Nate say" answer
   confidently with Nate Hagens (476 of his documents are already indexed).

None of these fail open. An empty allowlist allows nothing; an unmapped slug
raises; an unset flag disables the path entirely.
"""

import base64
import logging
import os
import re
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

# The domain name used on the wire and in the handler registry.
DOCUMENT_DOMAIN = "document"

# Default containment: only Nate B. Jones' newsletter may land. Overridable via
# KOI_FEDERATE_DOCUMENTS_RID_ALLOW (comma-separated substrings) when the sink is
# generalized to other publications — at which point NEWSLETTER_SLUG_AUTHORS
# must gain the matching entry, or ingestion of the new slug fails loudly.
DEFAULT_RID_ALLOWLIST = ("newsletter_nate-jones-substack_",)

# newsletter_slug → author. See module docstring: derived, never copied.
NEWSLETTER_SLUG_AUTHORS: Dict[str, str] = {
    "nate-jones-substack": "Nate B. Jones",
}

# newsletter_slug → the host a canonical post URL must live on. Any URL on some
# other host is discarded rather than stored (see `canonical_url`).
NEWSLETTER_SLUG_HOSTS: Dict[str, str] = {
    "nate-jones-substack": "natesnewsletter.substack.com",
}

# Provenance stamp distinguishing federated documents from locally-ingested
# ones in koi_memories.source_sensor.
FEDERATION_SOURCE_SENSOR = "koi-net-federation"


class UnmappedNewsletterSlug(Exception):
    """Raised when a document's slug has no author mapping.

    Deliberately fatal for the event rather than defaulted: writing a null
    author is silent coercion of an unknown into the benign value, and the
    resulting document is indistinguishable from another author's work at
    query time.
    """


def document_federation_enabled() -> bool:
    """Whether federated documents may land locally. Default **off**.

    Re-read on every call (matching `_knowledge_federation_enabled`) so the
    operator can flip it without restarting the service.
    """
    return os.getenv("KOI_FEDERATE_DOCUMENTS", "false").strip().lower() in {
        "1", "true", "yes", "on",
    }


def rid_allowlist() -> tuple[str, ...]:
    """Substrings a document RID must contain to be allowed to land.

    An unset variable yields the default. A variable set to an empty/whitespace
    string yields an EMPTY tuple, which allows nothing — the fail-closed
    reading. "Allow everything" has no representation on purpose.
    """
    raw = os.getenv("KOI_FEDERATE_DOCUMENTS_RID_ALLOW")
    if raw is None:
        return DEFAULT_RID_ALLOWLIST
    return tuple(p.strip() for p in raw.split(",") if p.strip())


def rid_allowed(rid: str) -> bool:
    """True when `rid` matches the containment allowlist."""
    if not rid:
        return False
    return any(pattern in rid for pattern in rid_allowlist())


def is_document_payload(contents: Any) -> bool:
    """True when `contents` has the shape of a federated document bundle.

    Coordinator bundles are `{document: {...}, metadata: {...}, processing:
    {...}}` and carry NO `_koi_domain` marker — that marker is written only by
    this node's own domain emitters. Shape is therefore the only signal
    available on the receive side.
    """
    if not isinstance(contents, Mapping):
        return False
    doc = contents.get("document")
    return isinstance(doc, Mapping) and bool(doc.get("content"))


def should_dispatch_as_document(rid: str, contents: Any) -> bool:
    """Whether the poller should route this event to the document handler.

    Matches either the bundle shape (a NEW/UPDATE carrying a body) or a
    contents-less event on an already-allowed RID (a FORGET). Both are then
    re-checked against the allowlist inside the handler, so this predicate
    being generous cannot itself land anything.
    """
    if not document_federation_enabled():
        return False
    if is_document_payload(contents):
        return True
    return rid_allowed(rid)


def newsletter_slug(contents: Mapping[str, Any]) -> Optional[str]:
    """Extract the publication slug from a bundle.

    Prefers `metadata.newsletter_slug` (populated 448/448 in the measured
    corpus) and falls back to the suffix of `document.source`
    (`newsletters:nate-jones-substack`).
    """
    meta = contents.get("metadata")
    if isinstance(meta, Mapping):
        slug = meta.get("newsletter_slug")
        if isinstance(slug, str) and slug.strip():
            return slug.strip()
    doc = contents.get("document")
    if isinstance(doc, Mapping):
        source = doc.get("source")
        if isinstance(source, str) and ":" in source:
            tail = source.split(":", 1)[1].strip()
            if tail:
                return tail
    return None


def resolve_author(slug: Optional[str]) -> str:
    """Map a publication slug to its author, or fail loudly.

    Never returns None and never consults `document.author` — that field is
    null across the entire measured corpus.
    """
    if not slug:
        raise UnmappedNewsletterSlug(
            "document has no newsletter_slug and no derivable document.source "
            "suffix; refusing to land it with a null author"
        )
    author = NEWSLETTER_SLUG_AUTHORS.get(slug)
    if not author:
        raise UnmappedNewsletterSlug(
            f"newsletter_slug {slug!r} has no entry in NEWSLETTER_SLUG_AUTHORS "
            f"(known: {sorted(NEWSLETTER_SLUG_AUTHORS)}). Add one before "
            f"widening KOI_FEDERATE_DOCUMENTS_RID_ALLOW — a null author makes "
            f"this document indistinguishable from another author's."
        )
    return author


def canonical_url(slug: Optional[str], contents: Mapping[str, Any]) -> Optional[str]:
    """The post's canonical URL, or None when the bundle does not carry one.

    **Measured 2026-08-21 across all 448 bundles — this corrects a plan
    assumption.** The plan recorded `document.url` as "448/448 canonical
    natesnewsletter.substack.com/p/...". It is **336/448**. The other 112 (25%)
    carry a tracking-pixel URL — `https://eotrx.substackcdn.com/o/<id>/p.gif
    ?token=<jwt>` — in BOTH `document.url` and `metadata.url`, so there is no
    fallback field inside the bundle. 224 such values base64-decode to a token
    containing the subscriber's own email address.

    Two consequences, both handled here by returning None:

    - A tracking pixel is not provenance. It does not resolve to the post and
      would be presented to a reader as the source link.
    - It is PII. `metadata.source_url` is copied onto every chunk so that search
      results can cite the source, which is precisely where a subscriber's email
      address should not appear.

    This deliberately does NOT reconstruct the URL by scanning the body: only 22
    of the 112 contain a canonical `/p/` link at all, and a body scan can match a
    link to a *different* post that the author referenced — producing a
    confidently wrong provenance link, which is worse than an absent one. The
    remaining 90 have no canonical URL anywhere in the snapshot; recovering those
    needs the archive listing, not a heuristic.
    """
    host = NEWSLETTER_SLUG_HOSTS.get(slug or "")
    doc = contents.get("document")
    meta = contents.get("metadata")
    candidates = []
    if isinstance(doc, Mapping):
        candidates.append(doc.get("url"))
    if isinstance(meta, Mapping):
        candidates.append(meta.get("url"))
    for url in candidates:
        if not isinstance(url, str) or not url.strip():
            continue
        url = url.strip()
        if not host:
            # Unknown publication: no host to validate against. Refuse rather
            # than trust — the allowlist should have stopped this already.
            return None
        if url.startswith(f"https://{host}/"):
            return url
    return None


# ── subscriber-PII redaction ────────────────────────────────────────────────
#
# Operator decision 2026-08-22: redact the subscriber's own address from
# federated document bodies at ingest. This MODIFIES STORED CONTENT — see
# `redact_subscriber_pii` for the fidelity tradeoff and what preserves the
# original.

REDACTED_EMAIL = "[redacted-subscriber-email]"
REDACTED_TOKEN = "[redacted-tracking-token]"

_B64_TOKEN = re.compile(r"eyJ[A-Za-z0-9_-]{20,}")

_warned_no_redaction_configured = False


def redaction_addresses() -> tuple[str, ...]:
    """Addresses to strip from federated document bodies.

    Read from `KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS` (comma-separated) and
    NEVER hardcoded: `regen-prod` pushes to `gaiaaiagent/koi-processor`, so a
    literal address in this file would be published. The operator's addresses
    live in the gitignored `config/personal.env`.
    """
    raw = os.getenv("KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS", "")
    return tuple(a.strip().lower() for a in raw.split(",") if a.strip())


def redact_subscriber_pii(text: str) -> tuple[str, int]:
    """Remove the subscriber's own address from a document body, in any encoding.

    Returns `(text, n_redactions)`.

    **Measured across the 448-bundle corpus (2026-08-21):** the subscriber
    address appears in plaintext **44 times in 44 documents**, and inside
    base64 tracking tokens **324 times in 112 documents**. Both forms are
    handled, because "the address must not survive ingestion" is one rule and a
    decodable token satisfies it no less than plaintext does.

    **FIDELITY TRADEOFF — this changes what is stored.** 121,064 of 16,412,636
    characters (**0.74%**) differ from what the publisher sent. What that costs
    and what protects against it:

    - The **verbatim original is preserved** in the Phase-1 snapshot at
      `~/.local/share/personal-koi/nate-jones-bundles/`, and the bundle's
      `manifest.sha256_hash` is computed over the untouched bundle, so
      provenance and integrity checking are unaffected. Only the INDEXED copy
      is redacted.
    - A quotation that happened to include the address will read with a
      placeholder. In this corpus every occurrence is Substack's own
      "you're receiving this at <address>" furniture, not authored prose.
    - Redacting a tracking token leaves the surrounding URL intact but inert.
      Those URLs are already refused as `source_url` (see `canonical_url`).

    Scoped deliberately to the CONFIGURED addresses. A blanket email-shaped
    redaction would be wrong: the corpus also contains the publisher's own
    contact addresses and reader addresses he quotes, which are content.
    """
    global _warned_no_redaction_configured
    addresses = redaction_addresses()
    if not addresses:
        if not _warned_no_redaction_configured:
            _warned_no_redaction_configured = True
            logger.warning(
                "KOI_FEDERATE_DOCUMENTS_REDACT_EMAILS is unset — federated document "
                "bodies will be indexed verbatim, including any subscriber address "
                "the publisher embedded in them."
            )
        return text, 0

    n = 0
    for addr in addresses:
        pattern = re.compile(re.escape(addr), re.IGNORECASE)
        text, hits = pattern.subn(REDACTED_EMAIL, text)
        n += hits

    # `subn` counts every token EXAMINED, not every one replaced, and counting
    # the placeholder in the output would miscount if it ever appeared in the
    # source. Count replacements at the point of replacement.
    replaced = 0

    def _scrub(match: "re.Match[str]") -> str:
        nonlocal replaced
        tok = match.group(0)
        try:
            payload = base64.urlsafe_b64decode(tok + "=" * (-len(tok) % 4))
        except Exception:  # noqa: BLE001 — a non-decodable lookalike is not a token
            return tok
        lowered = payload.lower()
        if any(addr.encode() in lowered for addr in addresses):
            replaced += 1
            return REDACTED_TOKEN
        return tok

    text = _B64_TOKEN.sub(_scrub, text)
    return text, n + replaced
