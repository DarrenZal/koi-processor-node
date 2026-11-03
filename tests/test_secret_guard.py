"""Tests for the credential write-path guard.

The positive controls below are SHAPES MEASURED FROM THE REAL CREDENTIAL FACTS
that sat in knowledge_facts for ~4.5 months (retracted 2026-09-01), not shapes
invented for the test. That distinction is the whole point: the obvious guard to
write requires mixed upper+lower+digit, and every one of the three real tokens is
32-char *lowercase hex*, so an invented control would have passed while the
actual secrets walked through.

No real secret value appears here. Only the measured shape is reproduced.
"""

import pytest

from api.secret_guard import check_fact


# --- positive controls: shapes measured from real credential facts -----------

@pytest.mark.parametrize(
    "predicate,literal,shape_source",
    [
        ("HAS_PASSWORD", "Xk7#mQ2$vLp9!wRt3@zN", "24-char with symbols, mixed case"),
        ("HAS_TOKEN", "a3f9c2e14b7d8065fa219c3e4d5b6a70", "32-char LOWERCASE HEX -- the shape a mixed-class check misses"),
        ("USES_TOKEN", "b7e2d9143c6a850fe2b1479c3d6e5a08", "32-char hex; found live and missed by predicate-name cleanup"),
        ("MENTIONS", "-----BEGIN OPENSSH PRIVATE KEY-----b3BlbnNzaC1rZXk", "PEM private-key header"),
    ],
)
def test_real_credential_shapes_are_rejected(predicate, literal, shape_source):
    flagged, reason = check_fact(predicate, literal)
    assert flagged, f"MISSED a real credential shape ({shape_source})"
    assert reason
    assert literal not in reason, "the reason must never echo the value"


@pytest.mark.parametrize(
    "literal",
    [
        "sk-abcdefghijklmnopqrstuvwxyz0123456789",
        "ghp_abcdefghijklmnopqrstuvwxyz0123456789",
        "AKIAIOSFODNN7EXAMPLE",
        "xoxb-1234567890-abcdefghij",
    ],
)
def test_vendor_prefixes_rejected_regardless_of_predicate(literal):
    flagged, _ = check_fact("MENTIONS", literal)
    assert flagged


# --- negative controls: shapes drawn from the real 1,233-row population -------

@pytest.mark.parametrize(
    "predicate,literal",
    [
        ("HAS_DOCUMENT", "https://example.org/speakers/jane-doe/"),
        ("LOCATED_IN", "/Users/example/projects/notes-app"),
        ("HAS_URI", "orn:personal-koi.entity:person-jane-doe-0a1b2c3d4e5f"),
        ("MENTIONS", "second-brain-open-kg"),
        ("HAS_DOCUMENT", "https://wiki.p2pfoundation.net/Category:Mutual_Coordination"),
        ("HAS_WIREGUARD_ADDRESS", "10.100.0.1"),
        ("HAS_SESSION", "550e8400-e29b-41d4-a716-446655440000"),
        ("MENTIONS", "a normal sentence fragment with spaces in it"),
        ("HAS_VERSION", "3.11.13"),
    ],
)
def test_legitimate_literals_pass(predicate, literal):
    flagged, reason = check_fact(predicate, literal)
    assert not flagged, f"false positive on legitimate literal: {reason}"


def test_content_hash_predicate_is_exempt_from_the_hex_rule():
    """A 64-char SHA256 under a digest-named predicate is legitimate.

    This exemption is why the measured false-positive rate is 0.081% and not
    higher -- HAS_CONTENT_HASH was the one FP before it was added.
    """
    sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert not check_fact("HAS_CONTENT_HASH", sha256)[0]
    # ...but the same value under a credential-named predicate is still refused.
    assert check_fact("HAS_TOKEN", sha256)[0]


def test_word_boundary_regression():
    """`\\bTOKEN\\b` never matches HAS_TOKEN because '_' is a word character.

    An early version of the pattern used word boundaries and was therefore
    silently inert on the exact real-world case. This test exists so that
    'refinement' cannot be reintroduced.
    """
    for predicate in ("HAS_TOKEN", "USES_TOKEN", "API_KEY", "HAS_PASSWORD", "STORES_SECRET"):
        assert check_fact(predicate, "x")[0], f"{predicate} must be caught"


def test_reason_never_contains_the_value():
    """The reason goes into a 422 body and the logs. A secret must not follow it."""
    secret = "a3f9c2e14b7d8065fa219c3e4d5b6a70"
    flagged, reason = check_fact("MENTIONS", secret)
    assert flagged
    assert secret not in reason


# --- USERNAME tier (added 2026-09-02 at operator request) --------------------

def test_username_with_a_literal_is_refused():
    """The real SSH_USERNAME fact was a 10-char lowercase string.

    No entropy or shape test can distinguish that from prose, so the predicate
    name is the only signal available. If this rule misses it, nothing else
    catches it.
    """
    assert check_fact("SSH_USERNAME", "janedoexy")[0]
    assert check_fact("HAS_USERNAME", "jane.doexy")[0]


@pytest.mark.parametrize("predicate", ["SENT_GITHUB_USERNAME_TO", "USERNAME_FOR"])
def test_relational_username_facts_with_no_literal_are_allowed(predicate):
    """Measured from the live corpus: these carry an ENTITY object, not a value.

    object_literal IS NULL means there is nothing to leak, so refusing them
    would be a false positive.
    """
    assert not check_fact(predicate, None)[0]
    assert not check_fact(predicate, "")[0]


def test_strong_credential_predicates_are_refused_even_with_no_literal():
    """Unlike usernames, these are refused unconditionally.

    A fact asserting a password/token is wrong regardless of where the value
    sits -- it may be in fact_text rather than object_literal.
    """
    for predicate in ("HAS_PASSWORD", "HAS_TOKEN", "API_KEY", "PRIVATE_KEY_FOR"):
        assert check_fact(predicate, None)[0], f"{predicate} must be refused even with no literal"
