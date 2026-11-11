"""Provenance RDF escaping is injection-safe (offline).

Exercises src/core/provenance_to_rdf.py. On the base commit _escape_literal does
not escape backslash, tab or control characters and there is no _escape_iri, so
these fail there and pass here. Only the standard library and httpx (already a
dependency of the module) are needed; the rdflib property test is skipped where
rdflib is absent.
"""
from __future__ import annotations

import pathlib
import sys

import pytest

CORE = pathlib.Path(__file__).resolve().parents[1] / "src" / "core"
sys.path.insert(0, str(CORE))

from provenance_to_rdf import ProvenanceToRDF  # noqa: E402

# Synthetic adversarial inputs (no real RIDs): each tries to break out of a
# string literal or an IRI term.
ADVERSARIAL = [
    'plain',
    'has "quotes"',
    'back\\slash',
    'new\nline',
    'carriage\rreturn',
    'tab\tchar',
    'null\x00byte',
    'del\x7fchar',
    '" . } INSERT DATA { <doc:x> a <evil> } ; #',
    'end"^^xsd:integer',
    '\x01\x02\x1f control run',
]

IRI_BREAKOUTS = [
    'a/b',
    'space here',
    'angle>bracket',
    'quote"mark',
    'frag#ment',
    '../../escape',
    'back\\slash',
]


@pytest.fixture(scope="module")
def prdf():
    return ProvenanceToRDF()


def _quotes_all_escaped(s: str) -> bool:
    for i, ch in enumerate(s):
        if ch == '"' and (i == 0 or s[i - 1] != "\\"):
            return False
    return True


@pytest.mark.parametrize("payload", ADVERSARIAL)
def test_literal_escaping_leaves_no_breakout(prdf, payload):
    escaped = prdf._escape_literal(payload)
    # No raw control characters survive.
    assert all(ord(c) >= 0x20 and ord(c) != 0x7F for c in escaped), escaped
    # Every double quote is backslash-escaped.
    assert _quotes_all_escaped(escaped), escaped
    # No literal newline/carriage-return/tab survive.
    assert "\n" not in escaped and "\r" not in escaped and "\t" not in escaped


def test_backslash_is_doubled(prdf):
    # The base version does not escape backslash at all.
    assert prdf._escape_literal("a\\b") == "a\\\\b"


def test_control_chars_become_unicode_escapes(prdf):
    assert prdf._escape_literal("\x00") == "\\u0000"
    assert prdf._escape_literal("\x1f") == "\\u001F"
    assert prdf._escape_literal("\x7f") == "\\u007F"


def test_none_literal_is_empty(prdf):
    assert prdf._escape_literal(None) == ""


@pytest.mark.parametrize("payload", IRI_BREAKOUTS)
def test_iri_escaping_percent_encodes_everything(prdf, payload):
    escaped = prdf._escape_iri(payload)
    for forbidden in ("/", " ", ">", "<", '"', "#", "\\"):
        assert forbidden not in escaped, (payload, escaped)


def test_escape_iri_exists(prdf):
    # Absent on the base commit.
    assert callable(getattr(prdf, "_escape_iri", None))


def test_adversarial_update_parses_to_one_insertdata(prdf):
    """AC5 (rdflib half): each escaped value yields exactly one valid InsertData
    operation. A broken escape would either fail to parse or split into more than
    one operation (the injection closing the literal/block)."""
    pytest.importorskip("rdflib")
    from rdflib.plugins.sparql.parser import parseUpdate

    for payload in ADVERSARIAL:
        lit = prdf._escape_literal(payload)
        iri = prdf._escape_iri(payload)
        update = prdf.prefixes + (
            f'\nINSERT DATA {{ <doc:{iri}> rdfs:label "{lit}" ; '
            f'koi:rid "{lit}" ; koi:eventType "NEW" . }}'
        )
        operations = list(parseUpdate(update)["request"])
        names = [getattr(op, "name", None) for op in operations]
        assert names == ["InsertData"], (payload, names)
