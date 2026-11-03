"""Tests for graph IRI generation and MsgAttest broadcast (Phase 3).

Verifies:
- URDNA2015 canonicalization → BLAKE2b-256 → base58check → regen:*.rdf IRI
- Graph IRI matches regen-server's generateIRIFromGraph algorithm
- MsgAttest broadcast function structure
- Invalid input handling
"""

import hashlib
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

from api.ledger_anchor import (
    ATTESTATION_JSONLD_CONTEXT,
    _base58_encode,
    _base58check_encode,
    _content_hash_graph_to_iri,
    build_attestation_jsonld,
    generate_graph_iri,
)


class TestBase58Encode(unittest.TestCase):
    """Test base58 encoding matches Bitcoin/btcsuite implementation."""

    def test_empty_bytes(self):
        result = _base58_encode(b"")
        self.assertEqual(result, "")

    def test_single_zero(self):
        result = _base58_encode(b"\x00")
        self.assertEqual(result, "1")

    def test_known_value(self):
        # "Hello" in base58 should be "9Ajdvzr"
        result = _base58_encode(b"Hello")
        self.assertEqual(result, "9Ajdvzr")


class TestBase58CheckEncode(unittest.TestCase):
    """Test base58check encoding with version byte + SHA256d checksum."""

    def test_deterministic(self):
        payload = b"\x01\x02\x03"
        r1 = _base58check_encode(payload, 0)
        r2 = _base58check_encode(payload, 0)
        self.assertEqual(r1, r2)

    def test_version_changes_output(self):
        payload = b"\x01\x02\x03"
        v0 = _base58check_encode(payload, 0)
        v1 = _base58check_encode(payload, 1)
        self.assertNotEqual(v0, v1)


class TestContentHashGraphToIRI(unittest.TestCase):
    """Test graph IRI construction from BLAKE2b-256 hash."""

    def test_produces_rdf_extension(self):
        fake_hash = b"\x00" * 32
        iri = _content_hash_graph_to_iri(fake_hash)
        self.assertTrue(iri.startswith("regen:"))
        self.assertTrue(iri.endswith(".rdf"))

    def test_deterministic(self):
        fake_hash = hashlib.blake2b(b"test data", digest_size=32).digest()
        iri1 = _content_hash_graph_to_iri(fake_hash)
        iri2 = _content_hash_graph_to_iri(fake_hash)
        self.assertEqual(iri1, iri2)

    def test_different_hashes_different_iris(self):
        h1 = hashlib.blake2b(b"data1", digest_size=32).digest()
        h2 = hashlib.blake2b(b"data2", digest_size=32).digest()
        iri1 = _content_hash_graph_to_iri(h1)
        iri2 = _content_hash_graph_to_iri(h2)
        self.assertNotEqual(iri1, iri2)


class TestGenerateGraphIRI(unittest.TestCase):
    """Test full graph IRI generation from JSON-LD documents."""

    def test_attestation_jsonld(self):
        """Generate graph IRI from a sample attestation JSON-LD doc."""
        doc = {
            "@context": ATTESTATION_JSONLD_CONTEXT,
            "@type": "rfs:Attestation",
            "attestation_rid": "orn:koi-net.attestation:test123",
            "claim_rid": "orn:koi-net.claim:claim456",
            "reviewer_uri": "orn:koi-net.entity:reviewer1",
            "verdict": "approved",
            "rationale": "Data verified against source",
            "evidence_uris": ["orn:koi-net.entity:ev1"],
        }
        iri = generate_graph_iri(doc)
        self.assertTrue(iri.startswith("regen:"))
        self.assertTrue(iri.endswith(".rdf"))

    def test_deterministic_output(self):
        """Same document always produces same IRI."""
        doc = {
            "@context": ATTESTATION_JSONLD_CONTEXT,
            "@type": "rfs:Attestation",
            "attestation_rid": "test",
            "claim_rid": "test",
            "reviewer_uri": "test",
            "verdict": "approved",
            "rationale": "",
            "evidence_uris": [],
        }
        iri1 = generate_graph_iri(doc)
        iri2 = generate_graph_iri(doc)
        self.assertEqual(iri1, iri2)

    def test_different_content_different_iri(self):
        """Different content produces different IRIs."""
        base = {
            "@context": ATTESTATION_JSONLD_CONTEXT,
            "@type": "rfs:Attestation",
            "attestation_rid": "test",
            "claim_rid": "test",
            "reviewer_uri": "test",
            "verdict": "approved",
            "rationale": "",
            "evidence_uris": [],
        }
        doc2 = {**base, "verdict": "rejected"}
        iri1 = generate_graph_iri(base)
        iri2 = generate_graph_iri(doc2)
        self.assertNotEqual(iri1, iri2)

    def test_empty_doc_raises(self):
        """Empty/invalid JSON-LD should raise ValueError."""
        with self.assertRaises(ValueError):
            generate_graph_iri({})

    def test_iri_matches_manual_computation(self):
        """Cross-check: manually compute the IRI and compare.

        Steps matching regen-server generateIRIFromGraph:
        1. URDNA2015 canonicalize → n-quads string
        2. BLAKE2b-256 of n-quads bytes
        3. Prefix bytes [1, 1, 0, 1] + hash
        4. base58check with version 0
        5. regen:{result}.rdf
        """
        from pyld import jsonld

        doc = {
            "@context": ATTESTATION_JSONLD_CONTEXT,
            "@type": "rfs:Attestation",
            "attestation_rid": "orn:koi-net.attestation:manual-test",
            "claim_rid": "orn:koi-net.claim:manual-claim",
            "reviewer_uri": "orn:koi-net.entity:manual-reviewer",
            "verdict": "approved",
            "rationale": "Manual test",
            "evidence_uris": [],
        }

        # Step 1: canonicalize
        nquads = jsonld.normalize(
            doc, {"algorithm": "URDNA2015", "format": "application/n-quads"}
        )
        self.assertTrue(len(nquads) > 0)

        # Step 2: BLAKE2b-256
        blake_hash = hashlib.blake2b(nquads.encode("utf-8"), digest_size=32).digest()

        # Step 3-5: construct IRI
        expected_iri = _content_hash_graph_to_iri(blake_hash)

        # Compare with generate_graph_iri
        actual_iri = generate_graph_iri(doc)
        self.assertEqual(actual_iri, expected_iri)


class TestBuildAttestationJsonLD(unittest.TestCase):
    """Test attestation JSON-LD document builder."""

    def test_required_fields(self):
        row = {
            "attestation_rid": "att1",
            "claim_rid": "claim1",
            "reviewer_uri": "reviewer1",
            "verdict": "approved",
            "rationale": "Looks good",
            "evidence_uris": ["ev1", "ev2"],
        }
        doc = build_attestation_jsonld(row)
        self.assertEqual(doc["@type"], "rfs:Attestation")
        self.assertIn("@context", doc)
        self.assertEqual(doc["attestation_rid"], "att1")
        self.assertEqual(doc["evidence_uris"], ["ev1", "ev2"])

    def test_empty_rationale(self):
        row = {
            "attestation_rid": "att1",
            "claim_rid": "claim1",
            "reviewer_uri": "reviewer1",
            "verdict": "approved",
            "rationale": None,
            "evidence_uris": None,
        }
        doc = build_attestation_jsonld(row)
        self.assertEqual(doc["rationale"], "")
        self.assertEqual(doc["evidence_uris"], [])

    def test_evidence_sorted(self):
        row = {
            "attestation_rid": "att1",
            "claim_rid": "claim1",
            "reviewer_uri": "reviewer1",
            "verdict": "approved",
            "rationale": "",
            "evidence_uris": ["z_ev", "a_ev", "m_ev"],
        }
        doc = build_attestation_jsonld(row)
        self.assertEqual(doc["evidence_uris"], ["a_ev", "m_ev", "z_ev"])


class TestBroadcastAttest(unittest.TestCase):
    """Test broadcast_attest() function structure (mocked CLI)."""

    @patch("api.ledger_anchor._check_regen_cli", return_value="/usr/bin/regen")
    @patch("api.ledger_anchor._run_async", new_callable=AsyncMock)
    def test_happy_path(self, mock_run, mock_cli):
        """Successful MsgAttest broadcast + confirmation."""
        import asyncio

        # First call: broadcast
        broadcast_response = MagicMock()
        broadcast_response.returncode = 0
        broadcast_response.stdout = '{"txhash": "ABCDEF123456"}'

        # Second call: query tx (confirmed)
        query_response = MagicMock()
        query_response.returncode = 0
        query_response.stdout = '{"code": 0, "timestamp": "2026-04-02T12:00:00Z"}'

        mock_run.side_effect = [broadcast_response, query_response]

        from api.ledger_anchor import broadcast_attest
        with patch("api.ledger_anchor.asyncio.sleep", new_callable=AsyncMock):
            result = asyncio.run(
                broadcast_attest("att-123", "regen:test123.rdf", signer="test-key")
            )
        self.assertTrue(result["ready_to_anchor"])
        self.assertEqual(result["tx_hash"], "ABCDEF123456")
        self.assertEqual(result["ledger_iri"], "regen:test123.rdf")

    @patch("api.ledger_anchor._check_regen_cli", return_value="/usr/bin/regen")
    @patch("api.ledger_anchor._run_async", new_callable=AsyncMock)
    def test_key_not_found(self, mock_run, mock_cli):
        """Key not found error returns ready_to_anchor=False."""
        import asyncio

        error_response = MagicMock()
        error_response.returncode = 1
        error_response.stderr = "Error: key not found"
        error_response.stdout = ""
        mock_run.return_value = error_response

        from api.ledger_anchor import broadcast_attest
        result = asyncio.run(
            broadcast_attest("att-123", "regen:test.rdf")
        )
        self.assertFalse(result["ready_to_anchor"])
        self.assertIn("key", result["reason"].lower())

    @patch("api.ledger_anchor._check_regen_cli", return_value="/usr/bin/regen")
    @patch("api.ledger_anchor._run_async", new_callable=AsyncMock)
    @patch("api.ledger_anchor.asyncio.sleep", new_callable=AsyncMock)  # skip actual sleeping
    def test_timeout(self, mock_sleep, mock_run, mock_cli):
        """Broadcast succeeds but confirmation times out."""
        import asyncio

        broadcast_response = MagicMock()
        broadcast_response.returncode = 0
        broadcast_response.stdout = '{"txhash": "TIMEOUT_TX"}'

        # All query attempts fail (tx not found)
        query_fail = MagicMock()
        query_fail.returncode = 1
        query_fail.stderr = "tx not found"

        mock_run.side_effect = [broadcast_response] + [query_fail] * 6

        from api.ledger_anchor import broadcast_attest
        result = asyncio.run(
            broadcast_attest("att-timeout", "regen:timeout.rdf")
        )
        self.assertFalse(result["ready_to_anchor"])
        self.assertEqual(result["tx_hash"], "TIMEOUT_TX")
        self.assertIn("timed out", result["reason"].lower())


if __name__ == "__main__":
    unittest.main()
