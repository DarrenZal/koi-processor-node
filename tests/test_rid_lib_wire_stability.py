"""rid-lib canonicalization must stay byte-stable across version bumps.

`sha256_hash_json` is what produces manifest hashes on the wire. Every peer in
the mesh compares those hashes to decide whether a file changed. If a rid-lib
upgrade altered canonicalization — key ordering, unicode normalization,
separator choice — every manifest hash would change at once. Nothing would
error; the mesh would simply decide every file everywhere had been modified.

That is a silent, total, cross-peer event, so it gets a pinned test rather than
a code review.

The vectors below were computed under rid-lib 3.2.12 and re-verified identical
under 3.3.0 on 2026-08-26. If this test fails after a bump, the bump is NOT
safe to deploy without a coordinated mesh-wide rehash — investigate before
updating the expected values.
"""

import pytest

from rid_lib.ext.utils import sha256_hash_json

# (label, payload, expected sha256) — pinned under rid-lib 3.2.12
VECTORS = [
    (
        "koi-net conformant manifest",
        {
            "rid": "orn:koi-net.vault-file:Shared/x.md",
            "timestamp": "2026-08-26T00:00:00+00:00",
            "sha256_hash": "a" * 64,
        },
        "ffecc739661717f6d46b66ee196a7f7451410fb66b8cd767ef079aad572b2940",
    ),
    (
        "key ordering + nested + null",
        {"b": 2, "a": 1, "nested": {"z": [1, 2, 3], "y": None}},
        "f623b1cc1b379b1ebcffe98d98969fb154114280d46049c339b71ed78ff150a0",
    ),
    (
        "unicode + empty containers",
        {"unicode": "héllo wörld ✓", "empty": {}, "list": []},
        "c0f7e42f26970566543af312d2a484b22ed008b8246895612ae824b02b645390",
    ),
]


@pytest.mark.parametrize("label,payload,expected", VECTORS, ids=[v[0] for v in VECTORS])
def test_canonical_hash_is_stable(label, payload, expected):
    assert sha256_hash_json(payload) == expected, (
        f"rid-lib canonicalization changed for {label!r}. Every manifest hash in "
        f"the mesh would change. Do not update this value without a coordinated rehash."
    )


def test_key_order_does_not_affect_hash():
    """Positive control: the hash must be order-independent, or the vectors above
    would be pinning an accident of dict construction rather than canonicalization."""
    assert sha256_hash_json({"a": 1, "b": 2}) == sha256_hash_json({"b": 2, "a": 1})
