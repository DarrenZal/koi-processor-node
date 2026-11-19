"""Divergence 14: a peer's base_url is joined to KOI-net paths the same way everywhere.

koi-net advertises the KOI-net API root (`http://host:8000/koi-net`) and its
RequestHandler calls `base_url + path`. Our nodes have advertised the host root and joined
`/koi-net/...`, so a stock node calling us got 404s and our poller calling a stock node
doubled the prefix. api.koi_protocol.koi_net_url accepts both forms, and every peer call
site goes through it.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from api.koi_protocol import koi_net_url

REPO = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("base", [
    "http://127.0.0.1:8000/koi-net",   # koi-net's form (ServerConfig.url)
    "http://127.0.0.1:8000/koi-net/",
    "http://127.0.0.1:8000",           # our nodes' form so far
    "http://127.0.0.1:8000/",
])
@pytest.mark.parametrize("path", ["/events/poll", "events/poll"])
def test_both_base_url_forms_reach_the_same_endpoint(base, path):
    assert koi_net_url(base, path) == "http://127.0.0.1:8000/koi-net/events/poll"


def test_a_base_url_behind_a_proxy_path_keeps_it():
    # ServerConfig: a node may be served at https://example.com/some/prefix/koi-net
    assert koi_net_url("https://example.com/some/prefix/koi-net", "/rids/fetch") == \
        "https://example.com/some/prefix/koi-net/rids/fetch"
    assert koi_net_url("https://example.com/some/prefix", "/rids/fetch") == \
        "https://example.com/some/prefix/koi-net/rids/fetch"


def test_no_call_site_joins_koi_net_paths_by_hand():
    """Census: every peer request in api/ builds its URL with koi_net_url."""
    offenders = []
    for path in sorted((REPO / "api").rglob("*.py")):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if re.search(r"\{[^}]*\}/koi-net/", line) or re.search(r"\+\s*[\"']/koi-net/", line):
                offenders.append(f"{path.relative_to(REPO)}:{n}: {line.strip()}")
    assert not offenders, "\n".join(offenders)
    # Positive control: the census would have caught the joins this change replaced.
    assert re.search(r"\{[^}]*\}/koi-net/", 'f"{base_url}/koi-net/events/poll"')
