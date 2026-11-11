"""Throwaway Postgres for the peer-guard tests.

The tests need real Postgres semantics (ON CONFLICT, delivered_to arrays, the vault-sync state rows). They always
start their own cluster with initdb in a temp directory, listening on a Unix socket only, build the schema from this
repository's own migration files, and reset only a database that carries this suite's marker table. There is no way
to point them at another database: no URL or environment variable is read.
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from peer_guards_db import MARKER, TEST_DATABASE

REPO = Path(__file__).resolve().parents[2]

# The migrations that create and alter the KOI-net and vault-sync tables, in file order, and federation_rid_blocks,
# without which the poll and fetch routes serve nothing. The same tables come out column for column as from a run of
# every migration.
MIGRATIONS = [
    "039_koi_net_events.sql",
    "040_koi_net_federation.sql",
    "042_koi_net_event_dedup.sql",
    "043_event_target_node.sql",
    "045_outbound_share_ledger.sql",
    "046_koi_net_nodes_ontology_columns.sql",
    "046_node_ontology_fields.sql",
    "047_event_dedup.sql",
    "048_event_target_node.sql",
    "049_peer_aliases.sql",
    "049_vault_sync.sql",
    "050_shared_documents.sql",
    "050_vault_sync_metrics.sql",
    "057_encryption_key.sql",
    "060_multi_peer_sync.sql",
    "061_edge_status_index.sql",
    "080_multi_folder_sync.sql",
    "100_federation_applied_events.sql",
    "137_federation_rid_blocks.sql",
]
def _find(binary: str):
    found = shutil.which(binary)
    if found:
        return found
    for pattern in (f"/usr/lib/postgresql/*/bin/{binary}", f"/opt/homebrew/opt/postgresql@*/bin/{binary}"):
        hits = sorted(glob.glob(pattern))
        if hits:
            return hits[-1]
    return None


# The cluster listens on no TCP address (listen_addresses=''), so the port only names the socket file inside this
# cluster's own new socket directory, and a fixed number cannot collide with another server.
PORT = 54329


@pytest.fixture(scope="session")
def pg():
    """Connection kwargs for this suite's marked database in a throwaway cluster."""
    initdb, pg_ctl, psql = _find("initdb"), _find("pg_ctl"), _find("psql")
    if not (initdb and pg_ctl and psql):
        pytest.skip("no initdb/pg_ctl/psql: these tests need a throwaway Postgres cluster")
    root = tempfile.mkdtemp(prefix="pgd-")
    data, sock = os.path.join(root, "data"), os.path.join(root, "s")
    started = False
    try:
        os.mkdir(sock)
        port = PORT
        subprocess.run([initdb, "-D", data, "-U", "peer_guards", "-A", "trust", "-E", "UTF8", "--no-locale"],
                       check=True, capture_output=True)
        subprocess.run([pg_ctl, "-D", data, "-l", os.path.join(root, "log"), "-w",
                        "-o", f"-p {port} -k {sock} -c listen_addresses=''", "start"], check=True, capture_output=True)
        started = True
        base = [psql, "-h", sock, "-p", str(port), "-U", "peer_guards", "-X", "-q", "-v", "ON_ERROR_STOP=1"]
        subprocess.run(base + ["-d", "postgres", "-c", f"CREATE DATABASE {TEST_DATABASE}"], check=True,
                       capture_output=True)
        subprocess.run(base + ["-d", TEST_DATABASE, "-c", f"CREATE TABLE {MARKER} (created_at TIMESTAMPTZ DEFAULT NOW())"],
                       check=True, capture_output=True)
        for name in MIGRATIONS:
            subprocess.run(base + ["-d", TEST_DATABASE, "-f", str(REPO / "migrations" / name)], check=True,
                           capture_output=True)
        yield {"host": sock, "port": port, "user": "peer_guards", "database": TEST_DATABASE}
    finally:
        if started:
            subprocess.run([pg_ctl, "-D", data, "-m", "immediate", "stop"], capture_output=True)
        shutil.rmtree(root, ignore_errors=True)
