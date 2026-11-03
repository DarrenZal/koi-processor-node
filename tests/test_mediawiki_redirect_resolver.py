"""
Tests for migration 083: mediawiki_resolve_redirect function + v_mediawiki_page_resolved view.

Validates: zero-hop identity, one-hop, two-hop chain, broken target, cycle,
depth cap, cross-wiki isolation, section-fragment strip, pipe-label strip,
duplicate (wiki_id, title) tie-break, view columns (canonical_rid, hops,
resolution_status).

Uses a transaction that rolls back after each test — no persistent data changes.
Requires a running PostgreSQL with the personal_koi schema and migration 083 applied.
"""

import uuid

import pytest
import asyncpg
from tests import koi_test_targets as targets


WIKI_NAME = f"test_plan083_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    """Single connection with a transaction that rolls back."""
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    yield _conn
    await tx.rollback()
    await _conn.close()


async def setup_wiki(conn) -> int:
    """Insert a test wiki and return its id."""
    return await conn.fetchval(
        """INSERT INTO mediawiki_wikis (base_url, api_url, wiki_name, status)
           VALUES ($1, $2, $3, 'active') RETURNING id""",
        f"https://{WIKI_NAME}", f"https://{WIKI_NAME}/api.php", WIKI_NAME,
    )


async def insert_page(conn, wiki_id: int, title: str, page_id: int,
                       is_redirect: bool = False, redirect_target: str = None) -> int:
    """Insert a page_state row and return its id."""
    return await conn.fetchval(
        """INSERT INTO mediawiki_page_state
           (wiki_id, page_id, title, is_redirect, redirect_target, source_rid)
           VALUES ($1, $2, $3, $4, $5, $6) RETURNING id""",
        wiki_id, page_id, title, is_redirect, redirect_target,
        f"mediawiki:{WIKI_NAME}:{page_id}",
    )


# =============================================================================
# Function tests: mediawiki_resolve_redirect
# =============================================================================

@pytest.mark.anyio
async def test_non_redirect_identity(conn):
    """Non-redirect page returns its own id."""
    wid = await setup_wiki(conn)
    pid = await insert_page(conn, wid, "Canonical Page", 100)
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "Canonical Page", wid)
    assert result == pid


@pytest.mark.anyio
async def test_one_hop_redirect(conn):
    """One-hop redirect returns target's id."""
    wid = await setup_wiki(conn)
    await insert_page(conn, wid, "Alias", 101, is_redirect=True, redirect_target="Target")
    target_id = await insert_page(conn, wid, "Target", 102)
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "Alias", wid)
    assert result == target_id


@pytest.mark.anyio
async def test_two_hop_chain(conn):
    """A → B → C resolves to C."""
    wid = await setup_wiki(conn)
    await insert_page(conn, wid, "A", 200, is_redirect=True, redirect_target="B")
    await insert_page(conn, wid, "B", 201, is_redirect=True, redirect_target="C")
    c_id = await insert_page(conn, wid, "C", 202)
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "A", wid)
    assert result == c_id


@pytest.mark.anyio
async def test_broken_target(conn):
    """Redirect to non-existent title returns NULL."""
    wid = await setup_wiki(conn)
    await insert_page(conn, wid, "Orphan", 300, is_redirect=True, redirect_target="NoSuchPage")
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "Orphan", wid)
    assert result is None


@pytest.mark.anyio
async def test_cycle_detection(conn):
    """A → B → A cycle returns NULL."""
    wid = await setup_wiki(conn)
    await insert_page(conn, wid, "CycA", 400, is_redirect=True, redirect_target="CycB")
    await insert_page(conn, wid, "CycB", 401, is_redirect=True, redirect_target="CycA")
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "CycA", wid)
    assert result is None


@pytest.mark.anyio
async def test_depth_exceeded(conn):
    """Chain of 6 hops (exceeds 5-hop cap) returns NULL."""
    wid = await setup_wiki(conn)
    for i in range(6):
        await insert_page(conn, wid, f"D{i}", 500 + i,
                          is_redirect=True, redirect_target=f"D{i+1}")
    await insert_page(conn, wid, "D6", 506)  # canonical, but too far
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "D0", wid)
    assert result is None


@pytest.mark.anyio
async def test_cross_wiki_isolation(conn):
    """Same title in wiki 1 vs wiki 2 resolves to different ids."""
    wid1 = await setup_wiki(conn)
    # Need a second wiki with a different base_url
    wid2 = await conn.fetchval(
        """INSERT INTO mediawiki_wikis (base_url, api_url, wiki_name, status)
           VALUES ($1, $2, $3, 'active') RETURNING id""",
        f"https://other-{WIKI_NAME}", f"https://other-{WIKI_NAME}/api.php",
        f"other-{WIKI_NAME}",
    )
    id1 = await insert_page(conn, wid1, "SharedTitle", 600)
    id2 = await insert_page(conn, wid2, "SharedTitle", 601)
    r1 = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "SharedTitle", wid1)
    r2 = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "SharedTitle", wid2)
    assert r1 == id1
    assert r2 == id2
    assert r1 != r2


@pytest.mark.anyio
async def test_section_fragment_strip(conn):
    """redirect_target 'Target#Section' resolves to 'Target'."""
    wid = await setup_wiki(conn)
    await insert_page(conn, wid, "SectAlias", 700, is_redirect=True,
                       redirect_target="SectTarget#SomeSection")
    target_id = await insert_page(conn, wid, "SectTarget", 701)
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "SectAlias", wid)
    assert result == target_id


@pytest.mark.anyio
async def test_pipe_label_strip(conn):
    """redirect_target 'Target|Label' resolves to 'Target'."""
    wid = await setup_wiki(conn)
    await insert_page(conn, wid, "PipeAlias", 800, is_redirect=True,
                       redirect_target="PipeTarget|Display Name")
    target_id = await insert_page(conn, wid, "PipeTarget", 801)
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "PipeAlias", wid)
    assert result == target_id


@pytest.mark.anyio
async def test_duplicate_title_tiebreak(conn):
    """Two rows with same (wiki_id, title): ORDER BY id DESC wins."""
    wid = await setup_wiki(conn)
    _old_id = await insert_page(conn, wid, "Dupe", 900)
    new_id = await insert_page(conn, wid, "Dupe", 901)
    result = await conn.fetchval(
        "SELECT mediawiki_resolve_redirect($1, $2)", "Dupe", wid)
    assert result == new_id


# =============================================================================
# View tests: v_mediawiki_page_resolved
# =============================================================================

@pytest.mark.anyio
async def test_view_non_redirect(conn):
    """Non-redirect: canonical_id = self, hops = 0, status = NULL."""
    wid = await setup_wiki(conn)
    pid = await insert_page(conn, wid, "ViewCanon", 1000)
    row = await conn.fetchrow(
        "SELECT canonical_id, hops, resolution_status, canonical_rid "
        "FROM v_mediawiki_page_resolved WHERE id = $1", pid)
    assert row["canonical_id"] == pid
    assert row["hops"] == 0
    assert row["resolution_status"] is None
    assert row["canonical_rid"] == f"mediawiki:{WIKI_NAME}:1000"


@pytest.mark.anyio
async def test_view_one_hop(conn):
    """One-hop redirect: canonical_id = target, hops = 1, status = 'resolved'."""
    wid = await setup_wiki(conn)
    alias_id = await insert_page(conn, wid, "ViewAlias", 1100,
                                  is_redirect=True, redirect_target="ViewTarget")
    target_id = await insert_page(conn, wid, "ViewTarget", 1101)
    row = await conn.fetchrow(
        "SELECT canonical_id, canonical_page_id, hops, resolution_status, canonical_rid "
        "FROM v_mediawiki_page_resolved WHERE id = $1", alias_id)
    assert row["canonical_id"] == target_id
    assert row["canonical_page_id"] == 1101
    assert row["hops"] == 1
    assert row["resolution_status"] == "resolved"
    assert row["canonical_rid"] == f"mediawiki:{WIKI_NAME}:1101"


@pytest.mark.anyio
async def test_view_two_hop(conn):
    """Two-hop: hops = 2."""
    wid = await setup_wiki(conn)
    a_id = await insert_page(conn, wid, "V2A", 1200, is_redirect=True, redirect_target="V2B")
    await insert_page(conn, wid, "V2B", 1201, is_redirect=True, redirect_target="V2C")
    c_id = await insert_page(conn, wid, "V2C", 1202)
    row = await conn.fetchrow(
        "SELECT canonical_id, hops, resolution_status "
        "FROM v_mediawiki_page_resolved WHERE id = $1", a_id)
    assert row["canonical_id"] == c_id
    assert row["hops"] == 2
    assert row["resolution_status"] == "resolved"


@pytest.mark.anyio
async def test_view_missing_target(conn):
    """Broken redirect: canonical_id = NULL, status = 'missing_target'."""
    wid = await setup_wiki(conn)
    orphan_id = await insert_page(conn, wid, "ViewOrphan", 1300,
                                   is_redirect=True, redirect_target="Gone")
    row = await conn.fetchrow(
        "SELECT canonical_id, hops, resolution_status "
        "FROM v_mediawiki_page_resolved WHERE id = $1", orphan_id)
    assert row["canonical_id"] is None
    assert row["hops"] is None
    assert row["resolution_status"] == "missing_target"


@pytest.mark.anyio
async def test_view_cycle(conn):
    """Cycle: canonical_id = NULL, status = 'cycle'."""
    wid = await setup_wiki(conn)
    cyc_id = await insert_page(conn, wid, "VCycA", 1400,
                                is_redirect=True, redirect_target="VCycB")
    await insert_page(conn, wid, "VCycB", 1401,
                       is_redirect=True, redirect_target="VCycA")
    row = await conn.fetchrow(
        "SELECT canonical_id, hops, resolution_status "
        "FROM v_mediawiki_page_resolved WHERE id = $1", cyc_id)
    assert row["canonical_id"] is None
    assert row["resolution_status"] in ("cycle", "missing_target")


@pytest.mark.anyio
async def test_view_depth_exceeded(conn):
    """6-hop chain: canonical_id = NULL, status = 'depth_exceeded'."""
    wid = await setup_wiki(conn)
    first_id = None
    for i in range(6):
        pid = await insert_page(conn, wid, f"VD{i}", 1500 + i,
                                 is_redirect=True, redirect_target=f"VD{i+1}")
        if i == 0:
            first_id = pid
    await insert_page(conn, wid, "VD6", 1506)
    row = await conn.fetchrow(
        "SELECT canonical_id, hops, resolution_status "
        "FROM v_mediawiki_page_resolved WHERE id = $1", first_id)
    assert row["canonical_id"] is None
    assert row["resolution_status"] == "depth_exceeded"
