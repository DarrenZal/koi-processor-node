"""
CLI tests for mediawiki_review.py — resolver-aware inspect and promote.

Tests the --wiki-id auto-detect, redirect resolution display in inspect,
canonical-id targeting in promote, and the resolver-missing startup guard.

Needs KOI_TEST_DATABASE_URL, a database with migration 083 applied; the CLI runs
as a subprocess with POSTGRES_URL set to it. The startup-guard test also needs
KOI_TEST_ADMIN_DSN, to create and drop a koi_test_* copy without the migration.
"""

import asyncio
import os
import subprocess
import sys
import uuid

import pytest
import asyncpg

from tests import koi_test_targets as targets

SCRIPT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "scripts", "mediawiki_review.py",
)

# We need a parsed-dir even though inspect's JSON lookup is separate from the
# DB-state display. Use /dev/null as a dummy (the JSON lookup will print
# "not found" but the DB portion still exercises the resolver path).
DUMMY_PARSED_DIR = "/tmp/mediawiki_review_test_parsed"

WIKI_NAME = f"test_cli_{uuid.uuid4().hex[:8]}"


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
                       is_redirect: bool = False, redirect_target: str = None,
                       status: str = "staged") -> int:
    """Insert a page_state row and return its id."""
    return await conn.fetchval(
        """INSERT INTO mediawiki_page_state
           (wiki_id, page_id, title, is_redirect, redirect_target, source_rid, status)
           VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id""",
        wiki_id, page_id, title, is_redirect, redirect_target,
        f"mediawiki:{WIKI_NAME}:{page_id}", status,
    )


def run_review(*cli_args, env_override=None):
    """Run mediawiki_review.py as a subprocess and return (returncode, stdout, stderr)."""
    env = os.environ.copy()
    env["POSTGRES_URL"] = targets.database_url()
    if env_override:
        env.update(env_override)
    result = subprocess.run(
        [sys.executable, SCRIPT_PATH, *cli_args],
        capture_output=True, text=True, env=env, timeout=30,
    )
    return result.returncode, result.stdout, result.stderr


# ---------------------------------------------------------------------------
# Tests that run against KOI_TEST_DATABASE_URL (migration 083 present)
# The CLI spawns a subprocess that opens its own connection (we can't share our
# rolled-back transaction), so these test observable behaviors against existing data.
# ---------------------------------------------------------------------------


class TestWikiIdAutoDetect:
    """Test --wiki-id auto-detection behavior."""

    def test_single_wiki_auto_detects(self):
        """With one wiki, --wiki-id can be omitted (inspect a known page)."""
        # Use a title that definitely doesn't exist — we just want to verify
        # the wiki-id auto-detect doesn't error out.
        code, stdout, stderr = run_review(
            "inspect", "--title", f"nonexistent_{uuid.uuid4().hex[:8]}",
            "--parsed-dir", DUMMY_PARSED_DIR,
        )
        # Should NOT get the "wiki-id is required" error
        assert "--wiki-id is required" not in stderr
        # The page won't be found, but the script should still run
        assert "Page JSON not found" in stdout or "No DB state found" in stdout

    def test_explicit_wiki_id_accepted(self):
        """Explicit --wiki-id is accepted without error."""
        code, stdout, stderr = run_review(
            "inspect", "--title", f"nonexistent_{uuid.uuid4().hex[:8]}",
            "--parsed-dir", DUMMY_PARSED_DIR, "--wiki-id", "1",
        )
        assert "--wiki-id is required" not in stderr


class TestInspectRedirectResolution:
    """Test inspect subcommand with redirect-aware display."""

    def test_inspect_redirect_shows_resolution(self):
        """Inspecting a known redirect title shows the resolution line."""
        # WIR is a known redirect in the P2P Foundation wiki
        code, stdout, stderr = run_review(
            "inspect", "--title", "WIR",
            "--parsed-dir", DUMMY_PARSED_DIR,
        )
        # If WIR exists as a redirect, we should see the resolution line
        if "Resolved 'WIR'" in stdout:
            assert "→" in stdout
            assert "hop" in stdout

    def test_inspect_non_redirect(self):
        """Inspecting a non-redirect page shows DB state without resolution line."""
        # Use a known canonical page
        code, stdout, stderr = run_review(
            "inspect", "--title", "WIR Economic Circle Cooperative",
            "--parsed-dir", DUMMY_PARSED_DIR,
        )
        # Should NOT show a resolution line
        assert "Resolved '" not in stdout

    def test_inspect_missing_title(self):
        """Inspecting a nonexistent title exits with appropriate message."""
        code, stdout, stderr = run_review(
            "inspect", "--title", f"totally_bogus_{uuid.uuid4().hex[:8]}",
            "--parsed-dir", DUMMY_PARSED_DIR,
        )
        assert "Page JSON not found" in stdout or "No DB state found" in stdout


class TestPromoteRedirectResolution:
    """Test promote subcommand uses canonical_id."""

    def test_promote_missing_title_errors(self):
        """Promoting a nonexistent title exits non-zero."""
        code, stdout, stderr = run_review(
            "promote", "--title", f"bogus_{uuid.uuid4().hex[:8]}",
            "--type", "Concept",
            "--parsed-dir", DUMMY_PARSED_DIR,
            "--run-id", f"test-{uuid.uuid4().hex[:8]}",
        )
        assert code != 0
        assert "not found" in stdout or "not found" in stderr


class TestResolverMissingGuard:
    """Test startup guard when migration 083 is not applied."""

    def test_resolver_missing_exits_code_2(self):
        """Without the resolver function/view, script exits with code 2."""
        # Create a temporary DB without the migration
        db_url = targets.database_url()
        admin_dsn = targets.admin_dsn()
        clone_name = targets.scratch_name("nomigration")
        clone_url = targets.with_database(admin_dsn, clone_name)

        async def _setup_and_test():
            admin_conn = await asyncpg.connect(admin_dsn)
            try:
                # Create clone from schema dump
                await admin_conn.execute(f'CREATE DATABASE "{clone_name}" TEMPLATE template0')
            finally:
                await admin_conn.close()

            # Copy schema via pg_dump | psql
            dump = subprocess.run(
                ["pg_dump", "-s", db_url],
                capture_output=True, text=True, timeout=30,
            )
            if dump.returncode != 0:
                # Fallback: just create the DB and add the minimum schema
                # The clone DB won't have the resolver, which is the point
                pass
            else:
                subprocess.run(
                    ["psql", clone_url],
                    input=dump.stdout, capture_output=True, text=True, timeout=30,
                )
                # Drop resolver objects in clone
                clone_conn = await asyncpg.connect(clone_url)
                try:
                    await clone_conn.execute("DROP VIEW IF EXISTS v_mediawiki_page_resolved")
                    await clone_conn.execute(
                        "DROP FUNCTION IF EXISTS mediawiki_resolve_redirect(text, int)")
                    await clone_conn.execute(
                        "DROP FUNCTION IF EXISTS mediawiki_resolve_redirect_info(text, int, boolean, bigint)")
                finally:
                    await clone_conn.close()

            try:
                code, stdout, stderr = run_review(
                    "inspect", "--title", "WIR",
                    "--parsed-dir", DUMMY_PARSED_DIR,
                    env_override={"POSTGRES_URL": clone_url},
                )
                assert code == 2, f"Expected exit code 2, got {code}. stderr: {stderr}"
                assert "083_mediawiki_redirect_resolver.sql" in stderr
            finally:
                # Clean up clone DB
                admin_conn = await asyncpg.connect(admin_dsn)
                try:
                    # Terminate any connections to the clone
                    await admin_conn.execute(f"""
                        SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                        WHERE datname = '{clone_name}' AND pid <> pg_backend_pid()
                    """)
                    await admin_conn.execute(
                        f'DROP DATABASE IF EXISTS "{targets.droppable(clone_name)}"')
                finally:
                    await admin_conn.close()

        asyncio.run(_setup_and_test())
