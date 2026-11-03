import json
import uuid

import psycopg2
import pytest


from tests import koi_test_targets as targets


def _connect():
    # A set but unreachable KOI_TEST_DATABASE_URL is a failure, not a skip.
    return psycopg2.connect(targets.database_url())


@pytest.fixture()
def db_conn():
    conn = _connect()
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


def test_fts_trigger_and_prefix_match(db_conn):
    cur = db_conn.cursor()

    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'koi_memories'
        """
    )
    if cur.fetchone() is None:
        pytest.skip("koi_memories table not present")

    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'koi_memories'
          AND column_name = 'content_tsv'
        """
    )
    if cur.fetchone() is None:
        pytest.skip("content_tsv column not present; run migration 025_add_content_tsv_fts.sql")

    rid = f"test:fts:{uuid.uuid4()}"
    content = {
        "title": "Gregory Landua",
        "text": "Claims engine overview for Gregory Landua."
    }

    cur.execute(
        """
        INSERT INTO koi_memories (rid, event_type, source_sensor, content, metadata)
        VALUES (%s, 'NEW', 'test', %s::jsonb, '{}'::jsonb)
        RETURNING id
        """,
        (rid, json.dumps(content)),
    )
    row_id = cur.fetchone()[0]

    cur.execute(
        "SELECT content_tsv IS NOT NULL FROM koi_memories WHERE id = %s",
        (row_id,),
    )
    assert cur.fetchone()[0] is True

    cur.execute(
        """
        SELECT
          ts_rank_cd(content_tsv, to_tsquery('english', 'claims & engine')) as rank,
          content_tsv @@ to_tsquery('english', 'greg & landua') as strict_match,
          content_tsv @@ to_tsquery('english', 'greg:* & landua:*') as prefix_match
        FROM koi_memories
        WHERE id = %s
        """,
        (row_id,),
    )
    rank, strict_match, prefix_match = cur.fetchone()

    assert float(rank) > 0
    assert prefix_match is True
    assert strict_match is False
