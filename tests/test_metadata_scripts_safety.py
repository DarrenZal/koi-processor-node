"""scripts/test-anchored-metadata.ts and scripts/test-mcp-metadata-tools.ts send, read and
write nothing unless --apply is given (D13).

They run under bun (skipped when bun is not installed). Offline: with --apply, a bun
preload replaces fetch with a recorder and the pg module with a fake pool, so the write
path is reached without a database or a network.
"""

from __future__ import annotations

import json
import os
import pathlib
import pwd
import shutil
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
ANCHORED = REPO / "scripts" / "test-anchored-metadata.ts"
MCP = REPO / "scripts" / "test-mcp-metadata-tools.ts"


def _bun():
    found = shutil.which("bun")
    if found:
        return found
    home_bun = pathlib.Path(pwd.getpwuid(os.getuid()).pw_dir) / ".bun" / "bin" / "bun"
    return str(home_bun) if home_bun.exists() else None


BUN = _bun()
pytestmark = pytest.mark.skipif(BUN is None, reason="bun is not installed")

PRELOAD = """
import { plugin } from "bun";
import { appendFileSync } from "fs";
const LOG = process.env.FAKE_LOG!;
const rec = (o: any) => appendFileSync(LOG, JSON.stringify(o) + "\\n");
globalThis.fetch = (async (url: any, init?: any) => {
  rec({ kind: "fetch", url: String(url), method: (init && init.method) || "GET" });
  return new Response(JSON.stringify({ error: { code: "FAKE", message: "fake" } }),
                      { status: 404, headers: { "content-type": "application/json" } });
}) as any;
class FakePool {
  constructor(opts: any) { rec({ kind: "pool", database: opts && opts.database }); }
  async query(sql: string, params?: any[]) {
    rec({ kind: "query", sql: String(sql).trim().slice(0, 60) });
    if (String(sql).includes("information_schema.tables")) {
      return { rows: [{ table_name: "anchored_metadata_records" }, { table_name: "metadata_derivations" },
                      { table_name: "derivation_allowlist" }], rowCount: 3 };
    }
    if (String(sql).includes("NOW()")) return { rows: [{ now: new Date(0) }], rowCount: 1 };
    return { rows: [], rowCount: 0 };
  }
  async connect() { return { query: this.query.bind(this), release() {} }; }
  async end() { rec({ kind: "end" }); }
}
plugin({ name: "fake-pg", setup(build) {
  build.module("pg", () => ({ exports: { Pool: FakePool, default: { Pool: FakePool } }, loader: "object" }));
} });
"""


@pytest.fixture
def run(tmp_path):
    log = tmp_path / "calls.jsonl"
    preload = tmp_path / "preload.ts"
    preload.write_text(PRELOAD)

    def _run(script, *args, mock=False, **env):
        cmd = [BUN]
        if mock:
            cmd += ["--preload", str(preload)]
        cmd += [str(script), *args]
        e = {"PATH": "/usr/bin:/bin", "HOME": str(tmp_path), "FAKE_LOG": str(log),
             "POSTGRES_URL": "postgresql://koi:pw@127.0.0.1:1/koi_test_meta",
             "KOI_API_ENDPOINT": "http://127.0.0.1:1/api/koi", **env}
        r = subprocess.run(cmd, cwd=REPO, env=e, capture_output=True, text=True, timeout=60)
        calls = [json.loads(l) for l in log.read_text().splitlines()] if log.exists() else []
        return r, calls
    return _run


@pytest.mark.parametrize("script", [ANCHORED, MCP])
def test_help(run, script):
    r, calls = run(script, "--help", mock=True)
    assert r.returncode == 0, r.stderr
    assert "Usage" in r.stdout
    assert calls == []


@pytest.mark.parametrize("script", [ANCHORED, MCP])
@pytest.mark.parametrize("args", [[], ["--dry-run"], ["--dry-run", "--apply"], ["--iri", "regen:x.rdf"]])
def test_without_apply_nothing_is_loaded_or_sent(run, script, args):
    r, calls = run(script, *args, mock=True)
    assert r.returncode == 0, r.stderr
    assert "DRY RUN" in r.stdout
    assert calls == []
    assert "pw@" not in r.stdout


def test_anchored_apply_reaches_the_database_and_the_resolver(run):
    r, calls = run(ANCHORED, "--apply", mock=True)
    kinds = [c["kind"] for c in calls]
    assert "pool" in kinds and "query" in kinds
    assert any(c["kind"] == "fetch" and "metadata-graph" in c["url"] for c in calls)
    assert "pw@" not in r.stdout


def test_mcp_apply_sends_the_requests(run):
    r, calls = run(MCP, "--apply", mock=True)
    sent = [(c["method"], c["url"].split("/api/koi", 1)[-1]) for c in calls if c["kind"] == "fetch"]
    assert ("POST", "/metadata/resolve") in sent
    assert ("POST", "/metadata/hectares") in sent
    assert ("GET", "/metadata/stats") in sent
    assert all(u.startswith("http://127.0.0.1:1/api/koi") for u in (c["url"] for c in calls))
