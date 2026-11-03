"""The four Fuseki deploy scripts in src/core are dry runs unless told otherwise.

clean_jena.py (DROP ALL), archive_and_deploy.py (CLEAR ALL) and deploy_refined_full.py
(DELETE WHERE over every refined statement) need --apply --confirm-destructive;
deploy_refined_graph.py (DROP GRAPH of the dated graph it then writes) needs --apply.
No test here opens a socket: `requests` is replaced in each loaded module.
"""

import importlib.util
import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
REFINED = "refined_graph_20251017_001349.ttl"


def _load(rel):
    name = "fuseki_cli_" + Path(rel).stem
    spec = importlib.util.spec_from_file_location(name, REPO / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Resp:
    def __init__(self, status, text=""):
        self.status_code = status
        self.text = text

    def json(self):
        return json.loads(self.text)


class FakeRequests:
    """Records every call; answers like a small Fuseki dataset."""

    def __init__(self):
        self.calls = []

    def _answer(self, method, url, kw):
        data = kw.get("data")
        form = data if isinstance(data, dict) else {}
        if url.endswith("/update"):
            return _Resp(200)
        if method == "PUT" or (method == "POST" and "/data" in url):
            return _Resp(201)
        query = form.get("query", "")
        if "COUNT" in query.upper():
            return _Resp(200, json.dumps({"results": {"bindings": [{"count": {"value": "3"}}]}}))
        if "SELECT" in query.upper():
            return _Resp(200, json.dumps({"results": {"bindings": []}}))
        return _Resp(200, "<urn:a> <urn:b> <urn:c> .\n")

    def post(self, url, **kw):
        self.calls.append(("POST", url, kw.get("data")))
        return self._answer("POST", url, kw)

    def get(self, url, **kw):
        self.calls.append(("GET", url, None))
        return self._answer("GET", url, kw)

    def put(self, url, **kw):
        self.calls.append(("PUT", url, None))
        return self._answer("PUT", url, kw)

    def updates(self):
        return [c[2]["update"] for c in self.calls
                if c[0] == "POST" and c[1].endswith("/update") and isinstance(c[2], dict)]


SCRIPTS = [
    # (path, flags that reproduce the old no-argument run, update text it must send)
    ("src/core/clean_jena.py", ["--apply", "--confirm-destructive"], "DROP ALL"),
    ("src/core/archive_and_deploy.py", ["--apply", "--confirm-destructive"], "CLEAR ALL"),
    ("src/core/deploy_refined_full.py", ["--apply", "--confirm-destructive"], "DELETE WHERE"),
    ("src/core/deploy_refined_graph.py", ["--apply"], "DROP GRAPH"),
]


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _prepare(rel, monkeypatch):
    mod = _load(rel)
    fake = FakeRequests()
    monkeypatch.setattr(mod, "requests", fake)
    if hasattr(mod, "time"):
        monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    return mod, fake


@pytest.mark.parametrize("rel,apply_flags,update", SCRIPTS)
def test_help_sends_nothing(rel, apply_flags, update, workdir, monkeypatch, capsys):
    mod, fake = _prepare(rel, monkeypatch)
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0
    assert "--apply" in capsys.readouterr().out
    assert fake.calls == []
    assert list(workdir.iterdir()) == []


@pytest.mark.parametrize("mode", ["noarg", "dry-run", "dry-run-wins"])
@pytest.mark.parametrize("rel,apply_flags,update", SCRIPTS)
def test_default_and_dry_run_send_nothing(rel, apply_flags, update, mode, workdir, monkeypatch, capsys):
    argv = {"noarg": [], "dry-run": ["--dry-run"], "dry-run-wins": ["--dry-run"] + apply_flags}[mode]
    (workdir / REFINED).write_text("@prefix regx: <https://regen.network/rdf/> .\n<urn:s> <urn:p> <urn:o> .\n")
    mod, fake = _prepare(rel, monkeypatch)
    assert mod.main(argv) == 0
    out = capsys.readouterr().out
    assert "Dry run: nothing was sent" in out
    assert "localhost:3030/koi" in out
    assert fake.calls == []
    assert sorted(p.name for p in workdir.iterdir()) == [REFINED]


@pytest.mark.parametrize("rel", [s[0] for s in SCRIPTS if "--confirm-destructive" in s[1]])
def test_apply_without_confirmation_refuses(rel, workdir, monkeypatch):
    mod, fake = _prepare(rel, monkeypatch)
    assert mod.main(["--apply"]) == 2
    assert fake.calls == []


@pytest.mark.parametrize("rel,apply_flags,update", SCRIPTS)
def test_opt_in_runs_the_old_write_path(rel, apply_flags, update, workdir, monkeypatch):
    (workdir / REFINED).write_text(
        "@prefix regx: <https://regen.network/rdf/> .\n"
        "<urn:s> <urn:p> <urn:o> .\n"
        ".\n"
    )
    mod, fake = _prepare(rel, monkeypatch)
    assert mod.main(apply_flags) == 0
    assert any(u.strip().startswith(update) or update in u for u in fake.updates()), fake.updates()
    assert all(c[1].startswith("http://localhost:3030/koi") for c in fake.calls)
    # every script loads the refined file after its destructive step
    assert any(c[0] in ("PUT", "POST") and ("/data" in c[1] or "INSERT DATA" in str(c[2]))
               for c in fake.calls), fake.calls


def test_endpoint_flag_redirects_every_request(workdir, monkeypatch):
    (workdir / REFINED).write_text("<urn:s> <urn:p> <urn:o> .\n")
    mod, fake = _prepare("src/core/clean_jena.py", monkeypatch)
    assert mod.main(["--endpoint", "http://127.0.0.1:59999/test/", "--apply", "--confirm-destructive"]) == 0
    assert fake.calls and all(c[1].startswith("http://127.0.0.1:59999/test/") for c in fake.calls)
