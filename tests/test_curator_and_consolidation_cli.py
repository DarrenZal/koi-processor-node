"""The weekly LLM curator and the predicate consolidation scripts are dry runs unless --apply.

Covers src/content/weekly_curator_llm.py, scripts/run_weekly_curator_llm.py and
src/core/consolidate_predicates_{contextual,openai,final}.py. Every test runs offline:
sockets are refused, and the opt-in tests replace the paid client and the database.
"""

import hashlib
import importlib.util
import math
import socket
import sys
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

CONSOLIDATE = [
    ("src/core/consolidate_predicates_contextual.py", 500),
    ("src/core/consolidate_predicates_openai.py", 1000),
    ("src/core/consolidate_predicates_final.py", 1000),
]


def _load(rel):
    name = "cc_cli_" + Path(rel).stem
    spec = importlib.util.spec_from_file_location(name, REPO / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def blocked(monkeypatch, tmp_path):
    """Refuse and record every socket connect; run in an empty tmp_path."""
    calls = []

    def deny(name):
        def f(*a, **k):
            calls.append(name)
            raise ConnectionRefusedError(f"test blocked {name}")
        return f

    monkeypatch.setattr(socket.socket, "connect", deny("socket.connect"))
    monkeypatch.setattr(socket.socket, "connect_ex", deny("socket.connect_ex"))
    monkeypatch.setattr(socket, "create_connection", deny("socket.create_connection"))
    monkeypatch.chdir(tmp_path)
    return calls


class FakeEmbeddings:
    """Stands in for client.embeddings; deterministic 8-dimensional vectors."""

    def __init__(self):
        self.requests = []

    def create(self, model, input):
        self.requests.append((model, len(input)))
        data = []
        for text in input:
            h = hashlib.sha256(text.encode()).digest()
            data.append(types.SimpleNamespace(embedding=[b / 255.0 + 0.01 for b in h[:8]]))
        return types.SimpleNamespace(data=data)


def _fake_client():
    return types.SimpleNamespace(embeddings=FakeEmbeddings())


# --- consolidation scripts ---------------------------------------------------------


@pytest.mark.parametrize("rel,limit", CONSOLIDATE)
def test_consolidate_import_needs_no_key_and_creates_no_client(rel, limit, blocked, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    mod = _load(rel)  # used to exit(1) here without a key
    assert mod.client is None
    assert blocked == []


@pytest.mark.parametrize("rel,limit", CONSOLIDATE)
def test_consolidate_help_does_nothing(rel, limit, blocked, monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    mod = _load(rel)
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0
    assert "--apply" in capsys.readouterr().out
    assert blocked == [] and list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--dry-run", "--apply"]])
@pytest.mark.parametrize("rel,limit", CONSOLIDATE)
def test_consolidate_default_and_dry_run_do_nothing(rel, limit, argv, blocked, monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    mod = _load(rel)
    monkeypatch.setattr(mod, "_get_client", lambda: pytest.fail("client created in a dry run"))
    assert mod.main(argv) is None
    out = capsys.readouterr().out
    assert "Dry run" in out and "localhost:3030/koi/sparql" in out
    assert blocked == [] and list(tmp_path.iterdir()) == []


def test_contextual_apply_embeds_at_most_limit_and_writes(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    mod = _load("src/core/consolidate_predicates_contextual.py")
    patterns = [{"predicate": f"pred {i}", "total_count": 10 + i,
                 "dominant_pattern": f"Person pred{i} {'Project' if i % 2 else 'Text'}",
                 "subject_types": {"Person": 3}, "object_types": {"Project": 3},
                 "sample_patterns": []} for i in range(250)]
    monkeypatch.setattr(mod, "get_triple_patterns", lambda: patterns)
    fake = _fake_client()
    monkeypatch.setattr(mod, "_get_client", lambda: fake)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    mod.main(["--apply", "--limit", "120"])
    assert sum(n for _, n in fake.embeddings.requests) == 120
    assert len(fake.embeddings.requests) == math.ceil(120 / 100)
    assert (tmp_path / "contextual_predicate_mapping.json").exists()
    assert (tmp_path / "contextual_predicate_consolidation.json").exists()


def test_contextual_apply_default_cap_is_500(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    mod = _load("src/core/consolidate_predicates_contextual.py")
    patterns = [{"predicate": f"p{i}", "total_count": 5, "dominant_pattern": f"Entity p{i} Text",
                 "subject_types": {}, "object_types": {}, "sample_patterns": []} for i in range(700)]
    monkeypatch.setattr(mod, "get_triple_patterns", lambda: patterns)
    fake = _fake_client()
    monkeypatch.setattr(mod, "_get_client", lambda: fake)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    mod.main(["--apply"])
    assert sum(n for _, n in fake.embeddings.requests) == 500


def test_openai_apply_embeds_and_writes(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    mod = _load("src/core/consolidate_predicates_openai.py")
    preds = [{"predicate": f"pred {i}", "count": 3 + i, "sample_subjects": ["a"],
              "sample_objects": ["b"]} for i in range(30)]
    monkeypatch.setattr(mod, "get_all_predicates_with_context", lambda: preds)
    fake = _fake_client()
    monkeypatch.setattr(mod, "_get_client", lambda: fake)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    mod.main(["--apply"])
    assert sum(n for _, n in fake.embeddings.requests) == len(mod.STANDARD_PREDICATES) + 30
    assert (tmp_path / "openai_predicate_mapping.json").exists()


def test_final_apply_embeds_and_writes(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    mod = _load("src/core/consolidate_predicates_final.py")
    preds = [{"predicate": f"pred {i}", "count": 3 + i, "subject_types": {"Person": 1},
              "object_types": {"Text": 1}, "sample_subjects": ["a"], "sample_objects": ["b"]}
             for i in range(40)]
    monkeypatch.setattr(mod, "get_predicate_patterns", lambda: preds)
    fake = _fake_client()
    monkeypatch.setattr(mod, "_get_client", lambda: fake)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    mod.main(["--apply", "-t", "0.30"])
    assert sum(n for _, n in fake.embeddings.requests) == 40
    assert (tmp_path / "predicate_embeddings.pkl").exists()
    assert (tmp_path / "final_consolidation_t0.30.json").exists()


@pytest.mark.parametrize("rel,limit", CONSOLIDATE)
def test_consolidate_apply_without_key_exits_1(rel, limit, blocked, monkeypatch, tmp_path):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    mod = _load(rel)
    monkeypatch.setattr(mod, "OPENAI_API_KEY", None)
    with pytest.raises(SystemExit) as exc:
        mod.main(["--apply"])
    assert exc.value.code == 1
    assert blocked == []


# --- weekly curator ----------------------------------------------------------------


def _curator(monkeypatch):
    added = []
    mod = _load("src/content/weekly_curator_llm.py")
    monkeypatch.setattr(mod.logger, "add", lambda *a, **k: added.append(a[0]) or 0)
    return mod, added


def test_curator_import_writes_nothing(blocked, monkeypatch, tmp_path):
    mod, added = _curator(monkeypatch)
    assert not (tmp_path / "logs").exists()
    assert added == [] and blocked == []


def test_curator_creating_a_curator_adds_the_file_log_once(blocked, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    mod, added = _curator(monkeypatch)
    mod.WeeklyCuratorLLM()
    mod.WeeklyCuratorLLM()
    assert added == ["logs/weekly_curator_llm.log"]
    assert blocked == []


@pytest.mark.parametrize("argv", [["--help"], [], ["--dry-run"], ["--dry-run", "--apply"]])
def test_curator_cli_default_does_nothing(argv, blocked, monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("POSTGRES_URL", "postgresql://u:changeme@127.0.0.1:1/x")
    mod, added = _curator(monkeypatch)
    monkeypatch.setattr(mod, "main", lambda: pytest.fail("main() ran"))
    if argv == ["--help"]:
        with pytest.raises(SystemExit) as exc:
            mod.cli_main(argv)
        assert exc.value.code == 0
    else:
        assert mod.cli_main(argv) == 0
        out = capsys.readouterr().out
        assert "Dry run" in out and "changeme" not in out
    assert added == [] and blocked == [] and list(tmp_path.iterdir()) == []


def test_curator_cli_apply_runs_generate_save_and_export(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-real")
    mod, added = _curator(monkeypatch)
    steps = []

    async def generate(self):
        steps.append("generate")
        return {"total_discussions": 1, "total_items": 2, "executive_summary": "x" * 120}

    async def save(self, digest):
        steps.append("save_to_database")
        return "review-1"

    async def export(self, digest):
        steps.append("export_files")

    async def export_nb(self, digest):
        steps.append("export_notebooklm_enhanced")

    monkeypatch.setattr(mod.WeeklyCuratorLLM, "generate_weekly_digest", generate)
    monkeypatch.setattr(mod.WeeklyCuratorLLM, "save_to_database", save)
    monkeypatch.setattr(mod.WeeklyCuratorLLM, "export_files", export)
    monkeypatch.setattr(mod.WeeklyCuratorLLM, "export_notebooklm_enhanced", export_nb)
    assert mod.cli_main(["--apply"]) == 0
    assert steps == ["generate", "save_to_database", "export_files", "export_notebooklm_enhanced"]
    assert added == ["logs/weekly_curator_llm.log"]


@pytest.mark.parametrize("argv", [["--help"], [], ["--dry-run", "--apply", "--start-date", "2026-01-01"]])
def test_runner_default_does_nothing(argv, blocked, monkeypatch, tmp_path, capsys):
    mod = _load("scripts/run_weekly_curator_llm.py")
    monkeypatch.setattr(mod, "main", lambda *a: pytest.fail("main() ran"))
    if argv == ["--help"]:
        with pytest.raises(SystemExit) as exc:
            mod.cli_main(argv)
        assert exc.value.code == 0
    else:
        assert mod.cli_main(argv) == 0
        assert "Dry run" in capsys.readouterr().out
    assert blocked == [] and not (tmp_path / "logs").exists()


def test_runner_apply_runs_the_curator(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _load("scripts/run_weekly_curator_llm.py")
    ran = []
    fake = types.ModuleType("src.content.weekly_curator_llm")

    async def curator_main():
        ran.append(True)
        return {"ok": True}
    fake.main = curator_main
    monkeypatch.setitem(sys.modules, "src.content.weekly_curator_llm", fake)
    # main() sets these; setenv first so the test's teardown removes them again
    monkeypatch.setenv("DIGEST_START_DATE", "unset")
    monkeypatch.setenv("DIGEST_END_DATE", "unset")
    assert mod.cli_main(["--apply", "--start-date", "2026-01-01", "--end-date", "2026-01-07"]) == 0
    assert ran == [True]
    import os
    assert os.environ["DIGEST_START_DATE"] == "2026-01-01"


def test_dashboard_still_passes_apply_to_the_curator():
    dashboard = REPO / "src/content/content_dashboard.py"
    if not dashboard.exists():
        pytest.skip("src/content/content_dashboard.py is not in this checkout")
    src = dashboard.read_text()
    assert "scripts/run_weekly_curator_llm.py', '--apply']" in src
    assert "src/content/weekly_curator_llm.py',\n                '--apply'" in src
