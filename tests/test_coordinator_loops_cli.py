"""The coordinator forwarders and the BGE registration loop start only with --run.

Covers src/core/coordinator_to_semantic_bridge.py,
scripts/coordinator_to_eventbridge_forwarder.py and
src/core/bge_server_with_registration.py. Every test runs offline: sockets are refused,
and the --run tests use httpx.MockTransport or patched clients and stop the loop after
one cycle.
"""

import asyncio
import importlib.util
import socket
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parents[1]


class _Stop(Exception):
    pass


def _load(rel):
    name = "loop_cli_" + Path(rel).stem
    spec = importlib.util.spec_from_file_location(name, REPO / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def blocked(monkeypatch, tmp_path):
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


@pytest.fixture
def paths(monkeypatch):
    monkeypatch.syspath_prepend(str(REPO / "scripts"))
    monkeypatch.syspath_prepend(str(REPO / "src" / "core"))
    for v in ("KOI_PRIVATE_KEY_PEM", "KOI_PRIVATE_KEY_PEM_PATH", "KOI_PRIVATE_KEY_PASSWORD"):
        monkeypatch.delenv(v, raising=False)


LOOPS = [
    ("src/core/coordinator_to_semantic_bridge.py", "main"),
    ("scripts/coordinator_to_eventbridge_forwarder.py", "forward_events"),
    ("src/core/bge_server_with_registration.py", "register_bge_server"),
]


@pytest.mark.parametrize("argv", [["--help"], [], ["--dry-run"], ["--dry-run", "--run"]])
@pytest.mark.parametrize("rel,loop", LOOPS)
def test_default_does_not_start(rel, loop, argv, paths, blocked, monkeypatch, tmp_path, capsys):
    mod = _load(rel)
    monkeypatch.setattr(mod, loop, lambda *a, **k: pytest.fail(f"{loop} started"))
    if argv == ["--help"]:
        with pytest.raises(SystemExit) as exc:
            mod.cli_main(argv)
        assert exc.value.code == 0
        assert "--run" in capsys.readouterr().out
    else:
        assert mod.cli_main(argv) == 0
        assert "Dry run" in capsys.readouterr().out
    assert blocked == [] and list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("rel,loop", LOOPS)
def test_run_starts_the_loop(rel, loop, paths, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _load(rel)
    started = []

    async def fake_loop(*a, **k):
        started.append(loop)
    monkeypatch.setattr(mod, loop, fake_loop)
    assert mod.cli_main(["--run"]) == 0
    assert started == [loop]


def test_semantic_bridge_loop_polls_forwards_and_confirms(paths, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("COORDINATOR_URL", "http://coordinator")
    monkeypatch.setenv("SEMANTIC_BRIDGE_URL", "http://semantic-bridge")
    mod = _load("src/core/coordinator_to_semantic_bridge.py")
    seen = []

    def handler(request):
        seen.append((request.url.host, request.url.port, request.url.path))
        if request.url.path == "/events/poll":
            return httpx.Response(200, json={"events": [{"rid": "orn:test:1", "event_type": "NEW",
                                                         "contents": {"text": "x"}}],
                                             "event_ids": ["e1"]})
        if request.url.path == "/events/process":
            return httpx.Response(200, json={"success": True, "chunks_created": 1})
        if request.url.path == "/events/confirm":
            return httpx.Response(200, json={"confirmed_count": 1})
        return httpx.Response(404)

    real = httpx.AsyncClient

    class _Client(real):
        def __init__(self, *a, **k):
            k["transport"] = httpx.MockTransport(handler)
            super().__init__(*a, **k)
    monkeypatch.setattr(mod.httpx, "AsyncClient", _Client)

    async def stop(_):
        raise _Stop()
    monkeypatch.setattr(mod.asyncio, "sleep", stop)
    with pytest.raises(_Stop):
        asyncio.run(mod.poll_and_forward())
    assert [p for _, _, p in seen] == ["/events/poll", "/events/process", "/events/confirm"]
    assert [h for h, _, _ in seen] == ["coordinator", "semantic-bridge", "coordinator"]


def test_bge_registration_registers_then_heartbeats(paths, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _load("src/core/bge_server_with_registration.py")
    calls = []

    async def register(self, **kw):
        calls.append(("register", self.registry_url, kw["component_id"]))
        return True

    async def heartbeat(self, component_id, status="active"):
        calls.append(("heartbeat", component_id))
        raise _Stop()

    monkeypatch.setattr(mod.InfrastructureClient, "register", register)
    monkeypatch.setattr(mod.InfrastructureClient, "heartbeat", heartbeat)

    async def no_wait(_):
        return None
    monkeypatch.setattr(mod.asyncio, "sleep", no_wait)
    with pytest.raises(_Stop):
        asyncio.run(mod.register_bge_server())
    assert calls == [("register", "http://localhost:8003", "bge-embeddings"),
                     ("heartbeat", "bge-embeddings")]
