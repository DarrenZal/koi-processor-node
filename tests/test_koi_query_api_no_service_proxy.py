"""R26 census: the query API exposes no generic service proxy.

The query API used to mount a generic `proxyToService` on
/api/koi/coordinator/, /event-bridge/, /bge/, /transformations and /rids that
forwarded `req.originalUrl` verbatim to the internal services (:8005, :8100,
:8090, :8002). Because nginx forwarded the raw request URI, a crafted path could
route around the nginx blocks and reach any internal path. These tests assert
that mechanism is gone and that the only surviving consumer, rid-lookup, is a
dedicated handler pinned to its own path space.

Static source checks only: no database, no network, no request payloads. Runs
under tools/sandboxed.sh. Fails on the base commit (proxy present); passes on
the branch.
"""
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
QUERY_API = REPO / "koi-query-api.ts"
PROXY_MODULE = REPO / "koi-service-proxy.ts"

SERVICE_PORTS = ("8005", "8100", "8090", "8002")


def _src(path: Path) -> str:
    assert path.exists(), f"{path.name} must exist"
    return path.read_text(encoding="utf-8")


def _code_lines(text: str):
    """Yield lines that are not comments (strips // line comments and /* */ blocks)."""
    no_block = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    for line in no_block.splitlines():
        stripped = line.strip()
        if stripped.startswith("//"):
            continue
        # drop trailing line comments
        line = re.split(r"//", line, maxsplit=1)[0]
        yield line


def test_proxytoservice_function_removed():
    code = "\n".join(_code_lines(_src(QUERY_API)))
    assert "proxyToService" not in code, (
        "the generic proxyToService must be removed (R26); found a live reference"
    )


def test_generic_service_mounts_removed():
    code = "\n".join(_code_lines(_src(QUERY_API)))
    for mount in (
        "/api/koi/coordinator/",
        "/api/koi/event-bridge/",
        "/api/koi/bge/",
        "/api/koi/transformations",
    ):
        assert f"app.use('{mount}'" not in code and f'app.use("{mount}"' not in code, (
            f"mount {mount} must be removed"
        )
    # the generic /rids and rid-lookup app.use mounts are gone
    assert "app.use('/api/koi/rids'" not in code and 'app.use("/api/koi/rids"' not in code
    assert "app.use('/api/koi/rid-lookup'" not in code and 'app.use("/api/koi/rid-lookup"' not in code


def test_no_request_path_forwarded_to_service_ports():
    """No code forwards a client-controlled request path to an internal service.

    The R26 pattern is a service-port base URL concatenated with req.originalUrl
    / req.url / req.path. Server-initiated calls with a literal path (e.g.
    fetch('http://localhost:8090/encode')) are legitimate and not flagged.
    """
    code_lines = list(_code_lines(_src(QUERY_API)))
    req_path_re = re.compile(r"req\.(originalUrl|url|path)")
    offenders = []
    for i, line in enumerate(code_lines, 1):
        if req_path_re.search(line) and any(p in line for p in SERVICE_PORTS):
            offenders.append((i, line.strip()))
    assert not offenders, f"request path forwarded to a service port: {offenders}"
    # The template literal form `${serviceUrl}${req.originalUrl}` must not exist.
    joined = "\n".join(code_lines)
    assert "${req.originalUrl}" not in joined, "req.originalUrl must not be interpolated into a target URL"


def test_legit_bge_encode_calls_still_present():
    """The query API's own embedding calls (literal path) are untouched."""
    code = _src(QUERY_API)
    assert code.count("http://localhost:8090/encode") >= 1, (
        "the server-side bge /encode calls use a literal path and are not part of R26"
    )


def test_rid_lookup_handler_is_pinned():
    """rid-lookup is preserved as a dedicated, path-pinned handler."""
    code = _src(QUERY_API)
    assert "createRidLookupHandler" in code and "RID_LOOKUP_ROUTE" in code, (
        "koi-query-api.ts must wire the dedicated rid-lookup handler"
    )
    mod = _src(PROXY_MODULE)
    # path pinned to the rid-lookup space
    assert "RID_LOOKUP_PREFIX" in mod and "startsWith(RID_LOOKUP_PREFIX)" in mod, (
        "the handler must verify the upstream pathname stays under /api/koi/rid-lookup/"
    )
    # RID re-encoded as a single segment
    assert "encodeURIComponent(rid)" in mod, "the RID must be re-encoded as one path segment"
    # only Authorization is forwarded; no blanket header spread
    assert "...req.headers" not in mod, "client headers must not be spread into the upstream request"
    assert "Authorization" in mod, "the Authorization header must still be forwarded for private-row access"
    # a timeout is set
    assert "AbortSignal.timeout" in mod or "timeoutMs" in mod, "the handler must set a request timeout"


if __name__ == "__main__":
    # Allow running without pytest: python3 tests/test_koi_query_api_no_service_proxy.py
    import sys
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failures = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL  {fn.__name__}: {e}")
    print(f"\n{'ALL PASS' if failures == 0 else 'FAILURES'}: {len(fns) - failures}/{len(fns)} passed")
    sys.exit(1 if failures else 0)
