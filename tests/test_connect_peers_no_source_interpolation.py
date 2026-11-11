"""connect-peers.sh must not paste shell values into inline Python source.

REMOTE_RID comes from the remote node's /health response. Pasting it into a
``python3 -c "..."`` body lets a crafted node ID run code on the operator's
machine with database access. Values must reach Python through the environment.
"""
import pathlib
import re
import subprocess

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "federation" / "connect-peers.sh"
INLINE_PY = re.compile(r'python3 -c "(.*?)"(?=\s|\)|$)', re.S)
SHELL_VAR = re.compile(r"\$\{?[A-Za-z_][A-Za-z0-9_]*")


def _inline_bodies():
    src = SCRIPT.read_text()
    return [(src[: m.start()].count("\n") + 1, m.group(1)) for m in INLINE_PY.finditer(src)]


def test_script_parses():
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


def test_inline_python_blocks_are_found():
    # Guard against the regex silently matching nothing.
    assert len(_inline_bodies()) >= 4


def test_no_shell_variable_inside_inline_python():
    offenders = [(line, SHELL_VAR.findall(body)) for line, body in _inline_bodies() if SHELL_VAR.search(body)]
    assert offenders == [], f"shell values pasted into Python source at lines: {offenders}"


def test_crafted_remote_rid_is_data_not_code(tmp_path):
    # Run the edge-lookup snippet the way the script does, with psycopg2 stubbed, and a
    # node ID that would break out of a quoted Python string if it were pasted in.
    marker = tmp_path / "ran"
    crafted = f"x'), __import__('pathlib').Path('{marker}').touch(), ('"
    body = next(b for _, b in _inline_bodies() if "CP_REMOTE_RID" in b and "SELECT edge_rid" in b)
    stub = tmp_path / "psycopg2.py"
    stub.write_text(
        "class _C:\n"
        "    def cursor(self): return self\n"
        "    def execute(self, sql, params): self.params = params\n"
        "    def fetchone(self): return None\n"
        "    def close(self): pass\n"
        "def connect(url): return _C()\n"
    )
    env = {"PATH": "/usr/bin:/bin", "PYTHONPATH": str(tmp_path),
           "CP_LOCAL_NODE_RID": "local", "CP_REMOTE_RID": crafted}
    out = subprocess.run(["python3", "-c", body], env=env, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert not marker.exists()


def test_admin_token_never_in_curl_argv():
    # A token on a command line is readable by any local user through the process list.
    assert not re.search(r'Authorization: Bearer \$', SCRIPT.read_text())
    assert "-H @<(printf 'Authorization: Bearer %s\\n'" in SCRIPT.read_text()


def test_header_fd_form_delivers_the_token():
    import http.server
    import threading

    seen = {}

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen["auth"] = self.headers.get("Authorization")
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.handle_request, daemon=True).start()
    port = srv.server_address[1]
    cmd = (f"curl -s -m 5 -o /dev/null -H @<(printf 'Authorization: Bearer %s\\n' \"$TOK\") "
           f"http://127.0.0.1:{port}/")
    subprocess.run(["bash", "-c", cmd], env={"PATH": "/usr/bin:/bin:/opt/homebrew/bin", "TOK": "test-only-value"},
                   check=True, timeout=10)
    srv.server_close()
    assert seen.get("auth") == "Bearer test-only-value"
