"""The invite flow's approval path must not paste remote values into inline Python or SQL
(register entry 26; connect-peers.sh had the same class). The admin's profile, RID and public key come from its remote /health.

compute_sas is run for real with bash. bootstrap-node.sh needs WireGuard and two live nodes,
so its approval-path blocks are checked as text, like tests/test_connect_peers_no_source_interpolation.py.
"""
import re
import subprocess

import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "scripts" / "federation" / "lib.sh"
BOOTSTRAP = ROOT / "scripts" / "federation" / "bootstrap-node.sh"


def _sas(a, b, cwd):
    r = subprocess.run(["bash", "-c", f'source "{LIB}"; compute_sas "$1" "$2"', "_", a, b],
                       capture_output=True, text=True, timeout=30, cwd=cwd)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


def test_compute_sas_treats_a_hostile_key_as_data(tmp_path):
    marker = tmp_path / "ran"
    hostile = f"x']); open({str(marker)!r}, 'w'); (['"
    out = _sas(hostile, "MFkw-admin", tmp_path)
    assert re.fullmatch(r"\d{6}", out) and not marker.exists()


def test_compute_sas_is_unchanged_for_ordinary_keys(tmp_path):
    import hashlib
    a, b = "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE-a", "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE-b"
    keys = sorted([a, b])
    expected = f"{int.from_bytes(hashlib.sha256(f'{keys[0]}:{keys[1]}'.encode()).digest()[:4], 'big') % 1000000:06d}"
    assert _sas(a, b, tmp_path) == _sas(b, a, tmp_path) == expected


@pytest.mark.skipif(not BOOTSTRAP.exists(), reason="scripts/federation/bootstrap-node.sh is not in this tree")
def test_bootstrap_approval_path_takes_remote_values_as_data():
    text = BOOTSTRAP.read_text()
    step = text[text.index("Step 11: Initiating handshake with admin"):text.index("# Configure vault sync")]
    assert "json.loads('$" not in step and step.count("json.loads(os.environ['BN_PROFILE'])") == 2
    assert "'$ADMIN_NODE_RID'" not in step and "-v admin=\"$ADMIN_NODE_RID\"" in step and ":'admin'" in step
    assert 'Bearer $LOCAL_ADMIN_TOKEN' not in step and "printf 'Authorization: Bearer %s\\n'" in step
    subprocess.run(["bash", "-n", str(BOOTSTRAP)], check=True)
