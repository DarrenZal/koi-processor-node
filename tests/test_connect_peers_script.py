"""scripts/federation/connect-peers.sh: syntax, the deferred-inbound-edge step, and an honest summary.

The script is interactive and needs two live nodes, so it is not run here; these check its
text (tests/test_connect_peers_no_source_interpolation.py checks that node IDs reach inline
Python only through the environment). The summary used to print the
inbound edge as "APPROVED (auto)" without looking.
"""
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "federation" / "connect-peers.sh"


def test_parses():
    r = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr


def test_it_looks_for_and_approves_a_proposed_inbound_edge():
    text = SCRIPT.read_text()
    step = text[text.index("STEP 7b"):text.index("STEP 8")]
    assert "(os.environ['CP_REMOTE_RID'], os.environ['CP_LOCAL_NODE_RID'])" in step and "status = 'PROPOSED'" in step
    assert "/koi-net/edges/approve" in step and "read -rp" in step


def test_the_summary_reads_the_stored_inbound_status():
    text = SCRIPT.read_text()
    assert 'INBOUND_STATUS="APPROVED (auto)"' not in text
    summary = text[text.index("Connection Status"):]
    assert "SELECT status FROM koi_net_edges" in summary and "UNKNOWN (lookup failed)" in summary
    assert summary.count("Local inbound edge:") == 1


def test_a_missing_admin_token_warns_and_carries_on():
    """The server refuses admin calls with no token configured (503); the approval steps must
    say so and continue to the summary, not end the script."""
    text = SCRIPT.read_text()
    steps = text[text.index("STEP 7:"):text.index("STEP 8")]
    assert "log_fatal" not in steps
    assert steps.count("No admin token (KOI_ADMIN_TOKEN or $LOCAL_TOKEN_FILE)") == 2
