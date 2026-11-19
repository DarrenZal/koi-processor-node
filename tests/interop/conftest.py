"""Collection rules for tests/interop/.

- test_gate.py runs only under tests/interop/run_gate.py, in .venv-koinet: it imports
  koi-net and psycopg from that venv and needs a running stock node. run_gate.py sets
  KOI_GATE_CASE.
- test_negotiation_db.py needs a database: run_gate.py runs it against a throwaway clone
  and sets KOI_TEST_DATABASE_URL.

Without those, a plain `pytest` on the main suite does not collect them: not collected,
rather than skipped or failed.
"""
import os

collect_ignore = []
if not os.environ.get("KOI_GATE_CASE"):
    collect_ignore.append("test_gate.py")
if not os.environ.get("KOI_TEST_DATABASE_URL"):
    collect_ignore += ["test_negotiation_db.py", "test_peer_rejection_db.py"]
