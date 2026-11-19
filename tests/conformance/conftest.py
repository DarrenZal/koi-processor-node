"""The conformance suite needs koi-net and a throwaway node to talk to.

It is collected only where koi-net is importable and KOI_TEST_API_URL names a node, for
example as the `conformance` case of tests/interop/run_gate.py. Elsewhere it is not
collected, rather than collected and skipped.
"""
import importlib.util
import os

collect_ignore = [] if (importlib.util.find_spec("koi_net") and os.environ.get("KOI_TEST_API_URL")) else ["test_wire_conformance.py"]
