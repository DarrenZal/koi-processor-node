"""Keep pytest out of the TerminusDB evaluation harnesses.

test_merge.py and test_federation.py are scripts, not pytest modules: run_phase0.sh
runs them with `python -m` against throwaway TerminusDB containers, and they import
terminusdb_client, which requirements.txt does not install. Without this file,
`pytest .` or `pytest scripts/` would import them and fail at collection. A file named
explicitly on the command line is still imported; run it with `python -m` instead.
"""

collect_ignore_glob = ["test_*.py"]
