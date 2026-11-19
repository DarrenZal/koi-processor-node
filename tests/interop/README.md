# KOI-net interop gate

Our node against stock [koi-net](https://github.com/DynamicalSystemsGroup/koi-net/tree/v2.1.2)
nodes, end to end, on loopback. It is the gate the
[conformance register](../../docs/architecture/koi-protocol-conformance.md) names under
"PENDING GATE". The stock nodes and the gate's tests run from the gate's own venv,
`.venv-koinet`. Our node runs from its own interpreter, which has imported `koi_net.protocol`
at runtime since 2026-11-05.

## Setup (once, uses the network)

```bash
tests/interop/setup_stock_nodes.sh
```

This builds `.venv-koinet` (Python 3.13, koi-net 2.1.2, rid-lib 3.3.0, pytest, psycopg)
and copies the stock node's sources from
[`koi-net-node-template` @ `c45b188`](https://github.com/DynamicalSystemsGroup/koi-net-node-template/tree/c45b188f1acdb357c907d2478b2804a0cf3b07d7)
into `.interop/`. It checks each file against a pinned sha256. Both directories are
gitignored.

## Run (offline)

```bash
tools/sandboxed.sh -- python3 tests/interop/run_gate.py --node-python <deps-complete python for this repo>
```

`--cases` picks a subset of `conformance`, `full-off`, `full-on`, `partial-off` and
`partial-on`. `--keep` keeps the scratch directory, which holds every node's log. It is
also kept whenever a case fails. `--check` checks the prerequisites and starts nothing.

Two limits turn a hang into a failure. `--test-timeout` (default 120 s) fails a test phase
(setup, call or teardown) that runs longer, through `gate_timeout.py`, which every pytest
run loads. Its message lists the asyncio tasks still waiting and the line each waits on.
`--wall-clock` (default 1800 s) bounds the whole run: past it, the runner stops every
process it started and the cluster, keeps the scratch directory and exits 1. `0` turns
either off.

For each case the runner:

1. starts a throwaway PostgreSQL cluster in a scratch directory. It listens on TCP at
   127.0.0.1 only and has no Unix socket. The runner builds `personal_koi` there once with
   `scripts/run_migrations.sh`, then clones it per case as `koi_test_interop_<case>`;
2. starts our node from this checkout under uvicorn, from an empty environment that names
   every target. That means the case database, a scratch HOME, state dir and vault,
   127.0.0.1 and a free port. The vault path is set explicitly because its default is a
   real vault;
3. starts one stock node (`stock_node.py`) whose first contact is our node. `full` runs
   the template unmodified. `partial` uses koi-net's own `PartialNode` with the template's
   handler. The conformance case starts no stock node;
4. runs `test_gate.py` (or `tests/conformance` for the conformance case) in `.venv-koinet`.
   Targets come in `KOI_TEST_*` variables and case details in `KOI_GATE_*` variables;
5. stops each process it started, by its recorded PID, and then the cluster.

In a case name, `full`/`partial` is the stock node's type, and `off`/`on` is
`KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL` (the membrane, divergence 18).

## What the tests check

Each test is one protocol step, observed from the state on both sides: our database and
log, and the stock node's rid-lib cache and log. The steps are the handshake, the stock
node caching our node bundle, the edge approved on both sides, the stock node fetching from
us, a vault-file event of ours reaching the stock node, and our node polling a full stock
node.

A step that an open divergence blocks is a strict xfail limited to `StepNotReached`. A
precondition that does not happen fails the test outright. When a fix makes a step pass,
the strict xfail fails until the marker is removed and the register entry is updated.
`tests/test_interop_gate_markers.py` (main suite) fails if a marker names a divergence the
register lists as fixed. The runner prints where each blocked step stopped. A missing
variable fails a test, so nothing skips.

Exit status: 0 when every case's pytest exits 0, 1 when a case fails, 2 for a refused
target or usage error, 3 for a missing prerequisite.
