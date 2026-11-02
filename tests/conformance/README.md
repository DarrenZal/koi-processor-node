# KOI-net conformance tests

These tests parse our node's KOI-net payloads with the upstream `koi_net.protocol`
models. koi-net is **not** a runtime dependency of this repository; it lives only in a
separate test venv.

```bash
python3.13 -m venv .venv-koinet            # koi-net 2.1.2 needs Python >= 3.12
.venv-koinet/bin/pip install -r tests/conformance/requirements.txt
```

Run them against a **throwaway** instance of this node, never a live one. The tests write
events and tasks through that node into its database, so the node's `POSTGRES_URL` must
point at a throwaway database in a throwaway cluster, never at your real `personal_koi`:

- a fresh `personal_koi` database in a separate PostgreSQL cluster or port (see
  [fresh-install-migrations.md](../../docs/operations/fresh-install-migrations.md)), with
  the node's `POSTGRES_URL` set to it;
- `KOI_NET_ENABLED=true`, `KOI_NODE_NAME=conformance-<anything>`,
  `KOI_STATE_DIR=<scratch dir>`, `KOI_BASE_URL=http://127.0.0.1:<port>/koi-net`,
  `KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL=false`, `KOI_FEDERATE_KNOWLEDGE=true`;
- `uvicorn api.personal_ingest_api:app --host 127.0.0.1 --port <port>` (not 8351).

**Set `POSTGRES_URL` explicitly.** Unset, the node connects to
`localhost:5432/personal_koi`, the default live database, and writes the test events
there. `source config/personal.env` exports your real `POSTGRES_URL`, `KOI_STATE_DIR` and
`KOI_NODE_NAME`, so start the throwaway node from a clean environment, with every target
named on the command line. The node starts without `OPENAI_API_KEY`, and these tests need
no embeddings. If you add the key, also add `EMBEDDING_MODEL=text-embedding-3-large` and
`EMBEDDING_DIMENSION=3072`, as in `config/personal.env.example`: with the key alone the
embedding provider defaults to a 1536-dimension model, and the node stops at startup with
`DIMENSION MISMATCH` because the schema's embedding columns are 3072-dimension.

```bash
env -i PATH="$PATH" HOME="$HOME" \
  POSTGRES_URL=postgresql://<user>@127.0.0.1:<test-pg-port>/personal_koi \
  KOI_STATE_DIR=/tmp/conformance-node-state KOI_NODE_NAME=conformance-local \
  KOI_NET_ENABLED=true KOI_BASE_URL=http://127.0.0.1:<port>/koi-net \
  KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL=false KOI_FEDERATE_KNOWLEDGE=true \
  venv/bin/python -m uvicorn api.personal_ingest_api:app --host 127.0.0.1 --port <port>
```

Then, in another shell:

```bash
KOI_TEST_API_URL=http://127.0.0.1:<port> .venv-koinet/bin/python -m pytest tests/conformance
```

The tests call `KOI_TEST_API_URL` + `/koi-net` and skip when it is unset. They refuse any
node not named `conformance-*` and any host but 127.0.0.1/localhost, and
`tests/conftest.py` refuses the live API's port 8351. In the main test venv this module
skips (no koi-net).

Divergences 12, 15, 16 and 17 are strict xfails, each limited to the failure that
divergence causes: fixing one makes its test fail until the marker is removed and
[the conformance register](../../docs/architecture/koi-protocol-conformance.md) is updated. A setup problem fails the test
outright. The other divergences have no test here.

