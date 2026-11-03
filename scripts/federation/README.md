# KOI-net Federation Scripts

These scripts automate peer onboarding, relay updates, security checks, and KOI-net handshake.

The one-command blank-host bootstrap, `bootstrap-node.sh` (not in this export), is left
out, and so is the peer side of the invite flow. Set up a peer with the manual flow below
(`join-request.sh`, `approve-peer.sh`, `setup-node.sh`, then `connect-peers.sh`).
`create-invite.sh` still prints an invite token, and nothing published redeems it: the
peer steps it prints are the manual flow's `join-request.sh` and `setup-node.sh`.
`activate-wireguard.sh` reads the WireGuard key that `join-request.sh` writes.

## Secure Peer Onboarding Flow

This is the setup path in this export. The peer machine needs what the bootstrap would
otherwise install. `wireguard-tools` is needed from step 1: `join-request.sh` generates the
WireGuard key with `wg genkey`, and step 3 runs `wg-quick`. Before step 4 it also needs
PostgreSQL with pgvector and `psql`, `curl`, an empty `personal_koi` database the current
user can connect to, and a Python 3.11 `venv` with `requirements.txt` installed at the top
of the checkout (step 1 of the
[quickstart](../../docs/operations/quickstart-personal-koi.md#1-install-and-config)). Do not
run the migration runner, `scripts/run_migrations.sh`, on that database before
`setup-node.sh`: `setup-node.sh` applies the federation migrations itself in its Step 6,
and it refuses a database that already holds a KOI schema (`koi_migrations` or
`koi_net_events`) but no `koi_schema_migrations`, which is what the runner leaves.

1. Peer machine:
   - `./join-request.sh <node-name>`
2. Admin machine:
   - `./approve-peer.sh --from-file <join-request.txt> <peer-number> [relay-ssh]`
3. Peer machine:
   - Insert private key into generated `wg-koi.conf`, then `wg-quick up wg-koi`
4. Peer machine:
   - `./setup-node.sh --yes <node-name> <wireguard-ip> <koi-processor-path>`
5. Either side:
   - `./connect-peers.sh http://<other-peer-wg-ip>:8351 <alias>`

## Validation

Run post-setup validation:

```bash
./validate-node.sh --expect-wg-ip <wg-ip>
```

Checks include:

- Required commands available (`wg`, `psql`, `curl`, `python3`)
- Security policy flags enabled in `config/personal.env`
- `KOI_BASE_URL` uses WireGuard address (not localhost)
- WireGuard tunnel reachability (`<hub-wg-ip>`)
- PostgreSQL connectivity (`personal_koi`)
- Local API and `/koi-net/health` status

Use `--strict` to fail on warnings.

## Federation Smoke Test (Recommended)

After both peers are approved and connected, run a bidirectional share smoke test.

```bash
# From peer A: queue a test share to peer B
curl -sS -X POST http://127.0.0.1:8351/koi-net/share \
  -H 'Content-Type: application/json' \
  -d '{
    "document_rid":"orn:personal-koi.testdoc:smoke-a-to-b-<ts>",
    "recipient":"<alias-for-peer-b>",
    "message":"smoke a->b",
    "contents":{"title":"smoke","body":"federation test"}
  }'

# On peer B: verify receipt
curl -sS "http://127.0.0.1:8351/koi-net/shared-with-me?from_peer=<alias-for-peer-a>&limit=10"
```

`/koi-net/shared-with-me` supports `since` as ISO-8601 datetime:

```bash
curl -sS "http://127.0.0.1:8351/koi-net/shared-with-me?since=2026-02-25T20:22:00Z&limit=10"
```

## Non-Interactive Setup

`setup-node.sh` supports automation flags:

```bash
./setup-node.sh --yes --force <node-name> <wireguard-ip> [koi-processor-path]
```

- `--yes`: auto-accepts firewall sudo prompts
- `--force`: overwrite `config/personal.env` (with backup)
- `--skip-firewall`: leaves firewall unchanged, so nothing restricts the API port (see
  [Network exposure](#network-exposure))

Migration note:

- `setup-node.sh` defaults to federation manifest mode via:
  `scripts/federation/migration-manifest-federation.txt`
- This runs only KOI-net + vault-sync migrations in explicit order.
- Override by exporting `KOI_FEDERATION_MIGRATION_MANIFEST=/path/to/manifest.txt`.
- Legacy numeric mode remains available if no manifest is found
  (or if you unset `KOI_FEDERATION_MIGRATION_MANIFEST` and set
  `KOI_MIGRATION_MIN_NUM` explicitly).

## Network exposure

A node set up by `setup-node.sh` listens on all interfaces. The `start.sh` it generates
from `start-sh.template` runs `uvicorn api.personal_ingest_api:app --host 0.0.0.0 --port 8351`,
and the manual start command it prints when no `start.sh` exists also uses
`--host 0.0.0.0`. Peers must reach the node over WireGuard, and most routes, writes
included, have no authentication (see [Access control](../../README.md#access-control) in
the top-level README). So the only thing that keeps port 8351 off other networks is the
firewall `setup-node.sh` applies in its Step 5: on Linux, iptables rules that accept the
port on `lo` and `wg-koi` and drop it elsewhere; on macOS, a pf anchor `koi-net` that
passes it on `lo0` and `utun0` to `utun3` and blocks it elsewhere.

Confirm the rules are in force before you rely on them:

- `--skip-firewall`, or answering no at the prompt, applies no rules at all.
- On macOS the script loads the anchor with `sudo pfctl -a koi-net -f ... 2>/dev/null || true`
  and logs "pf rules applied" whether or not that worked.
- It appends the anchor to `/etc/pf.conf` but does not reload that file, so the running
  ruleset may not evaluate the `koi-net` anchor until `/etc/pf.conf` is loaded again
  (`sudo pfctl -f /etc/pf.conf`).
- If `/etc/pf.anchors/koi-net` already exists, a rerun skips the whole pf step, the load
  included.
- The pass rules name only `utun0` to `utun3`. On macOS system services often hold those,
  and the WireGuard tunnel may get a higher number (`wg show` names it), in which case the
  block rule also drops your peers' traffic.
- `validate-node.sh` checks neither the firewall nor the bind address.

To check: from another machine on the same LAN, `curl -m 5 http://<node-lan-ip>:8351/health`
should fail. On the node, `sudo pfctl -s info` shows whether pf is enabled and
`sudo pfctl -a koi-net -s rules` lists the anchor's rules (macOS), or
`sudo iptables -S INPUT | grep 8351` lists them (Linux).

## Vault Sync Smoke Test

After both peers have vault sync enabled (`VAULT_SYNC_ENABLED=true` in `config/personal.env`) and the `Shared/` folder exists, run the two-mode smoke test.

### Exact commands (two-node example)

```bash
# Pass 1: watcher disabled (isolate poll-based path)
KOI_ADMIN_TOKEN=<local-token> \
  PEER_KOI_ADMIN_TOKEN=<peer-token> \
  VAULT_SYNC_WATCHER=false \
  MODE=two-peer \
  PEER_SSH=<user>@<peer-host> \
  PEER_NAME=<peer-name> \
  KOI_TEST_ALLOW_LIVE=1 KOI_TEST_API_URL=http://localhost:8351 \
  KOI_TEST_DATABASE_URL=postgresql:///personal_koi VAULT_PATH=<this node's vault> \
  PEER_VAULT_PATH=<the peer's vault> \
  bash scripts/federation/smoke-vault-sync.sh --apply
# Expect: 15/15

# Pass 2: watcher enabled (default)
KOI_ADMIN_TOKEN=<local-token> \
  PEER_KOI_ADMIN_TOKEN=<peer-token> \
  MODE=two-peer \
  PEER_SSH=<user>@<peer-host> \
  PEER_NAME=<peer-name> \
  KOI_TEST_ALLOW_LIVE=1 KOI_TEST_API_URL=http://localhost:8351 \
  KOI_TEST_DATABASE_URL=postgresql:///personal_koi VAULT_PATH=<this node's vault> \
  PEER_VAULT_PATH=<the peer's vault> \
  bash scripts/federation/smoke-vault-sync.sh --apply
# Expect: 15/15
```

### General usage

```bash
# Local mode (single node, no peer needed):
KOI_ADMIN_TOKEN=<token> PEER_NAME=<peer-alias> \
  KOI_TEST_ALLOW_LIVE=1 KOI_TEST_API_URL=http://localhost:8351 \
  KOI_TEST_DATABASE_URL=postgresql:///personal_koi VAULT_PATH=<this node's vault> \
  bash scripts/federation/smoke-vault-sync.sh --apply

# Two-peer mode (requires SSH to peer):
KOI_ADMIN_TOKEN=<local-token> \
  MODE=two-peer \
  PEER_NAME=<peer-alias> \
  PEER_SSH=<user>@<peer-ip> \
  PEER_KOI_ADMIN_TOKEN=<peer-token> \
  KOI_TEST_ALLOW_LIVE=1 KOI_TEST_API_URL=http://localhost:8351 \
  KOI_TEST_DATABASE_URL=postgresql:///personal_koi VAULT_PATH=<this node's vault> \
  PEER_VAULT_PATH=<the peer's vault> \
  bash scripts/federation/smoke-vault-sync.sh --apply
```

Note: `PEER_NAME`, `VAULT_PATH` and, in two-peer mode, `PEER_SSH` and `PEER_VAULT_PATH`
have no default, and without `--apply` the script only prints its plan. `--apply` runs
only if `KOI_TEST_API_URL` and `KOI_TEST_DATABASE_URL` pass the test-target guard
(`KOI_TEST_ALLOW_LIVE=1` on a live node). On exit it restores the `vault_sync_peers` rows it
changed. A `~` in `PEER_VAULT_PATH` expands on the remote shell via unquoted SSH paths
(fixed in `5ddd839e`).

### Recommended gate sequence

1. Run `MODE=local` on node A.
2. Run `MODE=local` on node B.
3. Run `MODE=two-peer` from A -> B.
4. Run `MODE=two-peer` from B -> A.
5. Proceed only if all runs end with `FAIL: 0` and no increase in `rejected_events`.

### Prerequisites

- Migrations 049 + 050 applied on both nodes
- `VAULT_SYNC_ENABLED=true` in both nodes' `config/personal.env`
- `watchdog>=4.0.0` installed in venv
- `Shared/` folder exists in both vaults
- Handshake refreshed so edge `rid_types` includes `Vault-file`
- API running on both nodes
- Admin tokens accessible (env var or `$KOI_STATE_DIR/admin_token`)

The smoke test validates: configure, file create/track, peer arrival, conflict detection, delete/tombstone, peer delete propagation, and no rejected events.

## Soak Monitoring

Run periodic checks during the 72h soak window:

```bash
bash scripts/federation/soak-check.sh
```

Captures status + reconcile drift from both peers and appends timestamped JSONL to `/tmp/vault-sync-soak.jsonl`. Set `PEER_SSH` (the peer's SSH login) and `PEER_NAME` (its node name) for the peer to compare against.

## Domain Event Bridge (Federation Sync)

Beyond document sharing and vault sync, the federation layer supports **domain event replication** — full database sync of entities, tasks, claims, attestations, commitments, and pools between peers.

### How it works

Mutation endpoints (`/register-entity`, `/tasks/ingest`, `/claims`, etc.) emit FUN events with a `_koi_domain` marker in the event contents. The receiving node's poller dispatches these to `api/domain_event_handlers.py`, which applies UPSERT semantics to the local database.

### Supported domain types

| Domain | Source table | Conflict key | Event source |
|--------|-------------|--------------|--------------|
| `entity` | `entity_registry` | `fuseki_uri` | `/register-entity`, `/ingest` |
| `task` | `task_registry` | `task_key` | `/tasks/ingest`, `PATCH /tasks/{key}` |
| `claim` | `claims` | `claim_rid` | `/claims` mutations |
| `attestation` | `claim_attestations` | `(claim_rid, reviewer_uri)` | `/claims/{rid}/attestations` |
| `commitment` | `commitments` | `commitment_rid` | `/commitments/create`, state changes |
| `commitment_pool` | `commitment_pools` | `pool_rid` | `/pools/create` |

### Key files

- `api/federation_events.py` — Producer: `FederationEvents.emit()` queues domain events
- `api/domain_event_handlers.py` — Consumer: UPSERT handlers for each domain type
- `api/event_queue.py` — `_koi_domain` events bypass `rid_types` filter (always delivered)
- `tests/test_federation_bridge.py` — 29 automated tests

### Event format

```json
{
  "_koi_domain": "entity",
  "rid": "orn:personal-koi.entity:person-jane-doe-abc123",
  "payload": {
    "fuseki_uri": "orn:personal-koi.entity:person-jane-doe-abc123",
    "entity_text": "Jane Doe",
    "entity_type": "Person",
    "normalized_text": "jane doe"
  }
}
```

### Verification

```bash
# Check domain events in queue
psql -d personal_koi -c "SELECT rid, event_type, contents->>'_koi_domain' AS domain FROM koi_net_events ORDER BY queued_at DESC LIMIT 10"

# Run bridge test suite
pytest tests/test_federation_bridge.py -v
```

## Dry Run

Use `--dry-run` on the approval and removal scripts before mutating:

```bash
./approve-peer.sh --dry-run --from-file ./join-request.txt 22
./remove-peer.sh --dry-run <peer-name>
```

