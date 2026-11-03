# Peer node KOI-net upgrade checklist

Purpose: upgrade a peer node to the current KOI-net router behavior used by the federation onboarding scripts (including `/koi-net/share`).

## 1. Preflight

1. Confirm shell access to the peer host.
2. Identify the KOI repo path on the host.
3. Backup DB and env:
   - `pg_dump "$POSTGRES_URL" > backup_personal_koi_$(date +%F_%H%M%S).sql`
   - `cp config/personal.env config/personal.env.bak.$(date +%s)`

## 2. Code + Dependencies

1. Pull latest code:
   - `git fetch --all && git pull --ff-only`
2. Activate venv and install required packages:
   - `source venv/bin/activate`
   - `pip install -U pip`
   - `pip install cryptography psycopg2-binary`

## 3. Database Migrations

Apply KOI-net migrations at minimum:

1. `040_koi_net_federation.sql`
2. `041_koi_net_cross_refs.sql`
3. `042_koi_net_event_dedup.sql`
4. `043_event_target_node.sql`
5. `044_shared_documents.sql`
6. `045_outbound_share_ledger.sql`
7. `046_koi_net_nodes_ontology_columns.sql`
8. `047_shared_documents_intake.sql` (required for commons intake staging/approval APIs)

Apply only the files the database's migration ledger does not already list, one at a
time, and record each one you apply, as
[Existing databases](../operations/fresh-install-migrations.md#existing-databases)
describes. This loop applies them in order and stops at the first error, but it records
nothing in the ledger:

```bash
for f in \
  040_koi_net_federation.sql \
  041_koi_net_cross_refs.sql \
  042_koi_net_event_dedup.sql \
  043_event_target_node.sql \
  044_shared_documents.sql \
  045_outbound_share_ledger.sql \
  046_koi_net_nodes_ontology_columns.sql \
  047_shared_documents_intake.sql
do
  psql "$POSTGRES_URL" -v ON_ERROR_STOP=1 -f "migrations/$f" || break
done
```

Never apply the whole `migrations/` directory this way
([migration-setup.md](../operations/migration-setup.md)).

## 4. Config Checks

In `config/personal.env` confirm:

1. `KOI_NET_ENABLED=true`
2. `KOI_BASE_URL` is peer-reachable (not `localhost`)
3. `KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL=true`
4. `KOI_ENFORCE_SOURCE_KEY_RID_BINDING=true`
5. `KOI_STATE_DIR` exists and is writable
6. For commons intake staging (recommended on commons nodes):
   - `KOI_COMMONS_INTAKE_ENABLED=true`
   - `KOI_COMMONS_AUTO_APPROVE=false`

## 5. Restart + Health

1. Restart service (`start.sh`, systemd, or supervisor).
2. Verify:
   - `curl -sS http://127.0.0.1:8351/koi-net/health | jq .status` returns `healthy`
   - `curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8351/koi-net/share` is **not** `404` (expected is method/validation error on GET)
   - `curl -sS http://127.0.0.1:8351/koi-net/commons/intake?status=staged | jq .count` returns JSON (not schema error)

## 6. Federation Smoke Test

1. Re-run peer connect from the coordinator side:
   - `./scripts/federation/connect-peers.sh http://<peer-host>:8351 <peer-name>`
2. Share a test doc from the coordinator:
   - `POST /koi-net/share` with `recipient: "<peer-name>"`
3. Confirm the peer can poll and receive that event.
4. If running as commons intake node, verify staged workflow:
   - Share with `recipient_type=commons`
   - Confirm intake row appears at `GET /koi-net/commons/intake?status=staged`
   - Approve via `POST /koi-net/commons/intake/decide`

## 7. Rollback (if needed)

1. Stop service.
2. Restore previous code commit.
3. Restore `personal.env` backup.
4. Restore DB backup dump.
5. Start service and verify health.
