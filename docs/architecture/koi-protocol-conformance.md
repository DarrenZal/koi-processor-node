# KOI protocol conformance

**Status:** living document · first written 2026-08-26 · interop gate run 2026-10-31
**Scope:** `api/koi_net_router.py`, `api/koi_poller.py`, `api/event_queue.py`, `api/vault_sync.py`

## Why this document exists

This repository implements the KOI-net node protocol by hand — roughly 2,500
lines in `koi_net_router.py` plus the poller and event queue. Upstream publishes a
reference implementation, `koi-net`, and we do not use it at runtime. We use only
`rid-lib`; the conformance tests use `koi-net` as a test-only dependency.

That is a fork. Forking is a legitimate choice; drifting into one is not. The
purpose of this file is to make every divergence a **decision with a reason**
rather than an accident nobody remembers making.

Each divergence below is classified:

- **CONFORM** — we match the reference.
- **INTENTIONAL FORK** — we deliberately differ, with the reason recorded.
- **DEFECT** — we differ by accident and it should be fixed.

## Upstream, as of 2026-08-26

| Package | Latest | Released | Releases | We use |
|---|---|---|---|---|
| `koi-net` (`DynamicalSystemsGroup/koi-net`) | 2.1.2 | 2026-08-06 | 80 | **not at runtime** (test-only, `tests/conformance/`) |
| `rid-lib` (`DynamicalSystemsGroup/rid-lib`) | 3.3.0 | 2026-06-18 | 24 | yes, pinned **3.3.0** (bumped 2026-08-26) |
| `koi-net-obsidian-manager-node` | 0.1.0 | 2026-04-01 | 1 | no |
| `koi-net-coordinator-node` | 0.1.0 | 2026-03-25 | 1 | no |
| `koi-net-graph-extension` | 0.1.3 | 2026-06-18 | 1 | no |

**Provenance.** The koi-net and rid-lib repositories moved from the BlockScience GitHub
organization to DynamicalSystemsGroup; GitHub redirects the old URLs. The core repositories are
[`koi-net`](https://github.com/DynamicalSystemsGroup/koi-net/tree/v2.1.2), [`rid-lib`](https://github.com/DynamicalSystemsGroup/rid-lib/tree/v3.3.0) and
[`koi-net-node-template`](https://github.com/DynamicalSystemsGroup/koi-net-node-template/tree/c45b188f1acdb357c907d2478b2804a0cf3b07d7), plus more in the lineage under the same
organization. Use those for any implementation claim.

The official spec at `dynamicalsystemsgroup.github.io/koi-net-spec` has **no public
source repo** (`koi-net-spec` → 404), so it cannot be cloned or diffed. It is
pinned by content hash instead — see "Edge direction and `rid_types`" below.

`koi-net` 2.1.2 is ~5,694 LOC across `protocol/` (event, edge, node, envelope,
secure, knowledge_object), `components/` (sync_manager, event_buffer, cache,
effector, graph, secure_manager, server, response_handler), `config/` and
`infra/`. It is actively maintained. `koi-net` 2.1.2 requires `rid-lib>=3.3.0`.

**A note on the Obsidian node**, because its name invites a wrong conclusion:
`koi-net-obsidian-manager-node` 0.1.0 is an early scaffold — an
`ObsidianVault`/`ObsidianNote` RID type pair plus a contact handler that proposes
edges to nodes advertising `ObsidianNote`. It does not yet watch, sync or detect
deletions, so it does not do what `api/vault_sync.py` does here.

---

## Decision: do not port to upstream `koi-net`

> **CORRECTED 2026-08-26.** The decision below was stated as "do not port to
> upstream `koi-net`" and justified entirely with `components/` reasons — the
> Postgres store, the durable queue. Those reasons are sound, and they say
> **nothing about `koi_net.protocol`**, which is a separate 676-line layer that
> imports `rid_lib`, pydantic, `cryptography`, `structlog` and `koi_net.exceptions`,
> and nothing from `components/` (verified against the imports of every module in
> `protocol/` in the 2.1.2 wheel). Treating one decision
> as covering both layers is how a storage constraint silently became a licence
> to diverge on the wire format — and we did diverge, in a way that made our
> vault-file events unparseable by any stock KOI-net node (divergence 11).
>
> **The decision is now split:**
>
> | layer | LOC | decision |
> |---|---|---|
> | `koi_net.protocol` (wire format, API paths, Event/Manifest/Edge/Node) | 676 | **CONFORM.** Match it. Validate against it. |
> | `koi_net.components` (cache, event buffers, effector, server) | 2,881 | **INTENTIONAL FORK**, for the reasons below. |

**Decision:** keep the local `components` implementation; conform to
`koi_net.protocol`. Bump `rid-lib` to 3.3.0 on its own merits. Revisit if the
constraints below change.

**Reasons, in order of weight:**

1. **Storage model mismatch.** `koi-net` is built around a `rid-lib` file `Cache`
   as the knowledge store. This deployment's entire value is knowledge in
   Postgres + pgvector — entity resolution, embeddings, SQL-queryable facts.
   Adoption means reimplementing `Cache` over Postgres, which is the majority of
   the port and the part most likely to be subtly wrong.
2. **Durability of the event queue.** Upstream `EventBuffer`s are in-process and
   die with the process. Our `koi_net_events` is a Postgres table with a 24 h TTL,
   and peers on a small mesh can be unreachable for **weeks**. Losing a durable queue
   is a regression, not an upgrade.
3. **No upstream equivalent** for commons intake, vault sync, share/retract, the
   E2EE `encryption_key` exchange, or `ontology_uri`. These are not incidental;
   they are the product.
4. **Cost is concrete.** Five endpoints would need re-hosting.

**What the decision does not excuse.** "We forked" is not a reason to keep the
defects below.

---

## Divergence register

### 1. `orn:koi-net.vault-file:` occupies the protocol's reserved namespace — **DEFECT**

`rid_lib.types` reserves the `koi-net` namespace for exactly two protocol
objects:

```
KoiNetNode.namespace = koi-net.node
KoiNetEdge.namespace = koi-net.edge
```

We mint `orn:koi-net.vault-file:Shared/…`, which is application data wearing
protocol clothes.

> **CORRECTED 2026-08-26.** This entry previously said "Upstream defines
> `obsidian.vault` / `obsidian.note` for exactly this content" and proposed
> migrating there. The first half is true; the conclusion is **wrong**: it
> would have broken every nested vault path.

`obsidian.note` is defined by `koi-net-obsidian-manager-node`, not by `rid_lib`
core (checked: `rid_lib.types` has no Obsidian type in 3.3.0). Its reference
grammar is:

```python
class ObsidianNote(ORN):
    namespace = "obsidian.note"
    @classmethod
    def from_reference(cls, reference):
        components = reference.split("/")
        if len(components) == 2: return cls(*components)
        raise ValueError("...must contain two '/'-separated components: <vault_id>/<note_id>")
```

**Exactly two components.** Vault paths are nested (folders within the shared
folder), and a nested path has more than two components.

Verified directly: `ObsidianNote.from_reference("Shared/Projects/Example.md")`
raises. `koi-net-obsidian-manager-node` 0.1.0's `ObsidianNote` type cannot express a
nested vault path.

**So this is a deliberate fork with a measured reason** — the bar this document
exists to enforce. We move to a namespace we *own*, `personal-koi.vault-file`,
matching the `personal-koi.*` convention already used for
entity/doclink/knowledge-episode. That fixes the actual defect (squatting a
reserved namespace) without adopting a grammar that cannot hold our data.

**Migration is staged** (expand → migrate → contract), because a RID change is a
wire change:

1. **EXPAND — done 2026-08-26.** Both namespaces are accepted inbound
   (`parse_vault_file_rid`), all SQL matches both (`vault_file_rid_sql_clause`),
   and emission still uses the legacy namespace. No peer can break.
2. **MIGRATE — not done.** Set `KOI_VAULT_FILE_NAMESPACE=personal-koi.vault-file`
   once every peer accepts both. A peer that has not been upgraded does not accept the
   new namespace, so this must not be flipped unilaterally.
3. **CONTRACT — not done.** Drop legacy acceptance only after the queue holds no
   legacy RIDs.

The cutover is a no-op for edge scoping: both namespaces resolve to rid type
`Vault-file`, so no edge needs re-negotiating. Pinned by
`test_legacy_and_migrated_namespaces_agree`.

**Prerequisite that was nearly missed:** `extract_rid_type` returned `None` for
`personal-koi.*`, and since divergence 10 made the filter fail closed, flipping
the namespace would have made vault files **undeliverable to every edge**. It now
parses ORNs with `rid_lib.core.RID.from_string` instead of substring-matching —
see divergence 13.

### 2. FORGET ships the full record — **INTENTIONAL FORK (newly deliberate), pending a pre-delete snapshot**

Upstream builds a delete as `Event.from_rid(EventType.FORGET, rid)` — a bare RID,
no manifest, no contents. Our vault-sync FORGET events carry a manifest and a
`contents` object, built by the same `VaultSyncManager._queue_event`
(`api/vault_sync.py`) as NEW and UPDATE events.

That is backwards as a privacy default. It is kept for now because queued event
payloads have served as a recovery path for a wrongly deleted file.

**Resolution:** the pre-delete snapshot belongs in version control of the vault, not in
the wire format. **Once that snapshot is relied on, strip payload from FORGET.**

### 3. Read-time filtering over a broadcast table — **INTENTIONAL FORK**

Upstream targets events into per-peer `EventBuffer`s at write time. We write one
broadcast row (`target_node IS NULL`) and filter per peer at read time (`poll()` and
`peek_undelivered()` in `api/event_queue.py`).

Kept deliberately: one durable row per event is far cheaper than N per-peer copies
for a mesh with long-absent peers, and it is what makes a late-returning peer able
to drain months of history.

**The cost is real, though**, and it is why exclusion had to be retrofitted as a
per-peer read-time filter — which is precisely where defects 4, 5 and 9 live. Any
future scoping mechanism has to be built on the read path, so it must be
fail-closed by construction.

### 4. Empty `rid_types` was fail-open — **DEFECT (fixed 2026-08-26)**

`event_queue.poll()` gated on `if rid_types:` — a truthiness test. An edge with
`rid_types = {}` therefore disabled filtering **entirely** and received
everything. Upstream does a membership test against a declared set, which is
fail-closed.

**Fixed 2026-08-26:** `api/event_queue.py` now tests `if rid_types is not None:` in both
read paths, so an empty list means "nothing".

### 5. `edge_rid` embeds node identities as a formatted string — **DEFECT (low)**

Spec §3.2.1 says edge RIDs MUST be `orn:koi-net.edge:<hash>`, the SHA-256 hash of the
source and target RIDs, and upstream derives them that way. We format
`orn:koi-net.edge:<source>><target>:poll`, which is a spec violation as well as a
legibility problem. Because the row is upserted on
`edge_rid`, a peer that re-keys leaves its **old** identity baked into the string
while `target_node` moves on.

So the string can name an identity the peer no longer uses. The filtering itself is
correct — the poll gate queries `target_node` — so it is not a security defect.

### 6. `koi_net_nodes.last_seen` was never updated on poll — **DEFECT (fixed 2026-08-26)**

It was written only by handshake and key-bootstrap paths, so it went stale while peers
kept polling, and a peer that was still delivering could be reported as offline.

**Fixed 2026-08-26:** the `/events/poll` handler now sets `last_seen` on every poll
(`api/koi_net_router.py`). Values written before then are stale.

### 7. `/handshake` with auto-APPROVE — **INTENTIONAL FORK, with a caveat**

Upstream has no such endpoint and approves an edge only after checking that the
requested `rid_types` are a subset of what the peer declares it provides. Our
local handshake auto-approves.

Kept for operator ergonomics on a small trusted mesh. **The caveat is that
auto-approval without the subset check is how an edge ends up broader than either
side intended** — which is the shape of defect 9. If the mesh grows beyond
hand-managed peers, adopt the subset check.

### 8. Two node identities sharing one `base_url` — **DEFECT (operational)**

`koi_net_nodes` is unique on `node_rid`, not `node_name`, and nothing prevents two
identities pointing at the same address. A `rejected` status is therefore not
enforceable by identity: a second identity at the same address is a separate row with
its own status, and two nodes can disagree about it.

Upstream has no status field at all; trust is the edge plus a pinned key.

### 9. `rid_types` conflates two orthogonal axes — **DEFECT (design)**

A single `rid_types` list is matched against *both* a RID type (`Vault-file`,
`Person`) and a domain-event name (`entity`, `task`, `knowledge_episode`). These
are different axes. Entity events are domain events: they carry `_koi_domain = entity`
and the payload's extraction-time `entity_type`, not the canonical type.

So an edge scoped `{SpecDoc}` matches against the domain name `entity`, which is
not in the list, and **carries no SpecDoc entity events**. It carries only RID-typed
`orn:koi-net.specdoc:*` objects (the `koi_rid` that `/register-entity` mints for an
entity registered with `publication_scope=federated`). The channel is empty except for
those and whatever the peer puts into it.

**A trap for anyone measuring this:** querying
`contents->'payload'->>'entity_type' = 'SpecDoc'` finds nothing even when SpecDoc
entities have federated, because the payload carries the extraction-time label. Any
scoping built on the payload label would also miss them.

**Fix direction:** either a qualified form (`entity:SpecDoc`) or separate
`domains` and `rid_types` columns on the edge. Whichever is chosen, it must key
off the canonical `entity_registry.entity_type`, not the payload label.

### 10. An unrecognized RID namespace bypasses the edge scope — **DEFECT (fixed 2026-08-26)**

Found while verifying divergence 9, and it is the same fail-open shape as defect 4
one branch further down. `extract_rid_type` returns `None` for any namespace outside
`koi-net.*` and `entity:`. The RID branch of the filter read:

```python
excluded = bool(rid_type) and rid_type.lower() not in lowered
```

`bool(None)` is `False`, so an unrecognized RID short-circuited to *not excluded* and
was delivered to every peer **regardless of what its edge declared**. Where defect 4
was "the edge declares nothing", this is "the event resolves to nothing".

Namespaces this affected: `obsidian.note` and test namespaces resolved to `None` and
bypassed the scope. `koi-net.vault-file` was filtered correctly, and
`personal-koi.doclink` and `.knowledge-episode` were not affected: they carry
`_koi_domain` and take the domain branch.

The exposure is narrow but it is precisely the wrong one. `orn:obsidian.note:` is the
namespace **our own vault frontmatter already uses** (divergence 1), so a vault file
emitted under it would have been served to any scoped edge. "No vault-file path reaches
a scoped edge" held for `orn:koi-net.vault-file:` RIDs and **not** for `obsidian.note`
ones.

**Fix:** `excluded = rid_type is None or rid_type.lower() not in lowered`, in both
`poll()` and `peek_undelivered()`. Unknown type matches no declared type. This makes
`obsidian.note` events undeliverable to *any* scoped edge, which is the safe default
and is already the status quo in effect: these events were already being left
unapplied. The real repair is divergence 1, the namespace
migration.

### 11. Vault-file manifests were unparseable by a stock KOI-net node — **DEFECT (fixed 2026-08-26)**

The most consequential divergence found so far, and it was invisible because
both ends of this mesh run our code.

`koi_net.protocol.Event.manifest` is typed as `rid_lib.ext.Manifest`, which
requires exactly three fields: `rid`, `timestamp`, `sha256_hash`. We emitted:

```json
{"bytes": 333, "deleted": false, "base_hash": null,
 "timestamp": "...", "origin_seq": 251, "origin_node": "orn:koi-net.node:...",
 "content_hash": "a40683fd...", "relative_path": "Meetings/..."}
```

`timestamp` matched. `rid` was absent. `sha256_hash` was present under the name
`content_hash`. Validating a **real payload pulled from `koi_net_events`**
against koi-net 2.1.2 + rid-lib 3.3.0:

```
2 validation errors for Event
  manifest.rid          Field required
  manifest.sha256_hash  Field required
```

So **every vault-file event this node has ever sent would be rejected by a stock
KOI-net node.** Domain events were unaffected — they carry `manifest=None`, so
there is nothing to validate, which is why the defect never surfaced.

We already held all three values, so this was a naming mismatch, not a missing
capability. `rid` and `sha256_hash` are now emitted alongside the existing
fields; pydantic ignores unknown keys, so upstream reads the three it needs and
all seven of our extensions survive on the wire. Verified: `Event`,
`EventsPayload` (the `/events/poll` response body) and the bundle round-trip all
parse.

The **inbound** path was fixed symmetrically: it read `manifest["content_hash"]`
and would have rejected a stock node's events. It now accepts either spelling and
derives `relative_path` from the RID when the peer sends only the upstream
three-field manifest.

**Not a divergence, worth recording:** our five KOI-net endpoint paths already
match `koi_net.protocol.api.paths` exactly (`/events/broadcast`, `/events/poll`,
`/rids/fetch`, `/manifests/fetch`, `/bundles/fetch`, under a `/koi-net` prefix).
The paths match; how a peer's `base_url` is joined to them does not (14), and the
conformance tests and the interop gate found more (15–21).

### 12. Edge shape and status vocabulary differ from `EdgeProfile` — **DEFECT (latent)**

Found by validating live API output against upstream models on 2026-08-26.

**Field names.** `GET /koi-net/edges` returns
`{edge_rid, edge_type, source_node, status, target_node}`. Upstream
`koi_net.protocol.edge.EdgeProfile` requires
`{source, target, edge_type, status, rid_types}`. Our edges therefore fail
`EdgeProfile.model_validate` on the first required field, `source`.

**Status vocabulary.** Upstream `EdgeStatus` has exactly two members,
`PROPOSED` and `APPROVED`. We use four — `APPROVED`, `PROPOSED`, `REJECTED`,
`REVOKED`. `REJECTED` and `REVOKED`
have no upstream representation at all.

**Why this is latent rather than breaking:** this code emits no `koi-net.edge`
events. We never exchange edges as bundles; `/koi-net/edges` is our own
administrative endpoint, which upstream does not define, and edge negotiation
here happens through `/handshake` (divergence 7). So nothing currently
serializes an edge to a peer.

It stops being latent the moment edges are exchanged as bundles — which is the
upstream mechanism, and the natural thing to adopt if the mesh grows past
hand-managed peers. **Fix direction:** rename on the wire (keep the DB columns),
and decide whether `REJECTED`/`REVOKED` become an extension field or collapse to
"absent edge". Do not silently emit a four-value status into a two-value enum.

**Verified conformant on the same pass**, so the register does not read as
all-bad news:

| surface | result |
|---|---|
| the five endpoint paths | match `koi_net.protocol.api.paths` exactly |
| `NodeProfile` on the wire | **parses** (`base_url`, `node_type`, `provides`, `public_key`); its `provides.event` content is wrong (21) |
| `Event` / `EventsPayload` / manifest | **parses**, after divergence 11 was fixed; a koi-net client still rejects a non-empty poll response's signature (16) |
| `rid-lib` canonical hashing | identical 3.2.12 → 3.3.0, pinned by test |

### 13. `extract_rid_type` guessed by substring instead of parsing — **DEFECT (fixed 2026-08-26)**

The poll filter's type extractor was two hardcoded substring tests:

```python
if "koi-net." in rid: ...split on "koi-net."
if "entity:"  in rid: ...split on "entity:"
```

Substring-matching a structured identifier, with three live consequences once
divergence 10 made the filter fail closed:

| RID | old result | correct |
|---|---|---|
| `orn:personal-koi.entity:abc` | **`'Abc'`** — the *slug*, as a type | `Entity` |
| `orn:personal-koi.doclink:abc` | `None` → excluded from every edge | `Doclink` |
| `orn:personal-koi.vault-file:x` | `None` → **blocked divergence 1 outright** | `Vault-file` |
| non-`str` input | **raised**, aborting the poll for every peer | `None` |

The node never mints the `orn:entity:{type}/{slug}` form the second branch existed
for, so that branch was dead code.

Now uses `rid_lib.core.RID.from_string` — upstream's own parser — and takes the
last dot-segment of the namespace. This is the small version of the whole
argument: we were hand-rolling a parser for a format someone else specifies.

One inherited behaviour change, pinned by test: `rid_lib` validates references
for types it registers, so `orn:koi-net.node:probe` (no `+hash`) now returns
`None` and is excluded, where the substring version returned `'Node'`. Failing
closed on a malformed protocol RID is correct; real node RIDs carry the hash and
are unaffected.

### 14. `base_url` means the host root here, the KOI-net API root upstream — **DEFECT (interop)**

Found preparing the interop gate, 2026-10-31. Upstream's `RequestHandler` calls
`get_base_url(node) + path`, with `base_url` such as `http://127.0.0.1:8000/koi-net`
(`koi_net.config.server_config.ServerConfig.url`). Our nodes advertise the host root
(`http://127.0.0.1:8351`) and `api/koi_poller.py` builds `f"{base_url}/koi-net/..."`.
So a stock node calling a root-advertising node of ours gets 404s, and our poller
calling a stock node doubles the prefix (`/koi-net/koi-net/...`). Not exercised live:
the throwaway node in the gate advertised the upstream form, and no edge formed (20).
**Fix direction:** accept both forms when building peer URLs, then advertise the
upstream form once every peer of ours accepts it.

### 15. The node's own `KoiNetNode` bundle is not served — **DEFECT (interop)**

Found by the conformance tests, 2026-10-31. `/bundles/fetch` for our own node RID answers
`not_found`, and `/rids/fetch` does not list it, even for `rid_types=["orn:koi-net.node"]`.
A koi-net node that asks for our profile gets nothing. The interop gate did not exercise
this: the stock node never requested `/bundles/fetch`, because core koi-net 2.1.2 does not
fetch a first contact's profile on its own (20). Pinned by
`tests/conformance/test_wire_conformance.py::test_fetch_bundles_returns_our_node_bundle`
(strict xfail).

### 16. `event_id` rides inside the signed poll payload — **DEFECT (interop, breaks poll)**

Found by the conformance tests, 2026-10-31. Poll events carry our `event_id` extension
(used by `/events/confirm`). koi-net verifies a response by re-serializing the parsed
payload; its `Event` model has no `event_id`, drops it, and the signature check fails.
Every koi-net client therefore rejects a non-empty poll response from us. **Fix
direction:** carry the id outside the signed payload, or omit it for peers that are
not ours. Pinned by `test_poll_events_signature_verifies` (strict xfail).

### 17. Domain events use application keys as RIDs and NEW events carry no manifest — **DEFECT (design)**

Found by the conformance tests, 2026-10-31. A task domain event has the task key
(`conformance::…`) as its `rid`; rid-lib accepts it only as a generic URI with scheme
`conformance`. Its NEW event has no manifest, which upstream NEW/UPDATE events always
carry. Pinned by `test_emitted_domain_events_use_koi_rids_and_manifests` (strict xfail).

### 18. The approved-edge gate refuses the upstream handshake — **INTENTIONAL FORK (membrane), decision open**

`config/personal.env.example` sets `KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL=true`. Then
`/events/broadcast` refuses any node without an approved edge, and the upstream handshake
(a new node broadcasting `FORGET` + `NEW` of its own bundle) can never succeed: the gate
run with it on ended with HTTP 403 and the stock node exited. That is the membrane working
as designed; while it is on, stock nodes cannot join.

The refused node is still recorded. On a first-contact broadcast, `_unwrap_request`
verifies the signature with the key in the node's own bundle and stores the node in
`koi_net_nodes` with status `active` before `/events/broadcast` reaches the gate (the
gate-on run's server log shows "registered new peer", then the 403). The gate still drops
its events and serves it nothing without an approved edge, but anyone with a key pair can
add a node row.

**Error type.** The refusal is sent as `invalid_signature`, the default of
`_ERROR_TYPE_MAP`, which tells the peer its signature is bad when it is not. Mapping it to
`unknown_node` was tried on 2026-10-31 and reverted the same day: koi-net 2.0.x clients
answer `unknown_node` by re-handshaking, the handshake broadcast meets the same gate, and
they loop. koi-net 2.1.2 does nothing on
`unknown_node` (its error handler notes that it used to handshake). `tests/test_koi_net_gate_error_type.py`
pins `invalid_signature`.

**Open decision.** Options:
- answer a gated broadcast with HTTP 200 and an empty body (spec §4.1.1; see 22), as the
  poll and fetch gates already answer with empty payloads;
- rate-limit refusals per source node;
- declare koi-net 2.1.0 the supported floor, then send `unknown_node`.

### 19. Manifests payload omitted `not_found` — **DEFECT (fixed 2026-10-31)**

`koi_net.protocol.api.models.ManifestsPayload` declares `not_found` with a default, so a
koi-net client re-serializes it before verifying, and every signed `/manifests/fetch`
response failed its signature check. `ManifestsPayloadResponse` now always carries
`not_found`. When the handler serves a request, every requested RID is in exactly one of
the two lists: `manifests`, or `not_found` (no manifest, or blocked). When it serves
nothing, both lists are empty: a strict-gate refusal (unsigned caller, or no approved poll
edge), an unidentified caller, or a missing `federation_rid_blocks` table. A request whose
`rids` is not a list of strings gets HTTP 400. On the wire its koi-net `error` field is
`invalid_signature` (the `_ERROR_TYPE_MAP` default), with `error_code: INVALID_PAYLOAD` as
our extension, so it carries the same misreport as 18 and waits on 18's decision. (Before
this check the `not_found` change answered a null RID with a 500; `regen-prod` answered it
with 200 and empty lists.) Pinned by `test_fetch_manifests_response_parses`, which sends a non-empty
request, and `tests/test_koi_net_manifests_fetch_rids.py`.

### 20. After a handshake, our node neither pushes its own bundle nor proposes an edge — **DEFECT (interop, blocks edge negotiation)**

Found by the interop gate, 2026-10-31. Core koi-net 2.1.2 learns a first contact's profile
only when that contact pushes it. The reference coordinator's
[handshake handler](https://github.com/DynamicalSystemsGroup/koi-net-coordinator-node/blob/00e6063d8763a309a88fdb9b82f6b4ee1ca3e5cf/src/koi_net_coordinator_node/handshake_handler.py) answers a node announcing itself (a `NEW` event for its own
RID) by sending its own node bundle back as a `NEW` event and proposing an edge (source:
the new node, target: itself, `rid_types` `[KoiNetNode, KoiNetEdge]`). Our
`/events/broadcast` queues the two events and does neither. In the gate the stock node
therefore never saw our profile and never asked for it.

The spec does not require either step. §5.1 says the responder MAY broadcast its own node
profile bundle, and §5.2 leaves edge proposals to a node that wants a subscription; nothing
obliges the responder to subscribe to the initiator. So 20 is a gap against koi-net 2.1.2
and the reference coordinator, not against the spec text. It is recorded as a defect
because stock nodes depend on that behaviour.

### 21. `provides.event` advertises entity-type names, never `KoiNetNode` or `KoiNetEdge` — **DEFECT (interop)**

Found by the interop gate, 2026-10-31. Our `NodeProfile.provides.event` is
`["Organization", "Person", "Project", "Concept", "Location", "Vault-file"]`: bare type
names, not the RID types our events carry, and without `orn:koi-net.node` or
`orn:koi-net.edge`. A stock node subscribes to the intersection of its
`rid_types_of_interest` (the template's default is `[KoiNetNode]`) with `provides.event`;
that intersection is empty, so even with our bundle it would propose no edge. Related to 9.

The spec is looser than the implementation here. §5.2 says `rid_types` SHOULD contain only
RID types the publisher provides in its node profile, "or koi-net.node or koi-net.edge,
which are implicitly provided by all nodes". By that text the missing `KoiNetNode` and
`KoiNetEdge` are not a violation; the entity-type names are. koi-net 2.1.2's
`NodeContactHandler` intersects with `provides.event` only, so stock nodes need them listed,
as the reference coordinator lists them.

**Upstream packaging note, not a divergence of ours:** `koi-net` 2.1.2 declares
`Requires-Python >=3.10` but uses PEP 695 `type` statements, so it fails to import below
Python 3.12. The conformance venv uses 3.13.

### 22. `/events/broadcast` answers with a body — **DEFECT (interop, low)**

Spec §4.1.1 says broadcast "Responses MUST contain an empty body", and koi-net 2.1.2's
`model_map` declares no response model for `/events/broadcast`. Our handler answers
`{"status": "ok", "queued": N}`. A koi-net client does not parse that body, so nothing
breaks today; the register records it because 18's first option would add another body.

## What the protocol actually governs — and what it therefore does NOT

This settles a question that kept recurring as "is our ownership policy a fork?"

> "KOI-nets are heterogenous compositions of KOI nodes, each of which is capable of
> **autonomously** inputting, processing, and outputting knowledge. The behavior of
> each node and configuration of each network can vary greatly… **The protocol only
> governs communication between nodes, not how they operate internally.**"
>
> — `DynamicalSystemsGroup/koi-net-demo-v1`, `README.md:11`, HEAD `e1483ec`

**Provenance:** the specification says the same. §1 (Introduction) of the
[KOI-net Protocol Specification Draft v1.0](https://dynamicalsystemsgroup.github.io/koi-net-spec/v1.0-draft-release.html)
reads: "The protocol only governs communication between nodes, not how they operate
internally." The spec is a draft, and its source repo is not public (`koi-net-spec` →
404), so the text can be read but not pinned to a commit. Re-check it when the spec
leaves draft.

On that basis, the following are **local effector policy, not protocol forks**, and
should stop being counted as divergences:

| our behaviour | why it is not a fork |
|---|---|
| `KOI_VAULT_READONLY_PATHS` — reject an inbound UPDATE/FORGET for a path we own | what a node does on receiving an event is internal |
| `KOI_VAULT_MIRROR_PATHS` — accept an owner's events without conflict copies | same |
| refusing to *emit* a FORGET for a file that is still on disk | this is our own emission correctness, not a wire change |
| conflict copies instead of overwriting | internal storage behaviour |

The genuine wire-level obligations remain the ones in the register: the five endpoint
paths, the `Event`/`Manifest` shape (divergence 11), the RID namespace (divergence 1),
and the edge scoping vocabulary (divergences 9 and 12). **`rid_types` on an edge is
the protocol's authorization boundary; `vault_sync_peers` is our emitter's
configuration.** They are different mechanisms and neither substitutes for the other
— which is exactly why re-enabling vault sync needs *both* controls.

## Edge direction and `rid_types` — pinned, including where the spec text is inconsistent

**Pinned source.** The rendered draft has no public source repo, so it is pinned by
content hash instead:

```
https://dynamicalsystemsgroup.github.io/koi-net-spec/v1.0-draft-release.html
sha256  b0959d7881cebe47fc3c82d4db858428a11294cd23356c2ee3d28a2e1072497f
retrieved 2026-08-26 (96,189 bytes; hash re-computed locally, matches)
```

### What `rid_types` is

`rid_types` on an edge is the **protocol's event-subscription / delivery scope** —
the declared set of RID types that flow along that edge. It is a *routing* concept.

**Requiring an `APPROVED` edge before serving a poll is our own local
authorization control**, layered on top. Upstream `EdgeStatus` has exactly two
members, `PROPOSED` and `APPROVED` ([`protocol/edge.py`](https://github.com/DynamicalSystemsGroup/koi-net/blob/7a4d631bcfec86a162d07d8f1b0a004b671c0f0f/src/koi_net/protocol/edge.py) at `7a4d631`); our
`REJECTED`/`REVOKED` are local additions (divergence 12). So: subscription scope is
protocol, enforcement posture is ours.

### Direction: `source` = publisher, `target` = subscriber

Verified in the current implementation rather than inferred:

| evidence | at |
|---|---|
| subscribers derived from **out**-edges | [`components/knowledge_handlers/basic_network_output_filter.py:52`](https://github.com/DynamicalSystemsGroup/koi-net/blob/7a4d631bcfec86a162d07d8f1b0a004b671c0f0f/src/koi_net/components/knowledge_handlers/basic_network_output_filter.py#L52) (`get_neighbors(direction="out", …)`) |
| `poll_neighbors()` applies **no** direction filter — both directions are polled | [`components/resolver.py:103`](https://github.com/DynamicalSystemsGroup/koi-net/blob/7a4d631bcfec86a162d07d8f1b0a004b671c0f0f/src/koi_net/components/resolver.py#L103) |
| our poller polls the `source_node` of edges whose `target_node` is us | `api/koi_poller.py:207-211` |

So an edge `A -> B` means **A publishes, B subscribes**, and a node consults edges
in *both* directions: out-edges to decide whom to serve, in-edges to decide whom to
poll.

### Two sentences in §5.2 appear to disagree, and which reading we adopt

§3.2 and the first paragraph of §5.2 say the same thing: an edge is "represented as a
directed edge in a graph, where the source is the publisher and the target is the
subscriber". The field rules later in §5.2 say "The source MUST be the subscriber's RID"
and "The target MUST be the publisher's RID". This looks like an editorial slip inside
§5.2 rather than a change of model.

**Adopted deliberately: §3.2 plus the current `koi-net` implementation at
`7a4d631`** — publisher→subscriber. Reasons, in order:

1. The implementation is unambiguous and is what any peer we actually federate with
   will be running.
2. Our own poller already matches it (`koi_poller.py:207-211`).
3. It agrees with §3.2 and with §5.2's own first paragraph.

**Recorded as an ambiguity to re-check when the spec leaves draft**, and worth raising
with the spec's maintainers, because if the field rules turn out to be the intended reading, every edge in this deployment is
backwards and the fix is a migration, not a patch.

## PENDING GATE — conformance is asserted against models, not against a running node

Everything claimed in divergences 11 and 12 was verified by importing upstream's
pydantic models in a scratch venv and validating real payloads pulled from
`koi_net_events`. That is real evidence and it caught a genuine break.

**Do not claim full conformance until** a disposable reference node built from
[`koi-net-node-template`](https://github.com/DynamicalSystemsGroup/koi-net-node-template/tree/c45b188f1acdb357c907d2478b2804a0cf3b07d7) (pinned to koi-net 2.1.2)
completes handshake → edge negotiation → poll → broadcast against this node, and the
result is recorded here with the commit it ran at.

### Gate run, 2026-10-31 — NOT PASSED

Setup: our node at a development commit made before this export, on a throwaway database
built by the personal-profile runner, `KOI_NET_ENABLED=true`, listening on 127.0.0.1:58351 only, advertising
`base_url=http://127.0.0.1:58351/koi-net` (the upstream form, see 14), no peers. A stock
node from [`koi-net-node-template` @ `c45b188`](https://github.com/DynamicalSystemsGroup/koi-net-node-template/tree/c45b188f1acdb357c907d2478b2804a0cf3b07d7), koi-net 2.1.2 on Python 3.13, 127.0.0.1:58400,
`first_contact` = our node, no other peers.

| Step | Stock → ours | Ours → stock |
|---|---|---|
| Handshake | **passes** with the approved-edge gate off: `FORGET` + `NEW` of its own bundle accepted (HTTP 200), signature verified, peer registered. With the gate on (the example config): HTTP 403 and the stock node exits (18). | not possible: our node has no upstream handshake initiator; it uses `/handshake` (7) |
| Edge negotiation | **does not start**: our node neither pushes its bundle nor proposes an edge (20), and its `provides.event` would not match the stock node's interest anyway (21). The stock node never requested `/bundles/fetch`, so 15 was not exercised. | not possible: our node does not negotiate edges as `koi-net.edge` bundles (7, 12) |
| Poll | not reached (no edge). Run directly by the conformance tests: payload parses, signature fails (16) | not reached (no edge) |
| Broadcast | the handshake broadcast above is the only one | not reached (no edge) |

Two runs at that commit, each on a freshly migrated database with fresh identities: with the
gate on, HTTP 403 and the stock node exits (at that commit the refusal was sent as
`unknown_node`, so it reported `RemoteUnknownNodeError`; that mapping was reverted before the
export, see 18); with it off, the handshake succeeds, and after 75 s (past one 60 s poller
cycle) there are 0 edges and no further requests in either direction. That commit differs
from the export's KOI-net code only in the 18 error mapping, the 19 `not_found` fill and the
400 for malformed `rids`.

The conformance tests (`tests/conformance/`, koi-net 2.1.2) run every protocol request
built and signed by koi-net against a throwaway node built from the export's own code: 9
pass and 4 are strict xfails, covering divergences 12, 15, 16 and 17. The other open
divergences have no test in that suite.

---

## Conformance summary

| # | Divergence | Class |
|---|---|---|
| 1 | `koi-net.vault-file` in reserved namespace | DEFECT (expand done; cutover pending peers) |
| 2 | FORGET ships full payload | INTENTIONAL FORK (until a pre-delete snapshot is relied on) |
| 3 | Broadcast table + read-time filtering | INTENTIONAL FORK |
| 4 | Empty `rid_types` fail-open | DEFECT (fixed 2026-08-26) |
| 5 | `edge_rid` embeds identities, not the §3.2.1 hash | DEFECT (low) |
| 6 | `last_seen` not updated on poll | DEFECT (fixed 2026-08-26) |
| 7 | `/handshake` auto-APPROVE | INTENTIONAL FORK (caveated) |
| 8 | Two identities per `base_url` | DEFECT (operational) |
| 9 | `rid_types` conflates domain and entity type | DEFECT (design) |
| 10 | Unrecognized RID namespace bypasses edge scope | DEFECT (fixed 2026-08-26) |
| 11 | Vault-file manifest unparseable by stock KOI-net | DEFECT (fixed 2026-08-26) |
| 12 | Edge shape + status vocabulary vs `EdgeProfile` | DEFECT (latent — edges are not exchanged as bundles) |
| 13 | `extract_rid_type` guessed by substring | DEFECT (fixed 2026-08-26) |
| 14 | `base_url` host root vs KOI-net API root | DEFECT (interop) |
| 15 | Own node bundle not served | DEFECT (interop; not exercised by the gate) |
| 16 | `event_id` inside the signed poll payload | DEFECT (interop; breaks poll) |
| 17 | Domain event RIDs are application keys; NEW without manifest | DEFECT (design) |
| 18 | Approved-edge gate refuses the upstream handshake | INTENTIONAL FORK (membrane); error type stays `invalid_signature`, decision open |
| 19 | Manifests payload omitted `not_found` | DEFECT (fixed 2026-10-31) |
| 20 | No bundle push or edge proposal after a handshake | DEFECT (interop; blocks edge negotiation) |
| 21 | `provides.event` holds entity-type names, no `KoiNetNode`/`KoiNetEdge` | DEFECT (interop) |
| 22 | `/events/broadcast` answers with a body | DEFECT (interop, low) |

## Related

Fix commits, from the history of the earlier copy at `gaiaaiagent/koi-processor` (private since 2026-11-02):

- `2c497f0` — domain-event scope fix (defect class of 3/4/9)
- `77ce423` — same fix in `peek_undelivered`
- `3bd94d0`, `68dcf90` — the vault-sync emitter fix
