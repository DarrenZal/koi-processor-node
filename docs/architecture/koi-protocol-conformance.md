# KOI protocol conformance

**Status:** living document · first written 2026-08-26 · interop gate run 2026-10-31
**Scope:** `api/koi_net_router.py`, `api/koi_poller.py`, `api/event_queue.py`, `api/vault_sync.py`

## Why this document exists

This repository implements the KOI-net node protocol by hand — roughly 2,500
lines in `koi_net_router.py` plus the poller and event queue. Upstream publishes a
reference implementation, `koi-net`. Until 2026-11-05 we used only `rid-lib` at runtime,
and `koi-net` only in the conformance tests. Since then the node also imports
`koi_net.protocol` at runtime ([Runtime use](#runtime-use-of-koi_netprotocol-2026-11-05)),
and keeps its own components layer ([decision](#decision-do-not-port-to-upstream-koi-net)).

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
| `koi-net` (`DynamicalSystemsGroup/koi-net`) | 2.1.2 | 2026-08-06 | 80 | `koi_net.protocol` at runtime, pinned **2.1.2** (since 2026-11-05; before that test-only) |
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

### 5. `edge_rid` embeds node identities as a formatted string — **DEFECT (low; fixed on the wire 2026-11-05)**

Spec §3.2.1 says edge RIDs MUST be `orn:koi-net.edge:<hash>`, the SHA-256 hash of the
source and target RIDs, and upstream derives them that way. We format
`orn:koi-net.edge:<source>><target>:poll`, which is a spec violation as well as a
legibility problem. Because the row is upserted on
`edge_rid`, a peer that re-keys leaves its **old** identity baked into the string
while `target_node` moves on.

So the string can name an identity the peer no longer uses. The filtering itself is
correct — the poll gate queries `target_node` — so it is not a security defect.

**Fixed on the wire 2026-11-05.** Every edge we send or match as a bundle has the §3.2.1
RID, `sha256(source + target)` (`api/koi_net_negotiation.py::edge_rid_for`). It is derived
from the endpoints and never read from the row. Edges negotiated as bundles are stored
under that RID too. Rows `/handshake` created keep their formatted local `edge_rid`. Negotiation finds rows by
`(source_node, target_node)` and never adds a second one, but `/handshake` can, because it
upserts on its own key. A `FORGET` therefore revokes every row for the pair (review
2026-11-06).
Re-keying the old rows is a separate migration.

### 6. `koi_net_nodes.last_seen` was never updated on poll — **DEFECT (fixed 2026-08-26)**

It was written only by handshake and key-bootstrap paths, so it went stale while peers
kept polling, and a peer that was still delivering could be reported as offline.

**Fixed 2026-08-26:** the `/events/poll` handler now sets `last_seen` on every poll
(`api/koi_net_router.py`). Values written before then are stale.

### 7. `/handshake` with auto-APPROVE — **INTENTIONAL FORK, narrowed 2026-11-05**

Upstream has no such endpoint and approves an edge only after checking that the
requested `rid_types` are a subset of what the peer declares it provides. Our
local handshake auto-approves.

Kept for operator ergonomics on a small trusted mesh. **The caveat is that
auto-approval without the subset check is how an edge ends up broader than either
side intended** — which is the shape of defect 9. If the mesh grows beyond
hand-managed peers, adopt the subset check.

**Narrowed 2026-11-05.** Edges negotiated as bundles get koi-net's checks before anything
is stored (`subscription_refusal`). The requested `rid_types` must be within our
`provides.event` plus node and edge objects. A webhook edge needs a full node with a URL.
A refusal is answered with a `FORGET` of the edge RID. A valid subscription stays
`PROPOSED` until an operator approves it (`/koi-net/edges/approve`), unless
`KOI_NET_AUTO_APPROVE_EDGES` is set (decision D1). The approval goes back as an `UPDATE`
of the approved edge bundle. An operator's rejection sticks: a later proposal from the same
node is answered with `FORGET`, whichever of the pair's rows holds the rejection, and a peer's
`FORGET` does not turn `REJECTED` into `REVOKED`. An approved edge proposed again with a subset of its types
stays approved. `/handshake` itself is unchanged: it writes `provides.event` as the edge's
`rid_types`, which is within what we provide by construction. Its other holes are entry 26.

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

### 12. Edge shape and status vocabulary differ from `EdgeProfile` — **DEFECT (fixed on the wire 2026-11-05)**

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
listing route, which upstream does not define, and edge negotiation
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

**Fixed on the wire 2026-11-05**, when edges started to be exchanged as bundles (20).
Edges on the wire are `EdgeProfile` (`@context`, `source`, `target`, `edge_type`,
`status`, `rid_types`, in that order), with only `PROPOSED` and `APPROVED`
(`edge_contents` refuses anything else). A rejection is sent as `FORGET` of the edge RID.
A peer's `FORGET` sets our row to `REVOKED`, and `REJECTED`/`REVOKED` stay local. The
divergence-12 conformance test still fails, as it should: it reads our own
`GET /koi-net/edges` listing route, which koi-net does not define and which keeps its own shape.

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

### 14. `base_url` means the host root here, the KOI-net API root upstream — **DEFECT (interop, fixed 2026-11-05 for calls we make)**

Found preparing the interop gate, 2026-10-31. Upstream's `RequestHandler` calls
`get_base_url(node) + path`, with `base_url` such as `http://127.0.0.1:8000/koi-net`
(`koi_net.config.server_config.ServerConfig.url`). Our nodes advertise the host root
(`http://127.0.0.1:8351`) and `api/koi_poller.py` builds `f"{base_url}/koi-net/..."`.
So a stock node calling a root-advertising node of ours gets 404s, and our poller
calling a stock node doubles the prefix (`/koi-net/koi-net/...`). Not exercised live:
the throwaway node in the gate advertised the upstream form, and no edge formed (20).
**Fix direction:** accept both forms when building peer URLs, then advertise the
upstream form once every peer of ours accepts it.

**Fixed 2026-11-05 for calls we make.** Every peer request our node makes (poll, webhook
push, confirm, `/handshake`, `/health` key refresh) builds its URL with
`api.koi_protocol.koi_net_url`. A `base_url` ending in `/koi-net` is used as is, and any
other gets `/koi-net` appended. `tests/test_koi_net_peer_url.py` covers both forms and has a
census that fails if `api/` joins a `/koi-net/` path by hand. What our node advertises is
still `KOI_BASE_URL`. A stock node can call us only if that names the KOI-net root
(`http://host:port/koi-net`), as the gate configures it. A peer on the previous code doubles
the prefix with that form, so switching a deployed node to it waits until its peers run
this code.

### 15. The node's own `KoiNetNode` bundle is not served — **DEFECT (interop, fixed 2026-11-05)**

Found by the conformance tests, 2026-10-31. `/bundles/fetch` for our own node RID answers
`not_found`, and `/rids/fetch` does not list it, even for `rid_types=["orn:koi-net.node"]`.
A koi-net node that asks for our profile gets nothing. Neither the 2026-10-31 run nor the
scripted gate exercises this yet: the stock node never requests `/bundles/fetch`, because
core koi-net 2.1.2 does not fetch a first contact's profile on its own (20). The scripted
gate's fetch step will reach it once an edge forms. Pinned by
`tests/conformance/test_wire_conformance.py::test_fetch_bundles_returns_our_node_bundle`
(strict xfail).

**Fixed 2026-11-05.** `/bundles/fetch`, `/manifests/fetch` and `/rids/fetch` serve our own
node: the bundle is rid-lib's (`Bundle.generate`) over the profile we advertise, so its
manifest hash is the one a stock node computes. They serve it to any caller, signed or not,
with the membrane on or off, and also when the `federation_rid_blocks` table is missing,
because `/koi-net/health` already publishes the same profile and a stock node needs it to
verify us. `/bundles/fetch` now answers HTTP 400 to a `rids` that is not a list of strings,
as `/manifests/fetch` does (19). `/rids/fetch` lists our node RID when the request
names `orn:koi-net.node` or no type. `test_fetch_bundles_returns_our_node_bundle` now passes,
with two companions for `/rids/fetch` and `/manifests/fetch`. The gate's fetch step still
waits on 20: nothing makes a stock node ask.

### 16. `event_id` rides inside the signed poll payload — **DEFECT (interop; fixed 2026-11-05)**

Found by the conformance tests, 2026-10-31. Poll events carry our `event_id` extension
(used by `/events/confirm`). koi-net verifies a response by re-serializing the parsed
payload; its `Event` model has no `event_id`, drops it, and the signature check fails.
Every koi-net client therefore rejects a non-empty poll response from us. **Fix
direction:** carry the id outside the signed payload, or omit it for peers that are
not ours. Pinned by `test_poll_events_signature_verifies` (strict xfail).

**Fixed 2026-11-05.** Every event we put on the wire is built by
`api.koi_protocol.koi_net_event` so that koi-net's re-serialisation reproduces it. The cause
was three things, and the gate found the last two once the first was fixed:
- `event_id` is sent only to a peer that runs our code, recognised by the `encryption_key` or
  `ontology_uri` its profile carries (koi-net nodes send neither; `_peer_reads_extensions`).
  Our poller confirms only events that carry one, so it never calls `/events/confirm` on a
  koi-net node.
- manifests are round-tripped through rid-lib's `Manifest`: `koi_net_events` stores them as
  jsonb, which reorders keys, and rid-lib dumps its declared fields first and normalises the
  timestamp. Its `extra="allow"` keeps our vault-sync fields.
- a `None`-valued manifest field is not sent: koi-net dumps with `exclude_none`, which drops a
  `None` extra on its model but not inside the dict we sign (vault-sync's `base_hash`).
`test_poll_events_signature_verifies` now passes, and a partial stock node verifies our
poll replies, learns our profile from the first one and forms an edge (gate).

**Corrected 2026-11-06 (review).** `koi_net_event` and `complete_manifest` no longer raise,
whatever a peer stored as a manifest. An event koi-net cannot
represent, such as a task key without a colon as its RID (17) or an unknown event type, is
withheld for good from a koi-net peer. A peer running our code gets it in the older raw form.
Before this, one such row lost a whole poll batch, or stalled a webhook edge until its TTL.
`tests/test_koi_net_wire_event.py::test_koi_net_verifies_what_we_sign` signs a payload with
our signer and verifies it with koi-net's `SignedEnvelope`.

### 17. Domain events use application keys as RIDs and NEW events carry no manifest — **DEFECT (design)**

Found by the conformance tests, 2026-10-31. A task domain event has the task key
(`conformance::…`) as its `rid`; rid-lib accepts it only as a generic URI with scheme
`conformance`. Its NEW event has no manifest, which upstream NEW/UPDATE events always
carry. Pinned by `test_emitted_domain_events_use_koi_rids_and_manifests` (strict xfail). Since
2026-11-06, a domain event whose key is not a RID at all (no colon, e.g.
`koi-null-embed-chunks`) is withheld from koi-net peers, which cannot parse it (16).

### 18. The approved-edge gate refuses the upstream handshake — **INTENTIONAL FORK (membrane); resolved 2026-11-05 (D1)**

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

**Resolved 2026-11-05 (the maintainer's decision).** Under the membrane, a signed node with no
approved edge may broadcast two things and nothing else: events for its own node object,
and edge bundles between it and us (`first_contact_allowed`). They go to the negotiator,
never into the event table, and are rate-limited per source node. Every other event is
dropped. The reply is HTTP 200 with an empty body (spec §4.1.1), so a refused peer is no
longer told its signature is bad. A subscription from such a node stays `PROPOSED` until an
operator approves it. A partial node with no approved edge is served, when it polls, only
the node and edge objects we queued for it (`EventQueue.poll_own_protocol_objects`). The
poll and fetch gates still answer refused requests with empty payloads, and
`invalid_signature` stays the error type for every other refusal
(`tests/test_koi_net_gate_error_type.py`). The gate's membrane-on cases now pass the
handshake, and the full-node case also forms an edge after an operator approval.

**Corrected 2026-11-06 (review).** Three things kept the membrane from holding:
- With `KOI_NET_RID_TYPES_OF_INTEREST` set, a node could approve its own way in. We proposed
  a subscription, it answered `APPROVED`, and that edge passed every gate. Under the
  membrane, a peer's approval of our proposal is now recorded (`peer_approved`) and the edge
  stays `PROPOSED` until an operator approves it.
- The fetch endpoints checked the edge in the wrong direction (source = requester,
  target = us, which is the edge where we subscribe to it). They now check the subscriber
  edge, source = us and target = requester, as `/events/poll` does.
- A first contact was answered under the caller's database connection, and a queued reply
  took a second one. A burst of first contacts from one fresh key could exhaust the pool and
  hang the API. Replies are now sent after the connection is released.
The gate's membrane-on cases now assert both refusals: the catch-up `/rids/fetch` before
approval, and our subscription until an operator approves it.
`tests/interop/test_negotiation_db.py` pins each of these, and each of its new tests fails
on the code before these fixes. The membrane is still open through `/handshake` (26).

### 19. Manifests payload omitted `not_found` — **DEFECT (fixed 2026-10-31)**

`koi_net.protocol.api.models.ManifestsPayload` declares `not_found` with a default, so a
koi-net client re-serializes it before verifying, and every signed `/manifests/fetch`
response failed its signature check. The reply now always carries `not_found` (since 2026-11-06 it
is koi-net's own `ManifestsPayload`). When the handler serves a request, every requested RID is in exactly one of
the two lists: `manifests`, or `not_found` (no manifest, or blocked). When it serves
nothing, both lists are empty: a strict-gate refusal (unsigned caller, or no approved poll
edge), an unidentified caller, or a missing `federation_rid_blocks` table. A request whose
`rids` is not a list of strings gets HTTP 400. On the wire its koi-net `error` field is
`invalid_signature` (the `_ERROR_TYPE_MAP` default), with `error_code: INVALID_PAYLOAD` as
our extension, so it carries the same misreport as 18 and waits on 18's decision. (Before
this check the `not_found` change answered a null RID with a 500; `regen-prod` answered it
with 200 and empty lists.) Pinned by `test_fetch_manifests_response_parses`, which sends a non-empty
request, and `tests/test_koi_net_manifests_fetch_rids.py`. Since 2026-11-05 (15), a request
for our own node RID is served its manifest in every case, including a refusal.

### 20. After a handshake, our node neither pushes its own bundle nor proposes an edge — **DEFECT (interop; fixed 2026-11-05)**

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

**Fixed 2026-11-05** (`api/koi_net_negotiation.py`). A `NEW` of the sender's own node bundle
is a first contact. We refresh the sender's profile (its public key is never changed) and answer
with our own node bundle: pushed to a full node, queued for a partial node's next poll.
When `KOI_NET_RID_TYPES_OF_INTEREST` names a type the contact provides (decision D4, empty
by default), we also send a `POLL` subscription proposal. Our subscriptions are `POLL`
because pushed events are not applied here (23). Subscription requests and approvals are
handled as in 7 and 12. Another node's node object is not handled or relayed. The scripted
gate shows it with a full stock node, membrane off and on. The handshake passes, the stock
node caches our bundle, the edge is `APPROVED` in both stores with the §3.2.1 RID, the stock
node catches up with `/rids/fetch`, and our node polls it and verifies its signed replies.
The partial node gets our bundle in its first poll and rejects the reply's signature, which
is 16.

**Hardened 2026-11-06 (review).** We push to the `base_url` in a node bundle that anyone can
sign, before any approval, so:
- the URL must be http or https with a host, and no user info, query, fragment,
  parameters, or control, format or separator characters (`api.koi_protocol.valid_peer_base_url`).
  Otherwise a `#` could cut off our path and aim the signed request anywhere. The check
  runs where a URL is stored (bootstrap, `/handshake`, first contact) and again where one is
  used (first-contact push, poll, webhook push, key refresh), so URLs stored before it are
  covered too.
- our own URL is refused in the spellings a client resolves to it: case, loopback names and
  addresses (`127.x`, `127.1`, `0.0.0.0`, IPv4-mapped IPv6), default ports, `/./` and `..`
  segments, `/koi-net`. A non-ASCII host is refused.
- there is one reply per request and per sender in a 10 s window.
- every identity is free to mint, so there is also a global budget of 120 pushes a minute,
  with at most 8 in flight and 64 queued.
- a failed push is logged and never kills the task.
Hosts are not filtered by address, because our peers are on private and loopback
addresses. So a node can still make us POST a signed bundle to
`<any host>/…/koi-net/events/broadcast`. That is the same exposure koi-net nodes accept, and
`/handshake` already allowed it through the poller.

### 21. `provides.event` advertises entity-type names, never `KoiNetNode` or `KoiNetEdge` — **DEFECT (interop, fixed 2026-11-05)**

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

**Fixed 2026-11-05.** `provides.event` and `provides.state` (`api/node_identity.py`) are RID
types: `orn:koi-net.node`, `orn:koi-net.edge`, `orn:koi-net.vault-file`, and the
`orn:koi-net.<type>` koi_rid types of the entity types listed before. `/handshake` writes
`provides.event` into a peer's edge, so changing it changes what that peer receives at its
next handshake. One matcher, `api/event_queue.py::edge_admits`, now scopes both poll and
the webhook peek, and it takes both forms. A bare name (`Vault-file`) matches the
namespace's last segment, as before. A RID type matches exactly, as koi-net compares, with
the two vault-file namespaces counted as one type while divergence 1 migrates.
`tests/test_koi_net_rid_type_scope.py` replays the filter as it was and pins the result.
An edge rewritten from the new `provides.event` receives what it received before, plus our
own node and edge objects. The one exception is a foreign namespace that shares a type
segment (`orn:other.person`), which no emitter of ours produces. Node and edge objects are
served only when this node is their source, whatever the edge declares (see 23).

**Backing out this change needs one extra step.** After a peer re-handshakes under this
code, our edge to it holds RID types. The previous filter compares bare names, so it would
match none of them, and it would mark every vault-file event excluded and delivered to
that peer. They would reach it only if vault reconcile sent them again. Before deploying the old code, restore those
edges' `rid_types` to the bare names: `Organization`, `Person`, `Project`, `Concept`,
`Location`, `Vault-file`.

The entity koi_rid types are listed although our node emits no events under those RIDs.
Entity events are domain events (9, 17). Listing them keeps existing edges unchanged.

**Upstream packaging note, not a divergence of ours:** `koi-net` 2.1.2 declares
`Requires-Python >=3.10` but uses PEP 695 `type` statements, so it fails to import below
Python 3.12. The conformance venv uses 3.13.

### 22. `/events/broadcast` answers with a body — **DEFECT (interop, low; fixed 2026-11-05)**

Spec §4.1.1 says broadcast "Responses MUST contain an empty body", and koi-net 2.1.2's
`model_map` declares no response model for `/events/broadcast`. Our handler answers
`{"status": "ok", "queued": N}`. A koi-net client does not parse that body, so nothing
breaks today; the register records it because 18's first option would add another body.

**Fixed 2026-11-05.** A koi-net peer's broadcast is answered with HTTP 200 and an empty body.
A peer that runs our code (recognised as in 16) still gets the signed `{"status", "queued"}`
reply its webhook pusher reads, so older nodes of ours keep working.

### 23. Inbound broadcast events are served to every poller, the sender included — **DEFECT (relay), decision open**

Found while planning the alignment, 2026-11-05, and confirmed by the scripted gate.
`/events/broadcast` stores each event a peer pushes in `koi_net_events` with no
`target_node`, the broadcast table that also holds our own outbound events (3). Nothing
handles it locally: the table's readers are poll, the webhook pusher's
`peek_undelivered`, the manifest and bundle fetch endpoints, share, and vault sync's count
and cleanup. Poll does not exclude an event's source. So every peer that polls us is
served a pushed event, and so is the peer that sent it. In the gate, a stock node's
handshake (`FORGET` + `NEW` of its own bundle) is queued this way, with `target=None`.

Upstream, a received event goes through the node's knowledge pipeline, and only a handler
that decides to forwards it. Relaying can be deliberate, as when a coordinator relays node
bundles, so the fix needs a decision: stop relaying, or relay only what a handler
forwards. Pinned by `tests/conformance/test_wire_conformance.py::test_inbound_broadcast_events_are_served_to_every_poller`,
which asserts today's behaviour, so a change to it fails until this entry is updated.

**Narrowed 2026-11-05 for node and edge objects.** Poll, the webhook peek and the fetch
endpoints never serve another node's `orn:koi-net.node` or `orn:koi-net.edge` object from this
table, whatever the edge declares (`edge_admits`, `_is_foreign_protocol_object`). The test is
a case-insensitive prefix on the RID string, applied before any other rule, so a malformed
reference or a domain-event wrapper does not get past it. The webhook peek skips such rows
without marking them delivered, which the webhook caller must do before any WEBHOOK edge
is approved (the note in `peek_undelivered`). Without
that, advertising `orn:koi-net.node` (21) would have relayed every stock node's handshake
bundle to every peer at its next handshake. Other pushed events are still served as above.

### 24. The webhook pusher needs a signed response body carrying our `queued` count — **DEFECT (interop; fixed 2026-11-05)**

Found by reading code, 2026-11-05. The webhook push in `api/koi_poller.py` parses the
peer's `/events/broadcast` response as a signed envelope, and it marks events delivered
only when the payload's `queued` count (our extension, see 22) equals the number pushed.
koi-net 2.1.2 declares no response model for that path, and spec §4.1.1 requires an empty
body. A stock full node's reply therefore fails envelope verification, and the pusher
records a failure and backs off (`_record_failure`). Every event is pushed again after the
backoff and never recorded as delivered. The gate does not reach this yet:
no edge to a stock node forms (20), so nothing is pushed. Pinned by the gate step
`test_vault_file_event_reaches_stock_node` in its full-node cases.

**Fixed 2026-11-05.** The pusher builds each event with `koi_net_event` (no `event_id` or row
fields for a koi-net peer), and an HTTP 200 with an empty or `null` body counts as delivery of
every event pushed, but only from a koi-net peer. From a node running our code, an empty
reply means its membrane dropped the events, so it is not delivery (review 2026-11-06). A
signed reply with `queued` is read as before. Manifests are completed (`rid`, `timestamp`,
`sha256_hash`) on push as on poll, so sensor manifests without those fields are no longer
dropped. It also marks
events its edge excludes as delivered (`peek_undelivered(mark_excluded=True)`), so a narrow
WEBHOOK edge cannot starve. The gate's full stock node receives our vault-file event and
caches the bundle with the note's contents.

### 25. `/rids/fetch` lists entity RIDs that `/bundles/fetch` cannot serve — **DEFECT (interop, latent)**

Found 2026-11-05 while fixing 21. `/rids/fetch` lists the `koi_rid` of every federated,
shareable entity (`entity_registry`). `/bundles/fetch` and `/manifests/fetch` serve only
what `koi_net_events` holds under that RID, and no emitter writes an event under a
`koi_rid` (entity events are domain events, 9). A stock node that subscribes to an entity
type and catches up with `fetch_rids` gets a list of RIDs that all come back `not_found`. It
is latent because no stock node in the gate subscribes to an entity type. The `koi_rid`
namespace `orn:koi-net.<type>` (minted by `/register-entity` in
`api/personal_ingest_api.py`) also sits in the protocol's reserved `koi-net` namespace, the
same problem as divergence 1.

### 26. `/handshake` approves every caller's edge and accepts unsigned profiles — **DEFECT (security; pre-existing; fixed 2026-11-06)**

Found by the review of the alignment, 2026-11-06. `/handshake` accepts an unsigned profile.
It reads `is_known` after its own insert, so the deferral for unknown nodes never triggers,
and every caller gets an `APPROVED` inbound edge (source = caller, target = us). That edge
passes the broadcast gate's "any" check, so under the membrane any caller can still push
events to us, and they are relayed (23). It no longer passes the fetch gate (18, corrected).
It also sets the caller's node row back to `active`, undoing `/edges/reject` with
`deactivate_node`. This predates the alignment.

**Status 2026-11-06.** The maintainer approved fixing it. The same holes were reported against the
public export (koi-processor-node issues #1-#3, with fix PR #15). That fix is being ported
on its own, ahead of this alignment, which will take it on rebase. A first attempt on this
branch was reviewed and was not enough. Besides the points above, the review found more:
- a node that introduces itself by broadcast already has a `koi_net_nodes` row, so it counts
  as known at its next `/handshake`;
- an unsigned handshake can still rewrite a known peer's address and encryption key, because
  both its RID and its key are public;
- any caller can take a name alias;
- a rejection holds per RID rather than per key.
The fix must cover all of these.

**Fixed 2026-11-06**, in parts that one rule ties together:
- **koi-processor-node PR #15**, ported and merged here. A known peer's
  profile changes only under its pinned key's signature, or from the operator. A node
  doesn't become known through a bootstrap row. Only the operator can move an alias. An
  unsigned poll gets nothing while the gate is on. Node IDs reach inline Python in
  `connect-peers.sh` only through the environment, and the admin token stays out of curl's
  argv.
- **Admission.** Every new inbound edge `/handshake` creates is `PROPOSED` until an operator
  approves it (`/koi-net/edges/approve`). No "known" test is read at all, so neither a
  bootstrap row nor an edge in the other direction can approve one. An existing edge keeps
  its status on conflict, whether `APPROVED`, `REVOKED` or `REJECTED`. Our own subscription
  proposals (D4) wait for an operator too: the peer's approval alone leaves them `PROPOSED`,
  marked `peer_approved`, because once approved our poller fetches and applies that peer's
  events.
  - A peer's negotiation only narrows an edge. A
    peer-initiated write never moves an edge out of `REVOKED` or `REJECTED`, in either
    direction: while no row for the pair is `APPROVED`, any `REVOKED` or `REJECTED` row holds
    it, because `remove-peer.sh` and a peer's FORGET revoke every row and `/handshake` may add
    another. A held subscription proposal is answered with FORGET, and first contact proposes
    nothing for a held pair. A peer whose own FORGET revoked its subscription re-subscribes
    through the operator, like a new peer. Still allowed: a type change demoting `APPROVED` to
    `PROPOSED`, and a peer's FORGET revoking any row that is not `REJECTED`. The operator's way
    back is `POST /koi-net/edges/reopen` (admin-only, refused for a barred node), which moves
    `REVOKED` or `REJECTED` to `PROPOSED`. `/edges/approve` then approves it as any other.
  - Every edge write a peer can drive goes through three `Negotiator` methods that re-check
    the stored state in the write itself (`RETURNING`; None when it no longer matches, which
    never logs success or claims an approval). Nothing serialises a peer's read-then-write
    against an operator's revoke, and two writers revived revoked edges (T12-T14). The rest
    are `/handshake`'s two upserts and the operator routes. `tests/test_koi_net_edge_writers.py`
    fails on any other writer under `api/`.
  - `/handshake`'s upserts never change an edge's status, and change its `rid_types` only
    while it is `PROPOSED`. So an operator's narrowed grant on an `APPROVED` edge, and a
    `REVOKED` or `REJECTED` edge's types, survive the peer's next signed re-handshake (PR #15's
    `may_update` rule rewrote them). This holds for the operator's own handshake too.
  - Opt-outs. `KOI_NET_DEFER_UNKNOWN_HANDSHAKE=false` restores the auto-approval of a new
    handshake edge, and with the membrane off, of our proposals once the peer approves them.
    With the membrane on, our proposals still wait for an operator.
    `KOI_NET_AUTO_APPROVE_EDGES=true` (default false) approves subscriptions a peer
    negotiates to us. It is a separate setting, unchanged here.
  - A peer we already serve (an `APPROVED` edge the other way) gets no exception: serving a
    peer does not admit it.
  - Our poller signs its handshakes, and falls back to unsigned only when a peer answers
    `unknown_node`.
  - A first handshake must carry a key its RID is bound to, and so must a handshake for an
    existing keyless row. A keyless row then changes only under a signature, as a keyed one
    does: its bound key is public. A signed handshake from a node we hold no key for verifies
    against its profile's key when the RID is bound to it, so first contact can be signed.
- **Operator authority fails closed.** `_enforce_local_admin` and `_enforce_commons_admin`
  now share one fail-closed guard:
  - with no admin token configured, every caller gets 503, loopback included;
  - a non-loopback caller gets 403, and loopback is decided by address with IPv4-mapped
    addresses unwrapped;
  - bearers are compared in constant time, and an unreadable token file counts as
    unconfigured;
  - the commons routes also accept a `KOI_COMMONS_SERVICE_TOKEN` bearer from any address.
  PR #15's local-operator check inherits all of this, so loopback alone is never authority.
  `api.auth.enforce_local_admin`, used by `api/routers/vault_sync_router.py` and
  `api/routers/discourse_links_router.py`, decides by the same rule: the rule lives once, in
  `api.auth.local_admin_refusal`, and both router guards call it. It used to admit a loopback
  caller when no token was configured.
  Deploy note: on a node with no token configured, callers that send no bearer now get 503.
  Known ones are personal-koi-mcp's commons intake tools, `smoke-vault-sync.sh`,
  `vault-sync-doctor.py`, `setup-node.sh`, `bootstrap-node.sh` and `soak-check.sh`.
- **Rejection.** A node an operator rejected or revoked stays out. So does its key: under any
  other RID, including a RID that held the key before the rejection, and in any encoding. The
  key is compared as canonical DER. The bar holds on:
  - every signed request, refused in `_unwrap_request` with the membrane on or off;
  - `/handshake`, signed or not;
  - bootstrap, which now refuses the request;
  - key refresh in the router and key learning in the poller, which also accept only a key
    the RID is bound to and never reactivate a barred node;
  - the negotiator, `/edges/approve` and the edge gates;
  - the poller's poll and webhook loops.
  An unsigned request off the membrane identifies nobody, so no bar can apply to it. Anyone
  can claim any RID there, which is the stock behaviour the membrane closes.
  - `/edges/reject` with `deactivate_node` now marks the peer, where it used to mark our own
    RID.
  - `POST /koi-net/nodes/readmit` lets an operator re-admit a node.
  - `/handshake` reports the edge status as stored.
- **Keys are EC only.** The key loader and envelope verification refuse any other key type. A
  RID can be bound to any key, because the binding is a hash, so an Ed25519 or RSA key used
  to pass the binding check and then end ECDSA verification in a 500.
- **Approval shows what it approves.** An unsigned first handshake can still register a RID
  nobody has used yet, pinning its real public key with the caller's own address and
  encryption key. The real node's next signed handshake replaces them (our poller signs
  first), and the new edge waits for an operator. So `/koi-net/edges/approve` returns the
  peer's stored `base_url` and its public-key and encryption-key fingerprints, and
  `scripts/federation/approve-peer-edges.sh` shows them and approves only after the operator
  confirms they match the values the peer reads off its own node
  (`scripts/federation/fingerprints.py own`), out of band, at the SAS step.
  `connect-peers.sh` needs no such prompt: its step 6 rewrites the local row as the operator
  from the remote's own `/health` before it approves. `bootstrap-node.sh` writes the admin's
  row on a fresh node from the profile it fetched at the invite's `admin_base_url`. If a row
  for that RID already differs, the unsigned handshake is refused and bootstrap stops before
  it approves. Unsigned first
  contact stays for now. Signed-only first contact, which the profile-key verification above
  makes possible, is planned.
- **Remote values pasted into scripts.** On the invite flow's approval path, `bootstrap-node.sh`
  and `lib.sh`'s `compute_sas` pasted the admin's profile, RID and public key (from its remote
  `/health`) into inline Python and SQL, and put the admin token on curl's argv. Those are now
  passed as data, the class already fixed in `connect-peers.sh`. The rest of
  `scripts/federation/` was swept afterwards: every inline Python block now takes its values
  from the environment or argv, and psql takes them as variables, read from stdin with
  `ON_ERROR_STOP=1`. Static tests check both across every script
  (`tests/test_federation_script_values.py`, `tests/test_federation_psql_values.py`). The one
  exception is the two-peer smoke tool `smoke-vault-sync.sh`, which still puts RIDs into its
  own `psql -c` SQL.
- **Replay of a signed request.** A signature proves who signed,
  not when. A replay to another node is refused only where `KOI_ENFORCE_TARGET_MATCH` (or
  `KOI_STRICT_MODE`) is on, because the target is signed but checked only then. Both default
  to off; turn the target check on in deployment. For a replay to the same node:
  - `/handshake` is protected. Our poller signs `issued_at` (UTC, ISO-8601) into the
    handshake's profile, where every version of ours ignores an unknown field
    (`HandshakeRequest` itself forbids one). A signed handshake that would change the row is
    refused with 409 `STALE_HANDSHAKE` unless `issued_at` is newer than the last one accepted
    (stored in `koi_net_nodes.metadata.handshake_issued_at`) and within ±10 minutes. Without a
    stored value, a handshake without `issued_at` is accepted as before. Unsigned, unchanged
    and operator handshakes are unaffected. The decision is repeated atomically in the profile
    upsert itself (`WHERE` the stored value is older; no row back means 409 before any edge or
    alias write), so two signed changes racing cannot leave the older one's profile under the
    newer timestamp (T16).
  - Profile authority, the ordering guard and timestamp storage all read the stored row in the
    same statement, never an earlier read. On a
    conflict a profile field (name, type, `base_url`, `provides`, `encryption_key`, ontology) is
    written only by the operator, by a self-signed handshake that passes the freshness condition,
    or by a self-signed legacy one (no `issued_at`) while nothing is stored. An unsigned caller
    on an existing row writes no profile field: its "unchanged" was judged on a read that a newer
    change may since have replaced (T18). Every self-signed handshake with an in-window
    `issued_at` must be newer than the stored one, changed or not, so a replay of the peer's own
    older handshake is refused (T19; a stale unchanged one gets a harmless 409). A legacy change
    is refused once a value is stored, even if its read saw none (T20). The key itself is pinned
    once (`COALESCE`), as before.
  - A value is stored only when it lies inside the window at receive time, on every path:
    an unchanged or first-contact handshake with an out-of-window `issued_at` is accepted but
    leaves nothing stored (T17). Otherwise one far-future stamp could lock the node's later
    changes out for as long as it lies ahead.
  - Residual (low): a peer whose clock runs ahead within the window stores a future
    `issued_at`, and can lock its own later changes out until real time passes it, at most the
    ±10 minute window.
  - The other signed routes keep the risk, accepted until the envelope can carry a timestamp
    (`koi-net` 2.1.2's `SignedEnvelope` cannot, because koi-net verifies by re-serialising the
    payload with its own models). What a same-node replay does on each:
    - `/events/poll` changes state: it serves the requester's undelivered events to whoever
      replays it and marks them delivered to the requester. Accepted: whoever can
      capture a signed poll envelope on the path, in practice a hop that sees plaintext HTTP
      between nodes, can already read the plaintext replies, so a replay adds lost events for
      the real requester, not new exposure.
    - `/events/broadcast`: an event carrying `event_id` (our peers) dedupes on
      `(source_node, event_id, target_node)`. One without it (koi-net peers send none) is
      queued again. A node bundle in it reaches the negotiator, which applies it only when its
      manifest timestamp is newer than the last one applied (stored in
      `koi_net_nodes.metadata.node_bundle_timestamp`, one conditional update), as koi-net's own
      `BasicManifestHandler` does. So a replayed older bundle no longer rolls the sender's
      `base_url`, `node_type` or `provides` back. Edge bundles only
      narrow (see Admission).
    - Residual (low): handshake freshness (`handshake_issued_at`) and
      bundle freshness (`node_bundle_timestamp`) are tracked separately. A replayed signed
      handshake that is newer than the last handshake can still override a newer bundle's
      `base_url`, but only within the handshake's ±10 minute window.
    - `/events/confirm` is idempotent, and the three fetch routes are read-only. A replay
      shows the replayer what the signer could fetch.
- **Edges approved before the fix stay approved.** The old code approved every caller's
  inbound edge, and the upsert never changes an existing edge's status. Before relying on the
  rule, list the inbound edges a node holds and reject (`/koi-net/edges/reject`) any that no
  operator meant to admit:

  ```sql
  SELECT e.edge_rid, e.source_node, n.node_name, n.status AS node_status, e.created_at
  FROM koi_net_edges e LEFT JOIN koi_net_nodes n ON n.node_rid = e.source_node
  WHERE e.target_node = '<this node''s RID>' AND e.status = 'APPROVED'
  ORDER BY e.created_at;
  ```

Tests:
- `tests/test_koi_net_peer_identity.py` and `tests/test_koi_net_bootstrap_not_known.py`
  come from PR #15. The known-peer control there was replaced deliberately, by "an outbound
  approval does not approve the new inbound edge" and the operator approval that does.
- `tests/interop/test_peer_rejection_db.py` and `tests/test_koi_net_admin_guard.py` were
  added here, along with `tests/test_koi_net_poller_handshake.py`. The interop gate runs the
  DB ones by default. `tests/interop/test_negotiation_db.py` pins that a peer cannot approve
  our proposal alone, membrane on or off.

### 27. Our poller ignores the `rid_types` of the edge it polls — **DEFECT (scope, low)**

Reported against the public export (koi-processor-node issue #4), and confirmed here on
2026-11-06. `KOIPoller._poll_peer` takes the in-edge's `rid_types` and never uses it. It
neither sends them in the poll request (koi-net's `PollEvents` has no field for them) nor
filters what comes back. So events outside the edge's declared scope are applied if the
publisher sends them. Upstream, a subscriber's own pipeline decides what it handles. The fix
direction is to drop received events whose RID type (or domain, 9) the edge does not name,
using `edge_admits`, before they are processed.

Before the fix ships, each node's copy of an in-edge has to name what the publisher's copy
names; otherwise events a node applies today are dropped. The fix should also log what it
drops, so that a mismatch shows instead of losing events silently.

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

### Gate scripted, 2026-11-05 — NOT PASSED (baseline)

The gate above is now a script: [`tests/interop/`](../../tests/interop/README.md). One
command builds a throwaway cluster and runs five cases against stock nodes from
`koi-net-node-template` @ `c45b188` on koi-net 2.1.2. Each protocol step is a test, and a
step an open divergence blocks is a strict xfail that accepts only "step not reached". The
cases are `conformance`, then a full and a partial stock node, each with the membrane
(`KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL`) off and on. The partial node is the case the
2026-10-31 run could not reach, because the template ships only a full node and a full
node never polls.

Baseline, before any fix, one run of all five cases at the commit that added the script (2026-11-05):

| Step | full, off | partial, off | full, on / partial, on |
|---|---|---|---|
| Handshake accepted | passes | passes | 403, and the stock node exits (18) |
| Stock node caches our node bundle | not reached (15, 20, 21) | not reached (15, 20, 21) | not reached: stock node gone (18) |
| Edge approved on both sides | not reached (5, 12, 20, 21) | not reached (5, 12, 20, 21) | not reached: stock node gone (18) |
| Stock node fetches RIDs and bundles from us | not reached (15, 20, 21) | not reached (15, 20, 21) | not reached: stock node gone (18) |
| Our vault-file event reaches the stock node | not reached (14, 20, 21, 24) | not reached (16, 20, 21) | not reached: stock node gone (18) |
| Our node polls the stock node | not reached (14, 20) | — (a partial node cannot be polled) | not reached: stock node gone (18) |

With the membrane on, a stock node shuts down about half a second after our node refuses
its handshake, so every later step is blocked by 18 before any other divergence. The
tests accept that cause only when our log shows the 403 and the stock process has exited.
A stock node that exits for any other reason fails the test outright.

The conformance case gives 10 passed and 4 strict xfails (12, 15, 16, 17). The tenth
passing test pins divergence 23.

With the membrane off, the partial node polls us every second and gets HTTP 200. It
rejects each reply as coming from an unknown node ("Couldn't resolve" our RID), because it
has never cached our bundle. With the membrane on, it polls once before its handshake and
gets HTTP 400 (we do not know it yet). Then our node refuses its handshake with a 403 sent
as `invalid_signature` (18), and it exits ("Startup error: Peer marked envelope signature
as invalid").
koi-net accepts an envelope from an unknown sender only if it is an `EventsPayload`
carrying a `NEW` event for the sender's own node bundle
(`SecureManager.handle_unknown_node`). So our first reply to a partial node has to carry
our own bundle, which is part of fixing 20. Our node RID's hash is already upstream's,
`sha256` of the base64 DER public key (`derive_node_rid_hash`, mode `b64_64`).

### Gate run, 2026-11-05 — PASSED

One run of all five cases, with the gate's own DB tests, at the commit that fixed
16, 22 and 24 (2026-11-05). Every protocol step passes, with no xfail and no skip:

| Step | full, off | full, on | partial, off | partial, on |
|---|---|---|---|---|
| Handshake accepted | passes | passes (first-contact path, 18) | passes | passes |
| Stock node caches our node bundle | passes | passes | passes (from its first poll) | passes |
| Edge approved on both sides, §3.2.1 RID | passes | passes after operator approval | passes | passes after operator approval |
| Stock node catches up with `/rids/fetch` | passes | passes | passes | passes |
| Our vault-file event reaches the stock node | passes (webhook push) | passes | passes (poll) | passes |
| Our node subscribes to the stock node and polls it | passes | passes | — | — |

The conformance case: 14 passed, 2 strict xfails. 12 now pins only our own
`GET /koi-net/edges` listing route, which koi-net does not define. 17 is domain-event RIDs, which is
outside this alignment. The gate's negotiation DB tests (refusals, unproposed approvals,
FORGET, first-contact poll, peek marking): 12 passed.

Our node advertised the upstream `base_url` form in this run (14). A deployed node needs
`KOI_BASE_URL` to name its KOI-net root before stock nodes can call it, which waits until its
peers run this code.

## Runtime use of `koi_net.protocol` (2026-11-05)

`koi-net==2.1.2` is a runtime dependency since 2026-11-05. It needs Python 3.12 or later,
because it uses PEP 695 `type` statements, although its metadata declares 3.10. On an older
interpreter, `api/koi_net_floor.py` turns the resulting `SyntaxError` into an `ImportError`.
The API then starts without KOI-net and logs an error, instead of failing to start. What it now
provides and what is still ours:

| Piece | Source | Reason when ours |
|---|---|---|
| Events we serve or push | `koi_net.protocol.event.Event` (`koi_protocol.koi_net_event`) | — |
| Edge bundles and edge validation | `edge.generate_edge_bundle`, `EdgeProfile` (`koi_net_negotiation`) | — |
| Events payload | `api.models.EventsPayload` for the negotiator's pushes and, since 2026-11-06, for a koi-net peer's poll reply; the webhook push, and the poll reply to a peer running our code, wrap `koi_net_event` output in the same two-field dict | — |
| Paths of the five protocol routes, push and poll | `api.paths` | — |
| Manifests, bundles, RIDs, hashing | `rid_lib` | — |
| Requests and replies of `/manifests/fetch`, `/bundles/fetch`, `/rids/fetch` | `api.models` `FetchManifests`, `FetchBundles`, `FetchRids`; `ManifestsPayload`, `BundlesPayload`, `RidsPayload` (2026-11-06) | — . A request koi-net's model rejects, such as a RID string rid-lib cannot parse, gets 400 `INVALID_PAYLOAD` (koi-net's server answers 422; we used to answer `not_found`). A stored record rid-lib cannot represent reads as `not_found`, and a `koi_rid` it cannot parse is not listed. No client of ours calls these routes. Parsing a request with rid-lib registers a RID type for each new scheme and namespace it names, and the registry never shrinks, so distinct request strings grow memory slowly. koi-net's own server parses the same way; accepted. |
| Poll reply | `api.models.EventsPayload` of `Event` for a koi-net peer | A peer running our code gets our raw form with `event_id` and RIDs koi-net cannot parse (16, 17): **INTENTIONAL FORK**, as below. |
| Parsing of `/events/broadcast` | ours | **INTENTIONAL FORK**. Older peers of ours push domain events whose RIDs koi-net's `Event` rejects (17). Telling them from koi-net nodes by `encryption_key` or `ontology_uri` is not safe enough to drop their events, since a peer without either field would lose them. So the payload is read as before, and node and edge objects still go to the negotiator. |
| Envelope signing and verification (`koi_envelope`) | `koi_net.protocol.secure` `PrivateKey.sign` and `PublicKey.verify` (2026-11-06), over the bytes koi-net signs, `UnsignedEnvelope.model_dump_json(exclude_none=True)` | The envelope is still a dict, not koi-net's `SignedEnvelope[T]`: that generic only admits koi-net's payload models, and our extensions (`/handshake`, `/events/confirm`, the poll and broadcast forms our older peers read) are not among them. A payload dumped from koi-net's model serialises to the same bytes. Fixtures: an envelope signed by the code before the swap verifies after it and the reverse, koi-net's typed envelope verifies here and ours in koi-net, and the gate's stock nodes verify both ways. koi-net logs each signature at debug through structlog; `api/koi_net_floor.py` filters structlog below WARNING unless the process configured it. Replay protection, which koi-net's envelope cannot carry, is register 26's R1. |
| `NodeProfile` for `/handshake` | ours | **INTENTIONAL FORK** (7). `/handshake` is our extension, and older peers send bare type names (`Vault-file`) in `provides`, which koi-net's `RIDType` rejects. It also carries our extension fields (`node_rid`, `node_name`, `encryption_key`, `ontology_uri`). |
| `event_id` on events, `/events/confirm`, the signed broadcast reply | ours, to peers running our code only | **INTENTIONAL FORK** (16, 22, 24): confirmation of delivery for our durable queue (3). A peer is recognised by the `encryption_key` or `ontology_uri` its profile carries, which koi-net nodes never send. |
| Components layer (store, queue, effector, server) | ours | **INTENTIONAL FORK**, as decided at the top of this document. |

---

## Conformance summary

| # | Divergence | Class |
|---|---|---|
| 1 | `koi-net.vault-file` in reserved namespace | DEFECT (expand done; cutover pending peers) |
| 2 | FORGET ships full payload | INTENTIONAL FORK (until a pre-delete snapshot is relied on) |
| 3 | Broadcast table + read-time filtering | INTENTIONAL FORK |
| 4 | Empty `rid_types` fail-open | DEFECT (fixed 2026-08-26) |
| 5 | `edge_rid` embeds identities, not the §3.2.1 hash | DEFECT (low; fixed on the wire 2026-11-05; old rows keep local keys) |
| 6 | `last_seen` not updated on poll | DEFECT (fixed 2026-08-26) |
| 7 | `/handshake` auto-APPROVE | INTENTIONAL FORK (narrowed 2026-11-05: bundle subscriptions get koi-net's checks and an operator) |
| 8 | Two identities per `base_url` | DEFECT (operational) |
| 9 | `rid_types` conflates domain and entity type | DEFECT (design) |
| 10 | Unrecognized RID namespace bypasses edge scope | DEFECT (fixed 2026-08-26) |
| 11 | Vault-file manifest unparseable by stock KOI-net | DEFECT (fixed 2026-08-26) |
| 12 | Edge shape + status vocabulary vs `EdgeProfile` | DEFECT (fixed on the wire 2026-11-05; `GET /koi-net/edges` keeps its own shape) |
| 13 | `extract_rid_type` guessed by substring | DEFECT (fixed 2026-08-26) |
| 14 | `base_url` host root vs KOI-net API root | DEFECT (interop; fixed 2026-11-05 for calls we make; advertising the upstream form is a deploy step) |
| 15 | Own node bundle not served | DEFECT (interop; fixed 2026-11-05) |
| 16 | `event_id` inside the signed poll payload | DEFECT (interop; fixed 2026-11-05) |
| 17 | Domain event RIDs are application keys; NEW without manifest | DEFECT (design) |
| 18 | Approved-edge gate refuses the upstream handshake | INTENTIONAL FORK (membrane); resolved 2026-11-05 by a first-contact path (D1) |
| 19 | Manifests payload omitted `not_found` | DEFECT (fixed 2026-10-31) |
| 20 | No bundle push or edge proposal after a handshake | DEFECT (interop; fixed 2026-11-05) |
| 21 | `provides.event` holds entity-type names, no `KoiNetNode`/`KoiNetEdge` | DEFECT (interop; fixed 2026-11-05) |
| 22 | `/events/broadcast` answers with a body | DEFECT (interop, low; fixed 2026-11-05) |
| 23 | Inbound broadcast events served to every poller, the sender included | DEFECT (relay; decision open; node and edge objects excluded since 2026-11-05) |
| 24 | Webhook pusher needs a signed response carrying our `queued` count | DEFECT (interop; fixed 2026-11-05) |
| 25 | `/rids/fetch` lists entity RIDs that the fetch endpoints cannot serve | DEFECT (interop, latent) |
| 26 | `/handshake` approves every caller's edge, accepts unsigned profiles | DEFECT (security; pre-existing; fixed 2026-11-06) |
| 27 | Our poller ignores the `rid_types` of the edge it polls | DEFECT (scope, low) |

## Related

Fix commits, from the history of the earlier copy at `gaiaaiagent/koi-processor` (private since 2026-11-02):

- `2c497f0` — domain-event scope fix (defect class of 3/4/9)
- `77ce423` — same fix in `peek_undelivered`
- `3bd94d0`, `68dcf90` — the vault-sync emitter fix
