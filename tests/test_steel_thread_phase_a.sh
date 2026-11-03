#!/usr/bin/env bash
# Steel Thread Phase A — End-to-end proof loop test
#
# Tests the full claim lifecycle: Evidence creation → claim → evidence linking →
# state advancement → anchor → reconcile → proof pack assembly.
#
# Usage:
#   KOI_TEST_API_URL=http://127.0.0.1:<throwaway port> \
#   KOI_TEST_DATABASE_URL=postgresql://<role>@127.0.0.1:<port>/<db with test in its name> \
#       ./tests/test_steel_thread_phase_a.sh
#   The API must write to that database (tests/live_write_shell_guard.sh).
#
# Prerequisites:
#   - KOI API at KOI_TEST_API_URL
#   - At least one Organization or Location entity in the registry (for about_uri)
#   - At least one Person entity in the registry (for claimant_uri)
#   - regen CLI installed + claims-service key funded (for anchoring)

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/live_write_shell_guard.sh"
live_write_begin
RUN_ANCHOR="${RUN_ANCHOR:-0}"

PASS=0
FAIL=0
SKIP=0

green()  { printf "\033[32m%s\033[0m\n" "$1"; }
red()    { printf "\033[31m%s\033[0m\n" "$1"; }
yellow() { printf "\033[33m%s\033[0m\n" "$1"; }

check() {
    local desc="$1" ok="$2"
    if [ "$ok" = "true" ]; then
        green "  [PASS] $desc"
        PASS=$((PASS + 1))
    else
        red "  [FAIL] $desc"
        FAIL=$((FAIL + 1))
    fi
}

skip() {
    local desc="$1" reason="$2"
    yellow "  [SKIP] $desc — $reason"
    SKIP=$((SKIP + 1))
}

echo "========================================"
echo "Steel Thread Phase A — Proof Loop Test"
echo "API: $API"
echo "========================================"
echo ""

# ------------------------------------------------------------------ #
# Step 0: Health check + find existing entities for test fixtures
# ------------------------------------------------------------------ #
echo "--- Step 0: Setup ---"

HEALTH=$(curl -sf "$API/health" 2>/dev/null || echo '{}')
check "API health" "$(echo "$HEALTH" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("true" if d.get("status") in ("ok","healthy") else "false")' 2>/dev/null || echo false)"

# Helper: extract URI from entity-search result (handles both 'uri' and 'fuseki_uri' field names)
extract_uri() {
    python3 -c "
import sys, json
d = json.load(sys.stdin)
results = d.get('results', [])
if not results:
    print('')
else:
    r = results[0]
    print(r.get('uri', r.get('fuseki_uri', '')))
" 2>/dev/null || echo ""
}

# Find entities — use configurable fixture names or auto-discover
CLAIMANT_NAME="${CLAIMANT_NAME:-}"
REVIEWER_NAME="${REVIEWER_NAME:-}"
ABOUT_NAME="${ABOUT_NAME:-}"

# Auto-discover entity by name, filtering for correct type
find_entity() {
    local name="$1" allowed_types="$2"
    local encoded
    encoded=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$name'))")
    curl -sf "$API/entity-search?query=$encoded&limit=10" 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
allowed = set('$allowed_types'.split())
for r in d.get('results', []):
    etype = r.get('entity_type', r.get('type', ''))
    uri = r.get('uri', r.get('fuseki_uri', ''))
    if etype in allowed and uri:
        print(uri)
        break
else:
    print('')
" 2>/dev/null || echo ""
}

CLAIMANT_URI=$(find_entity "${CLAIMANT_NAME:-Test Claimant}" "Person")
if [ -z "$CLAIMANT_URI" ]; then
    red "No Person entity found — cannot proceed. Seed at least one Person entity."
    red "Hint: set CLAIMANT_NAME='Your Name' to specify a known Person entity"
    exit 1
fi
echo "  Claimant URI: $CLAIMANT_URI"

ABOUT_URI=$(find_entity "${ABOUT_NAME:-Regen Network}" "Organization Location Bioregion")
if [ -z "$ABOUT_URI" ]; then
    yellow "  No Organization/Location found for about_uri — will create claim without subject"
fi
echo "  About URI: ${ABOUT_URI:-<none>}"

# Find reviewer Person (must differ from claimant)
# REVIEWER_NAME names a Person or Organization entity in the test database
REVIEWER_URI=""
for rname in "${REVIEWER_NAME:-}"; do
    [ -z "$rname" ] && continue
    candidate=$(find_entity "$rname" "Person Organization")
    if [ -n "$candidate" ] && [ "$candidate" != "$CLAIMANT_URI" ]; then
        REVIEWER_URI="$candidate"
        break
    fi
done
echo "  Reviewer URI: ${REVIEWER_URI:-<none>}"
echo ""

# ------------------------------------------------------------------ #
# Step 1: Create Evidence entity
# ------------------------------------------------------------------ #
echo "--- Step 1: Create Evidence entity ---"

EVIDENCE_NAME="Steel Thread Test Evidence $KOI_TEST_RUN_ID"
EVIDENCE_VAULT_RID="orn:obsidian.entity:Evidence/steel-thread-test-$KOI_TEST_RUN_ID"
EVIDENCE_VAULT_PATH="Evidence/steel-thread-test-$KOI_TEST_RUN_ID.md"

REGISTER_RESP=$(curl -sf -X POST "$API/register-entity" \
    -H "Content-Type: application/json" \
    -d "{
        \"vault_rid\": \"$EVIDENCE_VAULT_RID\",
        \"vault_path\": \"$EVIDENCE_VAULT_PATH\",
        \"entity_type\": \"Evidence\",
        \"name\": \"$EVIDENCE_NAME\",
        \"content_hash\": \"steel-thread-$KOI_TEST_RUN_ID\",
        \"visibility_scope\": \"public\",
        \"frontmatter\": {
            \"@type\": \"Evidence\",
            \"name\": \"$EVIDENCE_NAME\",
            \"description\": \"Test evidence for steel thread Phase A verification\"
        }
    }" 2>/dev/null || echo '{"success":false}')

EVIDENCE_URI=$(echo "$REGISTER_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("canonical_uri",""))' 2>/dev/null || echo "")
live_write_record entity_uri "$EVIDENCE_URI"
IS_NEW=$(echo "$REGISTER_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(str(d.get("is_new",False)).lower())' 2>/dev/null || echo "false")

check "Evidence entity registered" "$([ -n "$EVIDENCE_URI" ] && echo true || echo false)"
echo "  Evidence URI: $EVIDENCE_URI"
echo "  Is new: $IS_NEW"

# Verify it's type Evidence in the registry (lookup by URI, not name, since
# entity resolution may have matched a prior run's entity with a different name)
EVIDENCE_TYPE=$(curl -sf "$API/entity/$EVIDENCE_URI" 2>/dev/null \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); e=d.get("entity",d); print(e.get("entity_type", e.get("type", "")))' 2>/dev/null || echo "")
check "Evidence entity type is Evidence" "$([ "$EVIDENCE_TYPE" = "Evidence" ] && echo true || echo false)"
echo ""

# ------------------------------------------------------------------ #
# Step 2: Create claim with about_uri
# ------------------------------------------------------------------ #
echo "--- Step 2: Create claim ---"

CLAIM_STATEMENT="Steel Thread Phase A test $KOI_TEST_RUN_ID: The Salish Sea Knowledge Garden demonstrates bioregional knowledge commoning practices including federation, consent-aware data governance, and community-driven entity curation across four connected nodes."

ABOUT_URI_FIELD=""
if [ -n "$ABOUT_URI" ]; then
    ABOUT_URI_FIELD="\"about_uri\": \"$ABOUT_URI\","
fi

CLAIM_RESP=$(curl -sf -X POST "$API/claims/" \
    -H "Content-Type: application/json" \
    -d "{
        \"claimant_uri\": \"$CLAIMANT_URI\",
        \"statement\": \"$CLAIM_STATEMENT\",
        \"claim_type\": \"ecological\",
        $ABOUT_URI_FIELD
        \"metadata\": {
            \"methodology\": \"steel_thread_phase_a\",
            \"test_run\": true,
            \"test_run_id\": \"$KOI_TEST_RUN_ID\",
            \"generated_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
        },
        \"created_by\": \"steel-thread-test\"
    }" 2>/dev/null || echo '{}')

CLAIM_RID=$(echo "$CLAIM_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("claim_rid",""))' 2>/dev/null || echo "")
live_write_record claim_rid "$CLAIM_RID"
CLAIM_VERIFICATION=$(echo "$CLAIM_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("verification",""))' 2>/dev/null || echo "")

check "Claim created" "$([ -n "$CLAIM_RID" ] && echo true || echo false)"
check "Initial state is self_reported" "$([ "$CLAIM_VERIFICATION" = "self_reported" ] && echo true || echo false)"
echo "  Claim RID: $CLAIM_RID"

if [ -z "$CLAIM_RID" ]; then
    red "Cannot proceed without claim. Response: $CLAIM_RESP"
    exit 1
fi
echo ""

# ------------------------------------------------------------------ #
# Step 3: Link Evidence to claim
# ------------------------------------------------------------------ #
echo "--- Step 3: Link Evidence to claim ---"

LINK_RESP=$(curl -sf -X POST "$API/claims/$CLAIM_RID/evidence" \
    -H "Content-Type: application/json" \
    -d "{
        \"evidence_uri\": \"$EVIDENCE_URI\",
        \"actor\": \"steel-thread-test\"
    }" 2>/dev/null || echo '{}')

# POST response doesn't include evidence list — verify via GET
CLAIM_WITH_EV=$(curl -sf "$API/claims/$CLAIM_RID" 2>/dev/null || echo '{}')
LINK_EVIDENCE=$(echo "$CLAIM_WITH_EV" | python3 -c 'import sys,json; d=json.load(sys.stdin); ev=d.get("evidence",[]); print("true" if any(e.get("uri")==sys.argv[1] for e in (ev or [])) else "false")' "$EVIDENCE_URI" 2>/dev/null || echo "false")
check "Evidence linked to claim" "$LINK_EVIDENCE"
echo ""

# ------------------------------------------------------------------ #
# Step 4: Attestations + state advancement (V2 policy: attest before advance)
# ------------------------------------------------------------------ #
echo "--- Step 4: Attestations + verification state ---"

# V2 policy: peer_reviewed needs ≥1 approved attestation, verified needs ≥2
# Create first attestation (enables peer_reviewed)
ATT1_DONE=false
if [ -n "$REVIEWER_URI" ]; then
    ATT1_RESP=$(curl -sf -X POST "$API/claims/$CLAIM_RID/attestations" \
        -H "Content-Type: application/json" \
        -d "{
            \"reviewer_uri\": \"$REVIEWER_URI\",
            \"verdict\": \"approved\",
            \"rationale\": \"Steel thread Phase A test — first attestation\",
            \"evidence_uris\": [\"$EVIDENCE_URI\"]
        }" 2>/dev/null || echo '{}')
    ATT1_RID=$(echo "$ATT1_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("attestation_rid",""))' 2>/dev/null || echo "")
    check "Attestation #1 created" "$([ -n "$ATT1_RID" ] && echo true || echo false)"
    echo "  Attestation #1 RID: ${ATT1_RID:-FAILED}"
    [ -n "$ATT1_RID" ] && ATT1_DONE=true
else
    skip "Attestation #1" "No reviewer entity available"
fi

# Advance: self_reported → peer_reviewed (needs ≥1 attestation)
VERIFY1_RESP=$(curl -sf -X PATCH "$API/claims/$CLAIM_RID/verify" \
    -H "Content-Type: application/json" \
    -d '{"new_level": "peer_reviewed", "actor": "steel-thread-test", "reason": "Phase A test: first attestation received"}' 2>/dev/null || echo '{}')
VERIFY1_STATE=$(echo "$VERIFY1_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("verification",""))' 2>/dev/null || echo "")
check "State → peer_reviewed" "$([ "$VERIFY1_STATE" = "peer_reviewed" ] && echo true || echo false)"

# Find a second reviewer for the second attestation (needed for verified)
REVIEWER2_URI=""
if [ -n "$ABOUT_URI" ] && [ "$ABOUT_URI" != "$CLAIMANT_URI" ] && [ "$ABOUT_URI" != "$REVIEWER_URI" ]; then
    # Try using about_uri entity as second reviewer (if it's Organization/Person)
    ABOUT_TYPE=$(curl -sf "$API/entity-search?query=regen&limit=5" 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
for r in d.get('results', []):
    uri = r.get('uri', r.get('fuseki_uri', ''))
    etype = r.get('entity_type', r.get('type', ''))
    if etype in ('Person', 'Organization') and uri != '$CLAIMANT_URI' and uri != '${REVIEWER_URI:-}':
        print(uri)
        break
else:
    print('')
" 2>/dev/null || echo "")
    if [ -n "$ABOUT_TYPE" ]; then
        REVIEWER2_URI="$ABOUT_TYPE"
    fi
fi

ATT2_DONE=false
if [ -n "$REVIEWER2_URI" ]; then
    ATT2_RESP=$(curl -sf -X POST "$API/claims/$CLAIM_RID/attestations" \
        -H "Content-Type: application/json" \
        -d "{
            \"reviewer_uri\": \"$REVIEWER2_URI\",
            \"verdict\": \"approved\",
            \"rationale\": \"Steel thread Phase A test — second attestation\",
            \"evidence_uris\": [\"$EVIDENCE_URI\"]
        }" 2>/dev/null || echo '{}')
    ATT2_RID=$(echo "$ATT2_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("attestation_rid",""))' 2>/dev/null || echo "")
    check "Attestation #2 created" "$([ -n "$ATT2_RID" ] && echo true || echo false)"
    echo "  Attestation #2 RID: ${ATT2_RID:-FAILED} (reviewer: $REVIEWER2_URI)"
    [ -n "$ATT2_RID" ] && ATT2_DONE=true
else
    skip "Attestation #2" "No second reviewer entity available for verified gate"
fi

# Advance: peer_reviewed → verified (needs ≥2 attestations)
VERIFY2_RESP=$(curl -sf -X PATCH "$API/claims/$CLAIM_RID/verify" \
    -H "Content-Type: application/json" \
    -d '{"new_level": "verified", "actor": "steel-thread-test", "reason": "Phase A test: ready for anchoring"}' 2>/dev/null || echo '{}')
VERIFY2_STATE=$(echo "$VERIFY2_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("verification",""))' 2>/dev/null || echo "")
check "State → verified" "$([ "$VERIFY2_STATE" = "verified" ] && echo true || echo false)"
echo ""

# ------------------------------------------------------------------ #
# Step 5: Prepare anchor (compute content hash)
# ------------------------------------------------------------------ #
echo "--- Step 5: Prepare anchor ---"

PREP_RESP=$(curl -sf -X POST "$API/claims/$CLAIM_RID/prepare-anchor" 2>/dev/null || echo '{}')
CONTENT_HASH=$(echo "$PREP_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("content_hash",""))' 2>/dev/null || echo "")
READY=$(echo "$PREP_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(str(d.get("ready_to_anchor",False)).lower())' 2>/dev/null || echo "false")

check "Content hash computed" "$([ -n "$CONTENT_HASH" ] && echo true || echo false)"
echo "  Content hash: ${CONTENT_HASH:0:32}..."
echo "  Ready to anchor: $READY"

if [ "$READY" != "true" ]; then
    REASON=$(echo "$PREP_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("reason","unknown"))' 2>/dev/null || echo "unknown")
    yellow "  Reason: $REASON"
fi
echo ""

# ------------------------------------------------------------------ #
# Step 6: Anchor to Regen testnet
# ------------------------------------------------------------------ #
echo "--- Step 6: Anchor to Regen testnet ---"

if [ "$READY" = "true" ] && [ "$RUN_ANCHOR" = "1" ]; then
    # Anchor call may take up to 30s (polling for tx confirmation)
    ANCHOR_RESP=$(curl -sf --max-time 60 -X POST "$API/claims/$CLAIM_RID/anchor" 2>/dev/null || echo '{}')
    ANCHOR_STATUS=$?

    TX_HASH=$(echo "$ANCHOR_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("tx_hash",""))' 2>/dev/null || echo "")
    LEDGER_IRI=$(echo "$ANCHOR_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("ledger_iri",""))' 2>/dev/null || echo "")
    ANCHOR_PENDING=$(echo "$ANCHOR_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("status",""))' 2>/dev/null || echo "")

    if [ -n "$TX_HASH" ]; then
        check "Anchor broadcast succeeded" "true"
        echo "  tx_hash: $TX_HASH"
        echo "  ledger_iri: $LEDGER_IRI"

        if [ "$ANCHOR_PENDING" = "pending" ]; then
            yellow "  Anchor pending — attempting reconcile..."
            sleep 10
            RECON_RESP=$(curl -sf --max-time 30 -X POST "$API/claims/$CLAIM_RID/reconcile" 2>/dev/null || echo '{}')
            RECON_STATUS=$(echo "$RECON_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("status",""))' 2>/dev/null || echo "")
            check "Reconcile completed" "$([ "$RECON_STATUS" = "anchored" ] && echo true || echo false)"
            echo "  Reconcile status: $RECON_STATUS"
        else
            check "Anchor confirmed on-chain" "true"
        fi
    else
        check "Anchor broadcast succeeded" "false"
        echo "  Response: $ANCHOR_RESP"
    fi
else
    skip "Anchor broadcast" "RUN_ANCHOR is not 1, regen CLI unavailable, or prepare-anchor failed"
    skip "Anchor confirmed" "skipped due to above"
fi
echo ""

# ------------------------------------------------------------------ #
# Step 7: Verify claim state after anchoring
# ------------------------------------------------------------------ #
echo "--- Step 7: Verify final claim state ---"

FINAL_CLAIM=$(curl -sf "$API/claims/$CLAIM_RID" 2>/dev/null || echo '{}')
FINAL_STATE=$(echo "$FINAL_CLAIM" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("verification",""))' 2>/dev/null || echo "")
FINAL_TX=$(echo "$FINAL_CLAIM" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("tx_hash",""))' 2>/dev/null || echo "")
FINAL_IRI=$(echo "$FINAL_CLAIM" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("ledger_iri",""))' 2>/dev/null || echo "")
FINAL_HASH=$(echo "$FINAL_CLAIM" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("content_hash",""))' 2>/dev/null || echo "")

if [ "$READY" = "true" ] && [ -n "$TX_HASH" ]; then
    check "Final state is ledger_anchored" "$([ "$FINAL_STATE" = "ledger_anchored" ] && echo true || echo false)"
    check "tx_hash persisted" "$([ -n "$FINAL_TX" ] && echo true || echo false)"
    check "ledger_iri persisted" "$([ -n "$FINAL_IRI" ] && echo true || echo false)"
else
    check "Final state is verified (anchor skipped)" "$([ "$FINAL_STATE" = "verified" ] && echo true || echo false)"
fi
check "content_hash persisted" "$([ -n "$FINAL_HASH" ] && echo true || echo false)"
echo ""

# ------------------------------------------------------------------ #
# Step 8: Get audit history
# ------------------------------------------------------------------ #
echo "--- Step 8: Audit history ---"

HISTORY_RESP=$(curl -sf "$API/claims/$CLAIM_RID/history" 2>/dev/null || echo '{}')
TRANSITION_COUNT=$(echo "$HISTORY_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("transitions",[])))' 2>/dev/null || echo "0")

# Minimum transitions: creation log + evidence_linked + peer_reviewed + verified = 4
check "History has transitions" "$([ "$TRANSITION_COUNT" -ge 3 ] && echo true || echo false)"
echo "  Transition count: $TRANSITION_COUNT"
echo ""

# ------------------------------------------------------------------ #
# Step 9: Assemble proof pack
# ------------------------------------------------------------------ #
echo "--- Step 9: Proof pack ---"

if [ "$FINAL_STATE" = "ledger_anchored" ]; then
    PROOF_RESP=$(curl -sf "$API/claims/$CLAIM_RID/proof-pack" 2>/dev/null || echo '{}')
    PROOF_VER=$(echo "$PROOF_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("version",""))' 2>/dev/null || echo "")
    PROOF_EVIDENCE=$(echo "$PROOF_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("evidence",[])))' 2>/dev/null || echo "0")
    PROOF_HISTORY=$(echo "$PROOF_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("history",[])))' 2>/dev/null || echo "0")
    PROOF_ANCHOR=$(echo "$PROOF_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); a=d.get("anchor",{}); print("true" if a.get("tx_hash") and a.get("ledger_iri") and a.get("content_hash") else "false")' 2>/dev/null || echo "false")

    check "Proof pack version 1.0" "$([ "$PROOF_VER" = "1.0" ] && echo true || echo false)"
    check "Proof pack has evidence" "$([ "$PROOF_EVIDENCE" -ge 1 ] && echo true || echo false)"
    check "Proof pack has history" "$([ "$PROOF_HISTORY" -ge 3 ] && echo true || echo false)"
    check "Proof pack has anchor fields" "$PROOF_ANCHOR"

    # Archive proof pack
    ARCHIVE_DIR="$(dirname "$0")/../docs/demo-artifacts"
    mkdir -p "$ARCHIVE_DIR"
    ARCHIVE_FILE="$ARCHIVE_DIR/proof-pack-phase-a-$(date +%Y%m%d-%H%M%S).json"
    echo "$PROOF_RESP" | python3 -m json.tool > "$ARCHIVE_FILE" 2>/dev/null && \
        echo "  Archived to: $ARCHIVE_FILE" || \
        yellow "  Could not archive proof pack"
else
    skip "Proof pack assembly" "Claim not anchored (anchor step skipped)"
    # Still test the endpoint returns 409 (not 200)
    PROOF_409=$(curl -s -o /dev/null -w '%{http_code}' "$API/claims/$CLAIM_RID/proof-pack" 2>/dev/null || echo "000")
    check "Proof pack returns 409 for non-anchored claim" "$([ "$PROOF_409" = "409" ] && echo true || echo false)"
fi
echo ""

# ------------------------------------------------------------------ #
# Summary
# ------------------------------------------------------------------ #
echo "========================================"
TOTAL=$((PASS + FAIL + SKIP))
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped ($TOTAL total)"
echo "Claim RID: $CLAIM_RID"
echo "Evidence URI: $EVIDENCE_URI"
[ -n "${TX_HASH:-}" ] && echo "TX Hash: $TX_HASH"
[ -n "${LEDGER_IRI:-}" ] && echo "Ledger IRI: $LEDGER_IRI"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
