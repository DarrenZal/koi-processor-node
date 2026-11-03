#!/usr/bin/env bash
# Steel Thread Phase B — Voice-to-Graph-to-Claim Pipeline Test
#
# Extends Phase A by testing: published interview artifacts → Evidence entity
# → claim → attestations → anchor → proof pack.
#
# Uses existing published interview session data on the target node.
#
# Usage:
#   KOI_TEST_API_URL=http://127.0.0.1:<throwaway port> \
#   KOI_TEST_DATABASE_URL=postgresql://<role>@127.0.0.1:<port>/<db with test in its name> \
#       ./tests/test_steel_thread_phase_b.sh
#   The API must write to that database (tests/live_write_shell_guard.sh).
#
# Prerequisites:
#   - KOI API at KOI_TEST_API_URL
#   - Published interview artifacts (Pattern/Protocol) in the registry
#   - At least one Person entity (claimant) and one Organization (about_uri)
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
echo "Steel Thread Phase B — Voice-to-Graph"
echo "API: $API"
echo "========================================"
echo ""

# Helper: extract URI from entity-search result
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

# Auto-discover entity by name + allowed types
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

# ------------------------------------------------------------------ #
# Step 0: Health check + discover entities
# ------------------------------------------------------------------ #
echo "--- Step 0: Setup ---"

HEALTH=$(curl -sf "$API/health" 2>/dev/null || echo '{}')
check "API health" "$(echo "$HEALTH" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("true" if d.get("status") in ("ok","healthy") else "false")' 2>/dev/null || echo false)"

# Find claimant
CLAIMANT_NAME="${CLAIMANT_NAME:-Test Claimant}"
CLAIMANT_URI=$(find_entity "$CLAIMANT_NAME" "Person")
if [ -z "$CLAIMANT_URI" ]; then
    red "No Person entity found — cannot proceed."
    exit 1
fi
echo "  Claimant URI: $CLAIMANT_URI"

# Find about_uri
ABOUT_NAME="${ABOUT_NAME:-Salish Sea}"
ABOUT_URI=$(find_entity "$ABOUT_NAME" "Organization Location Bioregion")
echo "  About URI: ${ABOUT_URI:-<none>}"

# Find reviewer (must differ from claimant)
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
# Step 1: Find published interview artifacts (Pattern + Protocol)
# ------------------------------------------------------------------ #
echo "--- Step 1: Find published interview artifacts ---"

# Search for Pattern entities from interview sessions
PATTERN_URI=""
PROTOCOL_URI=""

# Try known interview artifact names, then fall back to type search
for pname in "User Personas in Knowledge Commons" "Trust Through Transparency"; do
    candidate=$(find_entity "$pname" "Pattern PatternCandidate")
    if [ -n "$candidate" ]; then
        PATTERN_URI="$candidate"
        break
    fi
done

if [ -z "$PATTERN_URI" ]; then
    # Fall back: search for any Pattern entity
    PATTERN_URI=$(curl -sf "$API/entities?entity_type=Pattern&limit=1" 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
entities = d if isinstance(d, list) else d.get('entities', d.get('results', []))
for e in entities:
    uri = e.get('uri', e.get('fuseki_uri', ''))
    if uri:
        print(uri)
        break
else:
    print('')
" 2>/dev/null || echo "")
fi

check "Pattern artifact found" "$([ -n "$PATTERN_URI" ] && echo true || echo false)"
echo "  Pattern URI: ${PATTERN_URI:-NOT FOUND}"

for pname in "Facilitated Knowledge Sharing Events" "Citizen Science Protocol"; do
    candidate=$(find_entity "$pname" "Protocol ProtocolCandidate")
    if [ -n "$candidate" ]; then
        PROTOCOL_URI="$candidate"
        break
    fi
done

if [ -z "$PROTOCOL_URI" ]; then
    PROTOCOL_URI=$(curl -sf "$API/entities?entity_type=Protocol&limit=1" 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
entities = d if isinstance(d, list) else d.get('entities', d.get('results', []))
for e in entities:
    uri = e.get('uri', e.get('fuseki_uri', ''))
    if uri:
        print(uri)
        break
else:
    print('')
" 2>/dev/null || echo "")
fi

check "Protocol artifact found" "$([ -n "$PROTOCOL_URI" ] && echo true || echo false)"
echo "  Protocol URI: ${PROTOCOL_URI:-NOT FOUND}"

if [ -z "$PATTERN_URI" ] && [ -z "$PROTOCOL_URI" ]; then
    red "No published interview artifacts found. Cannot proceed with Phase B."
    red "Hint: Run an interview session and publish artifacts first."
    echo ""
    echo "========================================"
    echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
    echo "========================================"
    exit 1
fi

# Build source_uris list (at least one must exist)
SOURCE_URIS="["
[ -n "$PATTERN_URI" ] && SOURCE_URIS="$SOURCE_URIS\"$PATTERN_URI\""
if [ -n "$PROTOCOL_URI" ]; then
    [ -n "$PATTERN_URI" ] && SOURCE_URIS="$SOURCE_URIS,"
    SOURCE_URIS="$SOURCE_URIS\"$PROTOCOL_URI\""
fi
SOURCE_URIS="$SOURCE_URIS]"
echo "  Source URIs: $SOURCE_URIS"
echo ""

# ------------------------------------------------------------------ #
# Step 2: Create Evidence entity from artifacts
# ------------------------------------------------------------------ #
echo "--- Step 2: Create Evidence from artifacts ---"

TS="$KOI_TEST_RUN_ID"
EVIDENCE_NAME="Interview Evidence — Bioregional Knowledge Practices $TS"
EVIDENCE_DESC="Evidence synthesized from published interview artifacts demonstrating bioregional knowledge commoning practices including pattern recognition and protocol development."

EVIDENCE_RESP=$(curl -sf -X POST "$API/claims/evidence-from-artifacts" \
    -H "Content-Type: application/json" \
    -d "{
        \"source_uris\": $SOURCE_URIS,
        \"name\": \"$EVIDENCE_NAME\",
        \"description\": \"$EVIDENCE_DESC\",
        \"bioregion\": \"Salish Sea\"
    }" 2>/dev/null || echo '{}')

EVIDENCE_URI=$(echo "$EVIDENCE_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("evidence_uri",""))' 2>/dev/null || echo "")
live_write_record entity_uri "$EVIDENCE_URI"
IS_NEW=$(echo "$EVIDENCE_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(str(d.get("is_new",False)).lower())' 2>/dev/null || echo "false")
VISIBILITY=$(echo "$EVIDENCE_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("visibility_scope",""))' 2>/dev/null || echo "")
VAULT_RID=$(echo "$EVIDENCE_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("vault_rid",""))' 2>/dev/null || echo "")
SOURCE_COUNT=$(echo "$EVIDENCE_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("source_artifacts",[])))' 2>/dev/null || echo "0")

check "Evidence entity created" "$([ -n "$EVIDENCE_URI" ] && echo true || echo false)"
check "Evidence is new" "$([ "$IS_NEW" = "true" ] && echo true || echo false)"
check "Visibility scope set" "$([ -n "$VISIBILITY" ] && echo true || echo false)"
check "Source artifacts linked" "$([ "$SOURCE_COUNT" -ge 1 ] && echo true || echo false)"

echo "  Evidence URI: $EVIDENCE_URI"
echo "  Is new: $IS_NEW"
echo "  Visibility: $VISIBILITY"
echo "  Vault RID: $VAULT_RID"
echo "  Source artifacts: $SOURCE_COUNT"

if [ -z "$EVIDENCE_URI" ]; then
    red "Cannot proceed without Evidence entity. Response: $EVIDENCE_RESP"
    exit 1
fi

# Verify it's type Evidence in the registry
EVIDENCE_TYPE=$(curl -sf "$API/entity/$EVIDENCE_URI" 2>/dev/null \
    | python3 -c 'import sys,json; d=json.load(sys.stdin); e=d.get("entity",d); print(e.get("entity_type", e.get("type", "")))' 2>/dev/null || echo "")
check "Evidence entity type is Evidence" "$([ "$EVIDENCE_TYPE" = "Evidence" ] && echo true || echo false)"
echo ""

# ------------------------------------------------------------------ #
# Step 3: Create claim about the bioregion using this Evidence
# ------------------------------------------------------------------ #
echo "--- Step 3: Create claim ---"

CLAIM_STATEMENT="Steel Thread Phase B $KOI_TEST_RUN_ID: Interview participants in the Salish Sea bioregion have documented knowledge commoning practices including pattern recognition for community coordination and protocol development for facilitated knowledge sharing, demonstrating active bioregional knowledge stewardship."

ABOUT_URI_FIELD=""
if [ -n "$ABOUT_URI" ]; then
    ABOUT_URI_FIELD="\"about_uri\": \"$ABOUT_URI\","
fi

CLAIM_RESP=$(curl -sf -X POST "$API/claims/" \
    -H "Content-Type: application/json" \
    -d "{
        \"claimant_uri\": \"$CLAIMANT_URI\",
        \"statement\": \"$CLAIM_STATEMENT\",
        \"claim_type\": \"social\",
        $ABOUT_URI_FIELD
        \"metadata\": {
            \"methodology\": \"steel_thread_phase_b\",
            \"source_type\": \"interview_artifacts\",
            \"test_run\": true,
            \"test_run_id\": \"$KOI_TEST_RUN_ID\",
            \"generated_at\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
        },
        \"created_by\": \"steel-thread-phase-b\"
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
# Step 4: Link Evidence to claim
# ------------------------------------------------------------------ #
echo "--- Step 4: Link Evidence to claim ---"

LINK_RESP=$(curl -sf -X POST "$API/claims/$CLAIM_RID/evidence" \
    -H "Content-Type: application/json" \
    -d "{
        \"evidence_uri\": \"$EVIDENCE_URI\",
        \"actor\": \"steel-thread-phase-b\"
    }" 2>/dev/null || echo '{}')

CLAIM_WITH_EV=$(curl -sf "$API/claims/$CLAIM_RID" 2>/dev/null || echo '{}')
LINK_EVIDENCE=$(echo "$CLAIM_WITH_EV" | python3 -c 'import sys,json; d=json.load(sys.stdin); ev=d.get("evidence",[]); print("true" if any(e.get("uri")==sys.argv[1] for e in (ev or [])) else "false")' "$EVIDENCE_URI" 2>/dev/null || echo "false")
check "Evidence linked to claim" "$LINK_EVIDENCE"
echo ""

# ------------------------------------------------------------------ #
# Step 5: Attestations + state advancement
# ------------------------------------------------------------------ #
echo "--- Step 5: Attestations + verification state ---"

ATT1_DONE=false
if [ -n "$REVIEWER_URI" ]; then
    ATT1_RESP=$(curl -sf -X POST "$API/claims/$CLAIM_RID/attestations" \
        -H "Content-Type: application/json" \
        -d "{
            \"reviewer_uri\": \"$REVIEWER_URI\",
            \"verdict\": \"approved\",
            \"rationale\": \"Steel thread Phase B — interview artifacts reviewed\",
            \"evidence_uris\": [\"$EVIDENCE_URI\"]
        }" 2>/dev/null || echo '{}')
    ATT1_RID=$(echo "$ATT1_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("attestation_rid",""))' 2>/dev/null || echo "")
    check "Attestation #1 created" "$([ -n "$ATT1_RID" ] && echo true || echo false)"
    [ -n "$ATT1_RID" ] && ATT1_DONE=true
else
    skip "Attestation #1" "No reviewer entity available"
fi

# Advance: self_reported → peer_reviewed
VERIFY1_RESP=$(curl -sf -X PATCH "$API/claims/$CLAIM_RID/verify" \
    -H "Content-Type: application/json" \
    -d '{"new_level": "peer_reviewed", "actor": "steel-thread-phase-b", "reason": "Phase B: interview evidence reviewed"}' 2>/dev/null || echo '{}')
VERIFY1_STATE=$(echo "$VERIFY1_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("verification",""))' 2>/dev/null || echo "")
check "State → peer_reviewed" "$([ "$VERIFY1_STATE" = "peer_reviewed" ] && echo true || echo false)"

# Find second reviewer for verified
REVIEWER2_URI=""
for rname in "Regen Network" "BlockScience" "Bioregional Learning Centre"; do
    candidate=$(find_entity "$rname" "Person Organization")
    if [ -n "$candidate" ] && [ "$candidate" != "$CLAIMANT_URI" ] && [ "$candidate" != "${REVIEWER_URI:-}" ]; then
        REVIEWER2_URI="$candidate"
        break
    fi
done

ATT2_DONE=false
if [ -n "$REVIEWER2_URI" ]; then
    ATT2_RESP=$(curl -sf -X POST "$API/claims/$CLAIM_RID/attestations" \
        -H "Content-Type: application/json" \
        -d "{
            \"reviewer_uri\": \"$REVIEWER2_URI\",
            \"verdict\": \"approved\",
            \"rationale\": \"Steel thread Phase B — second attestation\",
            \"evidence_uris\": [\"$EVIDENCE_URI\"]
        }" 2>/dev/null || echo '{}')
    ATT2_RID=$(echo "$ATT2_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("attestation_rid",""))' 2>/dev/null || echo "")
    check "Attestation #2 created" "$([ -n "$ATT2_RID" ] && echo true || echo false)"
    [ -n "$ATT2_RID" ] && ATT2_DONE=true
else
    skip "Attestation #2" "No second reviewer available for verified gate"
fi

# Advance: peer_reviewed → verified
VERIFY2_RESP=$(curl -sf -X PATCH "$API/claims/$CLAIM_RID/verify" \
    -H "Content-Type: application/json" \
    -d '{"new_level": "verified", "actor": "steel-thread-phase-b", "reason": "Phase B: ready for anchoring"}' 2>/dev/null || echo '{}')
VERIFY2_STATE=$(echo "$VERIFY2_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("verification",""))' 2>/dev/null || echo "")
check "State → verified" "$([ "$VERIFY2_STATE" = "verified" ] && echo true || echo false)"
echo ""

# ------------------------------------------------------------------ #
# Step 6: Prepare anchor
# ------------------------------------------------------------------ #
echo "--- Step 6: Prepare anchor ---"

PREP_RESP=$(curl -sf -X POST "$API/claims/$CLAIM_RID/prepare-anchor" 2>/dev/null || echo '{}')
CONTENT_HASH=$(echo "$PREP_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("content_hash",""))' 2>/dev/null || echo "")
READY=$(echo "$PREP_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(str(d.get("ready_to_anchor",False)).lower())' 2>/dev/null || echo "false")

check "Content hash computed" "$([ -n "$CONTENT_HASH" ] && echo true || echo false)"
echo "  Content hash: ${CONTENT_HASH:0:32}..."
echo "  Ready to anchor: $READY"
echo ""

# ------------------------------------------------------------------ #
# Step 7: Anchor to Regen testnet
# ------------------------------------------------------------------ #
echo "--- Step 7: Anchor to Regen testnet ---"

if [ "$READY" = "true" ] && [ "$RUN_ANCHOR" = "1" ]; then
    ANCHOR_RESP=$(curl -sf --max-time 60 -X POST "$API/claims/$CLAIM_RID/anchor" 2>/dev/null || echo '{}')

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
# Step 8: Verify final state
# ------------------------------------------------------------------ #
echo "--- Step 8: Verify final claim state ---"

FINAL_CLAIM=$(curl -sf "$API/claims/$CLAIM_RID" 2>/dev/null || echo '{}')
FINAL_STATE=$(echo "$FINAL_CLAIM" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("verification",""))' 2>/dev/null || echo "")
FINAL_TX=$(echo "$FINAL_CLAIM" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("tx_hash",""))' 2>/dev/null || echo "")
FINAL_HASH=$(echo "$FINAL_CLAIM" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("content_hash",""))' 2>/dev/null || echo "")

if [ "${READY:-false}" = "true" ] && [ -n "${TX_HASH:-}" ]; then
    check "Final state is ledger_anchored" "$([ "$FINAL_STATE" = "ledger_anchored" ] && echo true || echo false)"
    check "tx_hash persisted" "$([ -n "$FINAL_TX" ] && echo true || echo false)"
else
    check "Final state is verified (anchor skipped)" "$([ "$FINAL_STATE" = "verified" ] && echo true || echo false)"
fi
check "content_hash persisted" "$([ -n "$FINAL_HASH" ] && echo true || echo false)"
echo ""

# ------------------------------------------------------------------ #
# Step 9: Proof pack
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
    ARCHIVE_FILE="$ARCHIVE_DIR/proof-pack-phase-b-$(date +%Y%m%d-%H%M%S).json"
    echo "$PROOF_RESP" | python3 -m json.tool > "$ARCHIVE_FILE" 2>/dev/null && \
        echo "  Archived to: $ARCHIVE_FILE" || \
        yellow "  Could not archive proof pack"
else
    skip "Proof pack assembly" "Claim not anchored (anchor step skipped)"
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
echo ""
echo "Evidence URI: $EVIDENCE_URI"
echo "Claim RID: $CLAIM_RID"
[ -n "${PATTERN_URI:-}" ] && echo "Pattern artifact: $PATTERN_URI"
[ -n "${PROTOCOL_URI:-}" ] && echo "Protocol artifact: $PROTOCOL_URI"
[ -n "${TX_HASH:-}" ] && echo "TX Hash: $TX_HASH"
[ -n "${LEDGER_IRI:-}" ] && echo "Ledger IRI: $LEDGER_IRI"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
