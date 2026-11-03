#!/usr/bin/env bash
# TBFF Threshold Policy Tests
#
# Tests the POST /claims/claim-from-settlement endpoint across all three
# threshold bands (auto, semi, manual) plus manual_override behavior.
#
# Usage:
#   KOI_TEST_API_URL=http://127.0.0.1:<throwaway port> \
#   KOI_TEST_DATABASE_URL=postgresql://<role>@127.0.0.1:<port>/<db with test in its name> \
#       ./tests/test_tbff_threshold_policy.sh
#   The API must write to that database (tests/live_write_shell_guard.sh).
#
# Prerequisites:
#   - KOI API at KOI_TEST_API_URL
#   - At least one Person entity (claimant), one Person/Organization (reviewer),
#     and one Person/Organization (operator, different from both)
#   - For about_uri: an Organization or Location entity

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/live_write_shell_guard.sh"
live_write_begin

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
echo "TBFF Threshold Policy Tests"
echo "API: $API"
echo "========================================"
echo ""

# --------------------------------------------------------
# Step 0: Health check
# --------------------------------------------------------
echo "--- Step 0: Health check ---"
HEALTH=$(curl -sf "$API/health" 2>/dev/null | python3 -c "import sys,json; s=json.load(sys.stdin).get('status',''); print('ok' if s in ('ok','healthy') else s)" 2>/dev/null || echo "")
check "API is healthy" "$([ "$HEALTH" = "ok" ] && echo true || echo false)"

if [ "$HEALTH" != "ok" ]; then
    echo "API not reachable at $API — aborting."
    exit 1
fi

# --------------------------------------------------------
# Step 1: Discover test entities
# --------------------------------------------------------
echo ""
echo "--- Step 1: Discover test entities ---"

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

# Helper: get entities list from /entities endpoint (handles both {entities:[...]} and [...])
get_entities_list() {
    python3 -c "
import sys, json
d = json.load(sys.stdin)
if isinstance(d, list):
    items = d
elif isinstance(d, dict):
    items = d.get('entities', d.get('results', []))
else:
    items = []
json.dump(items, sys.stdout)
" 2>/dev/null || echo "[]"
}

# Find a Person for claimant
CLAIMANT_URI=$(find_entity "${CLAIMANT_NAME:-Test Claimant}" "Person")
if [ -z "$CLAIMANT_URI" ]; then
    CLAIMANT_URI=$(curl -sf "$API/entities?entity_type=Person&limit=1" 2>/dev/null \
        | get_entities_list \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0]['fuseki_uri'] if d else '')" 2>/dev/null || echo "")
fi
check "Found claimant Person" "$([ -n "$CLAIMANT_URI" ] && echo true || echo false)"
[ -n "$CLAIMANT_URI" ] && echo "    claimant: $CLAIMANT_URI"

# Find a different Person/Organization for reviewer
REVIEWER_URI=$(curl -sf "$API/entities?entity_type=Person&limit=5" 2>/dev/null \
    | get_entities_list \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
claimant = '$CLAIMANT_URI'
for r in d:
    uri = r.get('fuseki_uri', '')
    if uri and uri != claimant:
        print(uri)
        break
else:
    print('')
" 2>/dev/null || echo "")
check "Found reviewer Person (distinct from claimant)" "$([ -n "$REVIEWER_URI" ] && echo true || echo false)"
[ -n "$REVIEWER_URI" ] && echo "    reviewer: $REVIEWER_URI"

# Find a third distinct Person/Organization for operator (needed for auto band)
OPERATOR_URI=$(curl -sf "$API/entities?entity_type=Person&limit=10" 2>/dev/null \
    | get_entities_list \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
claimant = '$CLAIMANT_URI'
reviewer = '$REVIEWER_URI'
for r in d:
    uri = r.get('fuseki_uri', '')
    if uri and uri != claimant and uri != reviewer:
        print(uri)
        break
else:
    print('')
" 2>/dev/null || echo "")
check "Found operator Person (distinct from claimant and reviewer)" "$([ -n "$OPERATOR_URI" ] && echo true || echo false)"
[ -n "$OPERATOR_URI" ] && echo "    operator: $OPERATOR_URI"

# Find an Organization for about_uri
ABOUT_URI=$(find_entity "Victoria Landscape Hub" "Organization")
if [ -z "$ABOUT_URI" ]; then
    ABOUT_URI=$(curl -sf "$API/entities?entity_type=Organization&limit=1" 2>/dev/null \
        | get_entities_list \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d[0]['fuseki_uri'] if d else '')" 2>/dev/null || echo "")
fi
check "Found about_uri Organization" "$([ -n "$ABOUT_URI" ] && echo true || echo false)"
[ -n "$ABOUT_URI" ] && echo "    about: $ABOUT_URI"

if [ -z "$CLAIMANT_URI" ] || [ -z "$REVIEWER_URI" ]; then
    echo ""
    echo "Cannot proceed without claimant and reviewer entities."
    echo "TOTAL: $PASS passed, $FAIL failed, $SKIP skipped"
    exit 1
fi

# Unique suffix for this test run
RUN_ID="$KOI_TEST_RUN_ID"

# --------------------------------------------------------
# Step 2: Test AUTO band (< $500) — should auto-advance
# --------------------------------------------------------
echo ""
echo "--- Step 2: AUTO band (< \$500) ---"

AUTO_SETTLEMENT_ID="test-auto-${RUN_ID}"
AUTO_AMOUNT=150.00

AUTO_PAYLOAD=$(cat <<EOF
{
  "settlement": {
    "settlement_id": "$AUTO_SETTLEMENT_ID",
    "iterations": 3,
    "converged": true,
    "total_redistributed_usd": $AUTO_AMOUNT,
    "node_balances": [
      {"participant_name": "Alice", "initial_balance": 100, "final_balance": 75, "threshold": 80},
      {"participant_name": "Bob", "initial_balance": 100, "final_balance": 125, "threshold": 120}
    ],
    "bioregion": "Cascadia",
    "description": "Test auto-advance settlement under \$500 threshold for TBFF policy verification."
  },
  "claimant_uri": "$CLAIMANT_URI",
  "statement": "TBFF settlement auto-advance test: \$${AUTO_AMOUNT} redistributed across 2 participants in Cascadia bioregion.",
  "claim_type": "financial",
  $([ -n "$ABOUT_URI" ] && echo "\"about_uri\": \"$ABOUT_URI\"," || echo "")
  "reviewer_uri": "$REVIEWER_URI",
  $([ -n "$OPERATOR_URI" ] && echo "\"operator_uri\": \"$OPERATOR_URI\"," || echo "")
  "manual_override": false
}
EOF
)

AUTO_RESPONSE=$(curl -sf -X POST "$API/claims/claim-from-settlement" \
    -H "Content-Type: application/json" \
    -d "$AUTO_PAYLOAD" 2>/dev/null || echo "{}")

AUTO_BAND=$(echo "$AUTO_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('threshold_band',''))" 2>/dev/null || echo "")
AUTO_VERIFICATION=$(echo "$AUTO_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('verification',''))" 2>/dev/null || echo "")
AUTO_ADVANCED=$(echo "$AUTO_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('auto_advanced',False))" 2>/dev/null || echo "")
AUTO_CLAIM_RID=$(echo "$AUTO_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('claim_rid',''))" 2>/dev/null || echo "")
AUTO_EVIDENCE_URI=$(echo "$AUTO_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('evidence_uri',''))" 2>/dev/null || echo "")
live_write_record claim_rid "$AUTO_CLAIM_RID"
live_write_record entity_uri "$AUTO_EVIDENCE_URI"
AUTO_REASON=$(echo "$AUTO_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('auto_advance_reason','') or '')" 2>/dev/null || echo "")

check "Auto band: threshold_band=auto" "$([ "$AUTO_BAND" = "auto" ] && echo true || echo false)"
check "Auto band: claim created" "$([ -n "$AUTO_CLAIM_RID" ] && echo true || echo false)"
check "Auto band: evidence created" "$([ -n "$AUTO_EVIDENCE_URI" ] && echo true || echo false)"

# If we have 3 distinct entities (claimant, reviewer, operator), auto should reach verified
if [ -n "$OPERATOR_URI" ]; then
    check "Auto band: auto-advanced" "$([ "$AUTO_ADVANCED" = "True" ] && echo true || echo false)"
    check "Auto band: verification=verified" "$([ "$AUTO_VERIFICATION" = "verified" ] && echo true || echo false)"
else
    check "Auto band: auto-advanced to peer_reviewed (no operator)" "$([ "$AUTO_VERIFICATION" = "peer_reviewed" ] && echo true || echo false)"
    skip "Auto band: verification=verified" "needs operator_uri as second reviewer"
fi

if [ -n "$AUTO_REASON" ] && [ "$AUTO_REASON" != "None" ]; then
    echo "    auto_advance_reason: $AUTO_REASON"
fi

# Verify claim state via GET
if [ -n "$AUTO_CLAIM_RID" ]; then
    GET_AUTO=$(curl -sf "$API/claims/$AUTO_CLAIM_RID" 2>/dev/null || echo "{}")
    GET_VERIFICATION=$(echo "$GET_AUTO" | python3 -c "import sys,json; print(json.load(sys.stdin).get('verification',''))" 2>/dev/null || echo "")
    check "Auto band: GET confirms verification state" "$([ "$GET_VERIFICATION" = "$AUTO_VERIFICATION" ] && echo true || echo false)"
fi

# --------------------------------------------------------
# Step 3: Test SEMI band ($500–$5000) — auto to peer_reviewed
# --------------------------------------------------------
echo ""
echo "--- Step 3: SEMI band (\$500–\$5000) ---"

SEMI_SETTLEMENT_ID="test-semi-${RUN_ID}"
SEMI_AMOUNT=2500.00

SEMI_PAYLOAD=$(cat <<EOF
{
  "settlement": {
    "settlement_id": "$SEMI_SETTLEMENT_ID",
    "iterations": 7,
    "converged": true,
    "total_redistributed_usd": $SEMI_AMOUNT,
    "node_balances": [
      {"participant_name": "Avery", "initial_balance": 2000, "final_balance": 1500, "threshold": 1800},
      {"participant_name": "Rowan", "initial_balance": 1000, "final_balance": 1500, "threshold": 1200}
    ],
    "bioregion": "Cascadia",
    "description": "Test semi-auto settlement in \$500-\$5000 band for TBFF policy verification."
  },
  "claimant_uri": "$CLAIMANT_URI",
  "statement": "TBFF settlement semi-auto test: \$${SEMI_AMOUNT} redistributed for Cascadia landscape hub funding round.",
  "claim_type": "financial",
  $([ -n "$ABOUT_URI" ] && echo "\"about_uri\": \"$ABOUT_URI\"," || echo "")
  "reviewer_uri": "$REVIEWER_URI",
  "manual_override": false
}
EOF
)

SEMI_RESPONSE=$(curl -sf -X POST "$API/claims/claim-from-settlement" \
    -H "Content-Type: application/json" \
    -d "$SEMI_PAYLOAD" 2>/dev/null || echo "{}")

SEMI_BAND=$(echo "$SEMI_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('threshold_band',''))" 2>/dev/null || echo "")
SEMI_VERIFICATION=$(echo "$SEMI_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('verification',''))" 2>/dev/null || echo "")
SEMI_ADVANCED=$(echo "$SEMI_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('auto_advanced',False))" 2>/dev/null || echo "")
SEMI_CLAIM_RID=$(echo "$SEMI_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('claim_rid',''))" 2>/dev/null || echo "")
SEMI_EVIDENCE_URI=$(echo "$SEMI_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('evidence_uri',''))" 2>/dev/null || echo "")
live_write_record claim_rid "$SEMI_CLAIM_RID"
live_write_record entity_uri "$SEMI_EVIDENCE_URI"

check "Semi band: threshold_band=semi" "$([ "$SEMI_BAND" = "semi" ] && echo true || echo false)"
check "Semi band: auto-advanced" "$([ "$SEMI_ADVANCED" = "True" ] && echo true || echo false)"
check "Semi band: verification=peer_reviewed" "$([ "$SEMI_VERIFICATION" = "peer_reviewed" ] && echo true || echo false)"
check "Semi band: claim created" "$([ -n "$SEMI_CLAIM_RID" ] && echo true || echo false)"

# --------------------------------------------------------
# Step 4: Test MANUAL band (> $5000) — stays self_reported
# --------------------------------------------------------
echo ""
echo "--- Step 4: MANUAL band (> \$5000) ---"

MANUAL_SETTLEMENT_ID="test-manual-${RUN_ID}"
MANUAL_AMOUNT=15000.00

MANUAL_PAYLOAD=$(cat <<EOF
{
  "settlement": {
    "settlement_id": "$MANUAL_SETTLEMENT_ID",
    "iterations": 12,
    "converged": true,
    "total_redistributed_usd": $MANUAL_AMOUNT,
    "node_balances": [
      {"participant_name": "Eve", "initial_balance": 10000, "final_balance": 7500, "threshold": 8000},
      {"participant_name": "Frank", "initial_balance": 5000, "final_balance": 7500, "threshold": 7000}
    ],
    "bioregion": "Cascadia",
    "description": "Test manual-review settlement above \$5000 threshold requiring full attestation chain."
  },
  "claimant_uri": "$CLAIMANT_URI",
  "statement": "TBFF settlement manual test: \$${MANUAL_AMOUNT} redistributed — requires full multi-party attestation.",
  "claim_type": "financial",
  $([ -n "$ABOUT_URI" ] && echo "\"about_uri\": \"$ABOUT_URI\"," || echo "")
  "reviewer_uri": "$REVIEWER_URI",
  "manual_override": false
}
EOF
)

MANUAL_RESPONSE=$(curl -sf -X POST "$API/claims/claim-from-settlement" \
    -H "Content-Type: application/json" \
    -d "$MANUAL_PAYLOAD" 2>/dev/null || echo "{}")

MANUAL_BAND=$(echo "$MANUAL_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('threshold_band',''))" 2>/dev/null || echo "")
MANUAL_VERIFICATION=$(echo "$MANUAL_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('verification',''))" 2>/dev/null || echo "")
MANUAL_ADVANCED=$(echo "$MANUAL_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('auto_advanced',False))" 2>/dev/null || echo "")
MANUAL_CLAIM_RID=$(echo "$MANUAL_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('claim_rid',''))" 2>/dev/null || echo "")
MANUAL_EVIDENCE_URI=$(echo "$MANUAL_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('evidence_uri',''))" 2>/dev/null || echo "")
live_write_record claim_rid "$MANUAL_CLAIM_RID"
live_write_record entity_uri "$MANUAL_EVIDENCE_URI"

check "Manual band: threshold_band=manual" "$([ "$MANUAL_BAND" = "manual" ] && echo true || echo false)"
check "Manual band: NOT auto-advanced" "$([ "$MANUAL_ADVANCED" = "False" ] && echo true || echo false)"
check "Manual band: verification=self_reported" "$([ "$MANUAL_VERIFICATION" = "self_reported" ] && echo true || echo false)"
check "Manual band: claim created" "$([ -n "$MANUAL_CLAIM_RID" ] && echo true || echo false)"

# --------------------------------------------------------
# Step 5: Test manual_override on low amount
# --------------------------------------------------------
echo ""
echo "--- Step 5: Manual override (low amount forced to self_reported) ---"

OVERRIDE_SETTLEMENT_ID="test-override-${RUN_ID}"
OVERRIDE_AMOUNT=50.00

OVERRIDE_PAYLOAD=$(cat <<EOF
{
  "settlement": {
    "settlement_id": "$OVERRIDE_SETTLEMENT_ID",
    "iterations": 2,
    "converged": true,
    "total_redistributed_usd": $OVERRIDE_AMOUNT,
    "node_balances": [
      {"participant_name": "Grace", "initial_balance": 50, "final_balance": 25, "threshold": 30},
      {"participant_name": "Hank", "initial_balance": 50, "final_balance": 75, "threshold": 70}
    ],
    "bioregion": "Cascadia",
    "description": "Test manual override: low amount that would normally auto-advance, forced to manual review."
  },
  "claimant_uri": "$CLAIMANT_URI",
  "statement": "TBFF settlement override test: \$${OVERRIDE_AMOUNT} — manually flagged for review despite low threshold.",
  "claim_type": "financial",
  $([ -n "$ABOUT_URI" ] && echo "\"about_uri\": \"$ABOUT_URI\"," || echo "")
  "reviewer_uri": "$REVIEWER_URI",
  "manual_override": true
}
EOF
)

OVERRIDE_RESPONSE=$(curl -sf -X POST "$API/claims/claim-from-settlement" \
    -H "Content-Type: application/json" \
    -d "$OVERRIDE_PAYLOAD" 2>/dev/null || echo "{}")

OVERRIDE_BAND=$(echo "$OVERRIDE_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('threshold_band',''))" 2>/dev/null || echo "")
OVERRIDE_VERIFICATION=$(echo "$OVERRIDE_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('verification',''))" 2>/dev/null || echo "")
OVERRIDE_ADVANCED=$(echo "$OVERRIDE_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('auto_advanced',False))" 2>/dev/null || echo "")
OVERRIDE_CLAIM_RID=$(echo "$OVERRIDE_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('claim_rid',''))" 2>/dev/null || echo "")
OVERRIDE_EVIDENCE_URI=$(echo "$OVERRIDE_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('evidence_uri',''))" 2>/dev/null || echo "")
live_write_record claim_rid "$OVERRIDE_CLAIM_RID"
live_write_record entity_uri "$OVERRIDE_EVIDENCE_URI"

check "Override: threshold_band=manual (despite \$${OVERRIDE_AMOUNT})" "$([ "$OVERRIDE_BAND" = "manual" ] && echo true || echo false)"
check "Override: NOT auto-advanced" "$([ "$OVERRIDE_ADVANCED" = "False" ] && echo true || echo false)"
check "Override: verification=self_reported" "$([ "$OVERRIDE_VERIFICATION" = "self_reported" ] && echo true || echo false)"

# --------------------------------------------------------
# Step 6: Test missing reviewer_uri (auto band but no auto-advance)
# --------------------------------------------------------
echo ""
echo "--- Step 6: Auto band without reviewer (no auto-advance) ---"

NO_REVIEWER_SETTLEMENT_ID="test-norev-${RUN_ID}"

NO_REVIEWER_PAYLOAD=$(cat <<EOF
{
  "settlement": {
    "settlement_id": "$NO_REVIEWER_SETTLEMENT_ID",
    "iterations": 2,
    "converged": true,
    "total_redistributed_usd": 100.00,
    "node_balances": [
      {"participant_name": "Ivy", "initial_balance": 100, "final_balance": 50, "threshold": 60}
    ],
    "bioregion": "Cascadia",
    "description": "Test auto band without reviewer_uri — claim should stay at self_reported."
  },
  "claimant_uri": "$CLAIMANT_URI",
  "statement": "TBFF settlement no-reviewer test: auto band but no reviewer provided for attestation.",
  "claim_type": "financial"
}
EOF
)

NO_REVIEWER_RESPONSE=$(curl -sf -X POST "$API/claims/claim-from-settlement" \
    -H "Content-Type: application/json" \
    -d "$NO_REVIEWER_PAYLOAD" 2>/dev/null || echo "{}")

NO_REVIEWER_BAND=$(echo "$NO_REVIEWER_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('threshold_band',''))" 2>/dev/null || echo "")
NO_REVIEWER_VERIFICATION=$(echo "$NO_REVIEWER_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('verification',''))" 2>/dev/null || echo "")
NO_REVIEWER_REASON=$(echo "$NO_REVIEWER_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('auto_advance_reason',''))" 2>/dev/null || echo "")
NO_REVIEWER_CLAIM_RID=$(echo "$NO_REVIEWER_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('claim_rid',''))" 2>/dev/null || echo "")
NO_REVIEWER_EVIDENCE_URI=$(echo "$NO_REVIEWER_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('evidence_uri',''))" 2>/dev/null || echo "")
live_write_record claim_rid "$NO_REVIEWER_CLAIM_RID"
live_write_record entity_uri "$NO_REVIEWER_EVIDENCE_URI"

check "No reviewer: threshold_band=auto" "$([ "$NO_REVIEWER_BAND" = "auto" ] && echo true || echo false)"
check "No reviewer: verification=self_reported" "$([ "$NO_REVIEWER_VERIFICATION" = "self_reported" ] && echo true || echo false)"
check "No reviewer: reason mentions reviewer_uri" "$(echo "$NO_REVIEWER_REASON" | grep -qi "reviewer" && echo true || echo false)"

# --------------------------------------------------------
# Step 7: Verify receipt chain on one of the claims
# --------------------------------------------------------
echo ""
echo "--- Step 7: Receipt chain verification ---"

AUTO_RECEIPT_ID=$(echo "$AUTO_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('receipt_id','') or '')" 2>/dev/null || echo "")
if [ -n "$AUTO_RECEIPT_ID" ]; then
    CHAIN_HTTP=$(curl -s -o /tmp/tbff_chain.json -w "%{http_code}" "$API/receipts/$AUTO_RECEIPT_ID/chain" 2>/dev/null || echo "000")
    if [ "$CHAIN_HTTP" = "200" ]; then
        CHAIN_LEN=$(cat /tmp/tbff_chain.json | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
        check "Receipt chain exists" "$([ "$CHAIN_LEN" -ge 1 ] && echo true || echo false)"
    else
        skip "Receipt chain verification" "endpoint returned HTTP $CHAIN_HTTP (receipt chain API may not be mounted)"
    fi
else
    skip "Receipt chain verification" "no receipt_id from auto band test"
fi

# --------------------------------------------------------
# Summary
# --------------------------------------------------------
echo ""
echo "========================================"
TOTAL=$((PASS + FAIL + SKIP))
echo "TOTAL: $PASS passed, $FAIL failed, $SKIP skipped (of $TOTAL)"
echo "========================================"

[ "$FAIL" -eq 0 ] && exit 0 || exit 1
