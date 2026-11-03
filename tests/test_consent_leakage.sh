#!/usr/bin/env bash
# Consent Leakage Smoke Test — Data Classification Matrix 1C verification
#
# Creates a node_private Evidence entity, then verifies it does NOT appear
# on any of the 15 public endpoints that filter by node_private.
#
# Usage:
#   KOI_TEST_API_URL=http://127.0.0.1:<throwaway port> \
#   KOI_TEST_DATABASE_URL=postgresql://<role>@127.0.0.1:<port>/<db with test in its name> \
#       ./tests/test_consent_leakage.sh
#   The API must write to that database (tests/live_write_shell_guard.sh).

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

echo "================================================"
echo "Consent Leakage Smoke Test — node_private filter"
echo "API: $API"
echo "================================================"
echo ""

# ------------------------------------------------------------------ #
# Step 0: Health check
# ------------------------------------------------------------------ #
echo "--- Step 0: Health check ---"
HEALTH=$(curl -sf "$API/health" 2>/dev/null || echo '{}')
check "API health" "$(echo "$HEALTH" | python3 -c 'import sys,json; d=json.load(sys.stdin); print("true" if d.get("status") in ("ok","healthy") else "false")' 2>/dev/null || echo false)"
echo ""

# ------------------------------------------------------------------ #
# Step 1: Create a node_private Evidence entity
# ------------------------------------------------------------------ #
echo "--- Step 1: Create node_private Evidence entity ---"

TS="$KOI_TEST_RUN_ID"
PRIVATE_NAME="CONSENT-LEAKAGE-TEST-$TS"
PRIVATE_VAULT_RID="orn:obsidian.entity:Evidence/consent-leakage-test-$TS"
PRIVATE_VAULT_PATH="Evidence/consent-leakage-test-$TS.md"

REGISTER_RESP=$(curl -sf -X POST "$API/register-entity" \
    -H "Content-Type: application/json" \
    -d "{
        \"vault_rid\": \"$PRIVATE_VAULT_RID\",
        \"vault_path\": \"$PRIVATE_VAULT_PATH\",
        \"entity_type\": \"Evidence\",
        \"name\": \"$PRIVATE_NAME\",
        \"content_hash\": \"consent-leak-test-$TS\",
        \"visibility_scope\": \"node_private\",
        \"frontmatter\": {
            \"@type\": \"Evidence\",
            \"name\": \"$PRIVATE_NAME\",
            \"description\": \"This entity should NEVER appear on public endpoints\"
        }
    }" 2>/dev/null || echo '{"success":false}')

PRIVATE_URI=$(echo "$REGISTER_RESP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("canonical_uri",""))' 2>/dev/null || echo "")
live_write_record entity_uri "$PRIVATE_URI"
check "node_private Evidence registered" "$([ -n "$PRIVATE_URI" ] && echo true || echo false)"
echo "  URI: $PRIVATE_URI"

if [ -z "$PRIVATE_URI" ]; then
    red "Cannot proceed without entity. Response: $REGISTER_RESP"
    exit 1
fi
echo ""

# Helper: check if a JSON response contains the private entity name or URI.
# A python crash reads as "ERROR" (a broken check), never as a silent "false"
# (no-leak) — the two must not be indistinguishable to the caller.
contains_private() {
    local resp="$1"
    echo "$resp" | python3 -c "
import sys
data = sys.stdin.read()
name = '$PRIVATE_NAME'
uri = '$PRIVATE_URI'
found = name in data or uri in data
print('true' if found else 'false')
" 2>/dev/null || echo "ERROR"
}

# Fetch a URL (any curl args) and assert BOTH that it returns 2xx AND that the
# body doesn't leak the private entity. A transport failure or non-2xx status
# is a hard [FAIL] here, never a silent substitution into an empty '{}'/'[]'
# that would read as "no leak found". This is the structural fix for the
# consent-leakage gate previously failing OPEN on any of these 10 checks.
check_no_leak() {
    local desc="$1"; shift
    local tmp code body leaked
    tmp=$(mktemp)
    code=$(curl -s -o "$tmp" -w '%{http_code}' "$@" 2>/dev/null || echo "000")
    body=$(cat "$tmp" 2>/dev/null || echo "")
    rm -f "$tmp"
    # 501 is a genuine, intentional "capability not enabled in this deployment"
    # response (e.g. LLM enrichment off locally) — it means the check couldn't
    # exercise anything, so it's neither a real [PASS] nor a [FAIL]. Counting
    # it as PASS would recreate the exact masking bug this rewrite removes;
    # counting it as FAIL would make the suite permanently red on any
    # deployment profile that disables the capability.
    if [ "$code" = "501" ]; then
        yellow "  [SKIP] $desc (HTTP 501 — capability not enabled in this deployment)"
        SKIP=$((SKIP + 1))
        return
    fi
    if [[ "$code" != 2* ]]; then
        check "$desc" "false"
        red "    (request failed: HTTP $code)"
        return
    fi
    leaked=$(contains_private "$body")
    if [ "$leaked" = "ERROR" ]; then
        check "$desc" "false"
        red "    (contains_private check crashed on the response body)"
        return
    fi
    check "$desc" "$([ "$leaked" = "false" ] && echo true || echo false)"
}

# ------------------------------------------------------------------ #
# Step 2: Verify entity is invisible on all public endpoints
# ------------------------------------------------------------------ #
echo "--- Step 2: Verify invisibility on public endpoints ---"

# 2a. /entity-search
check_no_leak "/entity-search: private entity NOT found" \
    "$API/entity-search?query=CONSENT-LEAKAGE-TEST&limit=50"

# 2b. /entity/{uri} — should return 404 or empty for private entity
ENTITY_RESP=$(curl -s -o /dev/null -w '%{http_code}' "$API/entity/$PRIVATE_URI" 2>/dev/null || echo "000")
check "/entity/{uri}: returns 404 for private entity" "$([ "$ENTITY_RESP" = "404" ] && echo true || echo false)"

# 2c. /entities (list)
check_no_leak "/entities: private entity NOT in list" \
    "$API/entities?entity_type=Evidence&limit=500"

# 2d. /entity/{uri}/mentioned-in
MENTION_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$API/entity/$PRIVATE_URI/mentioned-in" 2>/dev/null || echo "000")
check "/entity/{uri}/mentioned-in: rejects or 404" "$([ "$MENTION_CODE" = "404" ] || [ "$MENTION_CODE" = "403" ] && echo true || echo false)"

# 2e. /entities/mentioned-in (batch) — request field is `uris`, not
# `entity_uris` (BatchMentionedInRequest); the old name 422'd and was masked
# by the same silent-substitution bug fixed above.
check_no_leak "/entities/mentioned-in: private entity filtered out" \
    -X POST "$API/entities/mentioned-in" \
    -H "Content-Type: application/json" \
    -d "{\"uris\": [\"$PRIVATE_URI\"]}"

# 2f. /stats
check_no_leak "/stats: private entity NOT in response" \
    "$API/stats"

# 2g. /entity/{uri}/evidence
EV_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$API/entity/$PRIVATE_URI/evidence" 2>/dev/null || echo "000")
check "/entity/{uri}/evidence: rejects or 404" "$([ "$EV_CODE" = "404" ] || [ "$EV_CODE" = "403" ] && echo true || echo false)"

# 2h. /relationships/{uri}
REL_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$API/relationships/$PRIVATE_URI" 2>/dev/null || echo "000")
check "/relationships/{uri}: rejects or 404" "$([ "$REL_CODE" = "404" ] || [ "$REL_CODE" = "403" ] && echo true || echo false)"

# 2i. /vault-entities
check_no_leak "/vault-entities: private entity NOT in list" \
    "$API/vault-entities?entity_type=Evidence"

# 2j. /vault-entity/{vault_rid}
VAULT_SINGLE_CODE=$(curl -s -o /dev/null -w '%{http_code}' "$API/vault-entity/$PRIVATE_VAULT_RID" 2>/dev/null || echo "000")
check "/vault-entity/{rid}: returns 404 for private" "$([ "$VAULT_SINGLE_CODE" = "404" ] && echo true || echo false)"

# 2k. /chat — search for the private entity name. ChatRequest's field is
# `query`; there is no `session_id` field at all. The old body 422'd and was
# masked by the same silent-substitution bug fixed above.
check_no_leak "/chat: private entity NOT in retrieval context" \
    -X POST "$API/chat" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"Tell me about $PRIVATE_NAME\"}"

# 2l. /entity/resolve (GET) — params are label/type_hint, not name/type; use
# the actual suffixed test name (matches what was registered), not the bare
# prefix. Both were bugs that made this check pass on a 422, not a real PASS.
check_no_leak "/entity/resolve (GET): private entity NOT resolved" \
    "$API/entity/resolve?label=$PRIVATE_NAME&type_hint=Evidence"

# 2m. /graph-version — entity count should NOT include private entity
check_no_leak "/graph-version: private entity NOT in response" \
    "$API/graph-version"

# 2n. /web/evaluate — entity context for LLM should exclude private
check_no_leak "/web/evaluate: private entity NOT in LLM context" \
    -X POST "$API/web/evaluate" \
    -H "Content-Type: application/json" \
    -d "{\"url\": \"https://example.com\", \"title\": \"Test\", \"content\": \"$PRIVATE_NAME\"}"

# 2o. /web/process — entity context for LLM should exclude private
check_no_leak "/web/process: private entity NOT in LLM context" \
    -X POST "$API/web/process" \
    -H "Content-Type: application/json" \
    -d "{\"url\": \"https://example.com\", \"title\": \"Test\", \"content\": \"$PRIVATE_NAME\", \"submission_id\": \"consent-test-$TS\"}"

echo ""

# ------------------------------------------------------------------ #
# Step 3: Verify entity IS visible via direct DB (sanity check)
# We can't query DB directly from bash, but we can verify
# the entity was actually created by checking the register response
# ------------------------------------------------------------------ #
echo "--- Step 3: Sanity check ---"
check "Entity was actually registered (URI exists)" "$([ -n "$PRIVATE_URI" ] && echo true || echo false)"
echo ""

# ------------------------------------------------------------------ #
# Summary
# ------------------------------------------------------------------ #
echo "================================================"
TOTAL=$((PASS + FAIL + SKIP))
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped ($TOTAL total)"
echo "Private entity URI: $PRIVATE_URI"
echo "================================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
