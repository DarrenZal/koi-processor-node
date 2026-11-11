#!/usr/bin/env bash
# lib.sh — Shared functions for KOI-net federation scripts
#
# Source this file: source "$(dirname "$0")/lib.sh"

set -euo pipefail

# ============================================
# CONSTANTS
# ============================================

PEER_REGISTRY="${PEER_REGISTRY:-$HOME/.config/personal-koi/peer-registry.json}"
KOI_STATE_DIR="${KOI_STATE_DIR:-$HOME/.config/personal-koi/koi-state}"
WG_STATE_DIR="${WG_STATE_DIR:-$HOME/.config/personal-koi/wireguard}"
KOI_PROCESSOR_DIR="${KOI_PROCESSOR_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

# ============================================
# LOGGING
# ============================================

log_info()  { echo "[INFO]  $*"; }
log_warn()  { echo "[WARN]  $*" >&2; }
log_error() { echo "[ERROR] $*" >&2; }
log_fatal() { echo "[FATAL] $*" >&2; exit 1; }

# ============================================
# DRY-RUN SUPPORT
# ============================================

DRY_RUN=false

parse_dry_run() {
    if [[ "${1:-}" == "--dry-run" ]]; then
        DRY_RUN=true
        shift
    fi
    echo "$@"
}

run_cmd() {
    # Execute a command, or print it if --dry-run
    if $DRY_RUN; then
        echo "[DRY-RUN] $*"
    else
        "$@"
    fi
}

run_ssh() {
    # Execute a command via SSH, or print it if --dry-run
    local target="$1"
    shift
    if $DRY_RUN; then
        echo "[DRY-RUN] ssh $target: $*"
    else
        ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new "$target" "$@"
    fi
}

# ============================================
# INPUT VALIDATION
# ============================================

validate_peer_name() {
    local name="$1"
    if [[ ! "$name" =~ ^[a-z][a-z0-9-]{1,30}$ ]]; then
        log_fatal "Invalid peer name '$name': must be lowercase alphanumeric + hyphens, 2-31 chars, start with letter"
    fi
}

validate_peer_number() {
    local num="$1"
    if ! [[ "$num" =~ ^[0-9]+$ ]] || (( num < 2 || num > 254 )); then
        log_fatal "Invalid peer number '$num': must be 2-254"
    fi
}

is_wg_pubkey() {
    # A WireGuard public key is 32 bytes in canonical base64: 43 characters and one '='.
    # The 43rd character carries 2 padding bits, which must be zero. The key is written into
    # WireGuard config that runs as root, so nothing else passes (no space, newline or quote).
    [[ "$1" =~ ^[A-Za-z0-9+/]{42}[AEIMQUYcgkosw048]=$ ]]
}

validate_wg_pubkey() {
    if ! is_wg_pubkey "$1"; then
        log_fatal "Invalid WireGuard public key: must be 32 bytes as 44-char base64"
    fi
}

is_node_rid() {
    # A node RID as KOI-net writes it: orn:koi-net.node:<name>+<16 to 64 hex>.
    [[ "$1" =~ ^orn:koi-net\.node:[A-Za-z0-9._-]+\+[0-9a-fA-F]{16,64}$ ]]
}

validate_join_identity() {
    # Check a join request's node RID and KOI public key before anything stores them. The key
    # must be canonical base64 of a P-256 public key (DER SubjectPublicKeyInfo, 91 bytes), and
    # the RID's hash must be derived from that key in one of the forms api/node_identity.py
    # accepts (b64_64, legacy16, der64).
    # Args: <node-rid> <koi-public-key>
    if ! is_node_rid "$1"; then
        log_fatal "Refusing: the join request's node_rid is not a well-formed node RID"
    fi
    local reason
    if ! reason=$(LIB_NODE_RID="$1" LIB_KOI_PUBKEY="$2" python3 -c "
import base64, binascii, hashlib, os, sys
P256_SPKI_PREFIX = bytes.fromhex('3059301306072a8648ce3d020106082a8648ce3d030107034200')
rid, key = os.environ['LIB_NODE_RID'], os.environ['LIB_KOI_PUBKEY']
try:
    der = base64.b64decode(key, validate=True)
except (binascii.Error, ValueError):
    der = b''
if len(der) != 91 or not der.startswith(P256_SPKI_PREFIX) or der[26] != 4 or base64.b64encode(der).decode() != key:
    print('the koi_public_key is not a base64 P-256 public key (DER SubjectPublicKeyInfo)')
    sys.exit(1)
suffix = rid.rsplit('+', 1)[1]
b64_64 = hashlib.sha256(key.encode()).hexdigest()
if suffix not in (b64_64, b64_64[:16], hashlib.sha256(der).hexdigest()):
    print('the node_rid is not derived from the koi_public_key')
    sys.exit(1)
"); then
        log_fatal "Refusing: ${reason:-the node_rid and koi_public_key of the join request could not be checked}"
    fi
}

validate_ssh_target() {
    local target="$1"
    if [[ ! "$target" =~ ^[a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+$ ]]; then
        log_fatal "Invalid SSH target '$target': expected user@host format"
    fi
}

# ============================================
# PEER REGISTRY (atomic read/write)
# ============================================

peer_registry_init() {
    local dir
    dir="$(dirname "$PEER_REGISTRY")"
    mkdir -p "$dir"
    if [[ ! -f "$PEER_REGISTRY" ]]; then
        echo '[]' > "$PEER_REGISTRY"
    fi
}

peer_registry_read() {
    # Output the full peer registry JSON
    peer_registry_init
    cat "$PEER_REGISTRY"
}

peer_registry_add() {
    # Atomic add: flock + write to temp + mv
    local entry_json="$1"
    peer_registry_init

    local lockfile="${PEER_REGISTRY}.lock"
    local tmpfile="${PEER_REGISTRY}.tmp.$$"

    if command -v flock >/dev/null 2>&1; then
        (
            flock -w 5 200 || log_fatal "Failed to acquire peer registry lock"

            # Check for duplicate peer_name
            local peer_name
            peer_name=$(echo "$entry_json" | python3 -c "import sys,json; print(json.load(sys.stdin)['peer_name'])")
            local existing
            # The entry can carry a joining peer's own values (approve-peer.sh --from-file), so
            # every value reaches Python through the environment, never pasted into its source.
            existing=$(LIB_PEER_REGISTRY="$PEER_REGISTRY" LIB_PEER_NAME="$peer_name" python3 -c "
import json, os, sys
reg = json.load(open(os.environ['LIB_PEER_REGISTRY']))
for p in reg:
    if p['peer_name'] == os.environ['LIB_PEER_NAME'] and p.get('status') == 'active':
        print('exists')
        sys.exit(0)
print('ok')
")
            if [[ "$existing" == "exists" ]]; then
                log_fatal "Peer '$peer_name' already exists in registry with status=active"
            fi

            # Append entry
            LIB_PEER_REGISTRY="$PEER_REGISTRY" LIB_ENTRY_JSON="$entry_json" LIB_TMPFILE="$tmpfile" python3 -c "
import json, os
reg = json.load(open(os.environ['LIB_PEER_REGISTRY']))
entry = json.loads(os.environ['LIB_ENTRY_JSON'])
reg.append(entry)
with open(os.environ['LIB_TMPFILE'], 'w') as f:
    json.dump(reg, f, indent=2)
"
            mv "$tmpfile" "$PEER_REGISTRY"

        ) 200>"$lockfile"
    else
        # Portable fallback (macOS): use Python fcntl lock.
        ENTRY_JSON="$entry_json" PEER_REGISTRY_PATH="$PEER_REGISTRY" LOCKFILE_PATH="$lockfile" python3 - <<'PY'
import fcntl, json, os, sys

entry = json.loads(os.environ["ENTRY_JSON"])
registry = os.environ["PEER_REGISTRY_PATH"]
lockfile = os.environ["LOCKFILE_PATH"]

os.makedirs(os.path.dirname(registry), exist_ok=True)
if not os.path.exists(registry):
    with open(registry, "w", encoding="utf-8") as f:
        f.write("[]")

with open(lockfile, "w", encoding="utf-8") as lf:
    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
    with open(registry, "r", encoding="utf-8") as rf:
        reg = json.load(rf)
    for p in reg:
        if p.get("peer_name") == entry.get("peer_name") and p.get("status") == "active":
            print(f"Peer '{entry.get('peer_name')}' already exists in registry with status=active", file=sys.stderr)
            sys.exit(1)
    reg.append(entry)
    tmp = f"{registry}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as wf:
        json.dump(reg, wf, indent=2)
    os.replace(tmp, registry)
PY
    fi
}

peer_registry_update_status() {
    # Atomic status update by peer_name (updates LAST live-status entry, not first)
    local peer_name="$1"
    local new_status="$2"
    local extra_field="${3:-}"  # e.g. "removed_at" for timestamp
    local extra_value="${4:-}"

    peer_registry_init
    local lockfile="${PEER_REGISTRY}.lock"
    local tmpfile="${PEER_REGISTRY}.tmp.$$"

    if command -v flock >/dev/null 2>&1; then
        (
            flock -w 5 200 || log_fatal "Failed to acquire peer registry lock"

            # remove-peer.sh can pass a remote node's own node_name, so every value reaches Python
            # through the environment, never pasted into its source.
            PEER_NAME="$peer_name" NEW_STATUS="$new_status" EXTRA_FIELD="$extra_field" EXTRA_VALUE="$extra_value" \
            PEER_REGISTRY_PATH="$PEER_REGISTRY" LIB_TMPFILE="$tmpfile" python3 -c "
import json, os, sys
peer_name = os.environ['PEER_NAME']
extra_field = os.environ['EXTRA_FIELD']
extra_value = os.environ['EXTRA_VALUE']
LIVE = {'invited', 'approving', 'active'}
reg = json.load(open(os.environ['PEER_REGISTRY_PATH']))
# Find last matching live entry (most recent append)
target_idx = None
for i, p in enumerate(reg):
    if p['peer_name'] == peer_name and p.get('status', '') in LIVE:
        target_idx = i
if target_idx is None:
    print(f'Peer not found (live): {peer_name}', file=sys.stderr)
    sys.exit(1)
reg[target_idx]['status'] = os.environ['NEW_STATUS']
if extra_field and extra_value:
    reg[target_idx][extra_field] = extra_value
with open(os.environ['LIB_TMPFILE'], 'w') as f:
    json.dump(reg, f, indent=2)
"
            mv "$tmpfile" "$PEER_REGISTRY"

        ) 200>"$lockfile"
    else
        PEER_NAME="$peer_name" NEW_STATUS="$new_status" EXTRA_FIELD="$extra_field" EXTRA_VALUE="$extra_value" \
        PEER_REGISTRY_PATH="$PEER_REGISTRY" LOCKFILE_PATH="$lockfile" python3 - <<'PY'
import fcntl, json, os, sys

peer_name = os.environ["PEER_NAME"]
new_status = os.environ["NEW_STATUS"]
extra_field = os.environ["EXTRA_FIELD"]
extra_value = os.environ["EXTRA_VALUE"]
registry = os.environ["PEER_REGISTRY_PATH"]
lockfile = os.environ["LOCKFILE_PATH"]
LIVE = {"invited", "approving", "active"}

os.makedirs(os.path.dirname(registry), exist_ok=True)
if not os.path.exists(registry):
    with open(registry, "w", encoding="utf-8") as f:
        f.write("[]")

with open(lockfile, "w", encoding="utf-8") as lf:
    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
    with open(registry, "r", encoding="utf-8") as rf:
        reg = json.load(rf)
    # Find last matching live entry (most recent append)
    target_idx = None
    for i, p in enumerate(reg):
        if p.get("peer_name") == peer_name and p.get("status", "") in LIVE:
            target_idx = i
    if target_idx is None:
        print(f"Peer not found (live): {peer_name}", file=sys.stderr)
        sys.exit(1)
    reg[target_idx]["status"] = new_status
    if extra_field and extra_value:
        reg[target_idx][extra_field] = extra_value
    tmp = f"{registry}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as wf:
        json.dump(reg, wf, indent=2)
    os.replace(tmp, registry)
PY
    fi
}

peer_registry_lookup() {
    # Look up a peer by name or node_rid, output JSON entry
    local query="$1"
    peer_registry_init
    # The query can come from a remote node's /health (connect-peers.sh passes its node_name),
    # so it reaches Python through the environment, never pasted into its source.
    LIB_PEER_REGISTRY="$PEER_REGISTRY" LIB_QUERY="$query" python3 -c "
import json, os, sys
query = os.environ['LIB_QUERY']
reg = json.load(open(os.environ['LIB_PEER_REGISTRY']))
for p in reg:
    if p['peer_name'] == query or p.get('node_rid') == query:
        json.dump(p, sys.stdout, indent=2)
        sys.exit(0)
print('', file=sys.stderr)
sys.exit(1)
" 2>/dev/null
}

# ============================================
# SSH HELPERS
# ============================================

ssh_file_exists() {
    local target="$1" path="$2"
    ssh -o ConnectTimeout=10 "$target" "test -f '$path'" 2>/dev/null
}

ssh_cmd_exists() {
    local target="$1" cmd="$2"
    ssh -o ConnectTimeout=10 "$target" "command -v '$cmd'" &>/dev/null
}

# ============================================
# TRANSACTIONAL RELAY CONFIG EDIT
# ============================================

relay_config_edit() {
    # Transactional edit of WireGuard config on relay VPS
    # Args: <ssh-target> <action> [action-args...]
    # Actions: add-peer <pubkey> <ip>, remove-peer <pubkey>
    # The script runs as root on the relay. The key and IP are checked here, whatever the
    # caller checked, and reach the script as quoted assignments that it reads as data: they
    # are never spliced into its source.
    local ssh_target="$1"
    local action="$2"
    shift 2

    local pubkey="${1:-}" peer_ip=""
    if ! is_wg_pubkey "$pubkey"; then
        log_error "Relay config edit refused: not a well-formed WireGuard public key"
        return 1
    fi

    local remote_script
    case "$action" in
        add-peer)
            peer_ip="${2:-}"
            if [[ ! "$peer_ip" =~ ^10\.100\.0\.([0-9]{1,3})$ ]] || (( BASH_REMATCH[1] < 2 || BASH_REMATCH[1] > 254 )); then
                log_error "Relay config edit refused: invalid peer IP '$peer_ip' (expected 10.100.0.2-254)"
                return 1
            fi
            remote_script="$(cat <<'REMOTE_EOF'
set -euo pipefail
# The lock lives in /run, which only root can write, and is never followed as a symlink.
LOCKFILE="/run/wg-koi.lock"
CONF="/etc/wireguard/wg-koi.conf"
if [[ -L "$LOCKFILE" ]]; then
    echo "FAIL: $LOCKFILE is a symlink"
    exit 1
fi

(
    flock -w 10 200 || { echo "FAIL: Could not acquire lock"; exit 1; }

    # Backup
    BACKUP="${CONF}.bak.$(date +%s)"
    cp "$CONF" "$BACKUP"

    # Check for duplicate
    if grep -qF -- "$PUBKEY" "$CONF"; then
        echo "FAIL: Peer with this public key already exists"
        exit 1
    fi

    # Append peer to temp copy
    TMPCONF="${CONF}.tmp.$$"
    cp "$CONF" "$TMPCONF"
    cat >> "$TMPCONF" <<PEER

[Peer]
PublicKey = $PUBKEY
AllowedIPs = $PEER_IP/32
PEER

    # Syntax check
    if ! wg-quick strip wg-koi < "$TMPCONF" > /dev/null 2>&1; then
        echo "FAIL: Syntax check failed, restoring backup"
        rm -f "$TMPCONF"
        exit 1
    fi

    # Atomic replace
    mv "$TMPCONF" "$CONF"

    # Live reload
    if wg show wg-koi > /dev/null 2>&1; then
        wg syncconf wg-koi <(wg-quick strip wg-koi)
    fi

    echo "OK: Peer added successfully"

) 200>"$LOCKFILE"
REMOTE_EOF
)"
            ;;

        remove-peer)
            remote_script="$(cat <<'REMOTE_EOF'
set -euo pipefail
# The lock lives in /run, which only root can write, and is never followed as a symlink.
LOCKFILE="/run/wg-koi.lock"
CONF="/etc/wireguard/wg-koi.conf"
if [[ -L "$LOCKFILE" ]]; then
    echo "FAIL: $LOCKFILE is a symlink"
    exit 1
fi

(
    flock -w 10 200 || { echo "FAIL: Could not acquire lock"; exit 1; }

    # Backup
    BACKUP="${CONF}.bak.$(date +%s)"
    cp "$CONF" "$BACKUP"

    # Remove peer block matching pubkey
    TMPCONF="${CONF}.tmp.$$"
    WG_PUBKEY="$PUBKEY" WG_CONF="$CONF" WG_TMPCONF="$TMPCONF" python3 -c "
import os, re, sys
conf = open(os.environ['WG_CONF']).read()
# Split into blocks, remove the one with matching pubkey
blocks = re.split(r'(?=\[Peer\])', conf)
filtered = []
removed = False
for b in blocks:
    if os.environ['WG_PUBKEY'] in b:
        removed = True
    else:
        filtered.append(b)
if not removed:
    print('FAIL: Peer not found in config', file=sys.stderr)
    sys.exit(1)
with open(os.environ['WG_TMPCONF'], 'w') as f:
    f.write(''.join(filtered).rstrip() + '\n')
"

    # Syntax check
    if ! wg-quick strip wg-koi < "$TMPCONF" > /dev/null 2>&1; then
        echo "FAIL: Syntax check failed, restoring from $BACKUP"
        cp "$BACKUP" "$CONF"
        rm -f "$TMPCONF"
        exit 1
    fi

    # Atomic replace
    mv "$TMPCONF" "$CONF"

    # Live reload
    if wg show wg-koi > /dev/null 2>&1; then
        wg syncconf wg-koi <(wg-quick strip wg-koi)
    fi

    echo "OK: Peer removed successfully"

) 200>"$LOCKFILE"
REMOTE_EOF
)"
            ;;

        *)
            log_fatal "Unknown relay config action: $action"
            ;;
    esac

    # The values go first, as assignments quoted by printf %q (bash on both ends).
    remote_script="$(printf 'PUBKEY=%q\nPEER_IP=%q\n' "$pubkey" "$peer_ip")"$'\n'"$remote_script"

    if $DRY_RUN; then
        echo "[DRY-RUN] Would execute transactional config edit on $ssh_target:"
        echo "[DRY-RUN]   Action: $action"
        return 0
    fi

    local result
    result=$(ssh -o ConnectTimeout=10 "$ssh_target" "sudo bash -s" <<< "$remote_script" 2>&1) || true

    if [[ "$result" == OK:* ]]; then
        log_info "$result"
        return 0
    else
        log_error "Relay config edit failed: $result"
        return 1
    fi
}

# ============================================
# FINGERPRINT COMPUTATION
# ============================================

compute_koi_fingerprint() {
    # Compute SHA256 fingerprint of the actual DER public key bytes
    # Input: base64-encoded DER public key string
    # Output: SHA256:<base64-hash>
    local der_b64="$1"
    local hash
    # Decode base64 to get raw DER bytes, then SHA256 those bytes
    hash=$(echo -n "$der_b64" | base64 -d | openssl dgst -sha256 -binary | base64 | tr -d '=')
    echo "SHA256:${hash}"
}

koi_node_identity() {
    # Load or create this node's KOI identity with koi-processor's node_identity module.
    # Args: <koi-processor-dir> <koi-state-dir> <node-name>
    # Output: node_rid=, koi_public_key= and fingerprint= lines (the fingerprint is SHA256 of
    # the DER bytes, as compute_koi_fingerprint computes it).
    # The paths and name reach Python through the environment, never pasted into its source.
    LIB_KOI_PATH="$1" KOI_STATE_DIR="$2" LIB_NODE_NAME="$3" python3 -c "
import os, sys
from base64 import b64decode, b64encode
import hashlib
sys.path.insert(0, os.environ['LIB_KOI_PATH'])
from api.node_identity import load_or_create_identity, get_public_key_der_b64

# load_or_create_identity may return (private_key, profile) or
# (private_key, profile, encryption_private_key) depending on version.
identity = load_or_create_identity(os.environ['LIB_NODE_NAME'])
if not isinstance(identity, tuple) or len(identity) < 2:
    raise RuntimeError(f'Unexpected identity return shape: {type(identity)}')
private_key, profile = identity[0], identity[1]
koi_pubkey = get_public_key_der_b64(private_key)
fp_hash = hashlib.sha256(b64decode(koi_pubkey)).digest()

print(f'node_rid={profile.node_rid}')
print(f'koi_public_key={koi_pubkey}')
print('fingerprint=SHA256:' + b64encode(fp_hash).decode().rstrip('='))
"
}

# ============================================
# MIGRATION RUNNER
# ============================================

run_migrations() {
    # Run pending SQL migrations with tracking
    # Args: <postgres-url> <migrations-dir> [manifest-file]
    local pg_url="$1"
    local mig_dir="$2"
    local manifest_path="${3:-}"

    if $DRY_RUN; then
        if [[ -n "$manifest_path" ]]; then
            echo "[DRY-RUN] Would run migrations from manifest $manifest_path (dir=$mig_dir)"
        else
            echo "[DRY-RUN] Would run migrations from $mig_dir"
        fi
        return 0
    fi

    if [[ -n "$manifest_path" && ! -f "$manifest_path" ]]; then
        log_fatal "Migration manifest not found: $manifest_path"
    fi

    ensure_python_module "psycopg2" "psycopg2-binary"

    # The URL and paths reach Python through the environment, never pasted into its source:
    # a quote in a password or path would otherwise end the string literal.
    LIB_PG_URL="$pg_url" LIB_MIG_DIR="$mig_dir" LIB_MANIFEST_PATH="$manifest_path" python3 -c "
import os, sys, glob, re, psycopg2

pg_url = os.environ['LIB_PG_URL']
mig_dir = os.environ['LIB_MIG_DIR']
manifest_path = os.environ['LIB_MANIFEST_PATH']

conn = psycopg2.connect(pg_url)
conn.autocommit = True
cur = conn.cursor()

# Create tracking table if not exists
# Refuse databases this runner must not migrate, before writing anything
# (docs/operations/fresh-install-migrations.md, Existing databases).
cur.execute(\"SELECT to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL, to_regclass('public.koi_schema_migrations') IS NOT NULL, to_regclass('public.koi_migrations') IS NOT NULL, to_regclass('public.koi_net_events') IS NOT NULL\")
has_marker, has_peer_ledger, has_koi_migrations, has_koi_net_events = cur.fetchone()
if has_marker:
    print('[ERROR] A fresh install of this database did not finish (koi_fresh_install_in_progress exists). Drop and recreate the database.', file=sys.stderr)
    sys.exit(1)
if not has_peer_ledger and (has_koi_migrations or has_koi_net_events) and os.environ.get('KOI_ALLOW_LEGACY_RUNNER') != '1':
    print('[ERROR] This database already holds a KOI schema but has no koi_schema_migrations, so this script did not build it. Refusing to migrate it. See docs/operations/fresh-install-migrations.md (Existing databases); override: KOI_ALLOW_LEGACY_RUNNER=1.', file=sys.stderr)
    sys.exit(1)

cur.execute('''
    CREATE TABLE IF NOT EXISTS koi_schema_migrations (
        filename TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ DEFAULT NOW()
    )
''')

# Migration files discovered on disk
all_mig_files = sorted(glob.glob(os.path.join(mig_dir, '*.sql')))
if not all_mig_files:
    print(f'[ERROR] No migration files found in {mig_dir}', file=sys.stderr)
    sys.exit(1)

mig_files = []

if manifest_path:
    # Manifest mode: explicit ordered list of migrations to run for federation setup.
    manifest_entries = []
    with open(manifest_path, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            manifest_entries.append(line)

    if not manifest_entries:
        print(f'[ERROR] Migration manifest is empty: {manifest_path}', file=sys.stderr)
        sys.exit(1)

    by_name = {os.path.basename(p): p for p in all_mig_files}
    missing = []
    resolved = []
    for entry in manifest_entries:
        path = by_name.get(entry)
        if path is None:
            alt = os.path.join(mig_dir, entry)
            if os.path.isfile(alt):
                path = alt
        if path is None:
            missing.append(entry)
        else:
            resolved.append(path)

    if missing:
        print(f'[ERROR] Manifest references missing migration files: {missing}', file=sys.stderr)
        sys.exit(1)

    mig_files = resolved
    # *_fresh_install.sql files use psql meta-commands and only act on a new personal_koi
    # database built by scripts/run_migrations.sh; psycopg2 cannot run them. Skip them here.
    runner_only = [mf for mf in mig_files if os.path.basename(mf).endswith('_fresh_install.sql')]
    if runner_only:
        print(f'[INFO] Skipping {len(runner_only)} fresh-install migration(s) (scripts/run_migrations.sh only)')
    mig_files = [mf for mf in mig_files if not os.path.basename(mf).endswith('_fresh_install.sql')]
    print(f'[INFO] Federation migration manifest mode: {len(mig_files)} file(s)')
else:
    # Legacy numeric-baseline mode retained for backward compatibility.
    mig_files = all_mig_files
    # *_fresh_install.sql files use psql meta-commands and only act on a new personal_koi
    # database built by scripts/run_migrations.sh; psycopg2 cannot run them. Skip them here.
    runner_only = [mf for mf in mig_files if os.path.basename(mf).endswith('_fresh_install.sql')]
    if runner_only:
        print(f'[INFO] Skipping {len(runner_only)} fresh-install migration(s) (scripts/run_migrations.sh only)')
    mig_files = [mf for mf in mig_files if not os.path.basename(mf).endswith('_fresh_install.sql')]
    # Drafts and down/rollback files are not forward migrations; never apply them.
    not_forward = [mf for mf in mig_files
                   if os.path.basename(mf).startswith('DRAFT_')
                   or os.path.basename(mf).lower().endswith(('_down.sql', '.down.sql', '.rollback.sql'))]
    if not_forward:
        print(f'[INFO] Skipping {len(not_forward)} draft or rollback file(s)')
    mig_files = [mf for mf in mig_files if mf not in not_forward]

    # Baseline detection: if table was just created but schema tables exist
    cur.execute(\"SELECT COUNT(*) FROM koi_schema_migrations\")
    count = cur.fetchone()[0]

    # Optional minimum migration number for federation-only bootstrap.
    # If set (e.g., KOI_MIGRATION_MIN_NUM=39), older migrations are marked baseline.
    min_num_raw = os.environ.get('KOI_MIGRATION_MIN_NUM', '').strip()
    min_num = 0
    if min_num_raw:
        try:
            min_num = int(min_num_raw)
        except ValueError:
            print(f'[WARN] Ignoring invalid KOI_MIGRATION_MIN_NUM={min_num_raw!r}')
            min_num = 0

    if min_num > 0:
        non_numeric = [mf for mf in mig_files if not re.match(r'^\\d+', os.path.basename(mf))]
        if non_numeric:
            print(f'[INFO] Federation bootstrap mode: skipping {len(non_numeric)} non-numbered migrations')
        mig_files = [mf for mf in mig_files if re.match(r'^\\d+', os.path.basename(mf))]

        baseline_count = 0
        for mf in mig_files:
            fname = os.path.basename(mf)
            m = re.match(r'(\\d+)', fname)
            if m and int(m.group(1)) < min_num:
                cur.execute(
                    \"INSERT INTO koi_schema_migrations (filename, applied_at) VALUES (%s, 'epoch') ON CONFLICT DO NOTHING\",
                    (fname,)
                )
                baseline_count += 1
        print(f'[INFO] Federation bootstrap baseline enabled: ensured {baseline_count} migrations with number < {min_num} are marked as baseline')
        cur.execute(\"SELECT COUNT(*) FROM koi_schema_migrations\")
        count = cur.fetchone()[0]

    if count == 0:
        # Check if this is a pre-tracking database
        cur.execute(\"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='koi_net_events')\")
        if cur.fetchone()[0]:
            print('[INFO] Pre-tracking database detected, recording baseline...')
            # Only mark migrations as baseline if their target table already exists
            # This prevents marking NEW migrations (like 045) as already applied
            # First pass: find highest migration number whose CREATE TABLEs all exist
            highest_existing_num = 0
            for mf in mig_files:
                fname = os.path.basename(mf)
                sql_content = open(mf).read()
                tables = re.findall(r'CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(\w+)', sql_content, re.IGNORECASE)
                if tables:
                    all_exist = True
                    for tbl in tables:
                        cur.execute(\"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s)\", (tbl,))
                        if not cur.fetchone()[0]:
                            all_exist = False
                            break
                    if all_exist:
                        try:
                            num = int(re.match(r'(\d+)', fname).group(1))
                            highest_existing_num = max(highest_existing_num, num)
                        except (AttributeError, ValueError):
                            pass
            print(f'[INFO] Baseline cutoff: migration number {highest_existing_num}')

            # Second pass: mark baselines
            baseline_count = 0
            for mf in mig_files:
                fname = os.path.basename(mf)
                # Read the SQL to check if its CREATE TABLE target already exists
                sql_content = open(mf).read()
                tables = re.findall(r'CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(\w+)', sql_content, re.IGNORECASE)
                if tables:
                    # Check if ALL tables in this migration already exist
                    all_exist = True
                    for tbl in tables:
                        cur.execute(\"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s)\", (tbl,))
                        if not cur.fetchone()[0]:
                            all_exist = False
                            break
                    if all_exist:
                        cur.execute(
                            \"INSERT INTO koi_schema_migrations (filename, applied_at) VALUES (%s, 'epoch') ON CONFLICT DO NOTHING\",
                            (fname,)
                        )
                        baseline_count += 1
                    else:
                        print(f'[INFO] Migration {fname} has new tables, will be applied')
                else:
                    # Non-CREATE-TABLE migrations (ALTER, INDEX, etc.)
                    # Only mark as baseline if their number is <= the highest CREATE-TABLE
                    # migration whose tables already exist. This avoids skipping new ALTERs.
                    try:
                        file_num = int(re.match(r'(\d+)', fname).group(1))
                    except (AttributeError, ValueError):
                        file_num = 999999
                    if file_num <= highest_existing_num:
                        cur.execute(
                            \"INSERT INTO koi_schema_migrations (filename, applied_at) VALUES (%s, 'epoch') ON CONFLICT DO NOTHING\",
                            (fname,)
                        )
                        baseline_count += 1
                    else:
                        print(f'[INFO] Migration {fname} (non-CREATE, num={file_num}) is newer than baseline cutoff ({highest_existing_num}), will be applied')
            print(f'[INFO] Marked {baseline_count} existing migrations as baseline')

# Get already-applied migrations
cur.execute('SELECT filename FROM koi_schema_migrations')
applied = {row[0] for row in cur.fetchall()}

# Find and run pending migrations
pending = [f for f in mig_files if os.path.basename(f) not in applied]

if not pending:
    print('[INFO] All migrations up to date')
else:
    for mf in pending:
        fname = os.path.basename(mf)
        print(f'[INFO] Applying migration: {fname}')
        try:
            sql = open(mf).read()
            cur.execute(sql)
            cur.execute(
                'INSERT INTO koi_schema_migrations (filename) VALUES (%s)',
                (fname,)
            )
            print(f'[INFO] Applied: {fname}')
        except Exception as e:
            print(f'[ERROR] Migration {fname} failed: {e}', file=sys.stderr)
            sys.exit(1)

conn.close()
print('[INFO] Migration check complete')
"
}

# ============================================
# JOIN REQUEST PARSING
# ============================================

parse_join_request() {
    # Parse a join request block, output key=value pairs
    # Args: <file-or-stdin>
    local input="${1:--}"
    # The path reaches Python through the environment, never pasted into its source.
    LIB_INPUT="$input" python3 -c "
import os, sys, re
from datetime import datetime, timezone

source = os.environ['LIB_INPUT']
text = open(source).read() if source != '-' else sys.stdin.read()

# Extract fields between markers
match = re.search(r'=== KOI-net Join Request ===(.*?)==========================', text, re.DOTALL)
if not match:
    print('FAIL: No join request block found', file=sys.stderr)
    sys.exit(1)

fields = {}
for line in match.group(1).strip().split('\n'):
    if ':' in line:
        key, val = line.split(':', 1)
        fields[key.strip()] = val.strip()

# Check expiry
expires = fields.get('expires', '')
if expires:
    try:
        exp_dt = datetime.fromisoformat(expires.replace('Z', '+00:00'))
        if exp_dt < datetime.now(timezone.utc):
            print('FAIL: Join request has expired', file=sys.stderr)
            sys.exit(1)
    except ValueError:
        print(f'WARN: Could not parse expires: {expires}', file=sys.stderr)

for k, v in fields.items():
    print(f'{k}={v}')
"
}

# ============================================
# PREREQUISITE CHECKS
# ============================================

require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" &>/dev/null; then
        log_fatal "Required command not found: $cmd"
    fi
}

require_python_3_11() {
    require_cmd python3
    local version
    version="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
    local major="${version%%.*}"
    local minor="${version##*.}"
    if (( major < 3 || (major == 3 && minor < 11) )); then
        log_fatal "Python 3.11+ required for federation scripts (found ${version}). Upgrade python3 and retry."
    fi
}

ensure_python_module() {
    # Ensure a Python module is importable by python3.
    # Attempts installation if missing.
    # Args: <module-name> [pip-package-name]
    local module="$1"
    local pip_package="${2:-$module}"

    # The name reaches Python as an argument, never pasted into its source, and must be a
    # dotted module name.
    if [[ ! "$module" =~ ^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$ ]]; then
        log_fatal "Invalid Python module name '$module'"
    fi
    if _python_module_importable "$module"; then
        return 0
    fi

    log_warn "Python module '$module' not found; attempting install of '$pip_package'..."

    # Try pip associated with python3 first (best chance to match runtime interpreter).
    if python3 -m pip --version >/dev/null 2>&1; then
        if python3 -m pip install -q "$pip_package" >/dev/null 2>&1; then
            :
        elif python3 -m pip install -q --user "$pip_package" >/dev/null 2>&1; then
            :
        fi
    fi

    # Fallback: project venv pip if available.
    if ! _python_module_importable "$module"; then
        if [[ -f "$KOI_PROCESSOR_DIR/venv/bin/pip" ]]; then
            "$KOI_PROCESSOR_DIR/venv/bin/pip" install -q "$pip_package" >/dev/null 2>&1 || true
        fi
    fi

    # Final fallback: pip3 if available.
    if ! _python_module_importable "$module"; then
        if command -v pip3 >/dev/null 2>&1; then
            pip3 install -q "$pip_package" >/dev/null 2>&1 || true
        fi
    fi

    if ! _python_module_importable "$module"; then
        log_fatal "Required Python module not found: $module (install with: python3 -m pip install $pip_package)"
    fi
}

_python_module_importable() {
    python3 -c 'import importlib, sys; importlib.import_module(sys.argv[1])' "$1" 2>/dev/null
}

require_python_module() {
    local module="$1"
    local pip_package="${2:-$module}"
    ensure_python_module "$module" "$pip_package"
}

check_wg_tools() {
    if ! command -v wg &>/dev/null; then
        log_warn "wireguard-tools not found"
        if [[ "$(uname)" == "Darwin" ]]; then
            log_info "Install with: brew install wireguard-tools"
        else
            log_info "Install with: sudo apt install wireguard-tools"
        fi
        return 1
    fi
    return 0
}

# ============================================
# SAS VERIFICATION
# ============================================

compute_sas() {
    # Canonical SAS: sort keys, SHA256, first 4 bytes as uint32 mod 1000000, zero-pad to 6 digits
    # Args: <koi_pubkey_a> <koi_pubkey_b>
    local key_a="$1" key_b="$2"
    # Keys reach Python through the environment, never pasted into its source: one is a
    # remote node's, from its /health.
    SAS_KEY_A="$key_a" SAS_KEY_B="$key_b" python3 -c "
import hashlib, os
keys = sorted([os.environ['SAS_KEY_A'], os.environ['SAS_KEY_B']])
digest = hashlib.sha256(f'{keys[0]}:{keys[1]}'.encode()).digest()
num = int.from_bytes(digest[:4], 'big') % 1000000
print(f'{num:06d}')
"
}

# ============================================
# INVITE TOKEN HELPERS
# ============================================

peer_registry_lookup_by_number() {
    # Look up a peer by peer_number (WG IP last octet), output JSON entry
    # Returns the most recent invited/approving/active entry (skips cancelled/removed)
    local peer_number="$1"
    # A lookup writes nothing: with no registry there is no match.
    [[ -f "$PEER_REGISTRY" ]] || return 1
    # Values reach Python through the environment, never pasted into its source. A peer
    # number that is not an integer matches nothing.
    LIB_PEER_REGISTRY="$PEER_REGISTRY" LIB_PEER_NUMBER="$peer_number" python3 -c "
import json, os, sys
reg = json.load(open(os.environ['LIB_PEER_REGISTRY']))
try:
    number = int(os.environ['LIB_PEER_NUMBER'])
except ValueError:
    sys.exit(1)
# Return last matching entry with a live status (most recent append wins)
LIVE_STATUSES = {'invited', 'approving', 'active'}
match = None
for p in reg:
    if p.get('peer_number') == number and p.get('status', '') in LIVE_STATUSES:
        match = p
if match:
    json.dump(match, sys.stdout, indent=2)
    sys.exit(0)
sys.exit(1)
" 2>/dev/null
}

decode_invite_token() {
    # Peer-side: decode invite token payload (no HMAC verification)
    # Args: <token_string>
    local token_str="$1"
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    # The token comes from the inviting side, so it reaches Python through the environment,
    # never pasted into its source: decode_token checks its format.
    LIB_SCRIPT_DIR="$script_dir" LIB_TOKEN="$token_str" python3 -c "
import json, os, sys
sys.path.insert(0, os.environ['LIB_SCRIPT_DIR'])
from invite_token import decode_token
payload = decode_token(os.environ['LIB_TOKEN'])
json.dump(payload, sys.stdout, indent=2)
"
}
