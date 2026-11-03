#!/bin/bash

# KOI Processor Database Migration Runner with Backup
# Creates backup before migrations and keeps last 5 backups
#
# Usage: bash scripts/run_migrations_with_backup.sh [--dry-run | --apply]
#   (default) dry run: read-only checks of DATABASE_URL, then the backup it would take,
#             the old backups it would remove and the migrations it would apply
#   --apply   take the backup, prune old backups and apply the migrations

set -e  # Exit on error

usage() {
    cat <<'EOF'
Usage: bash scripts/run_migrations_with_backup.sh [--dry-run | --apply]

Applies migrations/*.sql to DATABASE_URL (default postgresql://postgres:***@localhost:5433/eliza),
tracking them in schema_migrations, after a pg_dump backup into backups/ (keeping the last 5).
It refuses personal_koi, koi_migrations databases (unless KOI_ALLOW_LEGACY_RUNNER=1) and
federation peers.

  (default)    dry run: connect read-only and print what --apply would do
  --apply      take the backup, remove backups past the last 5, apply the migrations
  --dry-run    the default; wins over --apply
  -h, --help   this text
EOF
}

APPLY=0
DRY_RUN=0
for arg in "$@"; do
    case "$arg" in
        -h|--help) usage; exit 0 ;;
        --apply) APPLY=1 ;;
        --dry-run) DRY_RUN=1 ;;
        *) echo "Error: unknown argument: $arg" >&2; usage >&2; exit 2 ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DEFAULT_DB_URL="postgresql://postgres:postgres@localhost:5433/eliza"
DB_URL="${DATABASE_URL:-$DEFAULT_DB_URL}"
DB_SHOWN=$(printf '%s' "$DB_URL" | sed -E 's#(://[^:/@]*):[^@]*@#\1:***@#')
MIGRATION_DIR="$(dirname "$0")/../migrations"
BACKUP_DIR="$(dirname "$0")/../backups"
KEEP_BACKUPS=5  # Number of backups to retain

# Parse database URL for pg_dump
parse_db_url() {
    # postgresql://user:pass@host:port/dbname
    local url=$1
    
    # Remove postgresql:// prefix
    url=${url#postgresql://}
    
    # Extract components
    DB_USER=$(echo $url | cut -d: -f1)
    DB_PASS=$(echo $url | cut -d: -f2 | cut -d@ -f1)
    DB_HOST=$(echo $url | cut -d@ -f2 | cut -d: -f1)
    DB_PORT=$(echo $url | cut -d: -f3 | cut -d/ -f1)
    DB_NAME=$(echo $url | cut -d/ -f2)
}

# Migration files in apply order. Exclude down/rollback files (*_down.sql,
# *.rollback.sql, *.down.sql) — a forward runner must NEVER auto-execute them.
# DRAFT_* files are proposals, not migrations. *_fresh_install.sql files belong to the
# fresh personal_koi plan in scripts/run_migrations.sh and do nothing elsewhere.
list_migrations() {
    ls -1 "$MIGRATION_DIR"/*.sql 2>/dev/null | grep -viE '(\.rollback|_down|\.down)\.sql$' | grep -vE '/DRAFT_[^/]*$' | grep -vE '_fresh_install\.sql$' | LC_ALL=C sort
}

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  KOI Database Migration Runner with Backup    ${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""
echo "Target: $DB_SHOWN"
if [ "$APPLY" -ne 1 ] || [ "$DRY_RUN" -eq 1 ]; then
    echo "Mode: dry run (read-only checks; pass --apply to back up and migrate)"
fi
echo ""

# Check prerequisites
if ! command -v psql &> /dev/null; then
    echo -e "${RED}Error: psql command not found. Please install PostgreSQL client.${NC}"
    exit 1
fi

if ! command -v pg_dump &> /dev/null; then
    echo -e "${RED}Error: pg_dump command not found. Please install PostgreSQL client.${NC}"
    exit 1
fi

# Parse database URL
parse_db_url "$DB_URL"

# Test database connection
echo -e "${YELLOW}Testing database connection...${NC}"
if ! PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT version();" &> /dev/null; then
    echo -e "${RED}Error: Cannot connect to database.${NC}"
    echo "Database URL: $DB_SHOWN"
    exit 1
fi
echo -e "${GREEN}✓ Database connection successful${NC}"
echo ""

# Probe queries use -X so a ~/.psqlrc cannot change the output the checks parse.
probe() { PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -At -c "$1" 2>/dev/null | tr -d '[:space:]'; }
# Every check below fails closed: only an explicit 't' or 'f' lets the runner continue.
probe_bool() {
    local answer
    answer=$(probe "$1")
    if [ "$answer" != "t" ] && [ "$answer" != "f" ]; then
        echo -e "${RED}ABORT: could not tell $2 (got '${answer}').${NC}" >&2
        exit 1
    fi
    echo "$answer"
}

# The *_fresh_install.sql files this runner skips use psql 10 meta-commands; refuse
# older clients rather than reason about what they would do with them.
PSQL_MAJOR=$(psql --version | sed -E 's/^[^0-9]*([0-9]+).*/\1/')
case "$PSQL_MAJOR" in ''|*[!0-9]*) PSQL_MAJOR=0 ;; esac
if [ "$PSQL_MAJOR" -lt 10 ]; then
    echo -e "${RED}ABORT: psql 10 or later is required (found: $(psql --version)).${NC}"
    exit 1
fi

# personal_koi databases are built only by scripts/run_migrations.sh (personal profile,
# empty database). This runner never touches one.
DB_NAME_ACTUAL=$(probe "SELECT current_database();")
if [ -z "$DB_NAME_ACTUAL" ]; then
    echo -e "${RED}ABORT: could not read the database name.${NC}"
    exit 1
fi
if [ "$DB_NAME_ACTUAL" = "personal_koi" ]; then
    echo -e "${RED}ABORT: this runner does not apply migrations to personal_koi.${NC}"
    echo "See docs/operations/fresh-install-migrations.md, section \"Existing databases\"."
    exit 1
fi

# --- Safety guard -----------------------------------------------------------
# Refuse to run on a DB that uses the koi_migrations baseline system. This
# legacy runner tracks applied state in schema_migrations, which is DEPRECATED
# and stale on baseline-system DBs (a personal_koi can record only a small
# fraction of its applied migrations there) — so it would RE-RUN already-applied migrations against the live
# database. New migrations are tracked in koi_migrations + per-node baseline
# manifests (scripts/stamp_baseline.py). Apply a new migration scoped instead:
#   psql "$DATABASE_URL" -f migrations/NNN_x.sql   # then record in koi_migrations
# Override (expert / genuinely-fresh DB only): KOI_ALLOW_LEGACY_RUNNER=1
if [ "${KOI_ALLOW_LEGACY_RUNNER:-0}" != "1" ]; then
    USES_KOI_MIGRATIONS=$(probe_bool "SELECT to_regclass('public.koi_migrations') IS NOT NULL;" "whether '$DB_NAME' uses koi_migrations") || exit 1
    if [ "$USES_KOI_MIGRATIONS" = "t" ]; then
        echo -e "${RED}ABORT: '$DB_NAME' uses the koi_migrations baseline system.${NC}"
        echo "This legacy schema_migrations-based runner is UNSAFE here: its ledger is"
        echo "stale, so it would re-run already-applied migrations against the live DB."
        echo ""
        echo "Apply a new migration scoped instead, then record it in koi_migrations:"
        echo "  psql \"\$DATABASE_URL\" -f migrations/NNN_name.sql"
        echo "  (baseline stamping: scripts/stamp_baseline.py)"
        echo ""
        echo "If you REALLY mean to use this runner (fresh/empty DB): KOI_ALLOW_LEGACY_RUNNER=1 $0"
        exit 1
    fi
fi
# ---------------------------------------------------------------------------

# Federation peers track migrations in koi_schema_migrations and are migrated by
# scripts/federation/setup-node.sh. This runner's ledger knows nothing of them. No override.
USES_PEER_LEDGER=$(probe_bool "SELECT to_regclass('public.koi_schema_migrations') IS NOT NULL;" "whether '$DB_NAME' uses koi_schema_migrations") || exit 1
if [ "$USES_PEER_LEDGER" = "t" ]; then
    echo -e "${RED}ABORT: '$DB_NAME' tracks migrations in koi_schema_migrations (a federation peer).${NC}"
    echo "Peers are migrated by scripts/federation/setup-node.sh, not by this runner."
    exit 1
fi

# Dry run: report what --apply would do, using read-only queries only.
if [ "$APPLY" -ne 1 ] || [ "$DRY_RUN" -eq 1 ]; then
    MIGRATIONS=($(list_migrations))
    HAS_LEDGER=$(probe_bool "SELECT to_regclass('public.schema_migrations') IS NOT NULL;" "whether '$DB_NAME' has schema_migrations") || exit 1
    APPLIED_LIST=""
    if [ "$HAS_LEDGER" = "t" ]; then
        if ! APPLIED_LIST=$(PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -At -c "SELECT version FROM schema_migrations;" 2>/dev/null); then
            echo -e "${RED}ABORT: could not read schema_migrations.${NC}" >&2
            exit 1
        fi
    fi
    PENDING=()
    for MIGRATION_FILE in "${MIGRATIONS[@]}"; do
        MIGRATION_NAME=$(basename "$MIGRATION_FILE" .sql)
        if ! printf '%s\n' "$APPLIED_LIST" | grep -qxF "$MIGRATION_NAME"; then
            PENDING+=("$MIGRATION_NAME")
        fi
    done
    EXISTING=$(ls -1 "$BACKUP_DIR"/backup_*.sql.gz 2>/dev/null | wc -l | tr -d ' ')
    WOULD_REMOVE=$(( EXISTING + 1 > KEEP_BACKUPS ? EXISTING + 1 - KEEP_BACKUPS : 0 ))
    echo -e "${YELLOW}Dry run: nothing was written.${NC}"
    echo "  Would write a pg_dump backup to $BACKUP_DIR/backup_<timestamp>.sql.gz"
    echo "  Would remove $WOULD_REMOVE older backup(s) (keeps the last $KEEP_BACKUPS; $EXISTING now)"
    echo "  Would create the vector and uuid-ossp extensions and schema_migrations if missing"
    echo "  Would apply ${#PENDING[@]} of ${#MIGRATIONS[@]} migration files:"
    for MIGRATION_NAME in "${PENDING[@]}"; do
        echo "    $MIGRATION_NAME"
    done
    echo "Pass --apply to run."
    exit 0
fi

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Create backup
BACKUP_FILE="$BACKUP_DIR/backup_$(date +%Y%m%d_%H%M%S).sql"
echo -e "${YELLOW}Creating database backup...${NC}"
echo "Backup file: $BACKUP_FILE"

if PGPASSWORD=$DB_PASS pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME > "$BACKUP_FILE" 2>/dev/null; then
    BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    echo -e "${GREEN}✓ Backup created successfully (${BACKUP_SIZE})${NC}"
else
    echo -e "${RED}✗ Failed to create backup${NC}"
    echo "Aborting migration for safety."
    exit 1
fi

# Compress backup
echo -e "${YELLOW}Compressing backup...${NC}"
gzip "$BACKUP_FILE"
BACKUP_FILE="${BACKUP_FILE}.gz"
COMPRESSED_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
echo -e "${GREEN}✓ Backup compressed (${COMPRESSED_SIZE})${NC}"
echo ""

# Clean up old backups (keep only last N)
echo -e "${YELLOW}Managing backup retention (keeping last ${KEEP_BACKUPS} backups)...${NC}"
BACKUP_COUNT=$(ls -1 "$BACKUP_DIR"/backup_*.sql.gz 2>/dev/null | wc -l)

if [ $BACKUP_COUNT -gt $KEEP_BACKUPS ]; then
    OLD_BACKUPS=$(ls -1t "$BACKUP_DIR"/backup_*.sql.gz | tail -n +$((KEEP_BACKUPS + 1)))
    for OLD_BACKUP in $OLD_BACKUPS; do
        rm "$OLD_BACKUP"
        echo "  Removed old backup: $(basename $OLD_BACKUP)"
    done
    echo -e "${GREEN}✓ Old backups cleaned up${NC}"
else
    echo "  Current backups: $BACKUP_COUNT (no cleanup needed)"
fi
echo ""

# Check for required extensions
echo -e "${YELLOW}Checking for required extensions...${NC}"
EXTENSIONS_SQL="
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        CREATE EXTENSION vector;
        RAISE NOTICE 'Created extension: vector';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp') THEN
        CREATE EXTENSION \"uuid-ossp\";
        RAISE NOTICE 'Created extension: uuid-ossp';
    END IF;
END
\$\$;
"

if ! PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$EXTENSIONS_SQL" 2>&1 | grep -v NOTICE; then
    echo -e "${RED}Error: Failed to create required extensions.${NC}"
    echo ""
    echo "To restore from backup:"
    echo "  gunzip < $BACKUP_FILE | PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
    exit 1
fi
echo -e "${GREEN}✓ Required extensions verified${NC}"
echo ""

# Create migration tracking table
echo -e "${YELLOW}Setting up migration tracking...${NC}"
TRACKING_SQL="
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(255) PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"

if ! PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "$TRACKING_SQL" &> /dev/null; then
    echo -e "${RED}Error: Failed to create migration tracking table.${NC}"
    echo ""
    echo "To restore from backup:"
    echo "  gunzip < $BACKUP_FILE | PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
    exit 1
fi
echo -e "${GREEN}✓ Migration tracking ready${NC}"
echo ""

# Get list of migration files. Exclude down/rollback files (*_down.sql,
# *.rollback.sql, *.down.sql) — a forward runner must NEVER auto-execute them.
# DRAFT_* files are proposals, not migrations. *_fresh_install.sql files belong to the
# fresh personal_koi plan in scripts/run_migrations.sh and do nothing elsewhere.
MIGRATIONS=($(list_migrations))

if [ ${#MIGRATIONS[@]} -eq 0 ]; then
    echo -e "${RED}Error: No migration files found in $MIGRATION_DIR${NC}"
    exit 1
fi

echo "Found ${#MIGRATIONS[@]} migration files"
echo ""

# Run each migration
APPLIED=0
SKIPPED=0
FAILED=0

for MIGRATION_FILE in "${MIGRATIONS[@]}"; do
    MIGRATION_NAME=$(basename "$MIGRATION_FILE" .sql)
    
    # Check if already applied
    ALREADY_APPLIED=$(PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM schema_migrations WHERE version = '$MIGRATION_NAME';" 2>/dev/null | tr -d ' ')
    
    if [ "$ALREADY_APPLIED" = "1" ]; then
        echo -e "${YELLOW}⊘ Skipping $MIGRATION_NAME (already applied)${NC}"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi
    
    echo -e "${YELLOW}→ Applying $MIGRATION_NAME...${NC}"
    
    # Run the migration
    if PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f "$MIGRATION_FILE" > /tmp/migration_output.txt 2>&1; then
        # Record successful migration
        PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "INSERT INTO schema_migrations (version) VALUES ('$MIGRATION_NAME') ON CONFLICT (version) DO NOTHING;" &> /dev/null \
            || echo -e "${RED}  could not record $MIGRATION_NAME in schema_migrations${NC}"
        echo -e "${GREEN}✓ Applied $MIGRATION_NAME${NC}"
        APPLIED=$((APPLIED + 1))
    else
        echo -e "${RED}✗ Failed to apply $MIGRATION_NAME${NC}"
        echo "Error output:"
        cat /tmp/migration_output.txt
        FAILED=$((FAILED + 1))
        
        echo ""
        echo -e "${RED}Migration failed! Database backup available at:${NC}"
        echo "  $BACKUP_FILE"
        echo ""
        echo "To restore the database:"
        echo "  gunzip < $BACKUP_FILE | PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
        echo ""
        
        # Ask whether to continue; without a terminal (CI, pipes), stop: a backup run is
        # for live databases, where continuing past a failure is the wrong default.
        if [ -t 0 ]; then
            read -p "Continue with remaining migrations? (y/n) " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                break
            fi
        else
            echo "(no terminal: stopping after the failure)"
            break
        fi
    fi
done

# Clean up
rm -f /tmp/migration_output.txt

echo ""
echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}                Migration Summary               ${NC}"
echo -e "${BLUE}================================================${NC}"
echo "Applied: $APPLIED"
echo "Skipped: $SKIPPED"
echo "Failed:  $FAILED"
echo ""

# Verify final state
if [ $FAILED -eq 0 ]; then
    echo -e "${YELLOW}Verifying database schema...${NC}"
    
    # Check for expected tables
    TABLES=$(PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('transformation_receipts', 'agent_knowledge_permissions', 'koi_memories', 'koi_embeddings', 'koi_entities');" | tr -d ' ')
    
    if [ "$TABLES" -ge "3" ]; then
        echo -e "${GREEN}✓ Database schema verified${NC}"
        echo ""
        echo -e "${GREEN}✓ Migration complete!${NC}"
        echo ""
        echo "Backup saved at: $BACKUP_FILE"
        echo ""
        echo "Current backups:"
        ls -lh "$BACKUP_DIR"/backup_*.sql.gz | tail -5
        exit 0
    else
        echo -e "${RED}Warning: Some expected tables may be missing${NC}"
        echo ""
        echo "To restore from backup:"
        echo "  gunzip < $BACKUP_FILE | PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
        exit 1
    fi
else
    echo -e "${RED}Some migrations failed!${NC}"
    echo ""
    echo "Database backup available at:"
    echo "  $BACKUP_FILE"
    echo ""
    echo "To restore:"
    echo "  gunzip < $BACKUP_FILE | PGPASSWORD=$DB_PASS psql -X -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
    exit 1
fi