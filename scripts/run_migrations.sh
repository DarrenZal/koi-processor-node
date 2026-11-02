#!/bin/bash

# KOI Processor Database Migration Runner
# Runs all migrations in order for initial setup

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default database URL
DEFAULT_DB_URL="postgresql://postgres:postgres@localhost:5433/eliza"
DB_URL="${DATABASE_URL:-$DEFAULT_DB_URL}"

# Migration directory
MIGRATION_DIR="$(dirname "$0")/../migrations"

echo -e "${GREEN}KOI Processor Database Migration Runner${NC}"
echo "========================================"
echo ""

# Check if psql is installed
if ! command -v psql &> /dev/null; then
    echo -e "${RED}Error: psql command not found. Please install PostgreSQL client.${NC}"
    echo "Ubuntu/Debian: sudo apt-get install postgresql-client"
    echo "macOS: brew install postgresql"
    exit 1
fi

# Test database connection
echo -e "${YELLOW}Testing database connection...${NC}"
if ! psql -X "$DB_URL" -c "SELECT version();" &> /dev/null; then
    echo -e "${RED}Error: Cannot connect to database.${NC}"
    echo "Database URL: $DB_URL"
    echo ""
    echo "Please ensure:"
    echo "1. PostgreSQL is running"
    echo "2. Database 'eliza' exists"
    echo "3. Credentials are correct"
    echo ""
    echo "To use a different database, set DATABASE_URL environment variable:"
    echo "  DATABASE_URL=postgresql://user:pass@host:port/dbname $0"
    exit 1
fi

echo -e "${GREEN}✓ Database connection successful${NC}"
echo ""

# Probe queries use -X so a ~/.psqlrc cannot change the output the checks parse.
probe() { psql -X "$DB_URL" -At -c "$1" 2>/dev/null | tr -d '[:space:]'; }
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

# psql meta-commands (\gset, \if) in the *_fresh_install.sql files need psql 10+.
PSQL_MAJOR=$(psql --version | sed -E 's/^[^0-9]*([0-9]+).*/\1/')
case "$PSQL_MAJOR" in ''|*[!0-9]*) PSQL_MAJOR=0 ;; esac
if [ "$PSQL_MAJOR" -lt 10 ]; then
    echo -e "${RED}ABORT: psql 10 or later is required (found: $(psql --version)).${NC}"
    exit 1
fi

# --- Safety guard (added 2026-05-29) ----------------------------------------
# Refuse to run on a DB that uses the koi_migrations baseline system. This
# legacy runner tracks applied state in the DEPRECATED schema_migrations ledger,
# which is stale on baseline-system DBs — so it would RE-RUN already-applied
# migrations against the live database. New migrations are tracked in
# koi_migrations + baseline manifests (scripts/stamp_baseline.py); apply scoped:
#   psql "$DATABASE_URL" -f migrations/NNN_x.sql   # then record in koi_migrations
# Override (expert / genuinely-fresh DB only): KOI_ALLOW_LEGACY_RUNNER=1
if [ "${KOI_ALLOW_LEGACY_RUNNER:-0}" != "1" ]; then
    USES_KOI_MIGRATIONS=$(probe_bool "SELECT to_regclass('public.koi_migrations') IS NOT NULL;" "whether this database uses koi_migrations") || exit 1
    if [ "$USES_KOI_MIGRATIONS" = "t" ]; then
        echo -e "${RED}ABORT: this database uses the koi_migrations baseline system.${NC}"
        echo "This legacy schema_migrations-based runner is UNSAFE here: its ledger is"
        echo "stale, so it would re-run already-applied migrations against the live DB."
        echo "Apply a new migration scoped instead:  psql \"\$DATABASE_URL\" -f migrations/NNN_name.sql"
        echo "(then record it in koi_migrations; baseline: scripts/stamp_baseline.py)"
        echo "Override (fresh/empty DB only): KOI_ALLOW_LEGACY_RUNNER=1 $0"
        exit 1
    fi
fi
# ---------------------------------------------------------------------------

# Federation peers built by scripts/federation/setup-node.sh track their migrations in
# koi_schema_migrations and are updated by that script. This runner's ledger knows
# nothing of them, so it would re-run every file. No override.
USES_PEER_LEDGER=$(probe_bool "SELECT to_regclass('public.koi_schema_migrations') IS NOT NULL;" "whether this database uses koi_schema_migrations") || exit 1
if [ "$USES_PEER_LEDGER" = "t" ]; then
    echo -e "${RED}ABORT: this database tracks migrations in koi_schema_migrations (a federation peer).${NC}"
    echo "Peers are migrated by scripts/federation/setup-node.sh, not by this runner."
    echo "See docs/operations/fresh-install-migrations.md, section \"Existing databases\"."
    exit 1
fi

# The marker exists only while a fresh install runs. Finding one means an earlier run
# was killed part-way, and the database is half built.
MARKER_LEFT=$(probe_bool "SELECT to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL;" "whether a fresh-install marker is present") || exit 1
if [ "$MARKER_LEFT" = "t" ]; then
    echo -e "${RED}ABORT: a previous fresh install did not finish; drop and recreate the database.${NC}"
    echo "  dropdb <name> && createdb <name>, then run this script again."
    echo "  (If another run of this script is still going, let it finish instead.)"
    exit 1
fi

# Install profile: personal (database named exactly personal_koi; 086, 103 and 104
# refuse any other name) or regenai. Override with KOI_INSTALL_PROFILE=personal|regenai.
# See docs/operations/fresh-install-migrations.md.
DB_NAME_ACTUAL=$(psql -X "$DB_URL" -At -c "SELECT current_database();" 2>/dev/null)
case "${KOI_INSTALL_PROFILE:-auto}" in
    auto)
        if [ "$DB_NAME_ACTUAL" = "personal_koi" ]; then PROFILE=personal; else PROFILE=regenai; fi ;;
    personal)
        if [ "$DB_NAME_ACTUAL" != "personal_koi" ]; then
            echo -e "${RED}ABORT: the personal profile needs a database named exactly personal_koi (this is '${DB_NAME_ACTUAL}').${NC}"
            exit 1
        fi
        PROFILE=personal ;;
    regenai)
        if [ "$DB_NAME_ACTUAL" = "personal_koi" ]; then
            echo -e "${RED}ABORT: personal_koi is built only by the personal profile, from an empty database.${NC}"
            exit 1
        fi
        PROFILE=regenai ;;
    *) echo -e "${RED}ABORT: unknown KOI_INSTALL_PROFILE '${KOI_INSTALL_PROFILE}'${NC}"; exit 1 ;;
esac
# Personal runs resolve every unqualified name in public, whatever the role's search_path.
if [ "$PROFILE" = "personal" ]; then
    export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c search_path=public"
fi

# Get list of migration files. Exclude down/rollback files (*_down.sql,
# *.rollback.sql, *.down.sql) — a forward runner must NEVER auto-execute them.
# DRAFT_* files are proposals, not migrations; never auto-execute them either.
MIGRATIONS=($(ls -1 "$MIGRATION_DIR"/*.sql 2>/dev/null | grep -viE '(\.rollback|_down|\.down)\.sql$' | grep -vE '/DRAFT_[^/]*$' | LC_ALL=C sort))

if [ ${#MIGRATIONS[@]} -eq 0 ]; then
    echo -e "${RED}Error: No migration files found in $MIGRATION_DIR${NC}"
    exit 1
fi

# Fresh-install plan, used only by the personal profile (docs/operations/fresh-install-migrations.md).
# RegenAI-era migrations: they target the RegenAI eliza schema (agents, koi_content,
# koi_transformation_receipts, Milestone B dashboards, Jena, the code-graph provenance
# tables) and fail on a personal database. They are not recorded as applied. The parts
# personal code uses are created by 038a (from 013) and 044a (from 010 and 015).
PERSONAL_SKIP=(
    002_create_agent_knowledge_permissions
    005_create_dashboard_tables
    006_fix_cat_receipts
    007_improved_storage_architecture
    010_jena_integration
    011_adaptive_knowledge_query_log
    013_create_kg_tables
    015_add_privacy_column
    add_code_graph_provenance
)
# 083 refuses any database user but the maintainer's; 083a carries its statements
# without that guard and runs right after it.
PERSONAL_REPLACED=(083_mediawiki_redirect_resolver)
# 033 builds an index on koi_memory_chunks, which 044 creates (007, the other creator,
# is RegenAI-era). Run 033 right after 044.
PERSONAL_DEFER_FILE=033_email_sensor_tables
PERSONAL_DEFER_AFTER=044_koi_memories_rag

in_list() {
    local needle="$1"; shift
    local x
    for x in "$@"; do [ "$x" = "$needle" ] && return 0; done
    return 1
}

# The personal profile only builds a new database from empty. Every check that can stop
# it runs here, before the runner writes anything, so a refused run leaves the database
# as it was.
FRESH=no
if [ "$PROFILE" = "personal" ]; then
    if ! in_list "$MIGRATION_DIR/$PERSONAL_DEFER_AFTER.sql" "${MIGRATIONS[@]}"; then
        echo -e "${RED}ABORT: $PERSONAL_DEFER_FILE is deferred until after $PERSONAL_DEFER_AFTER, which is missing.${NC}"
        exit 1
    fi
    # Count from pg_catalog: information_schema shows only what this role may access.
    PUBLIC_RELATIONS=$(probe "SELECT count(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f');")
    case "$PUBLIC_RELATIONS" in
        ''|*[!0-9]*)
            echo -e "${RED}ABORT: could not count the tables in the public schema (got '${PUBLIC_RELATIONS}').${NC}"
            exit 1 ;;
    esac
    if [ "$PUBLIC_RELATIONS" != "0" ]; then
        echo -e "${RED}ABORT: personal_koi is not empty ($PUBLIC_RELATIONS tables, views or foreign tables in public).${NC}"
        echo "This runner only builds a new personal_koi from an empty database."
        echo "If an earlier run of it failed, drop and recreate the database and run it again."
        echo "To add a migration to an existing database, see"
        echo "docs/operations/fresh-install-migrations.md, section \"Existing databases\"."
        exit 1
    fi
    FRESH=yes
fi
echo "Install profile: $PROFILE (database $DB_NAME_ACTUAL); fresh install: $FRESH"

APPLIED=0
SKIPPED=0
FAILED=0
WITH_ERRORS=0
ERRORED_NAMES=()
OUTPUT_FILE=$(mktemp "${TMPDIR:-/tmp}/koi-migration.XXXXXX")
# Drop the marker only if this run created it: a run that found another run's marker,
# or failed to create its own, must leave it alone.
CREATED_MARKER=0
drop_fresh_marker() {
    if [ "$CREATED_MARKER" = "1" ]; then
        psql -X "$DB_URL" -q -c "DROP TABLE IF EXISTS public.koi_fresh_install_in_progress;" &> /dev/null || true
    fi
}
trap 'rm -f "$OUTPUT_FILE"; drop_fresh_marker' EXIT

fresh_abort() {
    echo -e "${RED}$1${NC}"
    echo "The database is now partly built. Drop and recreate it before running this script again."
    exit 1
}

if [ "$FRESH" = "yes" ]; then
    # First write of the run. The DO block re-checks emptiness at the moment it creates
    # the marker. Each *_fresh_install.sql file acts only while this marker exists and
    # koi.fresh_install is on, so applying one by hand to an existing database does nothing.
    if psql -X "$DB_URL" -q -v ON_ERROR_STOP=1 -c "DO \$\$ BEGIN
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = 'public' AND c.relkind IN ('r','p','v','m','f')) THEN
            RAISE EXCEPTION 'public schema is no longer empty';
        END IF;
        CREATE TABLE public.koi_fresh_install_in_progress (started_at TIMESTAMPTZ DEFAULT now());
    END \$\$;" &> /dev/null; then
        CREATED_MARKER=1
    else
        echo -e "${RED}ABORT: could not create the fresh-install marker (another run, or the KOI service, may be writing to this database).${NC}"
        echo "Stop the KOI service and any other run, drop and recreate the database, and run this script again."
        exit 1
    fi
    export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c koi.fresh_install=on"
fi
echo ""

# Check for pgvector extension
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

# Branch on psql's own exit status (a pipe into grep would report grep's).
EXT_RC=0
EXT_OUT=$(psql -X "$DB_URL" -v ON_ERROR_STOP=1 -c "$EXTENSIONS_SQL" 2>&1) || EXT_RC=$?
printf '%s\n' "$EXT_OUT" | grep -v NOTICE || true
if [ "$EXT_RC" -ne 0 ]; then
    echo -e "${RED}Error: Failed to create required extensions.${NC}"
    echo "Please ensure pgvector is installed and you have CREATE EXTENSION permission."
    [ "$FRESH" = "yes" ] && fresh_abort "ABORT: fresh install stopped before the first migration."
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

if ! psql -X "$DB_URL" -c "$TRACKING_SQL" &> /dev/null; then
    echo -e "${RED}Error: Failed to create migration tracking table.${NC}"
    [ "$FRESH" = "yes" ] && fresh_abort "ABORT: fresh install stopped before the first migration."
    exit 1
fi

echo -e "${GREEN}✓ Migration tracking ready${NC}"
echo ""

echo "Found ${#MIGRATIONS[@]} migration files"
echo ""

# Run each migration
# Counters use VAR=$((VAR + 1)): under `set -e`, ((VAR++)) returns status 1 when VAR is 0
# and silently ended the script after the first migration on a fresh database.
apply_one() {
    # Returns 0 when psql ran the file (even if some statements errored), 1 on a hard failure.
    psql -X "$DB_URL" -f "$1" > "$OUTPUT_FILE" 2>&1
}

process_migration() {
    local MIGRATION_FILE="$1"
    local MIGRATION_NAME
    MIGRATION_NAME=$(basename "$MIGRATION_FILE" .sql)

    # Check if already applied
    ALREADY_APPLIED=$(psql -X "$DB_URL" -t -c "SELECT COUNT(*) FROM schema_migrations WHERE version = '$MIGRATION_NAME';" 2>/dev/null | tr -d ' ')

    if [ "$ALREADY_APPLIED" = "1" ]; then
        echo -e "${YELLOW}⊘ Skipping $MIGRATION_NAME (already applied)${NC}"
        SKIPPED=$((SKIPPED + 1))
        return 0
    fi

    echo -e "${YELLOW}→ Applying $MIGRATION_NAME...${NC}"

    if apply_one "$MIGRATION_FILE"; then
        # Record the migration. psql without ON_ERROR_STOP keeps going past a failed
        # statement, so report those files instead of calling them clean.
        psql -X "$DB_URL" -c "INSERT INTO schema_migrations (version) VALUES ('$MIGRATION_NAME') ON CONFLICT (version) DO NOTHING;" &> /dev/null \
            || echo -e "${RED}  could not record $MIGRATION_NAME in schema_migrations${NC}"
        if grep -q "ERROR:" "$OUTPUT_FILE"; then
            echo -e "${YELLOW}! Applied $MIGRATION_NAME with $(grep -c 'ERROR:' "$OUTPUT_FILE") statement error(s)${NC}"
            grep -m 3 "ERROR:" "$OUTPUT_FILE" | sed 's/^/    /'
            WITH_ERRORS=$((WITH_ERRORS + 1))
            ERRORED_NAMES+=("$MIGRATION_NAME")
        else
            echo -e "${GREEN}✓ Applied $MIGRATION_NAME${NC}"
        fi
        APPLIED=$((APPLIED + 1))
    else
        echo -e "${RED}✗ Failed to apply $MIGRATION_NAME${NC}"
        echo "Error output:"
        cat "$OUTPUT_FILE"
        FAILED=$((FAILED + 1))

        # A fresh install stops at the first hard failure. Otherwise ask whether to
        # continue; without a terminal (CI, pipes), stop.
        if [ "$FRESH" = "yes" ]; then
            return 1
        elif [ -t 0 ]; then
            read -p "Continue with remaining migrations? (y/n) " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                return 1
            fi
        else
            echo "(no terminal: stopping after the failure)"
            return 1
        fi
    fi
    return 0
}

PROFILE_SKIPPED=0
for MIGRATION_FILE in "${MIGRATIONS[@]}"; do
    MIGRATION_NAME=$(basename "$MIGRATION_FILE" .sql)
    if [ "$FRESH" = "yes" ]; then
        if in_list "$MIGRATION_NAME" "${PERSONAL_SKIP[@]}"; then
            echo -e "${YELLOW}⊘ Skipping $MIGRATION_NAME (RegenAI-era; not used by personal installs)${NC}"
            PROFILE_SKIPPED=$((PROFILE_SKIPPED + 1))
            continue
        fi
        if in_list "$MIGRATION_NAME" "${PERSONAL_REPLACED[@]}"; then
            echo -e "${YELLOW}⊘ Skipping $MIGRATION_NAME (replaced by ${MIGRATION_NAME%%_*}a on personal installs)${NC}"
            PROFILE_SKIPPED=$((PROFILE_SKIPPED + 1))
            continue
        fi
        if [ "$MIGRATION_NAME" = "$PERSONAL_DEFER_FILE" ]; then
            echo -e "${YELLOW}↷ Deferring $MIGRATION_NAME until after $PERSONAL_DEFER_AFTER${NC}"
            continue
        fi
    elif [[ "$MIGRATION_NAME" == *_fresh_install ]]; then
        # Only part of the fresh-install plan; elsewhere they do nothing, so neither
        # apply nor record them.
        echo -e "${YELLOW}⊘ Skipping $MIGRATION_NAME (fresh installs only)${NC}"
        PROFILE_SKIPPED=$((PROFILE_SKIPPED + 1))
        continue
    fi
    process_migration "$MIGRATION_FILE" || break
    if [ "$FRESH" = "yes" ] && [ "$MIGRATION_NAME" = "$PERSONAL_DEFER_AFTER" ]; then
        process_migration "$MIGRATION_DIR/$PERSONAL_DEFER_FILE.sql" || break
    fi
done

# Second pass for ordering errors: some files reference tables that a later-numbered
# migration creates. Re-running a file that errored is safe only if it is idempotent, so
# this is opt-in (KOI_RETRY_ERRORED=1) and reports which files are now clean.
if [ "${KOI_RETRY_ERRORED:-0}" = "1" ] && [ ${#ERRORED_NAMES[@]} -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}Retrying ${#ERRORED_NAMES[@]} file(s) that had statement errors...${NC}"
    STILL=()
    for MIGRATION_NAME in "${ERRORED_NAMES[@]}"; do
        if apply_one "$MIGRATION_DIR/$MIGRATION_NAME.sql" && ! grep -q "ERROR:" "$OUTPUT_FILE"; then
            echo -e "${GREEN}✓ $MIGRATION_NAME clean on retry${NC}"
        else
            STILL+=("$MIGRATION_NAME")
        fi
    done
    ERRORED_NAMES=("${STILL[@]+"${STILL[@]}"}")
    WITH_ERRORS=${#ERRORED_NAMES[@]}
fi

echo ""
echo "========================================"
echo -e "${GREEN}Migration Summary${NC}"
echo "========================================"
echo "Applied: $APPLIED"
echo "Skipped: $SKIPPED (already applied), $PROFILE_SKIPPED (not part of this profile's plan)"
echo "Failed:  $FAILED"
echo "Applied with statement errors: $WITH_ERRORS"
if [ "$WITH_ERRORS" -gt 0 ]; then
    printf '    %s\n' "${ERRORED_NAMES[@]}"
    echo "    (see docs/operations/fresh-install-migrations.md)"
fi
echo ""

# Verify final state
if [ "$PROFILE" = "personal" ]; then
    MISSING=$(psql -X "$DB_URL" -At -c "SELECT string_agg(t, ' ') FROM unnest(ARRAY['entity_registry','document_entity_links','entity_rid_mappings','koi_memories','koi_memory_chunks','task_registry']) AS t WHERE to_regclass('public.' || t) IS NULL;")
    if [ "$FAILED" -eq 0 ] && [ "$WITH_ERRORS" -eq 0 ] && [ -z "$MISSING" ]; then
        echo -e "${GREEN}✓ Personal-KOI schema complete; no migration reported an error${NC}"
        exit 0
    fi
    [ -n "$MISSING" ] && echo -e "${RED}Missing tables: $MISSING${NC}"
    echo -e "${RED}Personal install is not clean: $FAILED failed, $WITH_ERRORS with statement errors${NC}"
    echo "Drop and recreate the database before running this script again."
    exit 1
fi

if [ $FAILED -eq 0 ]; then
    echo -e "${YELLOW}Verifying database schema...${NC}"
    
    # Check for expected tables
    TABLES=$(psql -X "$DB_URL" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('transformation_receipts', 'agent_knowledge_permissions', 'koi_memories', 'koi_embeddings', 'koi_entities');" | tr -d ' ')
    
    VIEWS=$(psql -X "$DB_URL" -t -c "SELECT COUNT(*) FROM information_schema.views WHERE table_schema = 'public' AND table_name = 'v_daily_content';" | tr -d ' ')
    
    if [ "$TABLES" -ge "5" ] && [ "$VIEWS" -ge "1" ]; then
        echo -e "${GREEN}✓ All expected tables and views are present${NC}"
        echo ""
        
        # Show table summary
        echo "Database tables:"
        psql -X "$DB_URL" -c "\dt koi_*" 2>/dev/null | grep -E "koi_|table"
        
        echo ""
        echo -e "${GREEN}✓ Migration setup complete!${NC}"
        echo ""
        echo "You can now run the Daily Curator:"
        echo "  python scripts/run_daily_curator.py status"
        exit 0
    else
        echo -e "${RED}Warning: Some expected tables/views may be missing${NC}"
        echo "Found $TABLES tables and $VIEWS views"
        echo ""
        echo "Please check the migration output above for errors"
        exit 1
    fi
else
    echo -e "${RED}Some migrations failed. Please fix the issues and re-run.${NC}"
    echo ""
    echo "To reset and start over:"
    echo "  psql $DB_URL -c 'DROP TABLE IF EXISTS koi_memories, koi_embeddings, koi_entities, transformation_receipts, agent_knowledge_permissions, schema_migrations CASCADE;'"
    echo "  $0"
    exit 1
fi