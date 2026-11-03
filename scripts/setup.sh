#!/bin/bash

# KOI Processor Complete Setup Script
# Sets up database, migrations, and all components

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                 KOI Processor Setup${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Step 1: Check Python version
echo -e "${YELLOW}Step 1: Checking Python version...${NC}"
if python3 -c "import sys; exit(0 if sys.version_info >= (3,8) else 1)"; then
    PYTHON_VERSION=$(python3 --version | cut -d" " -f2)
    echo -e "${GREEN}✓ Python $PYTHON_VERSION found${NC}"
else
    echo -e "${RED}✗ Python 3.8+ required${NC}"
    exit 1
fi

# Step 2: Create/activate virtual environment
echo ""
echo -e "${YELLOW}Step 2: Setting up Python virtual environment...${NC}"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
else
    echo -e "${GREEN}✓ Virtual environment already exists${NC}"
fi

source venv/bin/activate
echo -e "${GREEN}✓ Virtual environment activated${NC}"

# Step 3: Install dependencies
echo ""
echo -e "${YELLOW}Step 3: Installing Python dependencies...${NC}"
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo -e "${GREEN}✓ Dependencies installed${NC}"

# Step 4: PostgreSQL setup
echo ""
echo -e "${YELLOW}Step 4: PostgreSQL Setup...${NC}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5433}"
DB_NAME="${DB_NAME:-eliza}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"

# Check if PostgreSQL is accessible
python3 -c "
import psycopg2
import sys
try:
    conn = psycopg2.connect(
        host='$DB_HOST',
        port=$DB_PORT,
        database='$DB_NAME',
        user='$DB_USER',
        password='$DB_PASSWORD'
    )
    conn.close()
    print('✓ PostgreSQL connection successful')
    sys.exit(0)
except Exception as e:
    sys.exit(1)
" || {
    echo -e "${YELLOW}PostgreSQL not running. Would you like to start it with Docker? (y/n)${NC}"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo "Starting PostgreSQL with Docker..."
        docker run -d --name postgres -p 5433:5432 \
            -e POSTGRES_PASSWORD=postgres \
            -e POSTGRES_DB=eliza \
            ankane/pgvector
        echo "Waiting for PostgreSQL to start..."
        sleep 5
        echo -e "${GREEN}✓ PostgreSQL started with Docker${NC}"
    else
        echo -e "${RED}PostgreSQL is required. Please start it manually.${NC}"
        echo "Example Docker command:"
        echo "  docker run -d --name postgres -p 5433:5432 \\"
        echo "    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=eliza \\"
        echo "    ankane/pgvector"
        exit 1
    fi
}

# Step 5: Run migrations
echo ""
echo -e "${YELLOW}Step 5: Running database migrations...${NC}"
if [ ! -f "$SCRIPT_DIR/run_migrations.sh" ]; then
    echo -e "${RED}✗ scripts/run_migrations.sh not found; cannot run migrations${NC}"
    exit 1
fi
# Show the runner's full output and stop if it refuses or fails.
MIGRATION_RC=0
bash "$SCRIPT_DIR/run_migrations.sh" || MIGRATION_RC=$?
if [ "$MIGRATION_RC" -ne 0 ]; then
    echo -e "${RED}✗ Migrations did not complete (exit $MIGRATION_RC); see the runner output above${NC}"
    exit "$MIGRATION_RC"
fi
echo -e "${GREEN}✓ All migrations completed${NC}"

# Step 6: Create directories
echo ""
echo -e "${YELLOW}Step 6: Creating required directories...${NC}"
mkdir -p logs static templates config
echo -e "${GREEN}✓ Directories created${NC}"

# Step 7: Create configuration
echo ""
echo -e "${YELLOW}Step 7: Setting up configuration...${NC}"
if [ ! -f "config/dashboard_config.yaml" ]; then
    cat > config/dashboard_config.yaml << 'EOF'
dashboard:
  port: 8400
  host: "0.0.0.0"
  debug: true
  auth_enabled: false

database:
  host: localhost
  port: 5433
  name: eliza
  user: postgres
  password: postgres

thresholds:
  daily_bot:
    min_sources: 3
    style_score_warning: 0.7
    style_score_critical: 0.5
    
  weekly_digest:
    min_word_count: 800
    max_word_count: 1200
    min_sources: 10

schedule:
  daily_bot:
    hour: 16
    days: [0, 1, 2, 3, 4]
    
  weekly_digest:
    day: 4
    hour: 16
EOF
    echo -e "${GREEN}✓ Configuration created${NC}"
else
    echo -e "${GREEN}✓ Configuration already exists${NC}"
fi

# Step 8: Show summary
echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Setup Complete!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}Quick Start Commands:${NC}"
echo ""
echo "1. Start monitoring dashboard (the public export leaves out its script):"
echo -e "   ${YELLOW}source venv/bin/activate && python src/content/content_dashboard.py${NC} (not in this export)"
echo "   Open: http://localhost:8400"
echo ""
echo "2. Run daily curator:"
echo -e "   ${YELLOW}source venv/bin/activate && python scripts/run_daily_curator.py${NC}"
echo ""
echo "3. Check system status:"
echo -e "   ${YELLOW}source venv/bin/activate && python scripts/run_daily_curator.py status${NC}"
echo ""
echo -e "${BLUE}Database:${NC} postgresql://$DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"
echo ""

deactivate 2>/dev/null || true