#!/bin/bash

# KOI Services Monitor Script
# This script checks the health of all KOI services and can be run via cron

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Service endpoints
EVENT_BRIDGE_URL="http://localhost:8100"
BGE_SERVER_URL="http://localhost:8090"
POSTGRES_URL="postgresql://postgres:postgres@localhost:5433/eliza"

# Check function
check_service() {
    local name=$1
    local check_command=$2
    
    if eval $check_command > /dev/null 2>&1; then
        echo -e "${GREEN}✓${NC} $name is healthy"
        return 0
    else
        echo -e "${RED}✗${NC} $name is down"
        return 1
    fi
}

echo "========================================="
echo "KOI Services Health Check"
echo "Time: $(date)"
echo "========================================="

# Check Event Bridge
check_service "Event Bridge" "curl -s -f $EVENT_BRIDGE_URL/"

# Check BGE Server
check_service "BGE Server" "curl -s -f -X POST $BGE_SERVER_URL/encode -H 'Content-Type: application/json' -d '{\"text\":\"health check\"}'"

# Check PostgreSQL
check_service "PostgreSQL" "psql $POSTGRES_URL -c 'SELECT 1' -t"

# Check isolated tables
if psql $POSTGRES_URL -c "SELECT COUNT(*) FROM koi_memories WHERE created_at > NOW() - INTERVAL '1 hour'" -t > /dev/null 2>&1; then
    recent_count=$(psql $POSTGRES_URL -c "SELECT COUNT(*) FROM koi_memories WHERE created_at > NOW() - INTERVAL '1 hour'" -t)
    echo -e "${GREEN}✓${NC} Recent memories: $recent_count in last hour"
else
    echo -e "${YELLOW}⚠${NC} Could not check recent memories"
fi

# Check systemd services (if running as systemd)
if systemctl is-active --quiet koi-bridge; then
    echo -e "${GREEN}✓${NC} koi-bridge systemd service is active"
else
    # Check if running as process
    if pgrep -f "koi_event_bridge_v2.py" > /dev/null; then
        echo -e "${YELLOW}⚠${NC} Event Bridge running as process (not systemd)"
    else
        echo -e "${RED}✗${NC} Event Bridge not running"
    fi
fi

if systemctl is-active --quiet koi-bge; then
    echo -e "${GREEN}✓${NC} koi-bge systemd service is active"
else
    # Check if running as process
    if pgrep -f "bge_server.py" > /dev/null; then
        echo -e "${YELLOW}⚠${NC} BGE Server running as process (not systemd)"
    else
        echo -e "${RED}✗${NC} BGE Server not running"
    fi
fi

echo "========================================="

# Exit with error if any service is down
if ! check_service "Event Bridge" "curl -s -f $EVENT_BRIDGE_URL/" || \
   ! check_service "BGE Server" "curl -s -f -X POST $BGE_SERVER_URL/encode -H 'Content-Type: application/json' -d '{\"text\":\"test\"}'" || \
   ! check_service "PostgreSQL" "psql $POSTGRES_URL -c 'SELECT 1' -t"; then
    exit 1
fi

echo -e "${GREEN}All services healthy!${NC}"