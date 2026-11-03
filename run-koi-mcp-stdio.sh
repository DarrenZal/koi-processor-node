#!/bin/bash
# Launcher script for KOI Knowledge MCP Server (stdio mode)
# Ensures virtual environment is activated before running

cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run the stdio MCP server
exec python3 src/core/koi_knowledge_mcp_stdio.py
