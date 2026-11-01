#!/usr/bin/env bash
# ============================================================================
# Publish a SpacetimeDB module to ALL nodes in the cluster.
#
# spacetime-db-ha replicates reducer calls, not module deployments. This script
# publishes the same module to every SpacetimeDB instance so they all have
# identical schemas and reducers.
#
# Usage:
#   ./scripts/publish-all.sh <module_path> [db_name]
#
# For Docker Compose (default ports 5001-5003):
#   ./scripts/publish-all.sh ./my_module mydb
#
# For custom servers:
#   STDB_SERVERS="http://host1:3000 http://host2:3000 http://host3:3000" \
#     ./scripts/publish-all.sh ./my_module mydb
#
# Prerequisites:
#   - spacetime CLI (https://spacetimedb.com/install)
# ============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if [ $# -lt 1 ]; then
    echo "Usage: $0 <module_path> [db_name]"
    echo ""
    echo "  module_path  Path to the SpacetimeDB module directory"
    echo "  db_name      Database name (default: directory name of module_path)"
    echo ""
    echo "Environment:"
    echo "  STDB_SERVERS  Space-separated SpacetimeDB URLs"
    echo "                (default: http://localhost:5001 http://localhost:5002 http://localhost:5003)"
    exit 1
fi

MODULE_PATH="$1"
DB_NAME="${2:-$(basename "$MODULE_PATH")}"

# Default: Docker Compose exposed ports
SERVERS="${STDB_SERVERS:-http://localhost:5001 http://localhost:5002 http://localhost:5003}"

if ! command -v spacetime &>/dev/null; then
    echo -e "${RED}Error: spacetime CLI not found.${NC}"
    echo "Install it: https://spacetimedb.com/install"
    exit 1
fi

echo -e "${YELLOW}Publishing module '${DB_NAME}' from ${MODULE_PATH}${NC}"
echo ""

FAILED=0
for server in $SERVERS; do
    echo -n "  Publishing to ${server}... "
    if spacetime publish --server "$server" "$MODULE_PATH" "$DB_NAME" 2>&1; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
if [ "$FAILED" -eq 0 ]; then
    echo -e "${GREEN}Module '${DB_NAME}' published to all nodes successfully!${NC}"
    echo ""
    echo "Your app can now connect to any spacetime-db-ha node:"
    echo "  ws://localhost:3001  ws://localhost:3002  ws://localhost:3003"
else
    echo -e "${RED}${FAILED} node(s) failed. All nodes must have the same module for replication to work.${NC}"
    exit 1
fi
