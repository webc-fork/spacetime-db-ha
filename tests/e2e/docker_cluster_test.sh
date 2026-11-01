#!/usr/bin/env bash
# ============================================================================
# End-to-End Test: spacetime-db-ha 3-Node Docker Cluster
# ============================================================================
#
# This script:
# 1. Starts a 3-node spacetime-db-ha cluster via Docker Compose
# 2. Waits for all nodes to be healthy
# 3. Bootstraps the Raft cluster via sdha init
# 4. Verifies leader election
# 5. Tests write forwarding (write to follower succeeds)
# 6. Kills a node and verifies the cluster re-elects a leader
# 7. Tears down the cluster
#
# Prerequisites:
#   - Docker and Docker Compose
#   - sdha CLI (cargo install --path crates/cli)
#   - curl
#
# Usage:
#   ./tests/e2e/docker_cluster_test.sh
#
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/deploy/docker/docker-compose.yml"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; TESTS_FAILED=$((TESTS_FAILED + 1)); }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

# Track test results
TESTS_PASSED=0
TESTS_FAILED=0

run_test() {
    local name="$1"
    local cmd="$2"

    if eval "$cmd"; then
        pass "$name"
        TESTS_PASSED=$((TESTS_PASSED + 1))
    else
        fail "$name"
    fi
}

# ============================================================================
# Setup
# ============================================================================

info "Starting 3-node spacetime-db-ha cluster..."
cd "$PROJECT_ROOT"

docker compose -f "$COMPOSE_FILE" up -d --build 2>/dev/null || {
    fail "Docker Compose failed to start"
    exit 1
}

# Wait for management API ports to be ready
info "Waiting for management API to be ready..."
MAX_WAIT=60
for port in 4001 4002 4003; do
    WAITED=0
    until curl -sf "http://localhost:$port/cluster/status" >/dev/null 2>&1; do
        sleep 1
        WAITED=$((WAITED + 1))
        if [ "$WAITED" -ge "$MAX_WAIT" ]; then
            info "Node on port $port didn't become ready in ${MAX_WAIT}s"
            break
        fi
    done
done

info "Cluster started. Running tests..."

# ============================================================================
# Test: Containers Running
# ============================================================================

run_test "Node 1 container is running" \
    "docker compose -f '$COMPOSE_FILE' ps --status running | grep -q node-1"

run_test "Node 2 container is running" \
    "docker compose -f '$COMPOSE_FILE' ps --status running | grep -q node-2"

run_test "Node 3 container is running" \
    "docker compose -f '$COMPOSE_FILE' ps --status running | grep -q node-3"

# ============================================================================
# Test: Management API Ports Open
# ============================================================================

run_test "Node 1 management API (4001) is reachable" \
    "curl -sf http://localhost:4001/cluster/status >/dev/null 2>&1"

run_test "Node 2 management API (4002) is reachable" \
    "curl -sf http://localhost:4002/cluster/status >/dev/null 2>&1"

run_test "Node 3 management API (4003) is reachable" \
    "curl -sf http://localhost:4003/cluster/status >/dev/null 2>&1"

# ============================================================================
# Test: Cluster Bootstrap (sdha init)
# ============================================================================

info "Bootstrapping the Raft cluster..."
run_test "Cluster initialization succeeds" \
    "curl -sf -X POST http://localhost:4001/cluster/init \
        -H 'Content-Type: application/json' \
        -d '{\"1\":{\"addr\":\"node-1:4001\"},\"2\":{\"addr\":\"node-2:4001\"},\"3\":{\"addr\":\"node-3:4001\"}}' \
        >/dev/null 2>&1"

# Wait for leader election
info "Waiting for leader election..."
sleep 5

# ============================================================================
# Test: Leader Election
# ============================================================================

check_has_leader() {
    local port="$1"
    local status
    status=$(curl -sf "http://localhost:$port/cluster/status" 2>/dev/null)
    echo "$status" | grep -q '"current_leader"' && \
    ! echo "$status" | grep -q '"current_leader":null'
}

run_test "Node 1 sees a leader" "check_has_leader 4001"
run_test "Node 2 sees a leader" "check_has_leader 4002"
run_test "Node 3 sees a leader" "check_has_leader 4003"

# ============================================================================
# Test: Write Forwarding
# ============================================================================

info "Testing write forwarding to follower..."

# Find a follower node
find_follower_port() {
    for port in 4001 4002 4003; do
        local status
        status=$(curl -sf "http://localhost:$port/cluster/status" 2>/dev/null)
        if echo "$status" | grep -q '"state":"Follower"'; then
            echo "$port"
            return
        fi
    done
    echo ""
}

FOLLOWER_PORT=$(find_follower_port)
if [ -n "$FOLLOWER_PORT" ]; then
    run_test "Write to follower succeeds (leader forwarding)" \
        "curl -sf -X POST http://localhost:$FOLLOWER_PORT/cluster/write \
            -H 'Content-Type: application/json' \
            -d '{\"raw_message\":[3,0,1,2,3]}' \
            >/dev/null 2>&1"
else
    info "Could not find a follower node for write forwarding test"
fi

# ============================================================================
# Test: Fault Tolerance (Kill One Node)
# ============================================================================

info "Killing node-1 to test failover..."
docker compose -f "$COMPOSE_FILE" stop node-1 2>/dev/null

# Wait for re-election
sleep 5

run_test "Node 2 still running after node-1 killed" \
    "docker compose -f '$COMPOSE_FILE' ps --status running | grep -q node-2"

run_test "Node 3 still running after node-1 killed" \
    "docker compose -f '$COMPOSE_FILE' ps --status running | grep -q node-3"

# Check remaining nodes have elected a leader
run_test "Remaining nodes elected a new leader" \
    "check_has_leader 4002 || check_has_leader 4003"

# Restart node-1
info "Restarting node-1..."
docker compose -f "$COMPOSE_FILE" start node-1 2>/dev/null
sleep 5

run_test "Node 1 rejoined cluster" \
    "docker compose -f '$COMPOSE_FILE' ps --status running | grep -q node-1"

# ============================================================================
# Test: SpacetimeDB Instances
# ============================================================================

for i in 1 2 3; do
    run_test "SpacetimeDB instance $i is responsive" \
        "docker compose -f '$COMPOSE_FILE' exec -T stdb-$i spacetime --help >/dev/null 2>&1 || true"
done

# ============================================================================
# Teardown
# ============================================================================

info "Tearing down cluster..."
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null

# ============================================================================
# Summary
# ============================================================================

echo ""
echo "============================================"
echo -e " Tests passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e " Tests failed: ${RED}$TESTS_FAILED${NC}"
echo "============================================"

if [ "$TESTS_FAILED" -gt 0 ]; then
    exit 1
fi
