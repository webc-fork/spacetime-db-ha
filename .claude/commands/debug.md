# /debug — Diagnose spacetime-db-ha Cluster Issues

Comprehensive cluster diagnostics when something isn't working.

## Usage

`/debug` — Run full diagnostics
`/debug <topic>` — Focus on: `raft`, `websocket`, `stdb`, `network`

## Diagnostic Steps

### 1. Container health
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "node-|stdb-"
```
Check all 6 containers are running. If any are restarting, check their logs.

### 2. Cluster consensus state
Query all 3 nodes' management API:
```bash
for port in 4001 4002 4003; do
  echo "=== Node on port $port ==="
  curl -s http://localhost:$port/cluster/status 2>/dev/null || echo "UNREACHABLE"
done
```

Check for:
- **No leader** — Cluster not bootstrapped. Run `/cluster bootstrap`.
- **Split brain** — Multiple leaders. Likely network partition. Check `docker network inspect docker_raft-net`.
- **Stale term** — One node has lower term. It may have been partitioned and rejoined.
- **Different last_applied** — Replication lag. Check Raft logs for append errors.

### 3. Node logs (look for panics and errors)
```bash
for node in 1 2 3; do
  echo "=== node-$node ==="
  docker logs docker-node-$node-1 --tail 30 2>&1 | grep -iE "error|panic|warn|failed"
done
```

### 4. SpacetimeDB health
```bash
for port in 5001 5002 5003; do
  echo -n "stdb on $port: "
  curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/health 2>/dev/null || echo "UNREACHABLE"
  echo ""
done
```

### 5. WebSocket connectivity test
```bash
# Test if proxy accepts WebSocket upgrades (requires wscat or websocat)
# wscat -c ws://localhost:3001/database/subscribe/test --no-color -x "" 2>&1 | head -5
```

### 6. Network connectivity between containers
```bash
docker exec docker-node-1-1 curl -s http://node-2:4001/cluster/status 2>/dev/null | head -1
docker exec docker-node-1-1 curl -s http://node-3:4001/cluster/status 2>/dev/null | head -1
```

## Common Issues

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| "Forwarder: SpacetimeDB connect failed, 404" | Old image without path fix | Rebuild: `docker compose build --no-cache` |
| No leader after bootstrap | apply() panic (old bug) | Rebuild with latest code |
| Re-election doesn't happen | Raft core crashed silently | Check logs for panics, rebuild |
| Client gets 404 | Wrong WebSocket URL | Use `ws://localhost:3001/database/subscribe/<MODULE>` |
| "Connection refused" on 4001 | Container not started or crashed | `docker logs docker-node-1-1` |

## Output Format
Present findings as a summary with clear pass/fail indicators and actionable next steps.
