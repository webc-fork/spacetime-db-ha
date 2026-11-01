# End-to-End Tests

These tests verify spacetime-db-ha works as a complete system.

## Prerequisites

- Docker and Docker Compose
- `spacetime` CLI (for publishing test modules)

## Running

### Docker Cluster Test

Starts a 3-node cluster, verifies health, tests failover:

```bash
chmod +x tests/e2e/docker_cluster_test.sh
./tests/e2e/docker_cluster_test.sh
```

### What It Tests

1. All 3 nodes start and are reachable
2. WebSocket ports are open on all nodes
3. SpacetimeDB instances are healthy
4. Cluster bootstraps and elects a leader
5. Write forwarding through Raft consensus
6. Killing a node doesn't kill the cluster (re-election)
7. Dead node can rejoin and catch up

### Additional Test Coverage (manual / future automation)

- Prometheus metrics endpoint (`GET /metrics` on port 4001)
- Health check endpoint (`GET /cluster/health`)
- Leader discovery endpoint (`GET /cluster/leader`)
- Client reconnection hints in WebSocket close frames on shutdown
- TLS inter-node communication (requires cert setup)
- Graceful shutdown with connection draining
- Multi-shard: create shard, add module route, verify writes go to correct shard
- Multi-shard: per-shard status and leader election
- Multi-shard: membership changes propagate to all shards
