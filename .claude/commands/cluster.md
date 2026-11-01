# /cluster — spacetime-db-ha Cluster Management

Manage the local Docker Compose development cluster.

## Usage

The user will say `/cluster <action>` where action is one of:
- `up` — Start the 3-node cluster
- `down` — Stop the cluster and free resources
- `status` — Show cluster health (leader, followers, term)
- `bootstrap` — Initialize Raft consensus after first `up`
- `restart` — Stop then start the cluster
- `logs [node]` — Show recent logs (default: all nodes)

## Implementation

All commands operate from the repo root using `deploy/docker/docker-compose.yml`.

### `up`
```bash
docker compose -f deploy/docker/docker-compose.yml up -d
```
Wait 3 seconds, then verify all 6 containers are running with `docker ps`. Report status.

### `down`
```bash
docker compose -f deploy/docker/docker-compose.yml down -v
```
The `-v` flag removes volumes too. Verify no spacetime-db-ha containers remain.

### `status`
Query all 3 management API endpoints and display a summary table:
```bash
curl -s http://localhost:4001/cluster/status
curl -s http://localhost:4002/cluster/status
curl -s http://localhost:4003/cluster/status
```
Format output as:
```
Node 1: Leader  | Term 1 | Applied T1-N1-1
Node 2: Follower | Term 1 | Applied T1-N1-1
Node 3: Follower | Term 1 | Applied T1-N1-1
```

### `bootstrap`
```bash
curl -s -X POST http://localhost:4001/cluster/init \
  -H 'Content-Type: application/json' \
  -d '{"1":{"addr":"node-1:4001"},"2":{"addr":"node-2:4001"},"3":{"addr":"node-3:4001"}}'
```
Then immediately run `status` to verify.

### `restart`
Run `down` then `up` then wait and show `status`.

### `logs`
```bash
# All nodes:
docker compose -f deploy/docker/docker-compose.yml logs --tail 30
# Specific node:
docker logs docker-node-{N}-1 --tail 50
```

## Port reference
- **3001-3003**: Client WebSocket (connect your app here)
- **4001-4003**: Raft management API (bootstrap, status, add/remove nodes)
- **5001-5003**: SpacetimeDB HTTP (module publishing)
