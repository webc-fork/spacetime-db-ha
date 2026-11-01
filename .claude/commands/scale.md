# /scale — Scale the spacetime-db-ha Cluster

Add or remove nodes from the running cluster. Handles docker-compose entries, SpacetimeDB instances, module deployment, and Raft membership changes.

## Usage

- `/scale up [N]` — Add nodes to reach N total (default: +2, so 3→5)
- `/scale down [N]` — Remove nodes down to N total (minimum 3)
- `/scale to 7` — Scale to exactly 7 nodes

## Important Rules

- **Always use odd numbers** (3, 5, 7, 9...) for proper Raft quorum
  - 3 nodes → tolerates 1 failure
  - 5 nodes → tolerates 2 failures
  - 7 nodes → tolerates 3 failures
- **Never scale below 3** — minimum for Raft consensus
- The cluster must be running and bootstrapped before scaling

## Scale Up Procedure

### 1. Determine new node IDs and ports

Existing nodes use sequential IDs and port ranges:
- Node N: WS port `3000+N`, Raft port `4000+N`, SpacetimeDB port `5000+N`

For example, scaling from 3 to 5 adds nodes 4 and 5:
| Node | WS Port | Raft Port | StDB Port |
|------|---------|-----------|-----------|
| 4    | 3004    | 4004      | 5004      |
| 5    | 3005    | 4005      | 5005      |

### 2. Add new services to docker-compose.yml

For each new node N, add two services to `deploy/docker/docker-compose.yml`:

**SpacetimeDB instance:**
```yaml
  stdb-N:
    image: clockworklabs/spacetime:latest
    command: start --listen-addr 0.0.0.0:3000
    ports:
      - "500N:3000"
    volumes:
      - stdb-data-N:/app/data
    networks:
      - raft-net
```

**spacetime-db-ha proxy node:**
```yaml
  node-N:
    build:
      context: ../..
      dockerfile: deploy/docker/Dockerfile
    environment:
      SPACETIME_DB_HA_NODE_ID: "N"
      SPACETIME_DB_HA_LISTEN_ADDR: "0.0.0.0:3001"
      SPACETIME_DB_HA_RAFT_ADDR: "0.0.0.0:4001"
      SPACETIME_DB_HA_STDB_URL: "ws://stdb-N:3000"
      SPACETIME_DB_HA_PEERS: "<comma-separated list of ALL OTHER nodes: id=node-id:4001>"
      RUST_LOG: "spacetime_db_ha=info,openraft=warn"
    ports:
      - "300N:3001"
      - "400N:4001"
    depends_on:
      - stdb-N
    networks:
      - raft-net
```

Also add the volume:
```yaml
volumes:
  stdb-data-N:
```

**SPACETIME_DB_HA_PEERS format:** comma-separated `id=hostname:port` for ALL other nodes.
Example for node 4 in a 5-node cluster: `"1=node-1:4001,2=node-2:4001,3=node-3:4001,5=node-5:4001"`

### 3. Start only the new containers

```bash
docker compose -f deploy/docker/docker-compose.yml up -d stdb-N node-N
```

Do NOT restart existing nodes — the cluster stays live.

### 4. Deploy the module to new SpacetimeDB instances

All nodes must have identical modules. Publish to each new SpacetimeDB:
```bash
spacetime publish --server http://localhost:500N <module_path> <db_name>
```

If you don't know the module path/name, check existing nodes:
```bash
spacetime sql --server http://localhost:5001 <db_name> "SELECT 1"
```

### 5. Add new nodes to Raft membership

For each new node, run against any existing cluster member:
```bash
curl -s -X POST http://localhost:4001/cluster/add-node \
  -H 'Content-Type: application/json' \
  -d '{"node_id": N, "addr": "node-N:4001"}'
```

Or via the CLI:
```bash
cargo run -p sdha -- add-node --node-id N --addr node-N:4001 --cluster http://localhost:4001
```

### 6. Verify

```bash
curl -s http://localhost:4001/cluster/status
```

Check that `membership` now shows all nodes including the new ones.

## Scale Down Procedure

### 1. Remove nodes from Raft membership FIRST

Always remove from Raft before stopping containers:
```bash
curl -s -X POST http://localhost:4001/cluster/remove-node \
  -H 'Content-Type: application/json' \
  -d '{"node_id": N}'
```

Or via CLI:
```bash
cargo run -p sdha -- remove-node --node-id N --cluster http://localhost:4001
```

**Never remove the current leader** — remove followers first. Check who's leader with:
```bash
curl -s http://localhost:4001/cluster/status | grep current_leader
```

### 2. Stop and remove the containers

```bash
docker compose -f deploy/docker/docker-compose.yml stop node-N stdb-N
docker compose -f deploy/docker/docker-compose.yml rm -f node-N stdb-N
```

### 3. Clean up docker-compose.yml

Remove the `stdb-N`, `node-N` service blocks and `stdb-data-N` volume entry.

### 4. Verify

```bash
curl -s http://localhost:4001/cluster/status
```

Confirm membership no longer includes the removed nodes.

## Example: Scale 3 → 5

```
Current: nodes 1, 2, 3 (node 1 is leader)
Target:  nodes 1, 2, 3, 4, 5

Steps:
1. Edit docker-compose.yml — add stdb-4, node-4, stdb-5, node-5
2. docker compose up -d stdb-4 node-4 stdb-5 node-5
3. Publish module to http://localhost:5004 and http://localhost:5005
4. curl POST /cluster/add-node for node 4
5. curl POST /cluster/add-node for node 5
6. Verify: all 5 nodes in membership, leader stable
```

## Example: Scale 5 → 3

```
Current: nodes 1, 2, 3, 4, 5 (node 1 is leader)
Target:  nodes 1, 2, 3

Steps:
1. Remove node 5 from Raft: POST /cluster/remove-node {"node_id": 5}
2. Remove node 4 from Raft: POST /cluster/remove-node {"node_id": 4}
3. Stop containers: docker compose stop node-5 stdb-5 node-4 stdb-4
4. Remove from docker-compose.yml
5. Verify: membership shows {1, 2, 3}
```

## Quorum Reference

| Nodes | Quorum | Tolerates | Use case |
|-------|--------|-----------|----------|
| 3     | 2      | 1 failure | Dev / small prod |
| 5     | 3      | 2 failures | Production |
| 7     | 4      | 3 failures | High availability |
| 9     | 5      | 4 failures | Critical systems |
