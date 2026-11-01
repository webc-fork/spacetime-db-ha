# /production-deploy — Deploy spacetime-db-ha to Production Servers

Full production deployment of a spacetime-db-ha cluster across multiple bare-metal or VM servers with systemd, TLS, and load balancing.

## Usage

`/production-deploy <server_count> <server_list>`

Examples:
- `/production-deploy 3 server-a,server-b,server-c`
- `/production-deploy 5 10.0.1.1,10.0.1.2,10.0.1.3,10.0.1.4,10.0.1.5`

## Architecture per Server

Each server runs two processes:

| Process | Port | Purpose |
|---------|------|---------|
| SpacetimeDB | 3000 (localhost only) | The actual database engine |
| spacetime-db-ha proxy | 3001 (WS), 4001 (Raft API) | Consensus + client proxy |

```
Client → LB:443 → any server:3001 (spacetime-db-ha) → localhost:3000 (SpacetimeDB)
                       ↕ Raft consensus (port 4001) ↕
              other servers:4001 ←→ their localhost:3000
```

## Prerequisites

Before running this skill, verify the following. If any check fails, stop and tell the user what is missing.

### 1. Server count must be odd (for Raft quorum)

| Servers | Quorum | Tolerates failures | Recommended for |
|---------|--------|--------------------|-----------------|
| 3       | 2      | 1                  | Small production |
| 5       | 3      | 2                  | Production |
| 7       | 4      | 3                  | High availability |

If the user specifies an even number, warn them and suggest adding or removing one server.

### 2. Build machine requirements
- Rust toolchain (stable): `rustup show` should show a stable toolchain
- `wasm32-unknown-unknown` target: `rustup target list --installed` should include it
- `spacetime` CLI: `spacetime version` should return a version string

### 3. Network requirements

These ports must be open:

| Port | Protocol | Direction | Purpose |
|------|----------|-----------|---------|
| 4001 | TCP | Between all servers (mesh) | Raft consensus RPCs |
| 3001 | TCP | From load balancer to servers | Client WebSocket connections |
| 3000 | TCP | Localhost only | SpacetimeDB (never expose externally) |

Verify with:
```bash
# From server-a, test connectivity to server-b and server-c on port 4001:
nc -zv server-b 4001
nc -zv server-c 4001
```

### 4. Each server needs
- Linux with systemd
- A user account for running the services (e.g., `spacetime-db-ha`)
- Directories: `/opt/spacetime-db-ha/`, `/var/lib/spacetime-db-ha/`, `/etc/spacetime-db-ha/`
- SpacetimeDB standalone binary installed (from https://spacetimedb.com/install)

## Step 1: Build the Binary

On the build machine (can be any server or a CI box):

```bash
cd /path/to/spacetime-db-ha
cargo build --release
```

Verify the binary exists:
```bash
ls -la target/release/spacetime-db-ha
```

The binary is at `target/release/spacetime-db-ha`. It is a statically-linkable Linux binary (if built on Linux). If cross-compiling, use:
```bash
cargo build --release --target x86_64-unknown-linux-gnu
```

Also build the CLI tool:
```bash
cargo install --path crates/cli
```

Verify:
```bash
sdha --help
```

If either build fails, fix compilation errors before proceeding. Do not continue with a broken binary.

## Step 2: Prepare Each Server

Run the following on **every** server. This creates the required user, directories, and installs the binaries.

```bash
# Create service user (no login shell, no home directory needed)
sudo useradd --system --no-create-home --shell /usr/sbin/nologin spacetime-db-ha

# Create directories
sudo mkdir -p /opt/spacetime-db-ha/bin
sudo mkdir -p /var/lib/spacetime-db-ha
sudo mkdir -p /etc/spacetime-db-ha

# Set ownership
sudo chown spacetime-db-ha:spacetime-db-ha /var/lib/spacetime-db-ha
```

## Step 3: Copy Binaries to All Servers

From the build machine, copy the binary and CLI to each server:

```bash
SERVERS="server-a server-b server-c"

for server in $SERVERS; do
  echo "=== Deploying to $server ==="
  scp target/release/spacetime-db-ha ${server}:/tmp/spacetime-db-ha
  ssh ${server} "sudo mv /tmp/spacetime-db-ha /opt/spacetime-db-ha/bin/spacetime-db-ha && sudo chmod +x /opt/spacetime-db-ha/bin/spacetime-db-ha"
  echo "$server done"
done
```

Verify on each server:
```bash
ssh server-a "/opt/spacetime-db-ha/bin/spacetime-db-ha --help"
ssh server-b "/opt/spacetime-db-ha/bin/spacetime-db-ha --help"
ssh server-c "/opt/spacetime-db-ha/bin/spacetime-db-ha --help"
```

If any `scp` or `ssh` command fails, check SSH access and fix before continuing.

## Step 4: Create systemd Unit Files

### 4a. SpacetimeDB service

Create `/etc/systemd/system/spacetimedb.service` on **every** server:

```ini
[Unit]
Description=SpacetimeDB Standalone Instance
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=spacetime-db-ha
Group=spacetime-db-ha
ExecStart=/usr/local/bin/spacetimedb-standalone start --listen-addr 127.0.0.1:3000
Restart=always
RestartSec=5
LimitNOFILE=65536

# Security hardening
ProtectSystem=full
ProtectHome=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

**Note:** Adjust the `ExecStart` path to wherever `spacetimedb-standalone` is installed on your servers. Find it with `which spacetimedb-standalone` or `which spacetime`. The listen address is `127.0.0.1:3000` — localhost only, never exposed externally.

### 4b. spacetime-db-ha service

Create `/etc/systemd/system/spacetime-db-ha.service` on **every** server:

```ini
[Unit]
Description=spacetime-db-ha Consensus Proxy
After=spacetimedb.service
Requires=spacetimedb.service

[Service]
Type=simple
User=spacetime-db-ha
Group=spacetime-db-ha
EnvironmentFile=/etc/spacetime-db-ha/config.env
ExecStart=/opt/spacetime-db-ha/bin/spacetime-db-ha \
  --node-id ${SPACETIME_DB_HA_NODE_ID} \
  --listen-addr ${SPACETIME_DB_HA_LISTEN_ADDR} \
  --raft-addr ${SPACETIME_DB_HA_RAFT_ADDR} \
  --stdb-url ${SPACETIME_DB_HA_STDB_URL} \
  --peers ${SPACETIME_DB_HA_PEERS} \
  --data-dir ${SPACETIME_DB_HA_DATA_DIR}
# Uncomment for TLS (requires cert/key in /etc/spacetime-db-ha/certs/):
# ExecStart=/opt/spacetime-db-ha/bin/spacetime-db-ha \
#   --node-id ${SPACETIME_DB_HA_NODE_ID} \
#   --listen-addr ${SPACETIME_DB_HA_LISTEN_ADDR} \
#   --raft-addr ${SPACETIME_DB_HA_RAFT_ADDR} \
#   --stdb-url ${SPACETIME_DB_HA_STDB_URL} \
#   --peers ${SPACETIME_DB_HA_PEERS} \
#   --data-dir ${SPACETIME_DB_HA_DATA_DIR} \
#   --tls-cert ${SPACETIME_DB_HA_TLS_CERT} \
#   --tls-key ${SPACETIME_DB_HA_TLS_KEY} \
#   --tls-ca-cert ${SPACETIME_DB_HA_TLS_CA_CERT}
Restart=always
RestartSec=3
LimitNOFILE=65536
Environment=RUST_LOG=spacetime_db_ha=info,openraft=warn

# Security hardening
ProtectSystem=full
ProtectHome=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

Copy the unit files to all servers:
```bash
for server in $SERVERS; do
  scp spacetimedb.service ${server}:/tmp/spacetimedb.service
  scp spacetime-db-ha.service ${server}:/tmp/spacetime-db-ha.service
  ssh ${server} "sudo mv /tmp/spacetimedb.service /etc/systemd/system/spacetimedb.service && \
                 sudo mv /tmp/spacetime-db-ha.service /etc/systemd/system/spacetime-db-ha.service && \
                 sudo systemctl daemon-reload"
done
```

## Step 5: Create config.env for Each Server

Each server gets a **unique** config file at `/etc/spacetime-db-ha/config.env`. The `SPACETIME_DB_HA_NODE_ID` and `SPACETIME_DB_HA_PEERS` differ per server.

### 3-server example (server-a, server-b, server-c):

**server-a** (`/etc/spacetime-db-ha/config.env`):
```bash
SPACETIME_DB_HA_NODE_ID=1
SPACETIME_DB_HA_LISTEN_ADDR=0.0.0.0:3001
SPACETIME_DB_HA_RAFT_ADDR=0.0.0.0:4001
SPACETIME_DB_HA_STDB_URL=ws://127.0.0.1:3000
SPACETIME_DB_HA_PEERS=2=server-b:4001,3=server-c:4001
SPACETIME_DB_HA_DATA_DIR=/var/lib/spacetime-db-ha
RUST_LOG=spacetime_db_ha=info,openraft=warn

# TLS (optional — uncomment and set paths to enable encrypted inter-node communication)
# SPACETIME_DB_HA_TLS_CERT=/etc/spacetime-db-ha/certs/node.crt
# SPACETIME_DB_HA_TLS_KEY=/etc/spacetime-db-ha/certs/node.key
# SPACETIME_DB_HA_TLS_CA_CERT=/etc/spacetime-db-ha/certs/ca.crt
```

**server-b** (`/etc/spacetime-db-ha/config.env`):
```bash
SPACETIME_DB_HA_NODE_ID=2
SPACETIME_DB_HA_LISTEN_ADDR=0.0.0.0:3001
SPACETIME_DB_HA_RAFT_ADDR=0.0.0.0:4001
SPACETIME_DB_HA_STDB_URL=ws://127.0.0.1:3000
SPACETIME_DB_HA_PEERS=1=server-a:4001,3=server-c:4001
SPACETIME_DB_HA_DATA_DIR=/var/lib/spacetime-db-ha
RUST_LOG=spacetime_db_ha=info,openraft=warn

# TLS (optional)
# SPACETIME_DB_HA_TLS_CERT=/etc/spacetime-db-ha/certs/node.crt
# SPACETIME_DB_HA_TLS_KEY=/etc/spacetime-db-ha/certs/node.key
# SPACETIME_DB_HA_TLS_CA_CERT=/etc/spacetime-db-ha/certs/ca.crt
```

**server-c** (`/etc/spacetime-db-ha/config.env`):
```bash
SPACETIME_DB_HA_NODE_ID=3
SPACETIME_DB_HA_LISTEN_ADDR=0.0.0.0:3001
SPACETIME_DB_HA_RAFT_ADDR=0.0.0.0:4001
SPACETIME_DB_HA_STDB_URL=ws://127.0.0.1:3000
SPACETIME_DB_HA_PEERS=1=server-a:4001,2=server-b:4001
SPACETIME_DB_HA_DATA_DIR=/var/lib/spacetime-db-ha
RUST_LOG=spacetime_db_ha=info,openraft=warn

# TLS (optional)
# SPACETIME_DB_HA_TLS_CERT=/etc/spacetime-db-ha/certs/node.crt
# SPACETIME_DB_HA_TLS_KEY=/etc/spacetime-db-ha/certs/node.key
# SPACETIME_DB_HA_TLS_CA_CERT=/etc/spacetime-db-ha/certs/ca.crt
```

### Generating config.env programmatically

For N servers, generate each config with this pattern:

```bash
#!/bin/bash
# generate-configs.sh <server1> <server2> <server3> [server4] ...
SERVERS=("$@")
COUNT=${#SERVERS[@]}

for i in $(seq 0 $((COUNT - 1))); do
  NODE_ID=$((i + 1))
  SERVER=${SERVERS[$i]}

  # Build peers list: all servers EXCEPT this one
  PEERS=""
  for j in $(seq 0 $((COUNT - 1))); do
    if [ $j -ne $i ]; then
      PEER_ID=$((j + 1))
      PEER_HOST=${SERVERS[$j]}
      if [ -n "$PEERS" ]; then
        PEERS="${PEERS},"
      fi
      PEERS="${PEERS}${PEER_ID}=${PEER_HOST}:4001"
    fi
  done

  CONFIG="SPACETIME_DB_HA_NODE_ID=${NODE_ID}
SPACETIME_DB_HA_LISTEN_ADDR=0.0.0.0:3001
SPACETIME_DB_HA_RAFT_ADDR=0.0.0.0:4001
SPACETIME_DB_HA_STDB_URL=ws://127.0.0.1:3000
SPACETIME_DB_HA_PEERS=${PEERS}
SPACETIME_DB_HA_DATA_DIR=/var/lib/spacetime-db-ha
RUST_LOG=spacetime_db_ha=info,openraft=warn"

  echo "=== ${SERVER} (node ${NODE_ID}) ==="
  echo "$CONFIG"
  echo ""

  # Write and deploy
  echo "$CONFIG" > /tmp/config-${SERVER}.env
  scp /tmp/config-${SERVER}.env ${SERVER}:/tmp/config.env
  ssh ${SERVER} "sudo mv /tmp/config.env /etc/spacetime-db-ha/config.env && sudo chown spacetime-db-ha:spacetime-db-ha /etc/spacetime-db-ha/config.env && sudo chmod 600 /etc/spacetime-db-ha/config.env"
done
```

Usage:
```bash
./generate-configs.sh server-a server-b server-c
```

If using IP addresses instead of hostnames:
```bash
./generate-configs.sh 10.0.1.1 10.0.1.2 10.0.1.3
```

## Step 6: Start Services on All Servers

Start SpacetimeDB first, then spacetime-db-ha. Order matters because `spacetime-db-ha.service` depends on `spacetimedb.service`.

```bash
for server in $SERVERS; do
  echo "=== Starting services on $server ==="
  ssh ${server} "sudo systemctl enable spacetimedb spacetime-db-ha && \
                 sudo systemctl start spacetimedb && \
                 sleep 2 && \
                 sudo systemctl start spacetime-db-ha"
done
```

Verify both services are running on each server:
```bash
for server in $SERVERS; do
  echo "=== $server ==="
  ssh ${server} "systemctl is-active spacetimedb && systemctl is-active spacetime-db-ha"
done
```

Expected output per server:
```
active
active
```

If any service is not active, check its logs:
```bash
ssh <server> "sudo journalctl -u spacetimedb --no-pager -n 30"
ssh <server> "sudo journalctl -u spacetime-db-ha --no-pager -n 30"
```

Common startup failures:
- **SpacetimeDB "address already in use"** — another instance is running on port 3000. Kill it: `sudo ss -tlnp | grep 3000`
- **spacetime-db-ha "connection refused" to SpacetimeDB** — SpacetimeDB hasn't started yet. Increase the sleep or check SpacetimeDB logs.
- **spacetime-db-ha "address already in use" on 3001 or 4001** — another instance running. Check with `sudo ss -tlnp | grep -E '3001|4001'`

## Step 7: Publish the Module to All SpacetimeDB Instances

Every SpacetimeDB instance must have the **identical** module published. spacetime-db-ha replicates reducer calls, not module deployments.

```bash
MODULE_PATH="/path/to/your/module"
DB_NAME="your_module_name"

# Build the module first
cd ${MODULE_PATH} && cargo build --target wasm32-unknown-unknown --release

# Publish to each server's SpacetimeDB
# Note: SpacetimeDB listens on 3000 but only on localhost.
# You need SSH port forwarding or to temporarily expose it.

# Option A: SSH port forwarding (recommended — keeps port 3000 private)
for server in $SERVERS; do
  echo "=== Publishing to $server ==="
  ssh -L 13000:127.0.0.1:3000 ${server} -f -N  # Forward local 13000 → remote 3000
  spacetime publish --server http://localhost:13000 ${MODULE_PATH} ${DB_NAME}
  # Kill the SSH tunnel
  kill $(lsof -t -i:13000) 2>/dev/null
done

# Option B: Use spacetime CLI directly on each server
for server in $SERVERS; do
  scp -r ${MODULE_PATH} ${server}:/tmp/module
  ssh ${server} "cd /tmp/module && spacetime publish --server http://127.0.0.1:3000 . ${DB_NAME}"
done
```

Verify the module is deployed on all nodes:
```bash
for server in $SERVERS; do
  echo "=== $server ==="
  ssh ${server} "spacetime sql --server http://127.0.0.1:3000 ${DB_NAME} 'SELECT 1'"
done
```

Each should return a result, not an error. If any fail, re-publish to that server.

## Step 8: Bootstrap the Raft Cluster

Run this **exactly once** from any machine that can reach any server's port 4001:

### Option A: Using the sdha CLI

```bash
sdha init --nodes 1=server-a:4001 2=server-b:4001 3=server-c:4001
```

For 5 nodes:
```bash
sdha init --nodes 1=server-a:4001 2=server-b:4001 3=server-c:4001 4=server-d:4001 5=server-e:4001
```

### Option B: Using curl directly

```bash
curl -s -X POST http://server-a:4001/cluster/init \
  -H 'Content-Type: application/json' \
  -d '{
    "1": {"addr": "server-a:4001"},
    "2": {"addr": "server-b:4001"},
    "3": {"addr": "server-c:4001"}
  }'
```

This triggers leader election. Within 1-5 seconds, one node becomes the leader.

**Do NOT run init more than once.** If you get an error saying the cluster is already initialized, that is fine — it means bootstrap already happened.

## Step 9: Verify the Cluster

### 9a. Check Raft status on all nodes

```bash
sdha status --addr http://server-a:4001
sdha status --addr http://server-b:4001
sdha status --addr http://server-c:4001
```

Or with curl:
```bash
for server in server-a server-b server-c; do
  echo "=== $server ==="
  curl -s http://${server}:4001/cluster/status | python3 -m json.tool
done
```

Expected: one node shows `Leader`, all others show `Follower`. All are on the same `term`. All have the same `last_applied`.

### 9b. Test a WebSocket connection

```bash
# Using websocat (install: cargo install websocat)
echo '{"subscribe": "your_module_name"}' | websocat ws://server-a:3001/database/subscribe/your_module_name

# Using wscat (install: npm i -g wscat)
wscat -c ws://server-a:3001/database/subscribe/your_module_name
```

### 9c. Test write replication

Connect a SpacetimeDB client to one node, call a reducer, then verify the data appears on all nodes:
```bash
# Query via SpacetimeDB SQL on each server
for server in $SERVERS; do
  echo "=== $server ==="
  ssh ${server} "spacetime sql --server http://127.0.0.1:3000 ${DB_NAME} 'SELECT * FROM your_table LIMIT 5'"
done
```

All servers should return the same data.

## Step 10: Load Balancer Setup

The load balancer is **external** to spacetime-db-ha. It sits in front of the cluster and distributes client WebSocket connections across all nodes. spacetime-db-ha handles leader forwarding internally — clients can connect to any node (leader or follower).

### Option 1: nginx (WebSocket proxy)

Install nginx on a separate machine or use a managed nginx instance. Create `/etc/nginx/sites-available/spacetime-db-ha`:

```nginx
upstream spacetime-db-ha {
    # List all spacetime-db-ha nodes
    server server-a:3001;
    server server-b:3001;
    server server-c:3001;
}

server {
    listen 443 ssl;
    server_name your-domain.com;

    ssl_certificate /etc/ssl/certs/your-domain.crt;
    ssl_certificate_key /etc/ssl/private/your-domain.key;

    # WebSocket proxy
    location / {
        proxy_pass http://spacetime-db-ha;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket timeouts (keep alive)
        proxy_read_timeout 86400s;
        proxy_send_timeout 86400s;
    }
}
```

Enable and test:
```bash
sudo ln -s /etc/nginx/sites-available/spacetime-db-ha /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl reload nginx
```

Clients connect to: `wss://your-domain.com/database/subscribe/<module>`

### Option 2: HAProxy

Add to `/etc/haproxy/haproxy.cfg`:

```
frontend ws_front
    bind *:443 ssl crt /etc/ssl/certs/combined.pem
    default_backend ws_back

backend ws_back
    balance roundrobin
    option httpchk GET /cluster/health
    http-check expect status 200

    # Health check on Raft API port (4001) — /cluster/health returns 200 if leader known, 503 otherwise
    server node1 server-a:3001 check port 4001 inter 5s fall 3 rise 2
    server node2 server-b:3001 check port 4001 inter 5s fall 3 rise 2
    server node3 server-c:3001 check port 4001 inter 5s fall 3 rise 2
```

HAProxy checks health via the Raft management API (`GET /cluster/status` on port 4001). If a node is unreachable, HAProxy stops routing to it within 15 seconds (3 failed checks at 5-second intervals).

### Option 3: Cloud Load Balancers (AWS / GCP / Azure)

**AWS:**
- Create a Network Load Balancer (NLB) or Application Load Balancer (ALB)
- Target group: all servers on port 3001
- Health check: HTTP GET on port 4001, path `/cluster/health`, expect 200
- For WebSocket, ALB is preferred (handles upgrade headers automatically)
- Enable stickiness if your app needs it (not required for spacetime-db-ha correctness)

**GCP:**
- Use a TCP/SSL Load Balancer for WebSocket support
- Backend service: instance group with all servers, port 3001
- Health check: HTTP on port 4001, path `/cluster/status`

**Azure:**
- Use Azure Load Balancer (Standard SKU) or Application Gateway
- Backend pool: all servers on port 3001
- Health probe: HTTP GET on port 4001, path `/cluster/status`

### Load balancer verification

After setting up the load balancer, test through it:
```bash
# Test WebSocket upgrade through LB
wscat -c wss://your-domain.com/database/subscribe/your_module_name

# Test multiple connections land on different nodes (nginx/HAProxy)
for i in $(seq 1 6); do
  curl -s https://your-domain.com/cluster/status 2>/dev/null | grep -o '"node_id":[0-9]*'
done
```

You should see different node IDs, confirming round-robin distribution.

## Step 11: Monitoring

### Prometheus metrics

Each node exposes Prometheus metrics at `GET http://<node>:4001/metrics`. Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `spacetime-db-ha_raft_term` | Gauge | Current Raft term |
| `spacetime-db-ha_raft_is_leader` | Gauge | 1 if this node is leader, 0 otherwise |
| `spacetime-db-ha_connections_active` | Gauge | Active WebSocket client connections |
| `spacetime-db-ha_writes_total` | Counter | Total writes proposed through Raft |
| `spacetime-db-ha_reads_total` | Counter | Total reads forwarded directly |
| `spacetime-db-ha_write_latency_seconds` | Histogram | Write latency (Raft consensus + forward) |
| `spacetime-db-ha_entries_applied_total` | Counter | Raft log entries applied to state machine |

Add to your `prometheus.yml`:
```yaml
scrape_configs:
  - job_name: spacetime-db-ha
    static_configs:
      - targets:
          - server-a:4001
          - server-b:4001
          - server-c:4001
    metrics_path: /metrics
    scrape_interval: 10s
```

### Health check endpoints

| Endpoint | Returns | Use for |
|----------|---------|---------|
| `GET /cluster/health` | 200 if leader known, 503 otherwise | Load balancer health checks |
| `GET /cluster/leader` | `{"leader_id", "leader_addr", "this_node_is_leader"}` | Client reconnection / routing |
| `GET /cluster/status` | Full node status (state, term, membership) | Debugging / dashboards |

### systemd journal (primary log source)

```bash
# Follow spacetime-db-ha logs in real-time
sudo journalctl -u spacetime-db-ha -f

# Follow SpacetimeDB logs
sudo journalctl -u spacetimedb -f

# Show last 100 lines of spacetime-db-ha logs
sudo journalctl -u spacetime-db-ha -n 100 --no-pager

# Show only errors and warnings
sudo journalctl -u spacetime-db-ha -p warning --no-pager
```

### Cluster health check script

Create `/opt/spacetime-db-ha/bin/health-check.sh` on one server (or a monitoring box):

```bash
#!/bin/bash
# health-check.sh — Quick cluster health overview
SERVERS="server-a server-b server-c"
LEADER_COUNT=0
HEALTHY=0
TOTAL=0

for server in $SERVERS; do
  TOTAL=$((TOTAL + 1))
  STATUS=$(curl -s --max-time 3 http://${server}:4001/cluster/status 2>/dev/null)
  if [ $? -ne 0 ]; then
    echo "FAIL: $server — unreachable"
    continue
  fi

  HEALTHY=$((HEALTHY + 1))
  STATE=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','unknown'))" 2>/dev/null)
  TERM=$(echo "$STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('current_term','?'))" 2>/dev/null)

  if [ "$STATE" = "Leader" ]; then
    LEADER_COUNT=$((LEADER_COUNT + 1))
  fi
  echo "OK:   $server — $STATE (term $TERM)"
done

echo ""
echo "Summary: ${HEALTHY}/${TOTAL} healthy, ${LEADER_COUNT} leader(s)"

if [ $LEADER_COUNT -ne 1 ]; then
  echo "WARNING: Expected exactly 1 leader, found $LEADER_COUNT"
fi
if [ $HEALTHY -lt $((TOTAL / 2 + 1)) ]; then
  echo "CRITICAL: Cluster has lost quorum (${HEALTHY}/${TOTAL} nodes healthy, need $((TOTAL / 2 + 1)))"
fi
```

### What to watch for

| Signal | Meaning | Action |
|--------|---------|--------|
| Leader changes frequently | Network instability or overloaded leader | Check network latency between servers, check CPU/memory |
| High term numbers | Repeated elections | Same as above — Raft keeps incrementing term on each election |
| Split brain (2 leaders) | Network partition | Check firewall rules, verify port 4001 connectivity |
| `last_applied` diverges | Replication lag on a follower | Check that follower can reach leader on port 4001 |
| SpacetimeDB OOM | Module using too much memory | Increase server RAM or optimize module |
| "connection refused" in logs | SpacetimeDB crashed | Check `journalctl -u spacetimedb`, restart if needed |

### Optional: Cron-based alerting

```bash
# Add to crontab on a monitoring server
# Runs every minute, logs warnings to syslog
* * * * * /opt/spacetime-db-ha/bin/health-check.sh 2>&1 | logger -t spacetime-db-ha-health
```

## Complete Deployment Checklist

Run through this checklist to confirm everything is deployed correctly:

```
[ ] Binary built and copied to all servers
[ ] systemd unit files installed on all servers
[ ] config.env created with correct node ID and peers on each server
[ ] SpacetimeDB running on all servers (port 3000, localhost only)
[ ] spacetime-db-ha running on all servers (ports 3001 + 4001)
[ ] Module published to every SpacetimeDB instance
[ ] Cluster bootstrapped (sdha init, run once)
[ ] Exactly 1 leader, N-1 followers visible in sdha status
[ ] All nodes on the same term
[ ] WebSocket connection works to each node directly
[ ] Load balancer configured and routing to all nodes
[ ] WebSocket connection works through the load balancer
[ ] Health check script deployed and running
[ ] Firewall: port 4001 open between servers, port 3001 open from LB only
[ ] Firewall: port 3000 NOT exposed externally
```

## Troubleshooting

| Problem | Diagnosis | Fix |
|---------|-----------|-----|
| "No leader" after bootstrap | `sdha init` not run, or all nodes just restarted | Run `sdha init` again (safe to retry) |
| "Connection refused" on 4001 | spacetime-db-ha not running or firewall blocking | `systemctl status spacetime-db-ha`, check firewall |
| Module missing on one node | Forgot to publish to that SpacetimeDB instance | `spacetime publish --server http://127.0.0.1:3000 ...` on that server |
| Writes succeed but data missing on some nodes | Module not identical across nodes | Re-publish the same module to all nodes |
| WebSocket 404 through load balancer | LB not forwarding upgrade headers | Add `proxy_set_header Upgrade/Connection` in nginx, or use ALB on AWS |
| Cluster works but clients disconnect on leader change | Expected — clients on the old leader lose connection | Use load balancer so clients reconnect to a healthy node automatically |
| SpacetimeDB high CPU | Module reducers are expensive | Profile the module, optimize hot reducers |
| Disk full on `/var/lib/spacetime-db-ha` | Raft log growing without compaction | Check snapshot configuration, increase disk |

## Rolling Upgrades

To upgrade the spacetime-db-ha binary without downtime:

```bash
# Upgrade one node at a time, starting with followers
# 1. Identify the leader
sdha status --addr http://server-a:4001

# 2. Upgrade a FOLLOWER first
ssh server-b "sudo systemctl stop spacetime-db-ha && \
  sudo cp /opt/spacetime-db-ha/bin/spacetime-db-ha /opt/spacetime-db-ha/bin/spacetime-db-ha.bak"
scp target/release/spacetime-db-ha server-b:/tmp/spacetime-db-ha
ssh server-b "sudo mv /tmp/spacetime-db-ha /opt/spacetime-db-ha/bin/spacetime-db-ha && \
  sudo chmod +x /opt/spacetime-db-ha/bin/spacetime-db-ha && \
  sudo systemctl start spacetime-db-ha"

# 3. Wait for it to rejoin and verify
sleep 5
sdha status --addr http://server-b:4001

# 4. Repeat for the other follower(s)
# 5. Upgrade the leader LAST (this triggers a leader election, which is fine)
```

Never upgrade all nodes simultaneously. Always keep quorum operational during the upgrade.
