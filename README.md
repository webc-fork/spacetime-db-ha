<div align="center">

[中文](./doc/zh/README.md)

# spacetime-db-ha

### Make SpacetimeDB indestructible.

Distributed, fault-tolerant clustering for [SpacetimeDB](https://spacetimedb.com) — automatic replication, failover, and horizontal scaling without modifying SpacetimeDB itself.

[![CI](https://github.com/webc-fork/spacetime-db-ha/actions/workflows/ci.yml/badge.svg)](https://github.com/webc-fork/spacetime-db-ha/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85+-orange.svg)](https://www.rust-lang.org)

[Quick Start](#quick-start) | [How It Works](#how-it-works) | [Deploy Anywhere](#deploy-anywhere) | [Roadmap](#roadmap)

</div>

---

SpacetimeDB collapses your entire backend into the database — no servers, no microservices, just WASM modules running at memory speed. But it runs on a single node. If that node dies, everything dies.

**spacetime-db-ha fixes that.** It wraps SpacetimeDB in a [Raft consensus](https://raft.github.io/) layer over [Zenoh](https://zenoh.io/) + plaintext QUIC (`udp/<addr>#rel=1`) and [`bitcode`](https://github.com/SoftbearStudios/bitcode) serialization, replicating every write across multiple nodes with a single embedded [`fjall`](https://github.com/fjall-rs/fjall) LSM-tree storage engine per shard. Kill any node — the cluster keeps running. No data loss. No downtime.

```
Your app (unchanged)
       │
       ▼ (Zenoh + plaintext QUIC + bitcode)
  spacetime-db-ha ──── Raft ──── spacetime-db-ha ──── Raft ──── spacetime-db-ha
       │                        │                        │
  SpacetimeDB              SpacetimeDB              SpacetimeDB
  (identical)              (identical)              (identical)
```

## Quick Start

### Option 1: Docker Compose (easiest)

Spins up a 3-node cluster locally in seconds.

```bash
git clone https://github.com/webc-fork/spacetime-db-ha.git
cd spacetime-db-ha/deploy/docker
docker compose up
```

Connect your Zenoh client to `udp/127.0.0.1:3001#rel=1` (or `:3002`, `:3003` — any node works).

### Option 2: Pre-built Binary

Download from [Releases](https://github.com/webc-fork/spacetime-db-ha/releases) for your platform:

| Platform | Binary |
|----------|--------|
| Linux x86_64 | `spacetime-db-ha-x86_64-unknown-linux-gnu` |
| Windows x86_64 | `spacetime-db-ha-x86_64-pc-windows-msvc.exe` |
| macOS ARM | `spacetime-db-ha-aarch64-apple-darwin` |

```bash
# Start 3 nodes (each alongside a SpacetimeDB instance)
spacetime-db-ha --node-id 1 --peers 2=node2:4001,3=node3:4001 --stdb-url 127.0.0.1:3000
spacetime-db-ha --node-id 2 --peers 1=node1:4001,3=node3:4001 --stdb-url 127.0.0.1:3010
spacetime-db-ha --node-id 3 --peers 1=node1:4001,2=node2:4001 --stdb-url 127.0.0.1:3020
```

### Option 3: Build from Source

```bash
# Requires Rust 1.85+ (nightly recommended)
cargo install --path crates/proxy
cargo install --path crates/cli
spacetime-db-ha --help
sdha --help
```

## How It Works

SpacetimeDB reducers are **deterministic and replayable** — given the same ordered sequence of reducer calls, multiple SpacetimeDB instances converge to identical state.

spacetime-db-ha uses [Raft consensus](https://raft.github.io/) over Zenoh + plaintext QUIC to create that globally ordered sequence across a cluster.

### Single-Byte Tag Classification

The proxy inspects **a single byte** from each BSATN payload to classify it:

```
payload[0] = BSATN tag byte (SpacetimeDB v2 protocol)

Tag 0: Subscribe        → READ  → wait causal read barrier, then forward directly
Tag 1: Unsubscribe      → READ  → wait causal read barrier, then forward directly
Tag 2: OneOffQuery      → READ  → wait causal read barrier, then forward directly
Tag 3: CallReducer      → WRITE → replicate through Raft, then deliver in log order
Tag 4: CallProcedure    → WRITE → replicate through Raft, then deliver in log order
```

- **Writes** go through Raft consensus first, persist to the local `fjall` database (`raft_log` + `state_machine`), and are delivered to local SpacetimeDB in strict Raft log index order (`last_delivered` watermark)
- **Reads** wait for local `applied` to catch up to the node's known `last_log_index` (causal read barrier), then go straight to the local SpacetimeDB
- The entire BSATN message is replicated as an **opaque blob** — no BSATN schema deserialization needed

### Delivery & Consistency Guarantees

- **Unified At-Least-Once Delivery**: Every committed write is persisted in `fjall` before forwarding. A per-shard `last_delivered` watermark tracks upstream delivery; on restart, any committed entries in `(last_delivered, last_applied]` are automatically replayed before accepting new traffic.
- **Strict Log-Index Ordering**: Even when multiple clients write concurrently to the same shard, upstream delivery waits for `log_id.index - 1` to be delivered before sending `log_id.index`.
- **Single-Engine Atomic Storage**: Each shard shares one `fjall::Database` (`{data_dir}/shard-{id}/fjall`) with three keyspaces (`raft_log`, `raft_meta`, `state_machine`), ensuring atomic cross-keyspace `WriteBatch` log purges and startup watermark alignment.
- **Bounded Backpressure**: All internal queues (`FORWARDER_CHANNEL_CAPACITY = 4096`, `CONFIG_CHANNEL_CAPACITY = 256`, `UPSTREAM_QUEUE_CAPACITY = 1024`) are bounded to prevent unbounded memory growth under slow upstreams.

### Architectural Boundaries & Assumptions

1. **Deterministic & Idempotent Reducers**: Because crash recovery replays `(last_delivered, last_applied]` under at-least-once semantics, SpacetimeDB reducers should be deterministic and idempotent.
2. **Shard 0 Metadata Plane**: Shard creation, deletion, and module routing (`ShardConfigCommand`) are replicated through Shard 0's Raft group (prefixed with `SHARD_CONFIG_MAGIC = [0xFF, b'S', b'F', b'G']`). If Shard 0 loses quorum, shard topology changes pause while existing shards continue serving traffic.
3. **Accounting Snapshots**: Raft snapshots store consensus metadata (`last_applied`, `last_delivered`, membership, shard routes). SpacetimeDB business tables live inside each node's local SpacetimeDB instance.

### What Happens When a Node Dies

```
Before:  Node 1 (LEADER) ←→ Node 2 ←→ Node 3
                                         │
                                     [Node 3 dies]

After:   Node 1 (LEADER) ←→ Node 2
         (still serving, zero downtime)

Later:   Node 1 (LEADER) ←→ Node 2 ←→ Node 3 (rejoins, catches up)
```

1. Raft detects the failure (missed heartbeats, ~1-2 seconds)
2. If the leader died, remaining nodes elect a new leader (<5 seconds)
3. Cluster continues serving with the remaining majority
4. When the dead node comes back, it catches up from the Raft log and replays any undelivered entries
5. **Zero data loss** — Raft only acknowledges writes after majority commit

## Deploy Anywhere

spacetime-db-ha is a single binary. Run it wherever you want.

### Docker Compose (local dev / small deployments)

```bash
cd deploy/docker
docker compose up
```

3 SpacetimeDB instances + 3 spacetime-db-ha proxies. Ports 3001-3003 exposed.

### Docker (single node)

```bash
docker run -p 3001:3001/udp -p 4001:4001/udp -p 4001:4001/tcp \
  -e SPACETIME_DB_HA_NODE_ID=1 \
  -e SPACETIME_DB_HA_STDB_URL=host.docker.internal:3000 \
  -e SPACETIME_DB_HA_PEERS=2=node2:4001,3=node3:4001 \
  ghcr.io/webc-fork/spacetime-db-ha:latest
```

### Windows

Download `spacetime-db-ha.exe` from [Releases](https://github.com/webc-fork/spacetime-db-ha/releases) or build from source:

```powershell
cargo build --release
.\target\release\spacetime-db-ha.exe --node-id 1 --stdb-url 127.0.0.1:3000 --peers 2=node2:4001,3=node3:4001
```

### Linux / macOS

```bash
cargo build --release
./target/release/spacetime-db-ha --node-id 1 --stdb-url 127.0.0.1:3000 --peers 2=node2:4001,3=node3:4001
```

### Kubernetes (K8s / K3s)

```bash
helm install spacetime-db-ha deploy/helm/spacetime-db-ha \
  --set replicas=3 \
  --set spacetimedb.image=clockworklabs/spacetime:latest
```

Or apply the raw manifests:

```bash
kubectl apply -f deploy/k3s/
```

<details>
<summary>Example K8s manifest</summary>

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: spacetime-db-ha
spec:
  serviceName: spacetime-db-ha-raft
  replicas: 3
  selector:
    matchLabels:
      app: spacetime-db-ha
  template:
    metadata:
      labels:
        app: spacetime-db-ha
    spec:
      containers:
        - name: spacetime-db-ha
          image: ghcr.io/webc-fork/spacetime-db-ha:latest
          ports:
            - containerPort: 3001  # Client Zenoh QUIC (UDP)
              protocol: UDP
              name: client-quic
            - containerPort: 4001  # Raft Zenoh QUIC (UDP)
              protocol: UDP
              name: raft-quic
            - containerPort: 4001  # Health & Prometheus Metrics (TCP)
              protocol: TCP
              name: metrics-tcp
          env:
            - name: SPACETIME_DB_HA_NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: SPACETIME_DB_HA_STDB_URL
              value: "127.0.0.1:3000"
            - name: SPACETIME_DB_HA_PEERS
              value: "1=spacetime-db-ha-0.spacetime-db-ha-raft:4001,2=spacetime-db-ha-1.spacetime-db-ha-raft:4001,3=spacetime-db-ha-2.spacetime-db-ha-raft:4001"
        - name: spacetimedb
          image: clockworklabs/spacetime:latest
          command: ["spacetimedb-standalone", "start", "--listen-addr", "0.0.0.0:3000"]
          ports:
            - containerPort: 3000
          volumeMounts:
            - name: stdb-data
              mountPath: /app/data
  volumeClaimTemplates:
    - metadata:
        name: stdb-data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
```

</details>

### Bare Metal (3 machines, no orchestrator)

```bash
# Machine 1 (192.168.1.10)
spacetime-db-ha --node-id 1 --listen-addr 0.0.0.0:3001 --raft-addr 0.0.0.0:4001 \
  --raft-advertise-addr 192.168.1.10:4001 \
  --stdb-url 127.0.0.1:3000 --peers 2=192.168.1.11:4001,3=192.168.1.12:4001

# Machine 2 (192.168.1.11)
spacetime-db-ha --node-id 2 --listen-addr 0.0.0.0:3001 --raft-addr 0.0.0.0:4001 \
  --raft-advertise-addr 192.168.1.11:4001 \
  --stdb-url 127.0.0.1:3000 --peers 1=192.168.1.10:4001,3=192.168.1.12:4001

# Machine 3 (192.168.1.12)
spacetime-db-ha --node-id 3 --listen-addr 0.0.0.0:3001 --raft-addr 0.0.0.0:4001 \
  --raft-advertise-addr 192.168.1.12:4001 \
  --stdb-url 127.0.0.1:3000 --peers 1=192.168.1.10:4001,2=192.168.1.11:4001
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     spacetime-db-ha Node                     │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │        Zenoh Client Proxy (udp/:3001#rel=1)            │  │
│  │                                                        │  │
│  │  Client msg ──► Read tag byte ──► Write? ──────────┐   │  │
│  │                      │                             │   │  │
│  │                      │ Read (wait read barrier)    │   │  │
│  │                      ▼                             ▼   │  │
│  │              Forward directly              Raft consensus │
│  │              to SpacetimeDB           (Zenoh QUIC + fjall)│
│  │                      │                             │   │  │
│  │                      │         Log-ordered deliver │   │  │
│  │                      ▼                        ▼    │   │  │
│  │              ┌──────────────────────────────────┐  │   │  │
│  │              │    Local SpacetimeDB (:3000)     │  │   │  │
│  │              │    (identical on all nodes)      │  │   │  │
│  │              └──────────────────────────────────┘  │   │  │
│  └────────────────────────────────────────────────────┘   │  │
│                                                           │  │
│  ┌────────────────────────────────────────────────────┐   │  │
│  │  Raft & Mgmt Transport (udp/:4001#rel=1 + TCP)     │   │  │
│  │  Zenoh QUIC RPCs (bitcode) + /metrics & /health    │   │  │
│  └────────────────────────────────────────────────────┘   │  │
└───────────────────────────────────────────────────────────┘  │
                                                               │
        ◄──── Zenoh + Plaintext QUIC + bitcode ────►           │
                                                               │
┌──────────────────┐          ┌──────────────────┐             │
│ spacetime-db-ha  │          │ spacetime-db-ha  │             │
│ Node 2           │          │ Node 3           │             │
│ (same layout)    │          │ (same layout)    │             │
└──────────────────┘          └──────────────────┘             │
```

## Use Cases

### MMO Game Servers
Thousands of players across sharded game zones. Each zone is a SpacetimeDB module backed by its own Raft group. Kill a node — players barely notice. Add nodes — the world gets bigger.

### AI Agent Swarms
Hundreds of autonomous agents with shared state. Agents call reducers to update shared knowledge, subscribe to tables for real-time updates. The swarm is indestructible — pull the plug on any node, agents reconnect and keep going.

### Real-Time Collaboration
Shared documents, cursors, presence. SpacetimeDB's subscription system pushes deltas to all connected clients. spacetime-db-ha ensures the backing state survives failures.

### Any SpacetimeDB App That Can't Afford Downtime
If your app matters, it should be replicated. spacetime-db-ha makes that a config change, not a rewrite.

## CLI Tool (`sdha`)

All cluster and shard operations communicate over Zenoh + plaintext QUIC using `bitcode` payloads:

```bash
# Cluster management
sdha init --nodes 1=127.0.0.1:4001 2=127.0.0.1:4002 3=127.0.0.1:4003  # Bootstrap cluster (once)
sdha status --addr 127.0.0.1:4001                                     # Show node status
sdha add-node --node-id 4 --addr 127.0.0.1:4004 --cluster 127.0.0.1:4001
sdha remove-node --node-id 2 --cluster 127.0.0.1:4001

# Shard management (Multi-Raft)
sdha create-shard --shard-id 1 --cluster 127.0.0.1:4001
sdha remove-shard --shard-id 1 --cluster 127.0.0.1:4001
sdha add-route --module my_game --shard-id 1 --cluster 127.0.0.1:4001
sdha remove-route --module my_game --cluster 127.0.0.1:4001
sdha list-shards --addr 127.0.0.1:4001
sdha shard-status --shard-id 1 --addr 127.0.0.1:4001

# Module deployment across SpacetimeDB nodes
sdha deploy --module ./my_module.wasm --name my_game --servers 127.0.0.1:3000 127.0.0.1:3010
```

## Configuration

All options can be set via CLI flags or environment variables:

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--node-id` | `SPACETIME_DB_HA_NODE_ID` | *required* | Unique node identifier (`u64`) |
| `--listen-addr` | `SPACETIME_DB_HA_LISTEN_ADDR` | `0.0.0.0:3001` | Client Zenoh plaintext QUIC listen address (`udp/<addr>#rel=1`) |
| `--raft-addr` | `SPACETIME_DB_HA_RAFT_ADDR` | `0.0.0.0:4001` | Raft & management Zenoh QUIC (UDP) + `/metrics` & `/cluster/health` (TCP) |
| `--raft-advertise-addr` | `SPACETIME_DB_HA_RAFT_ADVERTISE_ADDR` | *falls back to `--raft-addr`* | Peer-reachable Raft address advertised in membership tables |
| `--stdb-url` | `SPACETIME_DB_HA_STDB_URL` | `127.0.0.1:3000` | Local SpacetimeDB Zenoh plaintext QUIC endpoint |
| `--peers` | `SPACETIME_DB_HA_PEERS` | `""` | Comma-separated peer Raft addresses (`id=host:port` or `host:port`) |
| `--data-dir` | `SPACETIME_DB_HA_DATA_DIR` | `./data` | Persistent data directory (`{data_dir}/shard-{id}/fjall`) |

## Production Features

spacetime-db-ha includes everything needed for production deployments:

- **Zenoh + Plaintext QUIC Transport** — low-latency `udp/<addr>#rel=1` transport for client proxy, upstream forwarding, Raft replication, and cluster management without HTTP/WebSocket/TLS overhead
- **Compact `bitcode` Serialization** — all RPC frames, Raft log entries, and management payloads are encoded with `bitcode` instead of JSON
- **Single-Engine `fjall` Persistence** — pure-Rust LSM-tree storage engine with atomic cross-keyspace batches (`raft_log`, `raft_meta`, `state_machine`) per shard
- **Prometheus Metrics** — available via TCP `GET /metrics` on `raft-addr` and Zenoh queryable `metrics` (exposes Raft term, leader status, active shards, delivery lag entries, connections, and write latency)
- **Linearizable Health Checks** — available via TCP `GET /cluster/health` on `raft-addr` and Zenoh queryable `cluster/health`; verifies the node is running, has a known leader, and (if leader) passes `ensure_linearizable()`
- **Leader Discovery & Reconnection Hints** — Zenoh queryable `cluster/leader` and pub/sub topic `stdb/client/{session_id}/control` broadcast `leader=<id>:<addr>` during graceful drain
- **Self-Healing Shard Reconciler** — background reconciler periodically checks uninitialized shards and drives idempotent Raft initialization
- **Multi-Shard (Multi-Raft)** — dynamically create and remove shards and module routes via Shard 0 consensus

## Roadmap

### Phase 1: Single-Shard Replication
- [x] Zenoh + plaintext QUIC proxy with BSATN single-byte tag inspection
- [x] Raft consensus integration (`openraft` + `bitcode`)
- [x] Docker Compose (3-node local cluster)
- [x] GitHub Actions CI (Linux, Windows, macOS)
- [x] Cluster bootstrap and leader election
- [x] Persistent log & state machine store (`fjall` LSM-tree)
- [x] Crash-recovery replay via `last_delivered` watermark

### Phase 2: Production Ready
- [x] Prometheus metrics (`GET /metrics` + Zenoh `metrics`, including shard count & delivery lag)
- [x] Snapshot support & startup watermark alignment
- [x] Client reconnection hints on failover
- [x] Graceful shutdown and connection draining
- [x] Linearizable health check and leader discovery endpoints
- [x] Helm chart for K8s/K3s

### Phase 3: Horizontal Scaling (Multi-Raft)
- [x] Each SpacetimeDB module gets its own Raft group
- [x] Shard router (route clients to correct Raft group by module name)
- [x] Shard management API + CLI (`create-shard`, `remove-shard`, `add-route`, `remove-route`)
- [x] Shard config replicated via Shard 0 Raft consensus (`SHARD_CONFIG_MAGIC`)
- [x] Per-shard `fjall` instances with background initialization reconciler
- [x] Backwards compatible — existing single-shard clusters work unchanged
- [ ] Shard splitting when memory threshold exceeded
- [ ] Coalesced heartbeats across Raft groups (CockroachDB pattern)
- [ ] Batched persistence writes (TiKV pattern)
- [ ] Cross-shard communication (saga pattern)

### Phase 4: Kubernetes Operator
- [ ] `SpacetimeCluster` Custom Resource Definition
- [ ] Auto-scaling based on connection count / memory
- [ ] Rolling upgrades with zero downtime
- [ ] Automatic shard rebalancing

## Tech Stack

| Component | Choice | Why |
|-----------|--------|-----|
| Language | [Rust](https://www.rust-lang.org) | Same as SpacetimeDB. Memory safe. Fast. |
| Consensus | [openraft](https://github.com/databendlabs/openraft) (`openraft-rt-compio`) | Async/Compio event-driven Raft consensus |
| Transport | [Zenoh](https://zenoh.io/) (`udp/<addr>#rel=1`) | Unified pub/sub & query/reply over plaintext QUIC (`LinkUnicastQuicUnsecure`) |
| Serialization | [bitcode](https://github.com/SoftbearStudios/bitcode) | Ultra-fast, compact bitwise serialization (replaces JSON & `serde`) |
| Channels | [crossfire](https://github.com/frostyplanet/crossfire-rs) | High-performance lockless MPSC/MPMC async & blocking channels |
| Runtime | [compio](https://github.com/compio-rs/compio) | Completion-based async runtime (`io_uring` / IOCP / polling) |
| Allocator | [rimalloc](https://github.com/moggedhedien/rimalloc) | High-performance pure-Rust global memory allocator (`Rimalloc`) |
| Protocol | BSATN (SpacetimeDB native) | One-byte classification, opaque blob replication |
| Metrics | Native Lockless Atomics (`const &str` + `itoa` + `zmij`) | Zero-lock `const` static HTTP JSON exposition via `compio` TCP & Zenoh |
| Persistence | [fjall](https://github.com/fjall-rs/fjall) | Pure Rust LSM-tree storage engine with cross-keyspace atomic batches |
| K8s *(planned)* | [kube-rs](https://kube.rs) | Rust-native K8s operator framework, CNCF Sandbox |

## Design Decisions

<details>
<summary><b>Why a proxy instead of forking SpacetimeDB?</b></summary>

We don't modify SpacetimeDB at all. New SpacetimeDB versions work automatically. The proxy reads one byte per message — it's invisible to both clients and SpacetimeDB. Your existing modules work unchanged.
</details>

<details>
<summary><b>Why Zenoh + plaintext QUIC + bitcode instead of HTTP/WebSocket/JSON?</b></summary>

Zenoh over plaintext QUIC (`udp/<addr>#rel=1`) provides multiplexed streams, built-in pub/sub and queryables, and eliminates HTTP/WebSocket framing and TLS handshake overhead on internal networks. Pairing it with `bitcode` yields compact binary payloads and lower CPU/serialization latency than JSON.
</details>

<details>
<summary><b>Why fjall over redb?</b></summary>

Write-intensive Raft logs benefit from an LSM-tree architecture (`fjall`), and sharing a single `fjall::Database` with multiple keyspaces (`raft_log`, `raft_meta`, `state_machine`) per shard allows atomic cross-keyspace `WriteBatch` commits without managing multiple database files per shard.
</details>

<details>
<summary><b>Why openraft over raft-rs (TiKV)?</b></summary>

openraft is event-driven (not tick-based), so operations proceed at network speed instead of polling intervals. It's natively async with Tokio. raft-rs is more battle-tested (powers TiKV) but requires building the entire event loop yourself.
</details>

<details>
<summary><b>Why no shared storage (Longhorn/Ceph)?</b></summary>

Raft replication handles durability. Adding distributed storage underneath is a redundant replication layer with added latency. Each node uses local disk — same approach CockroachDB and TiKV use.
</details>

## Contributing

We'd love your help! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

This project is in **early development** — perfect time to get involved and shape the direction.

## License

[Apache 2.0](LICENSE)
