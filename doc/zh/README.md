<div align="center">

[English](../../README.md)

# spacetime-db-ha

### 让 SpacetimeDB 坚不可摧

面向 [SpacetimeDB](https://spacetimedb.com) 的分布式容错集群代理 — 无需修改 SpacetimeDB 源码，即可实现自动复制、故障转移与水平扩展。

[![CI](https://github.com/webc-fork/spacetime-db-ha/actions/workflows/ci.yml/badge.svg)](https://github.com/webc-fork/spacetime-db-ha/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](../../LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85+-orange.svg)](https://www.rust-lang.org)

[快速开始](#快速开始) | [工作原理](#工作原理) | [部署方式](#部署方式) | [路线图](#路线图)

</div>

---

SpacetimeDB 将整个后端浓缩进数据库中 — 无需独立业务服务器或微服务，直接以内存速度运行 WASM 模块。但它默认运行在单节点上，一旦单节点宕机，服务即中断。

**spacetime-db-ha 解决了这一问题。** 它基于 [Zenoh](https://zenoh.io/) + 明文 QUIC（`udp/<addr>#rel=1`）传输层与 [`bitcode`](https://github.com/SoftbearStudios/bitcode) 序列化，在 SpacetimeDB 外层构建了 [Raft 共识](https://raft.github.io/)层，并通过每个分片独立的 [`fjall`](https://github.com/fjall-rs/fjall) LSM-tree 存储引擎将每次写操作复制到多个节点。任意节点宕机时，集群继续运行，零数据丢失、零停机。

```
业务客户端（无需修改业务逻辑）
       │
       ▼ (Zenoh + 明文 QUIC + bitcode)
  spacetime-db-ha ──── Raft ──── spacetime-db-ha ──── Raft ──── spacetime-db-ha
       │                        │                        │
  SpacetimeDB              SpacetimeDB              SpacetimeDB
  (状态一致)                (状态一致)                (状态一致)
```

## 快速开始

### 方式 1：Docker Compose（最简）

数秒内在本地启动 3 节点集群：

```bash
git clone https://github.com/webc-fork/spacetime-db-ha.git
cd spacetime-db-ha/deploy/docker
docker compose up
```

将客户端连接至 `udp/127.0.0.1:3001#rel=1`（或 `:3002`、`:3003`，任意节点均可）。

### 方式 2：预编译二进制

从 [Releases](https://github.com/webc-fork/spacetime-db-ha/releases) 下载对应平台二进制文件：

| 平台 | 二进制文件名 |
|------|-------------|
| Linux x86_64 | `spacetime-db-ha-x86_64-unknown-linux-gnu` |
| Windows x86_64 | `spacetime-db-ha-x86_64-pc-windows-msvc.exe` |
| macOS ARM | `spacetime-db-ha-aarch64-apple-darwin` |

```bash
# 启动 3 个节点（每个节点旁路运行一个本地 SpacetimeDB 实例）
spacetime-db-ha --node-id 1 --peers 2=node2:4001,3=node3:4001 --stdb-url 127.0.0.1:3000
spacetime-db-ha --node-id 2 --peers 1=node1:4001,3=node3:4001 --stdb-url 127.0.0.1:3010
spacetime-db-ha --node-id 3 --peers 1=node1:4001,2=node2:4001 --stdb-url 127.0.0.1:3020
```

### 方式 3：从源码构建

```bash
# 需要 Rust 1.85+（推荐 nightly）
cargo install --path crates/proxy
cargo install --path crates/cli
spacetime-db-ha --help
sdha --help
```

## 工作原理

SpacetimeDB 的 reducer 是**确定性且可重放的** — 只要向多个 SpacetimeDB 实例按相同顺序输入相同的 reducer 调用序列，所有实例最终会收敛到完全一致的状态。

spacetime-db-ha 基于 Zenoh + 明文 QUIC 运行 [Raft 共识](https://raft.github.io/)，在集群范围内生成这一全局有序序列。

### 单字节标签分类

代理层仅读取每条 BSATN 消息的**第一个字节**即可完成读写分类：

```
payload[0] = BSATN 标签字节（SpacetimeDB v2 协议）

Tag 0: Subscribe        → 读操作 → 等待因果读屏障后直接转发本地 SpacetimeDB
Tag 1: Unsubscribe      → 读操作 → 等待因果读屏障后直接转发本地 SpacetimeDB
Tag 2: OneOffQuery      → 读操作 → 等待因果读屏障后直接转发本地 SpacetimeDB
Tag 3: CallReducer      → 写操作 → 经 Raft 复制落盘后，按日志序号严格保序投递
Tag 4: CallProcedure    → 写操作 → 经 Raft 复制落盘后，按日志序号严格保序投递
```

- **写操作**：先经 Raft 多数派提交并写入本地 `fjall` 存储（`raft_log` + `state_machine`），随后严格按照 Raft 日志索引顺序（`last_delivered` 水位）投递至本地 SpacetimeDB
- **读操作**：先等待本地状态机 `applied` 追平已知最新日志索引 `last_log_index`（因果读屏障），再直接转发至本地 SpacetimeDB
- 整条 BSATN 消息作为**不透明二进制块（opaque blob）**复制，无需解析业务 Schema

### 投递与一致性保障

- **统一至少一次（At-Least-Once）投递语义**：所有已提交写请求在转发上游前均先持久化至 `fjall`。每个分片通过 `last_delivered` 水位记录上游投递进度；节点崩溃重启时，自动重放 `(last_delivered, last_applied]` 区间内未投递的日志条目。
- **跨连接严格日志序投递**：即便多个客户端并发写入同一分片，投递层也会等待 `log_id.index - 1` 投递完成后再发送 `log_id.index`，保证同节点上游执行顺序与 Raft 日志严格一致。
- **单库多 Keyspace 原子存储**：每个分片共享单个 `fjall::Database`（`{data_dir}/shard-{id}/fjall`），内含 `raft_log`、`raft_meta`、`state_machine` 三个 keyspace，通过跨 keyspace `WriteBatch` 实现原子日志清理，并在启动时校验 `applied >= last_purged` 水位对齐。
- **有界队列背压**：内部转发通道（`FORWARDER_CHANNEL_CAPACITY = 4096`）、配置通道（`CONFIG_CHANNEL_CAPACITY = 256`）与上游连接队列（`UPSTREAM_QUEUE_CAPACITY = 1024`）全部有界，防止上游慢速或断连重连期间内存无限增长。

### 架构边界与假设

1. **Reducer 确定性与幂等性**：由于崩溃恢复采用至少一次重放语义 `(last_delivered, last_applied]`，业务层的 SpacetimeDB reducer 应当满足确定性与幂等性。
2. **分片 0 元数据控制面**：分片创建、删除与模块路由变更（`ShardConfigCommand`）通过分片 0 的 Raft 组复制（使用 4 字节魔数 `SHARD_CONFIG_MAGIC = [0xFF, b'S', b'F', b'G']` 标识）。若分片 0 失去多数派，拓扑变更会暂停，但已存在的业务分片仍正常服务。
3. **记账式快照**：Raft 快照仅保存共识元数据（`last_applied`、`last_delivered`、成员表、分片路由表），业务表数据由各节点本地的 SpacetimeDB 实例维护。

### 节点故障恢复流程

```
故障前： Node 1 (LEADER) ←→ Node 2 ←→ Node 3
                                         │
                                    [Node 3 宕机]

故障中： Node 1 (LEADER) ←→ Node 2
         （多数派存活，继续服务，零停机）

恢复后： Node 1 (LEADER) ←→ Node 2 ←→ Node 3（重新加入并追平日志）
```

1. Raft 检测到心跳超时（约 1-2 秒）
2. 若 Leader 宕机，剩余多数派节点自动选举新 Leader（<5 秒）
3. 集群依靠剩余多数派继续对外提供读写服务
4. 故障节点恢复后，自动从 Raft 日志追平并重放未投递条目
5. **零数据丢失** — 所有写操作仅在多数派落盘提交后才向客户端确认

## 部署方式

spacetime-db-ha 编译为单一静态二进制文件，可在任意环境运行。

### Docker Compose（本地开发 / 小规模部署）

```bash
cd deploy/docker
docker compose up
```

启动 3 个 SpacetimeDB 实例与 3 个 spacetime-db-ha 代理节点，暴露端口 3001-3003。

### Docker（单节点）

```bash
docker run -p 3001:3001/udp -p 4001:4001/udp -p 4001:4001/tcp \
  -e SPACETIME_DB_HA_NODE_ID=1 \
  -e SPACETIME_DB_HA_STDB_URL=host.docker.internal:3000 \
  -e SPACETIME_DB_HA_PEERS=2=node2:4001,3=node3:4001 \
  ghcr.io/webc-fork/spacetime-db-ha:latest
```

### Windows

从 [Releases](https://github.com/webc-fork/spacetime-db-ha/releases) 下载 `spacetime-db-ha.exe` 或从源码编译：

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

或直接应用清单文件：

```bash
kubectl apply -f deploy/k3s/
```

<details>
<summary>K8s 清单示例</summary>

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
            - containerPort: 3001  # 客户端 Zenoh QUIC (UDP)
              protocol: UDP
              name: client-quic
            - containerPort: 4001  # Raft Zenoh QUIC (UDP)
              protocol: UDP
              name: raft-quic
            - containerPort: 4001  # 健康检查与 Prometheus 指标 (TCP)
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

### 物理机 / 裸金属（3 台机器）

```bash
# 机器 1 (192.168.1.10)
spacetime-db-ha --node-id 1 --listen-addr 0.0.0.0:3001 --raft-addr 0.0.0.0:4001 \
  --raft-advertise-addr 192.168.1.10:4001 \
  --stdb-url 127.0.0.1:3000 --peers 2=192.168.1.11:4001,3=192.168.1.12:4001

# 机器 2 (192.168.1.11)
spacetime-db-ha --node-id 2 --listen-addr 0.0.0.0:3001 --raft-addr 0.0.0.0:4001 \
  --raft-advertise-addr 192.168.1.11:4001 \
  --stdb-url 127.0.0.1:3000 --peers 1=192.168.1.10:4001,3=192.168.1.12:4001

# 机器 3 (192.168.1.12)
spacetime-db-ha --node-id 3 --listen-addr 0.0.0.0:3001 --raft-addr 0.0.0.0:4001 \
  --raft-advertise-addr 192.168.1.12:4001 \
  --stdb-url 127.0.0.1:3000 --peers 1=192.168.1.10:4001,2=192.168.1.11:4001
```

## 架构图

```
┌──────────────────────────────────────────────────────────────┐
│                     spacetime-db-ha 节点                     │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │        Zenoh 客户端代理 (udp/:3001#rel=1)              │  │
│  │                                                        │  │
│  │  客户端消息 ──► 读取首字节 ──► 写操作? ────────────┐   │  │
│  │                      │                             │   │  │
│  │                      │ 读操作（等待因果读屏障）     │   │  │
│  │                      ▼                             ▼   │  │
│  │              直接转发至本地              Raft 共识复制  │  │
│  │              SpacetimeDB               (Zenoh + fjall) │  │
│  │                      │                             │   │  │
│  │                      │             按日志序严格投递 │   │  │
│  │                      ▼                        ▼    │   │  │
│  │              ┌──────────────────────────────────┐  │   │  │
│  │              │    本地 SpacetimeDB (:3000)      │  │   │  │
│  │              │    (所有节点状态一致)             │  │   │  │
│  │              └──────────────────────────────────┘  │   │  │
│  └────────────────────────────────────────────────────┘   │  │
│                                                           │  │
│  ┌────────────────────────────────────────────────────┐   │  │
│  │  Raft 与管理面传输 (udp/:4001#rel=1 + TCP)         │   │  │
│  │  Zenoh QUIC RPC (bitcode) + /metrics & /health     │   │  │
│  └────────────────────────────────────────────────────┘   │  │
└───────────────────────────────────────────────────────────┘  │
                                                               │
        ◄──── Zenoh + 明文 QUIC + bitcode ────►                │
                                                               │
┌──────────────────┐          ┌──────────────────┐             │
│ spacetime-db-ha  │          │ spacetime-db-ha  │             │
│ 节点 2           │          │ 节点 3           │             │
│ (相同结构)       │          │ (相同结构)       │             │
└──────────────────┘          └──────────────────┘             │
```

## 适用场景

### MMO 游戏服
成千上万名玩家分布在不同游戏分区，每个分区对应一个由独立 Raft 组支撑的 SpacetimeDB 模块。单节点故障对玩家无感，水平增加分片即可扩展世界容量。

### AI Agent 集群
数百个共享状态的自治 Agent 并发调用 reducer 更新共享知识库，并通过订阅实时接收增量状态。拔掉任意节点电源，Agent 自动重连并继续运行。

### 实时协作应用
共享文档、光标同步、在线状态。SpacetimeDB 的订阅系统向客户端推送增量更新，spacetime-db-ha 保证底层状态在节点故障时安全无损。

### 任何高可用 SpacetimeDB 业务
无需重写业务代码，仅需通过代理层配置即可获得多副本容灾能力。

## CLI 命令行工具 (`sdha`)

所有集群与分片管理命令均基于 Zenoh + 明文 QUIC 传输，采用 `bitcode` 二进制编码：

```bash
# 集群管理
sdha init --nodes 1=127.0.0.1:4001 2=127.0.0.1:4002 3=127.0.0.1:4003  # 初始化引导集群（仅执行一次）
sdha status --addr 127.0.0.1:4001                                     # 查看节点状态
sdha add-node --node-id 4 --addr 127.0.0.1:4004 --cluster 127.0.0.1:4001
sdha remove-node --node-id 2 --cluster 127.0.0.1:4001

# 分片管理 (Multi-Raft)
sdha create-shard --shard-id 1 --cluster 127.0.0.1:4001
sdha remove-shard --shard-id 1 --cluster 127.0.0.1:4001
sdha add-route --module my_game --shard-id 1 --cluster 127.0.0.1:4001
sdha remove-route --module my_game --cluster 127.0.0.1:4001
sdha list-shards --addr 127.0.0.1:4001
sdha shard-status --shard-id 1 --addr 127.0.0.1:4001

# 跨节点批量部署 SpacetimeDB WASM 模块
sdha deploy --module ./my_module.wasm --name my_game --servers 127.0.0.1:3000 127.0.0.1:3010
```

## 配置参数

所有参数均可通过命令行标志或环境变量设置：

| 命令行参数 | 环境变量 | 默认值 | 说明 |
|-----------|---------|--------|------|
| `--node-id` | `SPACETIME_DB_HA_NODE_ID` | *必填* | 节点唯一标识 (`u64`) |
| `--listen-addr` | `SPACETIME_DB_HA_LISTEN_ADDR` | `0.0.0.0:3001` | 客户端 Zenoh 明文 QUIC 监听地址 (`udp/<addr>#rel=1`) |
| `--raft-addr` | `SPACETIME_DB_HA_RAFT_ADDR` | `0.0.0.0:4001` | Raft 与管理面 Zenoh QUIC 地址 (UDP) + `/metrics` 与 `/cluster/health` (TCP) |
| `--raft-advertise-addr` | `SPACETIME_DB_HA_RAFT_ADVERTISE_ADDR` | *默认回退至 `--raft-addr`* | 写入 Raft 成员表的可路由广播地址 |
| `--stdb-url` | `SPACETIME_DB_HA_STDB_URL` | `127.0.0.1:3000` | 本地 SpacetimeDB Zenoh 明文 QUIC 地址 |
| `--peers` | `SPACETIME_DB_HA_PEERS` | `""` | 逗号分隔的对端节点 Raft 地址列表 (`id=host:port` 或 `host:port`) |
| `--data-dir` | `SPACETIME_DB_HA_DATA_DIR` | `./data` | 持久化数据目录 (`{data_dir}/shard-{id}/fjall`) |

## 生产级特性

- **Zenoh + 明文 QUIC 传输**：客户端代理、上游转发、Raft 复制与管理面统一使用低延迟 `udp/<addr>#rel=1`，剥离 HTTP/WebSocket 帧封装与 TLS 握手开销
- **`bitcode` 紧凑二进制序列化**：所有 RPC 消息、Raft 日志条目与管理命令统一使用 `bitcode` 替代 JSON
- **`fjall` 单引擎原子持久化**：基于纯 Rust LSM-tree 存储引擎，每个分片通过单库多 keyspace（`raft_log`、`raft_meta`、`state_machine`）实现跨表原子写入与清理
- **Prometheus 监控指标**：支持通过 `raft-addr` 的 TCP `GET /metrics` 或 Zenoh `metrics` 查询（暴露 Raft term、Leader 状态、活跃分片数 `spacetime_db_ha_shards_active`、全分片投递积压 `spacetime_db_ha_delivery_lag_entries`、连接数及写入延迟）
- **线性一致性健康检查**：支持通过 TCP `GET /cluster/health` 或 Zenoh `cluster/health` 查询；校验节点未关停、存在 Leader，且当自身为 Leader 时通过 `ensure_linearizable()` 多数派校验
- **Leader 发现与优雅重连提示**：支持 Zenoh `cluster/leader` 查询；优雅停机期间通过 `stdb/client/{session_id}/control` 广播 `leader=<id>:<addr>` 引导客户端重连
- **后台分片自愈协调器**：后台定时巡检未初始化分片并幂等驱动 Raft 初始化，防止切主或网络抖动导致新建分片卡死
- **动态多分片 (Multi-Raft)**：支持通过分片 0 共识动态创建、删除分片（级联清理路由与本地资源）及绑定模块路由

## 路线图

### 第一阶段：单分片复制
- [x] 基于 Zenoh + 明文 QUIC 的代理与 BSATN 单字节标签分类
- [x] Raft 共识集成 (`openraft` + `bitcode`)
- [x] Docker Compose 本地 3 节点集群
- [x] GitHub Actions CI（Linux、Windows、macOS）
- [x] 集群引导与 Leader 选举
- [x] 持久化日志与状态机存储（`fjall` LSM-tree）
- [x] 基于 `last_delivered` 水位的崩溃恢复重放

### 第二阶段：生产就绪
- [x] Prometheus 监控指标（`GET /metrics` + Zenoh `metrics`，含分片数与投递积压）
- [x] 快照支持与启动水位对齐校验
- [x] 故障转移与停机时的客户端重连提示
- [x] 优雅停机与连接排空
- [x] 线性一致性健康检查与 Leader 发现接口
- [x] K8s/K3s Helm Chart

### 第三阶段：水平扩展 (Multi-Raft)
- [x] 每个 SpacetimeDB 模块独立 Raft 组
- [x] 分片路由器（按模块名路由至对应 Raft 组）
- [x] 分片管理接口与 CLI（`create-shard`、`remove-shard`、`add-route`、`remove-route`）
- [x] 分片配置经分片 0 Raft 共识复制（`SHARD_CONFIG_MAGIC`）
- [x] 每分片独立 `fjall` 实例与后台初始化自愈循环
- [x] 向后兼容单分片部署模式
- [ ] 内存阈值触发分片分裂
- [ ] 跨 Raft 组心跳合并（CockroachDB 模式）
- [ ] 批量持久化写入（TiKV 模式）
- [ ] 跨分片事务通信（Saga 模式）

### 第四阶段：Kubernetes Operator
- [ ] `SpacetimeCluster` CRD 自定义资源
- [ ] 基于连接数与内存的自动扩缩容
- [ ] 零停机滚动升级
- [ ] 自动分片再平衡

## 技术栈

| 组件 | 选型 | 原因 |
|------|------|------|
| 语言 | [Rust](https://www.rust-lang.org) | 与 SpacetimeDB 同语言，内存安全、高性能 |
| 共识引擎 | [openraft](https://github.com/databendlabs/openraft) (`openraft-rt-compio`) | 基于 Compio 异步运行时、事件驱动的 Raft 实现 |
| 网络传输 | [Zenoh](https://zenoh.io/) (`udp/<addr>#rel=1`) | 基于明文 QUIC (`LinkUnicastQuicUnsecure`) 的统一发布订阅与查询响应网络 |
| 序列化 | [bitcode](https://github.com/SoftbearStudios/bitcode) | 极速、紧凑的位级原生二进制序列化（替代 JSON 与 `serde`） |
| 消息通道 | [crossfire](https://github.com/frostyplanet/crossfire-rs) | 高性能无锁 MPSC/MPMC 异步与阻塞混合通道 |
| 异步运行时 | [compio](https://github.com/compio-rs/compio) | 基于完成驱动（`io_uring` / IOCP / polling）的高性能 Rust 异步运行时 |
| 内存分配器 | [rimalloc](https://github.com/moggedhedien/rimalloc) | 高性能纯 Rust 全局内存分配器 (`Rimalloc`) |
| 业务协议 | BSATN（SpacetimeDB 原生） | 首字节读写分类，消息体不透明二进制复制 |
| 监控指标 | 原生无锁原子指标 (`const &str` + `itoa` + `zmij`) | 编译期 `const` 静态片段拼接，基于 `compio` TCP 与 Zenoh 暴露最小开销 HTTP JSON 指标 |
| 存储引擎 | [fjall](https://github.com/fjall-rs/fjall) | 纯 Rust LSM-tree 嵌入式存储，支持跨 keyspace 原子批写入 |
| K8s *(规划中)* | [kube-rs](https://kube.rs) | Rust 原生 K8s Operator 框架 |

## 设计决策

<details>
<summary><b>为什么采用代理模式而不是 Fork SpacetimeDB？</b></summary>

不修改 SpacetimeDB 源码意味着上游发布新版本时可直接升级。代理层仅读取每条消息的首字节，对客户端和 SpacetimeDB 均完全透明，现有 WASM 模块无需任何改动即可运行。
</details>

<details>
<summary><b>为什么选择 Zenoh + 明文 QUIC + bitcode 替代 HTTP/WebSocket/JSON？</b></summary>

基于明文 QUIC（`udp/<addr>#rel=1`）的 Zenoh 原生提供多路复用流、发布订阅与查询路由，消除了内网环境下的 HTTP/WebSocket 协议栈开销与 TLS 握手延迟；配合 `bitcode` 可大幅缩减序列化体积与 CPU 开销。
</details>

<details>
<summary><b>为什么选择 fjall 替代 redb？</b></summary>

Raft 日志属于高频顺序/批量追加写场景，LSM-tree 架构（`fjall`）在写入吞吐上更具优势；同时每个分片只需维护单个 `fjall::Database` 下的 3 个 keyspace（`raft_log`、`raft_meta`、`state_machine`），即可通过 `WriteBatch` 实现跨表原子提交，避免了双文件不一致问题。
</details>

<details>
<summary><b>为什么选择 openraft 而不是 raft-rs (TiKV)？</b></summary>

openraft 采用事件驱动而非固定 Tick 轮询，操作以网络速度推进，且原生深度集成 Tokio 异步生态。
</details>

<details>
<summary><b>为什么不使用共享分布式存储（Longhorn/Ceph）？</b></summary>

Raft 复制本身已提供多副本持久化保障。在 Raft 之下再叠加分布式块存储会产生双重复制开销与额外写入延迟。每个节点直接使用本地磁盘是 CockroachDB 与 TiKV 等系统的标准实践。
</details>

## 参与贡献

欢迎提交 Issue 与 PR！详情请参阅 [CONTRIBUTING.md](../../CONTRIBUTING.md)。

## 开源协议

[Apache 2.0](../../LICENSE)
