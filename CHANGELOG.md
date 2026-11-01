# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure with Cargo workspace
- WebSocket proxy with BSATN tag byte inspection (read/write classification)
- Raft consensus integration via openraft
- State machine that forwards committed writes to SpacetimeDB
- CLI tool (`sdha`) with cluster management commands
- Docker Compose setup for local 3-node cluster
- Dockerfile with multi-stage Rust build
- Persistent Raft log store using redb (pure Rust, ACID) — survives restarts
- TLS encryption for inter-node Raft RPCs and management API (`--tls-cert`, `--tls-key`, `--tls-ca-cert`)
- Prometheus metrics endpoint (`GET /metrics`) with Raft term, leader status, connections, write latency, entries applied
- Health check endpoint (`GET /cluster/health`) — returns 200 when leader known, 503 otherwise
- Leader discovery endpoint (`GET /cluster/leader`) — returns current leader ID and address
- Client reconnection hints — WebSocket close frames include `leader=<id>:<addr>` on graceful shutdown
- Graceful shutdown with Ctrl+C / SIGTERM handling, coordinated task shutdown, and 5-second drain period
- Snapshot caching for fast new-node catch-up
- Helm chart and K3s manifests for Kubernetes deployment
- End-to-end Docker cluster test script
- GitHub Actions CI for Linux, Windows, and macOS
- Multi-shard (Multi-Raft) support — each SpacetimeDB module can run in its own Raft group for parallel write throughput
- `RaftPool` replaces `RaftNode` — manages multiple Raft groups with per-shard state machines, forwarders, and log stores
- Shard-aware Raft RPC routing (`/raft/{shard_id}/append`, `/raft/{shard_id}/vote`, `/raft/{shard_id}/snapshot`)
- Shard management API: create shards, add/remove module routes, list shards, per-shard status
- Shard configuration replicated via Raft consensus (0xFF magic byte in shard 0's log) — no external config store
- Per-shard persistent log stores at `{data_dir}/shard-{id}/raft-log.redb` with automatic migration of legacy files
- Module-based shard routing — WebSocket proxy extracts module name from path and routes writes to the correct shard
- CLI shard commands: `create-shard`, `add-route`, `remove-route`, `list-shards`, `shard-status`
- Membership changes (`add-node`/`remove-node`) now propagate to all active shards
- Full backwards compatibility — existing single-shard clusters work with zero config changes (shard 0 is the default)

### Changed
- Renamed the project from RaftimeDB to spacetime-db-ha: crate and binary names, CLI command (`sdha`), environment variables (`SPACETIME_DB_HA_*`), Prometheus metric prefix (`spacetime_db_ha_*`), Helm chart, k3s manifests, Docker image and CI artifacts
- Writes are forwarded only after the entry is applied locally, so nothing executes before consensus commits and each connection keeps read-your-writes
- Raft entries now carry the target `db_path`; the per-shard forwarder multiplexes one shared upstream connection per database instead of reusing the first client's path
- The state machine persists its applied watermark, membership and shard routing to `state-machine.redb` — a restart resumes from that watermark instead of replaying every historical write
- Split `spacetime-db-ha` into a library + thin binary (`lib.rs` / `server.rs`) so integration tests can live in `crates/proxy/tests/`
- Message classification uses a compile-time 256-entry lookup table instead of per-message branching
- Write payloads carry `bytes::Bytes` end-to-end — leader forwarding and log serialization no longer copy the message
- State machine takes one lock per applied entry (`parking_lot`) and reuses installed snapshot bytes
- `get_log_state` reports `last_purged` for a fully purged log, so openraft no longer sees a state rollback after restart
- Log store helpers use a small internal error type; `truncate`/`purge` propagate redb errors instead of dropping them
- Management API returns structured errors instead of panicking when shard 0 is unavailable

### Removed
- Unused in-memory log store (`raft/log_store.rs`) and empty `websocket/upstream.rs` placeholder
- Dead exports: `is_read_tag`, `is_known_tag`, `ShardRouter::route_count`, `ShardRouter::set_default_shard`, `StateMachineStore::last_applied`, `StateMachineStore::applied_count`
- Unused `toml` workspace dependency and stale workspace metadata
