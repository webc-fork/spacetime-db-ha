# Contributing to spacetime-db-ha

Thanks for your interest in contributing! spacetime-db-ha is early-stage and there's a lot to build.

## Getting Started

### Prerequisites

- **Rust** (latest stable): https://rustup.rs
- **Docker** (for running SpacetimeDB locally): https://docker.com
- **SpacetimeDB CLI** (optional, for publishing modules): https://spacetimedb.com/install

### Building

```bash
git clone https://github.com/webc-fork/spacetime-db-ha.git
cd spacetime-db-ha
cargo build
```

### Running the Tests

```bash
./test.sh          # cargo nextest: unit + integration tests (crates/proxy/tests/)
./sh/clippy.sh     # fixrs + rustfmt + clippy (-D warnings)
```

### Running Locally (Docker Compose)

```bash
cd deploy/docker
docker compose up
```

This starts a 3-node cluster. Connect to `ws://localhost:3001`.

## Project Structure

```
crates/
├── proxy/          # Core proxy binary (Raft + WebSocket)
│   └── src/
│       ├── raft/       # Raft consensus (openraft)
│       ├── websocket/  # WebSocket proxy (client ↔ SpacetimeDB)
│       └── router/     # Shard routing (multi-raft)
├── cli/            # CLI tool (sdha)
deploy/
├── docker/         # Docker Compose for local dev
├── helm/           # Helm chart for K8s/K3s
└── k3s/            # K3s-specific examples
```

## What Needs Work

Check the [GitHub Issues](https://github.com/webc-fork/spacetime-db-ha/issues) for current priorities. The big areas:

### Completed
- ~~Cluster bootstrap (initial leader election)~~
- ~~Persistent log store (redb)~~
- ~~TLS encryption for inter-node communication~~
- ~~Prometheus metrics~~
- ~~Client reconnection hints on failover~~
- ~~Graceful shutdown and connection draining~~
- ~~Snapshot support for node catch-up~~
- ~~Health check and leader discovery endpoints~~

### Completed (Phase 3)
- ~~Multi-Raft (one Raft group per SpacetimeDB module)~~
- ~~Shard management API and CLI~~
- ~~Module-based shard routing~~

### Phase 4: Further Scaling
- Shard splitting and rebalancing
- Per-shard membership subsets (not all nodes in all shards)

### Phase 5: Kubernetes Operator
- `SpacetimeCluster` Custom Resource Definition
- Auto-scaling based on connection count / memory
- Rolling upgrades with zero downtime

## Pull Request Process

1. Fork the repo and create a branch from `main`
2. Make your changes
3. Ensure `./test.sh` and `./sh/clippy.sh` pass
4. Write a clear PR description explaining what and why
5. Submit!

## Code Style

- Run `cargo fmt` before committing
- Run `cargo clippy` and fix warnings
- Keep functions focused and small
- Add comments for non-obvious logic (but don't over-comment)
- Error handling: use `anyhow` for applications, `thiserror` for libraries

## Communication

- **GitHub Issues**: Bug reports, feature requests, questions
- **Pull Requests**: Code contributions
- **Discussions**: Architecture decisions, design proposals

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
