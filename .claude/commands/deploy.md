# /deploy — Deploy a SpacetimeDB Module to the Cluster

Build and publish a SpacetimeDB module to all nodes in the spacetime-db-ha cluster.

## Usage

`/deploy <module_path> [db_name]`

- `module_path` — Path to the SpacetimeDB module directory (contains Cargo.toml with `cdylib` target)
- `db_name` — Optional database name (defaults to directory name)

## Steps

### 1. Verify the module builds
```bash
cd <module_path> && cargo build --target wasm32-unknown-unknown --release
```
If this fails, help the user fix compilation errors before proceeding.

### 2. Check cluster is running
```bash
curl -s http://localhost:4001/cluster/status
```
If this fails, tell the user to run `/cluster up` and `/cluster bootstrap` first.

### 3. Publish to all SpacetimeDB instances
Use the `sdha deploy` CLI if available, otherwise use `spacetime publish` directly:

```bash
# Option A: sdha CLI
cargo run -p sdha -- deploy --module <module_path> --name <db_name>

# Option B: Direct spacetime CLI
spacetime publish --server http://localhost:5001 <module_path> <db_name>
spacetime publish --server http://localhost:5002 <module_path> <db_name>
spacetime publish --server http://localhost:5003 <module_path> <db_name>

# Option C: publish-all.sh script
./scripts/publish-all.sh <module_path> <db_name>
```

### 4. Verify deployment
After publishing, tell the user:
```
Module '<db_name>' deployed to all 3 nodes!

Connect your client to any node:
  ws://localhost:3001/database/subscribe/<db_name>
  ws://localhost:3002/database/subscribe/<db_name>
  ws://localhost:3003/database/subscribe/<db_name>

Writes are replicated via Raft consensus. Reads are served locally.
```

## Troubleshooting
- **"spacetime CLI not found"** — Install from https://spacetimedb.com/install
- **"wasm32-unknown-unknown target not found"** — Run `rustup target add wasm32-unknown-unknown`
- **Connection refused on 5001-5003** — SpacetimeDB containers not running, do `/cluster up`
- **Module fails on some nodes** — All nodes MUST have identical modules. Re-deploy to fix.
