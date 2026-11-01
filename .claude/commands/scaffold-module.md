# /scaffold-module — Generate a SpacetimeDB Module

Create a new SpacetimeDB module (Rust crate) ready to deploy on spacetime-db-ha.

## Usage

`/scaffold-module <name> [description]`

Examples:
- `/scaffold-module chat` — Chat app with users and messages
- `/scaffold-module game-state "Multiplayer game with players, scores, and a game loop"`
- `/scaffold-module inventory "Item tracking with categories and stock levels"`

## What to Generate

### Directory structure
```
modules/<name>/
  Cargo.toml
  src/
    lib.rs
```

### Cargo.toml template
```toml
[package]
name = "<name>"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
spacetimedb = "2.0"
log = "0.4"
```

### src/lib.rs structure

Generate based on the user's description. Always include:

1. **Imports**:
```rust
use spacetimedb::{table, reducer, Table, ReducerContext, Identity, Timestamp};
```

2. **Tables** with `#[table(name = x, public)]`:
   - Use `#[primary_key]` on identity/id fields
   - Use `#[auto_inc]` on integer IDs
   - Use `#[unique]` where appropriate
   - Mark tables `public` unless they contain sensitive data

3. **Reducers** with `#[reducer]`:
   - First arg is always `ctx: &ReducerContext`
   - Return `Result<(), String>` for reducers that can fail
   - Use `ctx.sender` for caller identity
   - Use `ctx.timestamp` for current time
   - Use `ctx.db.table_name()` for table access

4. **Lifecycle reducers**:
   - `#[reducer(client_connected)]` — Track user presence
   - `#[reducer(client_disconnected)]` — Clean up on disconnect
   - `#[reducer(init)]` — Only if initialization logic is needed

5. **Validation helpers** — Validate inputs before inserting

### Table operations reference (for generating code)
```rust
// Insert
ctx.db.user().insert(User { ... });

// Find by primary key
ctx.db.user().identity().find(ctx.sender)

// Update by primary key
ctx.db.user().identity().update(User { ... })

// Delete by primary key
ctx.db.user().identity().delete(&id)

// Iterate all rows
for row in ctx.db.user().iter() { ... }
```

## After Generation

Tell the user:
```
Module '<name>' created at modules/<name>/

Next steps:
  1. Review and customize src/lib.rs
  2. Build:    cd modules/<name> && spacetime build
  3. Start cluster: /cluster up && /cluster bootstrap
  4. Deploy:   /deploy modules/<name> <name>
  5. Connect:  ws://localhost:3001/database/subscribe/<name>
```

## Design Guidelines

- Keep reducers small and focused (one action per reducer)
- Validate all inputs — return `Err(String)` on bad input
- Use `Identity` for user-related primary keys
- Use `u64` with `#[auto_inc]` for entity IDs
- Prefer `Option<T>` for nullable fields
- Add `log::info!()` in reducers for debugging
- Each reducer runs in a transaction — Err rolls back all changes
