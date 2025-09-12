## Alkanes Contract Indexer

A Rust service that monitors new blocks via Metashrew, fans out concurrent jobs to decode and index Alkanes-related data, and writes results to Postgres. It leverages the deezel toolkit for all Alkanes/Bitcoin RPC interactions.

### Highlights
- **Background polling**: Reliable loop that queries Metashrew and derives a canonical tip height (`metashrew_height - 1`), with exponential backoff and reorg awareness.
- **Pools/state refresh on new tip**: When a higher tip is detected, the service first refreshes pools and inserts new `PoolState` snapshots only if values changed.
- **Concurrent block-processing pipeline**: For each new block height, per-block tasks (placeholders today) run concurrently:
  - collecting block transactions and decoding Protostones
  - tracing contract calls for txs to collect events
- **Postgres-ready**: A connection pool is initialized; tasks will later batch-write by `blockHeight` and `txid`.

### Repository Structure
- `src/main.rs`: Program entrypoint; initializes config, DB, provider; runs background poller until Ctrl-C.
- `src/config.rs`: Loads configuration from environment variables.
- `src/db.rs`: Postgres pool initialization and re-exports DB submodules.
- `src/db/pools.rs`: All SQL for `Pool` (read existing, batch insert, resolve IDs for pairs).
- `src/db/pool_state.rs`: All SQL for `PoolState` (fetch latest per pool, batch insert snapshots).
- `src/helpers/pools.rs`: Uses deezel's `AmmManager` helpers to simulate via Sandshrew and return decoded pools and details (no local decoders).
- `src/provider.rs`: Builds a `deezel_common::provider::ConcreteProvider` for RPC calls.
- `src/pipeline.rs`: Orchestrates per-tip work; now delegates decoding to helpers and DB writes to `src/db/*` modules.
- `src/poller.rs`: `BlockPoller` that polls `metashrew_height`, detects new heights, and invokes the pipeline.
- `reference/deezel/`: Vendored reference copy of deezel source for exploration only (do not import from here at build time).

### Dependencies
- Rust toolchain (stable)
- Postgres (local or remote)
- deezel (via git dependency)

We depend on deezel’s common crate for provider and RPC traits. Upstream reference: [`Sprimage/deezel`](https://github.com/Sprimage/deezel).

### Environment Variables
Create a `.env` file at the repo root (you can copy from `example.env`) or export variables in your shell.

```env
DATABASE_URL=postgres://user:pass@localhost:5432/alkanes_indexer

# Where Metashrew/Sandshrew JSON-RPC is available. Defaults to http://localhost:18888
SANDSHREW_RPC_URL=http://localhost:18888

# Optional: direct Bitcoin Core RPC (if different from Sandshrew)
#BITCOIN_RPC_URL=http://user:pass@127.0.0.1:8332

# Optional: Esplora base URL (if applicable)
#ESPLORA_URL=http://localhost:3002

# Network identity used by provider constructor (default: regtest)
NETWORK=regtest

# Poll interval for metashrew height (ms); default 2000
POLL_INTERVAL_MS=2000

# Optional: start height for historical catch-up.
# - If set: a catch-up coordinator will process sequentially from this height (or
#   the last persisted progress) up to the current tip. The coordinator starts
#   only after the poller has initialized tip and refreshed pools.
# - If unset: no catch-up is performed; the poller immediately processes the
#   current tip on startup and then continues with subsequent blocks.
#START_HEIGHT=800000

# Required: Factory contract ID for AMM pools discovery
# These must be the numeric string IDs (lo parts) expected by Metashrew
FACTORY_BLOCK_ID=0
FACTORY_TX_ID=0
```

Notes:
- The service builds a deezel `ConcreteProvider`. Pool discovery calls pass `SANDSHREW_RPC_URL` directly to deezel's `AmmManager` helpers.
- `BITCOIN_RPC_URL` and `ESPLORA_URL` are optional; leave unset for Sandshrew-only routing.

## Update deezel-common to latest
```bash
cargo update -p deezel-common
```

### Build
```bash
cargo build
```

### Database Schema Management (CLI)
We provide a small CLI to manage the database schema.

```bash
# Push or update schema to DATABASE_URL
cargo run --bin dbctl -- push

# Drop all tables and recreate schema
cargo run --bin dbctl -- reset

# Drop all tables only (no re-push)
cargo run --bin dbctl -- drop
```

The schema mirrors the previous Prisma-based design (types mapped to Postgres):
- strings as `text`/`uuid`, JSON as `jsonb`, datetimes as `timestamptz`.
- tables include: `alkane_transaction`, `trace_event`, `clock_in`, `processed_blocks`,
  `clock_in_block_summary`, `clock_in_summary`, `corp_data`, `profile`, `pool`, `pool_state`,
  `pool_creation`, `pool_swap`, `pool_burn`, `pool_mint`, `curated_pools`, and `kv_store`.

### Schema naming and compatibility
- Table and column names preserve the original Prisma casing by using quoted identifiers (e.g., `"AlkaneTransaction"`, `"blockHeight"`).
- UUID fields use Postgres `gen_random_uuid()`; the service enables `pgcrypto` automatically if available.
- Foreign keys and indexes match the original relationships and composite unique constraints where provided.

### Run
```bash
# With INFO logs
RUST_LOG=info cargo run

# With more verbose logs
RUST_LOG=debug cargo run
```

The service will:
1) Connect to Postgres
2) Construct a deezel provider
3) Start the `BlockPoller` loop which:
   - reads canonical tip height via `metashrew_height - 1`
   - detects new heights (filling gaps)
   - on first observation (no previous height): triggers `Pipeline::fetch_pools_for_tip(provider, tip)` once
   - on first observation AND no `START_HEIGHT`: also processes the current tip immediately
   - on height increase: first triggers `Pipeline::fetch_pools_for_tip(provider, tip)`
   - then processes each new block via `Pipeline::process_block_sequential`
   - on no height change: skips pools/state refresh and block processing
4) If `START_HEIGHT` is set, start the catch-up coordinator which:
   - waits for the poller to initialize tip (and perform the initial pools/state refresh) before starting
   - reads canonical tip height and computes `[next..=tip]` from `START_HEIGHT` and the last stored progress from DB
   - sequentially processes `[next..=tip]` via `Pipeline::process_block_sequential`
   - persists `last_processed_height` in `kv_store`
   - after catch-up, the poller continues processing subsequent new blocks as they arrive

### Metashrew height off-by-one
- Metashrew's `get_metashrew_height()` reports the next height (tip + 1). The indexer normalizes this by subtracting 1 to obtain the canonical chain tip.
- Implementation: `helpers/height.rs` provides `canonical_tip_height(provider)` used by both the poller and catch-up coordinator.

Shutdown with Ctrl-C.

### Current Status
- Poller, pipeline pools fetch, and coordinator are implemented. The per-block tasks are placeholders and will later:
  - collect block txs, filter OP_RETURN, decode Protostones, stage writes
  - trace txs to collect contract events and stage writes
- A minimal `kv_store` table is auto-created for progress tracking. The pool discovery and snapshotting flow (via deezel's `AmmManager`):
  - calls `get_all_pools_via_raw_simulate(&url, factory_block, factory_tx)` using `SANDSHREW_RPC_URL`
  - calls `get_all_pools_details_via_raw_simulate(&url, ...)` to fetch and decode each pool's details
  - batch upserts `Pool` and inserts new `PoolState` snapshots on change

### Pool discovery implementation details
- We rely on deezel-common's `alkanes::amm::AmmManager` helpers, which accept a Sandshrew/Metashrew URL parameter.
- The indexer reads `SANDSHREW_RPC_URL` from the environment and passes it to these helpers; we do not mutate process env at runtime.
- Local hex decoding utilities have been removed from `src/helpers/pools.rs` to avoid drift from upstream decode logic.

### Troubleshooting
- Verify `DATABASE_URL` is reachable.
- Ensure `SANDSHREW_RPC_URL` points to a running endpoint that supports `metashrew_height`.
- Increase `POLL_INTERVAL_MS` if your environment is resource-constrained.
- Enable debug logs to inspect simulate responses:
  - `RUST_LOG=alkanes_contract_indexer=debug,deezel_common=debug cargo run`
- If you see `Other error: Failed to decode get_all_pools result`:
  - Confirm `SANDSHREW_RPC_URL` is correct (no localhost fallback).
  - Ensure the factory IDs (`FACTORY_BLOCK_ID`, `FACTORY_TX_ID`) are correct for your network.
  - Upstream can sometimes return placeholder IDs like `{"block":"0","tx":"0"}`; these will cause per-pool detail simulate to fail with `unexpected end-of-file` and are skipped upstream when present.

### References
- deezel toolkit (used for RPC/provider): [`Sprimage/deezel`](https://github.com/Sprimage/deezel)
