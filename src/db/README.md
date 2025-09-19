## Database: Schema and Write Paths

This document describes the current database schema for hot tables and the write paths used by the indexer. It focuses on correctness, scalability, and predictable performance at tens of millions to billions of rows.

### Tables and Keys

- AlkaneTransaction
  - Primary key: `transactionId` (text)
  - Columns: `blockHeight` int, `transactionIndex` int, flags `hasTrace`/`traceSucceed`, `transactionData` jsonb, timestamps
  - Indexes:
    - btree: (`blockHeight`, `transactionIndex`) for per-block ordering/queries
    - BRIN: `blockHeight` for range scans
  - Storage: `transactionData` set to STORAGE EXTERNAL

- TraceEvent
  - Primary key: `id` uuid
  - Columns: `transactionId` text (FK -> AlkaneTransaction.transactionId), `blockHeight` int, `vout` int, `eventType` text, `data` jsonb, `alkaneAddressBlock` text, `alkaneAddressTx` text, timestamps
  - Indexes:
    - btree: `transactionId`
    - btree: (`blockHeight`, `eventType`)
    - BRIN: `blockHeight`
  - Storage/Tuning: `data` STORAGE EXTERNAL; table `fillfactor=80`, `autovacuum_vacuum_scale_factor=0.01`, `autovacuum_vacuum_threshold=5000`, `autovacuum_analyze_scale_factor=0.02`

- DecodedProtostone
  - Primary key: (`transactionId`, `vout`, `protostoneIndex`)
  - Columns: `blockHeight` int, `decoded` jsonb, timestamps
  - Indexes:
    - BRIN: `blockHeight`
  - Storage/Tuning: `decoded` STORAGE EXTERNAL; table `fillfactor=80`, `autovacuum_vacuum_scale_factor=0.01`, `autovacuum_vacuum_threshold=5000`, `autovacuum_analyze_scale_factor=0.02`

Other tables (Pools, PoolState, PoolSwap/Creation/Mint/Burn, ClockIn*) are documented in `src/schema.rs`. Notable updates:

- Pool* success flag
  - `PoolSwap`, `PoolMint`, and `PoolBurn` include `successful boolean not null default true`.
  - Indexes: composite btree indexes on (`successful`,`blockHeight`,`transactionIndex`) exist on these tables to accelerate filtered queries.
  - Writers (`replace_pool_swaps`, `replace_pool_mints`, `replace_pool_burns`) accept a trailing `successful: bool` and always write a row per candidate invoke. Failed attempts are recorded with zero amounts and `successful=false`.
  - `PoolCreation` remains success-only (it also has a `successful` column defaulting to true for schema consistency), enforced by decode logic; failures are not recorded in this table due to FK guarantees on (`poolBlockId`,`poolTxId`).

### Write Paths (batching and replacements)

- Upsert AlkaneTransaction
  - Function: `db::transactions::upsert_alkane_transactions`
  - Batches rows and uses `ON CONFLICT (transactionId) DO UPDATE` with a no-op guard via `IS DISTINCT FROM` to avoid unnecessary updates/WAL.
  - Batch size clamped to keep each INSERT under parameter limits and ~1s execution.

- Replace TraceEvent
  - Function: `db::transactions::replace_trace_events`
  - Shape per row: `(transactionId, blockHeight, vout, eventType, data, alkaneAddressBlock, alkaneAddressTx)`
  - Deletes existing rows for txids using CTE + `unnest($1::text[])` for better plans on large arrays, then inserts in chunks.

- Replace DecodedProtostone
 - Replace PoolSwap/PoolMint/PoolBurn
  - Functions: `db::transactions::{replace_pool_swaps, replace_pool_mints, replace_pool_burns}`
  - Behavior: delete existing rows for the provided txids, then insert provided rows in chunks under parameter limits.
  - Shape per row:
    - PoolSwap: `(transactionId, blockHeight, transactionIndex, poolBlockId, poolTxId, soldTokenBlockId, soldTokenTxId, boughtTokenBlockId, boughtTokenTxId, soldAmount double, boughtAmount double, sellerAddress, successful, timestamp)`
    - PoolMint: `(transactionId, blockHeight, transactionIndex, poolBlockId, poolTxId, lpTokenAmount text, token0BlockId, token0TxId, token1BlockId, token1TxId, token0Amount text, token1Amount text, minterAddress, successful, timestamp)`
    - PoolBurn: `(transactionId, blockHeight, transactionIndex, poolBlockId, poolTxId, lpTokenAmount text, token0BlockId, token0TxId, token1BlockId, token1TxId, token0Amount text, token1Amount text, burnerAddress, successful, timestamp)`

  - Function: `db::transactions::replace_decoded_protostones`
  - Shape per row: `(transactionId, vout, protostoneIndex, blockHeight, decoded)`
  - Deletes existing rows for txids using CTE + `unnest`; inserts with `ON CONFLICT ... DO UPDATE` guarded by `IS DISTINCT FROM` on `decoded`.

### Performance Considerations

- BRIN on `blockHeight` localizes range-scoped reads and maintenance even for very large tables.
- Redundant indexes were avoided; composite and PKs cover most access. This reduces write amplification and index churn.
- JSONB moved to EXTERNAL storage to reduce heap page churn and improve HOT update chances for metadata columns.
- Autovacuum settings are more aggressive on high-churn tables to keep bloat in check; tune per deployment if needed.
- Batch sizes for INSERTs are clamped to avoid slow statements; deletes use `unnest` to avoid bad planner choices with huge `= ANY($1)` arrays.

### Operational Notes

- Schema management via `cargo run --bin dbctl -- push|reset|drop`.
- The indexer writes the three hot tables in a single transaction per block, minimizing partial states.
- If you need to reprocess a block: run `cargo run --bin reprocess -- --height <H>`; it will recompute and fully replace rows for that height’s txids.


