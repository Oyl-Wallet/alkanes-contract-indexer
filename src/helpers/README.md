# Helpers: Technical Documentation

This directory groups helper modules used by the indexer pipeline. These modules isolate RPC access, decoding logic, and domain-specific processing so they can be extended or optimized independently.

## block.rs
- canonical_tip_height(provider): Returns metashrew_height - 1 to correct Metashrew's off-by-one. Use this everywhere you need the chain tip.
- get_block_hash(provider, height): Thin wrapper over Bitcoin RPC to get the block hash by height.
- get_block_txids(provider, block_hash): Calls JSON-RPC esplora_block::txids on the configured Sandshrew/Metashrew endpoint (from SANDSHREW_RPC_URL or provider default).
- get_transactions_info(provider, txids, batch_size): Concurrent fan-out using Futures streams to fetch esplora_tx for each txid. Returns a Vec<serde_json::Value> (preserving inputs order after collection is not guaranteed; callers that require order should re-map).
- tx_has_op_return(tx_json): Utility to detect OP_RETURN outputs based on scriptpubkey_type, scriptpubkey_asm, or hex prefix 6a.

Tips:
- Adjust batch_size based on your RPC capacity. The helper already applies backpressure via .buffer_unordered(batch_size).

## pools.rs
- Uses `deezel_common::alkanes::amm::AmmManager` to discover pools and fetch per-pool details via upstream raw simulate APIs.
- `fetch_all_pools_with_details(provider, factory_block, factory_tx)`: Two-step concurrent flow that respects `SANDSHREW_RPC_URL` from the environment.
  1. Calls `AmmManager::get_all_pools_via_raw_simulate(&url, factory_block, factory_tx)` to obtain pool IDs.
  2. Fetches each pool's details with bounded parallelism (10 in-flight) via `AmmManager::get_pool_details_via_raw_simulate(&url, pool_block, pool_tx)` and collects results.
- fetch_and_upsert_pools_for_tip(provider, pool, factory_block, factory_tx, tip_height): E2E helper to fetch pools, insert any new pools, then insert PoolState snapshots only when values changed since the last snapshot.

Tips:
- Database writes are batched in a transaction for consistency. Use the same pattern if you add new upserts.
 - Pool detail RPC fetches are performed concurrently with a fixed concurrency of 10 to balance throughput and upstream load.

## protostone.rs
Implements the Runestone/Protostone decode + trace flow with 10-way batched parallelism for OP_RETURN transactions and returns structured results for DB writes.

- decode_and_trace_for_block(provider, txs, _, _): Returns `Vec<TxDecodeTraceResult>`; processes only OP_RETURN transactions in up to 10 concurrent batches:
  1. Fetch raw tx hex using EsploraProvider::get_tx_hex(txid); fallback to BitcoinRpcProvider::get_transaction_hex(txid) with timeout/backoff retries and INFO/WARN logs.
  2. Deserialize hex into bitcoin::Transaction.
  3. Decode runestone/protostones via deezel_common::runestone_enhanced::format_runestone_with_decoded_messages.
  4. Compute shadow vouts: start = tx.output.len() + 1; vout = start + i for i-th protostone.
  5. Reverse txid to little-endian and call alkanes_trace per protostone; collect `decoded_protostones` and `trace_events`.

Types:
- `TxDecodeTraceResult { transaction_id, transaction_json, decoded_protostones, trace_events, has_trace, trace_succeed }`
- `DecodedProtostoneItem { vout, protostone_index, decoded }`
- `TraceEventItem { vout, event_type, data, alkane_address_block, alkane_address_tx }`

Concurrency model:
- OP_RETURN transactions are split into ceil(total/10) sized chunks; each chunk is processed concurrently. This yields significant end-to-end speedups while keeping RPC pressure bounded.
- The signature includes `_max_decode_in_flight` and `_max_trace_concurrency` for future fine-grained controls if needed.

Extension points:
- Swap format_runestone_with_decoded_messages with a different decoder if protocol evolves.
- If you need strict ordering, carry index metadata through TraceJob and re-order at write time.

Operational considerations:
- SANDSHREW_RPC_URL is used as the default JSON-RPC endpoint. EsploraProvider will also use a direct HTTP ESPLORA_URL if compiled with native-deps.
- alkanes_trace expects little-endian txid hex; the helper converts the standard big-endian string before calling.
- Logs now include per-batch summaries (size, decoded, trace_ok/trace_err, skipped, elapsed_ms) and overall totals with elapsed time.

## Coding Guidelines
- Error handling: Prefer early returns and clear anyhow::Context messages so upstream callers get actionable logs.
- Logging: Use INFO for high-signal steps (fetch, decode, trace) and DEBUG for verbose payloads. Avoid spamming at INFO in hot loops unless debugging.
- Concurrency: When re-enabling, bound concurrency and use bounded channels to apply backpressure to upstream services.
