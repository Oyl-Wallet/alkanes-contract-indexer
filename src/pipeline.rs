use anyhow::Result;
use deezel_common::provider::ConcreteProvider;
use deezel_common::traits::{DeezelProvider, JsonRpcProvider, BitcoinRpcProvider};
use sqlx::PgPool;
use tracing::info;
use crate::helpers::pools::{fetch_and_upsert_pools_for_tip};
use crate::helpers::block::{get_block_hash as helper_get_block_hash, get_block_txids as helper_get_block_txids, get_transactions_info as helper_get_transactions_info, tx_has_op_return};
use crate::helpers::protostone::decode_and_trace_for_block;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct BlockContext {
	pub height: u64,
}

#[derive(Clone, Debug)]
pub struct Pipeline {
	pool: PgPool,
	factory_block_id: String,
	factory_tx_id: String,
}

impl Pipeline {
	pub fn new(pool: PgPool, factory_block_id: String, factory_tx_id: String) -> Self {
		Self { pool, factory_block_id, factory_tx_id }
	}

	// Runs on every new tip height (even during catch-up)
	pub async fn fetch_pools_for_tip(&self, provider: &ConcreteProvider, tip_height: u64) -> Result<()> {
		fetch_and_upsert_pools_for_tip(
			provider,
			&self.pool,
			&self.factory_block_id,
			&self.factory_tx_id,
			tip_height,
		).await
	}

	// Sequential per-block processing (historical and then following tip)
	pub async fn process_block_sequential<P>(&self, provider: &P, ctx: BlockContext) -> Result<()>
	where
		P: DeezelProvider + JsonRpcProvider + BitcoinRpcProvider + Send + Sync,
	{
		// Resolve block hash via bitcoind and print/log it
		let block_hash = helper_get_block_hash(provider, ctx.height).await?;
		info!(height = ctx.height, %block_hash, "resolved block hash");

		// Fetch txids for the block via JSON-RPC helper
		let txids = helper_get_block_txids(provider, &block_hash).await?;
		info!(height = ctx.height, count = txids.len(), "esplora_block::txids fetched");

		// Fetch tx infos concurrently using helper and maintain order
		let txs = helper_get_transactions_info(provider, &txids, 25).await?;
		info!(height = ctx.height, txs = txs.len(), "esplora_tx fetched");

		// Filter for OP_RETURN outputs
		let opret_count: usize = txs.iter().filter(|tx| tx_has_op_return(tx)).count();
		info!(height = ctx.height, op_return_txs = opret_count, "OP_RETURN transactions in block");

		// Build filtered list of OP_RETURN transactions only
		let op_return_txs: Vec<_> = txs.iter().filter(|tx| tx_has_op_return(tx)).cloned().collect();

		// Decode+trace protostones for this block (only OP_RETURN txs) with timing
		if !op_return_txs.is_empty() {
			let count = op_return_txs.len();
			let t0 = Instant::now();
			info!(height = ctx.height, op_return_txs = count, "decode_and_trace_for_block: start");
			decode_and_trace_for_block(provider, &op_return_txs, 32, 16).await?;
			let elapsed_ms = t0.elapsed().as_millis() as u64;
			info!(height = ctx.height, op_return_txs = count, elapsed_ms, "decode_and_trace_for_block: done");
		}
		Ok(())
	}
}


