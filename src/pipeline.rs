use anyhow::Result;
use deezel_common::provider::ConcreteProvider;
use deezel_common::traits::{BitcoinRpcProvider, EsploraProvider};
use sqlx::PgPool;
use tracing::info;
use crate::helpers::pools::fetch_all_pools_with_details;
use crate::db::{pools as db_pools, pool_state as db_pool_state};
use crate::helpers::opreturn::collect_block_opreturns;

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
		let fetched = fetch_all_pools_with_details(provider, &self.factory_block_id, &self.factory_tx_id).await?;
		if fetched.is_empty() {
			return Ok(());
		}

		// DB upserts in a transaction
		let mut txdb = self.pool.begin().await?;

		// Existing pools map for this factory
		let existing_map = db_pools::get_existing_pools_for_factory(&mut txdb, &self.factory_block_id, &self.factory_tx_id).await?;

		// Identify new pools
		let new_pools: Vec<(String, String, String, String, String, String, String)> = fetched.iter()
			.filter(|it| !existing_map.contains_key(&(it.pool_block.clone(), it.pool_tx.clone())))
			.map(|it| (
				it.pool_block.clone(),
				it.pool_tx.clone(),
				it.details.token0.block.clone(),
				it.details.token0.tx.clone(),
				it.details.token1.block.clone(),
				it.details.token1.tx.clone(),
				it.details.pool_name.clone(),
			))
			.collect();

		if !new_pools.is_empty() {
			db_pools::insert_new_pools(&mut txdb, &self.factory_block_id, &self.factory_tx_id, &new_pools).await?;
		}

		// Fetch all pool DB IDs for the set of pools we have details for
		let pool_pairs: Vec<(String, String)> = fetched.iter().map(|p| (p.pool_block.clone(), p.pool_tx.clone())).collect();
		let id_map = db_pools::get_pool_ids_for_pairs(&mut txdb, &self.factory_block_id, &self.factory_tx_id, &pool_pairs).await?;

		// Fetch latest PoolState per pool
		let id_vec: Vec<&str> = id_map.values().map(|s| s.as_str()).collect();
		let last_state = db_pool_state::get_latest_pool_states(&mut txdb, &id_vec).await?;

		// Prepare new snapshots for changed states
		let mut snapshots: Vec<(String, i32, String, String, String)> = Vec::new();
		for item in &fetched {
			if let Some(pool_id) = id_map.get(&(item.pool_block.clone(), item.pool_tx.clone())) {
				let new_t0 = item.details.token0_amount.to_string();
				let new_t1 = item.details.token1_amount.to_string();
				let new_sup = item.details.token_supply.to_string();
				let changed = match last_state.get(pool_id) {
					Some((t0, t1, sup)) => t0 != &new_t0 || t1 != &new_t1 || sup != &new_sup,
					None => true,
				};
				if changed {
					snapshots.push((pool_id.clone(), tip_height as i32, new_t0, new_t1, new_sup));
				}
			}
		}

		// Batch insert snapshots
		if !snapshots.is_empty() {
			db_pool_state::insert_pool_state_snapshots(&mut txdb, &snapshots).await?;
		}

		txdb.commit().await?;
		info!(height = tip_height, pools = fetched.len(), inserts = snapshots.len(), "pools and states updated");
		Ok(())
	}

	// Sequential per-block processing (historical and then following tip)
	pub async fn process_block_sequential<P>(&self, provider: &P, ctx: BlockContext) -> Result<()>
	where
		P: BitcoinRpcProvider + EsploraProvider + Send + Sync,
	{
		// 1) Resolve block hash via bitcoind
		let block_hash = provider.get_block_hash(ctx.height).await?;

		// 2) Collect OP_RETURN txs with their index efficiently via Esplora paging
		let op_returns = collect_block_opreturns(provider, &block_hash).await?;
		info!(height = ctx.height, txs = op_returns.len(), "OP_RETURN txs collected for block");

		// TODO: decode protostones and trace calls in subsequent steps
		Ok(())
	}
}


