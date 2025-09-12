use anyhow::Result;
use deezel_common::provider::ConcreteProvider;
use std::sync::Arc;
use deezel_common::alkanes::amm::AmmManager;
use std::env;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct TypesAlkaneId { pub block: String, pub tx: String }

#[derive(Debug, Clone)]
pub struct PoolDetailsResult {
    pub token0: TypesAlkaneId,
    pub token1: TypesAlkaneId,
    pub token0_amount: u64,
    pub token1_amount: u64,
    pub token_supply: u64,
    pub pool_name: String,
}

#[derive(Debug, Clone)]
pub struct PoolWithDetails {
    pub pool_block: String,
    pub pool_tx: String,
    pub details: PoolDetailsResult,
}

pub async fn fetch_all_pools_with_details(
    provider: &ConcreteProvider,
    factory_block: &str,
    factory_tx: &str,
) -> Result<Vec<PoolWithDetails>> {
    // Use AmmManager helpers which perform raw simulate and decode for us
    let amm = AmmManager::new(Arc::new(provider.clone()));
    // Use SANDSHREW_RPC_URL from environment (.env loaded in main)
    let url = env::var("SANDSHREW_RPC_URL").unwrap_or_else(|_| "http://localhost:18888".to_string());
    debug!(url = %url, factory_block, factory_tx, "fetch pools via AmmManager");
    let details = amm
        .get_all_pools_details_via_raw_simulate(&url, factory_block.to_string(), factory_tx.to_string())
        .await?;

    let mut out = Vec::with_capacity(details.count);
    for p in details.pools {
        let mapped = PoolWithDetails {
            pool_block: p.pool_id.block.to_string(),
            pool_tx: p.pool_id.tx.to_string(),
            details: PoolDetailsResult {
                token0: TypesAlkaneId { block: p.token0.block.to_string(), tx: p.token0.tx.to_string() },
                token1: TypesAlkaneId { block: p.token1.block.to_string(), tx: p.token1.tx.to_string() },
                token0_amount: p.token0_amount,
                token1_amount: p.token1_amount,
                token_supply: p.token_supply,
                pool_name: p.pool_name,
            },
        };
        out.push(mapped);
    }
    Ok(out)
}

