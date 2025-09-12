use anyhow::Result;
use deezel_common::provider::ConcreteProvider;
use deezel_common::traits::AlkanesProvider;
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::warn;

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
    let contract_id = format!("{}:{}", factory_block, factory_tx);
    let sim_all = provider.simulate(&contract_id, Some("3")).await?;
    let data_hex = sim_all.get("execution").and_then(|e| e.get("data")).and_then(|v| v.as_str()).unwrap_or("0x");
    let all = decode_get_all_pools(data_hex).unwrap_or_default();

    let mut futs = FuturesUnordered::new();
    for id in &all.pools {
        let cid = format!("{}:{}", id.block, id.tx);
        let provider_ref = provider;
        let pool_block = id.block.clone();
        let pool_tx = id.tx.clone();
        futs.push(async move {
            let res = provider_ref.simulate(&cid, Some("999")).await;
            (pool_block, pool_tx, res)
        });
    }

    let mut out = Vec::new();
    while let Some((pool_block, pool_tx, res)) = futs.next().await {
        match res {
            Ok(json) => {
                if let Some(hex) = json.get("execution").and_then(|e| e.get("data")).and_then(|v| v.as_str()) {
                    if let Some(d) = decode_pool_details(hex) {
                        out.push(PoolWithDetails { pool_block, pool_tx, details: d });
                    }
                }
            }
            Err(e) => {
                warn!(block = %pool_block, tx = %pool_tx, error = %e, "simulate pool details failed");
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Default)]
struct GetAllPoolsResult { pools: Vec<TypesAlkaneId> }

fn strip_0x(s: &str) -> &str { s.strip_prefix("0x").unwrap_or(s) }

fn read_u64_le(bytes: &[u8], offset: usize) -> Option<u64> {
    if bytes.len() < offset + 8 { return None; }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[offset..offset+8]);
    Some(u64::from_le_bytes(buf))
}

fn parse_alkane_id_from_hex(hex_str: &str) -> Option<TypesAlkaneId> {
    let clean = strip_0x(hex_str);
    if clean.len() < 64 { return None; }
    let block_hex = &clean[0..32];
    let tx_hex = &clean[32..64];

    let mut block_bytes = hex::decode(block_hex).ok()?;
    block_bytes.reverse();
    let mut tx_bytes = hex::decode(tx_hex).ok()?;
    tx_bytes.reverse();

    let block = {
        let slice = if block_bytes.len() >= 8 { &block_bytes[0..8] } else { return None; };
        let mut buf = [0u8; 8];
        buf.copy_from_slice(slice);
        u64::from_be_bytes(buf).to_string()
    };
    let tx = {
        let slice = if tx_bytes.len() >= 8 { &tx_bytes[0..8] } else { return None; };
        let mut buf = [0u8; 8];
        buf.copy_from_slice(slice);
        u64::from_be_bytes(buf).to_string()
    };
    Some(TypesAlkaneId { block, tx })
}

fn decode_get_all_pools(data_hex: &str) -> Option<GetAllPoolsResult> {
    if data_hex == "0x" { return None; }
    let clean = strip_0x(data_hex);
    if clean.len() < 32 { return None; }
    let mut count_bytes = hex::decode(&clean[0..32]).ok()?;
    count_bytes.reverse();
    let count = u128::from_str_radix(&hex::encode(count_bytes), 16).ok()? as usize;
    let mut pools = Vec::new();
    for i in 0..count {
        let offset = 32 + i * 64;
        if clean.len() < offset + 64 { break; }
        let entry_hex = &clean[offset..offset+64];
        if let Some(id) = parse_alkane_id_from_hex(entry_hex) {
            pools.push(id);
        }
    }
    Some(GetAllPoolsResult { pools })
}

fn decode_pool_details(data_hex: &str) -> Option<PoolDetailsResult> {
    if data_hex == "0x" { return None; }
    let clean = strip_0x(data_hex);
    let bytes = hex::decode(clean).ok()?;

    let token0_block = read_u64_le(&bytes, 0)?.to_string();
    let token0_tx = read_u64_le(&bytes, 16)?.to_string();
    let token1_block = read_u64_le(&bytes, 32)?.to_string();
    let token1_tx = read_u64_le(&bytes, 48)?.to_string();
    let token0_amount = read_u64_le(&bytes, 64)?;
    let token1_amount = read_u64_le(&bytes, 80)?;
    let token_supply = read_u64_le(&bytes, 96)?;
    let pool_name = if bytes.len() > 116 {
        String::from_utf8_lossy(&bytes[116..]).to_string()
    } else { String::new() };

    Some(PoolDetailsResult {
        token0: TypesAlkaneId { block: token0_block, tx: token0_tx },
        token1: TypesAlkaneId { block: token1_block, tx: token1_tx },
        token0_amount, token1_amount, token_supply, pool_name
    })
}


