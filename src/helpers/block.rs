use anyhow::Result;
use deezel_common::traits::{BitcoinRpcProvider, JsonRpcProvider, DeezelProvider, MetashrewRpcProvider};
use serde_json::Value as JsonValue;
use serde_json::json;
use std::env;

// Resolve block hash by height via Bitcoin RPC provider
pub async fn get_block_hash<P>(provider: &P, height: u64) -> Result<String>
where
	P: BitcoinRpcProvider + DeezelProvider + Send + Sync,
{
	let hash = <P as BitcoinRpcProvider>::get_block_hash(provider, height).await?;
	Ok(hash)
}

// Get txids for a block via JSON-RPC method `esplora_block::txids`
pub async fn get_block_txids<P>(provider: &P, block_hash: &str) -> Result<Vec<String>>
where
	P: JsonRpcProvider + DeezelProvider + Send + Sync,
{
	let url = env::var("SANDSHREW_RPC_URL")
		.ok()
		.or_else(|| provider.get_bitcoin_rpc_url())
		.unwrap_or_else(|| "http://localhost:18888".to_string());
	let txids_val = provider.call(&url, "esplora_block::txids", json!([block_hash]), 1).await?;
	let txids: Vec<String> = txids_val
		.as_array()
		.map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
		.unwrap_or_default();
	Ok(txids)
}

// Fetch tx infos for a list of txids concurrently (batch size controls max in-flight)
pub async fn get_transactions_info<P>(provider: &P, txids: &[String], batch_size: usize) -> Result<Vec<JsonValue>>
where
	P: JsonRpcProvider + DeezelProvider + Send + Sync,
{
	use futures::stream::{self, StreamExt};
	let url = env::var("SANDSHREW_RPC_URL")
		.ok()
		.or_else(|| provider.get_bitcoin_rpc_url())
		.unwrap_or_else(|| "http://localhost:18888".to_string());
	let results: Vec<Option<JsonValue>> = stream::iter(txids.iter().cloned().enumerate())
		.map(|(_idx, txid)| {
			let url_inner = url.clone();
			let provider_ref = provider;
			async move {
				match provider_ref.call(&url_inner, "esplora_tx", json!([txid]), 1).await {
					Ok(v) => Some(v),
					Err(_e) => None,
				}
			}
		})
		.buffer_unordered(batch_size)
		.collect()
		.await;
	let txs: Vec<JsonValue> = results.into_iter().flatten().collect();
	Ok(txs)
}

// Determine if a transaction JSON has any OP_RETURN outputs
pub fn tx_has_op_return(tx_json: &JsonValue) -> bool {
	let Some(vout) = tx_json.get("vout").and_then(|v| v.as_array()) else { return false };
	for o in vout {
		if let Some(t) = o.get("scriptpubkey_type").and_then(|v| v.as_str()) {
			if t.eq_ignore_ascii_case("op_return") { return true; }
		}
		if let Some(asm) = o.get("scriptpubkey_asm").and_then(|v| v.as_str()) {
			if asm.starts_with("OP_RETURN") { return true; }
		}
		if let Some(spk) = o.get("scriptpubkey").and_then(|v| v.as_str()) {
			if spk.starts_with("6a") { return true; }
		}
	}
	false
}

// Returns the canonical chain tip height by subtracting 1 from Metashrew's reported height,
// which is known to be off-by-one (reports next height).
pub async fn canonical_tip_height<P: MetashrewRpcProvider>(provider: &P) -> Result<u64> {
	let h = provider.get_metashrew_height().await?;
	if h == 0 {
		return Err(anyhow::anyhow!("unexpected metashrew height 0"));
	}
	Ok(h - 1)
}


