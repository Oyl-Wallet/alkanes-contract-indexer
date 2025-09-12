use anyhow::{anyhow, Result};
use deezel_common::traits::EsploraProvider;
use futures::stream::{self, StreamExt};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

pub async fn collect_block_opreturns<P>(provider: &P, block_hash: &str) -> Result<Vec<(u32, String)>>
where
	P: EsploraProvider + Send + Sync,
{
	let txids_json = provider.get_block_txids(block_hash).await?;
	let txids: Vec<String> = serde_json::from_value(txids_json)
		.map_err(|e| anyhow!("invalid esplora_block:txids response: {e}"))?;
	if txids.is_empty() {
		return Ok(Vec::new());
	}

	let index_map: HashMap<String, u32> = txids
		.iter()
		.enumerate()
		.map(|(i, t)| (t.clone(), i as u32))
		.collect();

	let page_size: u32 = 25;
	let starts: Vec<u32> = (0..(txids.len() as u32)).step_by(page_size as usize).collect();
	let concurrency: usize = 6;
	let index_map_clone = index_map.clone();
	let results: Vec<Vec<(u32, String)>> = stream::iter(starts)
		.map(move |start| {
			let index_map_inner = index_map_clone.clone();
			async move {
				let page_json: JsonValue = provider
					.get_block_txs(block_hash, Some(start))
					.await
					.unwrap_or(JsonValue::Array(vec![]));
				extract_op_return_txids_from_block_page(&page_json, &index_map_inner)
			}
		})
		.buffer_unordered(concurrency)
		.collect()
		.await;

	let mut flat: Vec<(u32, String)> = results.into_iter().flatten().collect();
	flat.sort_by_key(|(idx, _)| *idx);
	Ok(flat)
}

fn extract_op_return_txids_from_block_page(page_json: &JsonValue, index_map: &HashMap<String, u32>) -> Vec<(u32, String)> {
	let mut found: Vec<(u32, String)> = Vec::new();
	let Some(arr) = page_json.as_array() else { return found };
	for tx in arr {
		let Some(txid) = tx.get("txid").and_then(|v| v.as_str()) else { continue };
		let Some(vout) = tx.get("vout").and_then(|v| v.as_array()) else { continue };
		let mut has_opret = false;
		for o in vout {
			if let Some(t) = o.get("scriptpubkey_type").and_then(|v| v.as_str()) {
				if t.eq_ignore_ascii_case("op_return") { has_opret = true; break; }
			}
			if let Some(asm) = o.get("scriptpubkey_asm").and_then(|v| v.as_str()) {
				if asm.starts_with("OP_RETURN") { has_opret = true; break; }
			}
			if let Some(spk) = o.get("scriptpubkey").and_then(|v| v.as_str()) {
				if spk.starts_with("6a") { has_opret = true; break; }
			}
		}
		if has_opret {
			if let Some(idx) = index_map.get(txid) {
				found.push((*idx, txid.to_string()));
			}
		}
	}
	found
}


