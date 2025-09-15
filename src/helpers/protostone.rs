use anyhow::{Context, Result};
use bitcoin::consensus::encode::deserialize;
use bitcoin::Transaction;
use deezel_common::runestone_enhanced::format_runestone_with_decoded_messages;
use deezel_common::traits::{DeezelProvider, JsonRpcProvider, BitcoinRpcProvider, EsploraProvider};
use serde_json::{json, Value as JsonValue};
use tokio::time::{sleep, timeout, Duration};
use futures::stream::{self, StreamExt};
use tracing::{debug, error, info, warn};

use crate::helpers::block::tx_has_op_return;

#[derive(Debug, Clone)]
struct TraceJob {
    txid_le_hex: String,
    vout: u32,
    protostone_idx: usize,
}

fn to_little_endian_hex(txid_be_hex: &str) -> String {
    match hex::decode(txid_be_hex) {
        Ok(mut b) => {
            b.reverse();
            hex::encode(b)
        }
        Err(_) => txid_be_hex.to_string(),
    }
}

async fn trace_call<P: DeezelProvider + JsonRpcProvider + Send + Sync>(
    provider: &P,
    url: &str,
    job: TraceJob,
) -> Result<JsonValue> {
    let req = json!([{ "txid": job.txid_le_hex, "vout": job.vout }]);
    let res = provider
        .call(url, "alkanes_trace", req, 1)
        .await
        .context("alkanes_trace call failed")?;
    Ok(res)
}

async fn tx_from_json_or_fetch_hex<P: DeezelProvider + JsonRpcProvider + BitcoinRpcProvider + EsploraProvider + Send + Sync>(
    provider: &P,
    tx_json: &JsonValue,
) -> Result<Transaction> {
    // Prefer embedded hex if present; fallback to JSON-RPC "esplora_tx::hex"
    if let Some(hex_str) = tx_json.get("hex").and_then(|v| v.as_str()) {
        let raw = hex::decode(hex_str).context("failed to decode tx hex")?;
        let tx: Transaction = deserialize(&raw).context("failed to deserialize tx")?;
        return Ok(tx);
    }

    let txid = tx_json
        .get("txid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("txid missing in tx json"))?;
    // First try EsploraProvider::get_tx_hex (works with native-deps or JSON-RPC proxy), then fall back to bitcoind getrawtransaction
    info!(%txid, "fetching tx hex via EsploraProvider::get_tx_hex");
    let mut last_err: Option<anyhow::Error> = None;
    let hex_str = {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let fut = provider.get_tx_hex(txid);
            match timeout(Duration::from_secs(20), fut).await {
                Ok(Ok(h)) => break h,
                Ok(Err(e)) => {
                    last_err = Some(anyhow::anyhow!(e));
                    warn!(%txid, attempt, "get_tx_hex error; will retry or fall back");
                }
                Err(_elapsed) => {
                    last_err = Some(anyhow::anyhow!("timeout"));
                    warn!(%txid, attempt, "get_tx_hex timed out; will retry or fall back");
                }
            }
            if attempt >= 2 { break String::new(); }
            sleep(Duration::from_millis(200 * attempt as u64)).await;
        }
    };
    let hex_str = if !hex_str.is_empty() { hex_str } else {
        info!(%txid, "falling back to BitcoinRpcProvider::get_transaction_hex");
        let mut attempt = 0;
        loop {
            attempt += 1;
            let fut = provider.get_transaction_hex(txid);
            match timeout(Duration::from_secs(20), fut).await {
                Ok(Ok(h)) => break h,
                Ok(Err(e)) => {
                    last_err = Some(anyhow::anyhow!(e));
                    warn!(%txid, attempt, "get_transaction_hex error; will retry");
                }
                Err(_elapsed) => {
                    last_err = Some(anyhow::anyhow!("timeout"));
                    warn!(%txid, attempt, "get_transaction_hex timed out; will retry");
                }
            }
            if attempt >= 3 {
                return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("get_transaction_hex failed"))).context("get_transaction_hex call failed");
            }
            sleep(Duration::from_millis(200 * attempt as u64)).await;
        }
    };
    let raw = hex::decode(hex_str).context("failed to decode tx hex")?;
    let tx: Transaction = deserialize(&raw).context("failed to deserialize tx")?;
    debug!(%txid, size = raw.len(), "decoded tx hex");
    Ok(tx)
}

fn resolve_sandshrew_url<P: JsonRpcProvider + DeezelProvider>(provider: &P) -> String {
    std::env::var("SANDSHREW_RPC_URL")
        .ok()
        .or_else(|| provider.get_bitcoin_rpc_url())
        .unwrap_or_else(|| "http://localhost:18888".to_string())
}

/// Decode runestones for OP_RETURN txs and process them in 10 concurrent batches.
pub async fn decode_and_trace_for_block<P>(
    provider: &P,
    txs: &[JsonValue],
    _max_decode_in_flight: usize,
    _max_trace_concurrency: usize,
) -> Result<()>
where
    P: DeezelProvider + JsonRpcProvider + BitcoinRpcProvider + EsploraProvider + Send + Sync,
{
    let url = resolve_sandshrew_url(provider);
    info!(txs = txs.len(), "decode_and_trace_for_block: start (batched parallel)");
    // Only OP_RETURN txs
    let op_return_txs: Vec<JsonValue> = txs.iter().filter(|t| tx_has_op_return(t)).cloned().collect();
    let total = op_return_txs.len();
    info!(op_return_txs = total, "filtered OP_RETURN transactions");
    if total == 0 { return Ok(()); }

    // Split into up to 10 batches and process each batch concurrently.
    let num_batches = usize::min(10, total);
    let batch_size = (total + num_batches - 1) / num_batches; // ceildiv
    let batches: Vec<Vec<JsonValue>> = op_return_txs
        .chunks(batch_size)
        .map(|c| c.to_vec())
        .collect();

    stream::iter(batches.into_iter().enumerate())
        .for_each_concurrent(num_batches, |(batch_idx, batch)| {
            let url = url.clone();
            async move {
            info!(batch = batch_idx, size = batch.len(), "batch start");
            for (local_idx, tx_json) in batch.into_iter().enumerate() {
                let txid_str = tx_json.get("txid").and_then(|v| v.as_str()).unwrap_or("<no-txid>");
                info!(batch = batch_idx, index = local_idx, %txid_str, "fetching tx hex");
                let tx = match tx_from_json_or_fetch_hex(provider, &tx_json).await {
                    Ok(t) => t,
                    Err(e) => { error!(batch = batch_idx, %txid_str, error = %e, "failed to materialize tx; skipping"); continue; }
                };
                info!(batch = batch_idx, index = local_idx, %txid_str, outputs = tx.output.len(), "tx ready; decoding runestone");
                match format_runestone_with_decoded_messages(&tx) {
                    Ok(formatted) => {
                        let txid_be = formatted.get("transaction_id").and_then(|v| v.as_str()).map(|s| s.to_string()).unwrap_or_else(|| tx.compute_txid().to_string());
                        let txid_le = to_little_endian_hex(&txid_be);
                        let start = (tx.output.len() as u32) + 1;
                        let protos = formatted.get("protostones").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                        info!(batch = batch_idx, %txid_be, protostones = protos.len(), start_vout = start, "decoded runestone");
                        for (i, _p) in protos.iter().enumerate() {
                            let vout = start + i as u32;
                            info!(batch = batch_idx, %txid_be, protostone_idx = i, vout, "calling trace");
                            let job = TraceJob { txid_le_hex: txid_le.clone(), vout, protostone_idx: i };
                            match trace_call(provider, &url, job).await {
                                Ok(res) => { info!(batch = batch_idx, %txid_be, protostone_idx = i, vout, "trace ok"); debug!(result = %res); }
                                Err(e) => { error!(batch = batch_idx, %txid_be, protostone_idx = i, vout, error = %e, "trace failed"); }
                            }
                        }
                    }
                    Err(e) => { debug!(batch = batch_idx, %txid_str, error = %e, "format_runestone_with_decoded_messages failed"); }
                }
            }
            info!(batch = batch_idx, "batch complete");
            }
        })
        .await;

    info!("decode_and_trace_for_block: complete (batched parallel)");

    Ok(())
}


