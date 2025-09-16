use anyhow::Result;
use clap::Parser;
use dotenvy::dotenv;
use serde_json::Value as JsonValue;
use sqlx::Row;
use tracing::{debug, info, warn};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(name = "inspect", about = "Inspect a transaction's trace, protostones, and pool swap decoding")] 
struct Cli {
    /// Transaction ID (big-endian txid string)
    transaction_id: String,

    /// Print full JSON for events and protostones
    #[arg(long)]
    verbose_json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(env_filter).init();

    let cli = Cli::parse();
    let cfg = alkanes_contract_indexer::config::AppConfig::from_env()?;
    let pool = alkanes_contract_indexer::db::connect(&cfg.database_url, 5).await?;

    info!(txid = %cli.transaction_id, "inspecting transaction");

    // Fetch AlkaneTransaction metadata
    let tx_row = sqlx::query(r#"select "blockHeight", "transactionIndex", "hasTrace", "traceSucceed", "transactionData" from "AlkaneTransaction" where "transactionId" = $1"#)
        .bind(&cli.transaction_id)
        .fetch_optional(&pool)
        .await?;

    if let Some(row) = tx_row {
        let bh: i32 = row.get("blockHeight");
        let idx: i32 = row.get("transactionIndex");
        let has_trace: bool = row.get("hasTrace");
        let trace_ok: bool = row.get("traceSucceed");
        let tx_json: JsonValue = row.get("transactionData");
        info!(block_height = bh, transaction_index = idx, has_trace, trace_ok, "AlkaneTransaction found");
        if cli.verbose_json {
            println!("Transaction JSON:\n{}", serde_json::to_string_pretty(&tx_json)?);
        }
    } else {
        warn!("AlkaneTransaction not found");
    }

    // Fetch decoded protostones
    let protos = sqlx::query(r#"select "vout", "protostoneIndex", "decoded" from "DecodedProtostone" where "transactionId" = $1 order by "vout", "protostoneIndex""#)
        .bind(&cli.transaction_id)
        .fetch_all(&pool)
        .await?;
    info!(count = protos.len(), "decoded protostones fetched");
    if cli.verbose_json {
        for r in &protos {
            let vout: i32 = r.get("vout");
            let idx: i32 = r.get("protostoneIndex");
            let decoded: JsonValue = r.get("decoded");
            println!("Protostone vout={}, idx={} ->\n{}", vout, idx, serde_json::to_string_pretty(&decoded)?);
        }
    }

    // Fetch stored swaps (if any)
    let swaps = sqlx::query(r#"select "poolBlockId", "poolTxId", "soldTokenBlockId", "soldTokenTxId", "boughtTokenBlockId", "boughtTokenTxId", "soldAmount", "boughtAmount", "sellerAddress" from "PoolSwap" where "transactionId" = $1"#)
        .bind(&cli.transaction_id)
        .fetch_all(&pool)
        .await?;
    if swaps.is_empty() {
        info!("no stored PoolSwap rows for this txid");
    } else {
        info!(rows = swaps.len(), "stored PoolSwap rows found");
        for r in &swaps {
            let pb: String = r.get("poolBlockId");
            let pt: String = r.get("poolTxId");
            let sb: String = r.get("soldTokenBlockId");
            let st: String = r.get("soldTokenTxId");
            let bb: String = r.get("boughtTokenBlockId");
            let bt: String = r.get("boughtTokenTxId");
            let s_amt: f64 = r.get("soldAmount");
            let b_amt: f64 = r.get("boughtAmount");
            let seller: Option<String> = r.get("sellerAddress");
            println!("Stored swap -> pool=({}:{}) sold=({}:{}) bought=({}:{}) amounts=({}, {}) seller={:?}", pb, pt, sb, st, bb, bt, s_amt, b_amt, seller);
        }
    }

    // Fetch trace events
    let events_rows = sqlx::query(r#"select "vout", "eventType", "data", "alkaneAddressBlock", "alkaneAddressTx" from "TraceEvent" where "transactionId" = $1 order by "vout" asc, case when "eventType"='invoke' then 0 when "eventType"='return' then 1 else 2 end"#)
        .bind(&cli.transaction_id)
        .fetch_all(&pool)
        .await?;
    info!(count = events_rows.len(), "trace events fetched");

    let mut events: Vec<JsonValue> = Vec::with_capacity(events_rows.len());
    for r in &events_rows {
        let vout: i32 = r.get("vout");
        let etype: String = r.get("eventType");
        let data: JsonValue = r.get("data");
        let blk: String = r.get("alkaneAddressBlock");
        let tx: String = r.get("alkaneAddressTx");
        let obj = serde_json::json!({
            "vout": vout,
            "eventType": etype,
            "data": data,
            "alkaneAddressBlock": blk,
            "alkaneAddressTx": tx,
        });
        if cli.verbose_json {
            println!("Event vout={} type={} ->\n{}", vout, obj.get("eventType").and_then(|v| v.as_str()).unwrap_or(""), serde_json::to_string_pretty(&obj)?);
        }
        events.push(obj);
    }

    // Run swap decoding simulation with detailed decisions
    simulate_pool_swaps(&pool, &events).await?;
    Ok(())
}

async fn fetch_pool_tokens(
    pool: &sqlx::PgPool,
    pb: &str,
    pt: &str,
) -> Result<Option<((String, String), (String, String))>> {
    let row = sqlx::query(r#"select "token0BlockId", "token0TxId", "token1BlockId", "token1TxId" from "Pool" where "poolBlockId" = $1 and "poolTxId" = $2"#)
        .bind(pb)
        .bind(pt)
        .fetch_optional(pool)
        .await?;
    if let Some(r) = row {
        let t0b: String = r.get("token0BlockId");
        let t0t: String = r.get("token0TxId");
        let t1b: String = r.get("token1BlockId");
        let t1t: String = r.get("token1TxId");
        Ok(Some(((t0b, t0t), (t1b, t1t))))
    } else { Ok(None) }
}

fn u128_from_json(obj: &JsonValue) -> Option<u128> {
    match (obj.get("hi"), obj.get("lo")) {
        (Some(hi), Some(lo)) => Some(((hi.as_u64()? as u128) << 64) | (lo.as_u64()? as u128)),
        _ => None,
    }
}

fn hex_to_u128(hex_str: &str) -> Option<u128> {
    let s = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    u128::from_str_radix(s, 16).ok()
}

fn value_u128_from_json(v: &JsonValue) -> Option<u128> {
    if let Some(obj) = v.as_object() {
        if obj.contains_key("hi") && obj.contains_key("lo") {
            return u128_from_json(v);
        }
    }
    if let Some(s) = v.as_str() {
        if s.starts_with("0x") || s.chars().any(|c| matches!(c, 'a'..='f' | 'A'..='F')) {
            return hex_to_u128(s);
        }
        return s.parse::<u128>().ok();
    }
    v.as_u64().map(|x| x as u128)
}

fn calculate_token_total(alkanes: &[JsonValue], target_block: &str, target_tx: &str) -> u128 {
    let mut total: u128 = 0;
    for a in alkanes {
        if let Some(id) = a.get("id") {
            let b = id.get("block").and_then(value_u128_from_json).unwrap_or(0).to_string();
            let t = id.get("tx").and_then(value_u128_from_json).unwrap_or(0).to_string();
            if b == target_block && t == target_tx {
                if let Some(v) = a.get("value").and_then(value_u128_from_json) {
                    total = total.saturating_add(v);
                }
            }
        }
    }
    total
}

fn is_delegate_invoke(event: &JsonValue) -> bool {
    if event.get("eventType").and_then(|v| v.as_str()) != Some("invoke") { return false; }
    let data = event.get("data").and_then(|v| v.as_object()).cloned();
    if data.is_none() { return false; }
    let data = data.unwrap();
    if data.get("type").and_then(|v| v.as_str()) != Some("delegatecall") { return false; }
    let inputs = data.get("context").and_then(|v| v.get("inputs")).and_then(|v| v.as_array()).cloned().unwrap_or_default();
    if inputs.is_empty() { return false; }
    if let Some(opcode_hex) = inputs.get(0).and_then(|v| v.as_str()) {
        return hex_to_u128(opcode_hex) == Some(3);
    }
    false
}

fn find_matching_return<'a>(invoke_idx: usize, events: &'a [JsonValue], invoke_vout: i64) -> Option<&'a JsonValue> {
    for i in (invoke_idx+1)..events.len() {
        let ev = &events[i];
        if ev.get("eventType").and_then(|v| v.as_str()) == Some("return") && ev.get("vout").and_then(|v| v.as_i64()) == Some(invoke_vout) {
            return Some(ev);
        }
    }
    None
}

fn find_matching_return_strict<'a>(
    invoke_idx: usize,
    events: &'a [JsonValue],
    invoke_vout: i64,
    expected_block: &str,
    expected_tx: &str,
    allowed_amounts: &[u128],
) -> Option<&'a JsonValue> {
    for i in (invoke_idx+1)..events.len() {
        let ev = &events[i];
        if ev.get("eventType").and_then(|v| v.as_str()) != Some("return") { continue; }
        if ev.get("vout").and_then(|v| v.as_i64()) != Some(invoke_vout) { continue; }
        let alks = ev
            .get("data").and_then(|d| d.get("response")).and_then(|r| r.get("alkanes")).and_then(|a| a.as_array());
        if let Some(arr) = alks {
            for a in arr {
                if let Some(id) = a.get("id") {
                    let b = id.get("block").and_then(u128_from_json).unwrap_or(0).to_string();
                    let t = id.get("tx").and_then(u128_from_json).unwrap_or(0).to_string();
                    if b == expected_block && t == expected_tx {
                        if allowed_amounts.is_empty() { return Some(ev); }
                        if let Some(v) = a.get("value").and_then(value_u128_from_json) {
                            if allowed_amounts.iter().any(|x| *x == v) { return Some(ev); }
                        }
                    }
                }
            }
        }
    }
    None
}

async fn simulate_pool_swaps(db: &sqlx::PgPool, events: &[JsonValue]) -> Result<()> {
    info!(events = events.len(), "simulating pool swap decoding");

    for (i, ev) in events.iter().enumerate() {
        if !is_delegate_invoke(ev) { continue; }

        let pool_block = ev.get("alkaneAddressBlock").and_then(|v| v.as_str()).unwrap_or("");
        let pool_tx = ev.get("alkaneAddressTx").and_then(|v| v.as_str()).unwrap_or("");
        if pool_block.is_empty() || pool_tx.is_empty() { debug!(index = i, "invoke missing pool id; skip"); continue; }

        info!(index = i, pool_block, pool_tx, "delegate invoke candidate");
        let Some(((t0b, t0t), (t1b, t1t))) = fetch_pool_tokens(db, pool_block, pool_tx).await? else {
            warn!(pool_block, pool_tx, "pool tokens not found; skip");
            continue;
        };

        let incoming: Vec<JsonValue> = ev
            .get("data").and_then(|d| d.get("context")).and_then(|c| c.get("incomingAlkanes")).and_then(|a| a.as_array()).cloned().unwrap_or_default();

        let has_t0_incoming = calculate_token_total(&incoming, &t0b, &t0t) > 0;
        let has_t1_incoming = calculate_token_total(&incoming, &t1b, &t1t) > 0;
        info!(has_t0_incoming, has_t1_incoming, "incoming token presence");
        if !has_t0_incoming && !has_t1_incoming { info!("no pool tokens in incoming; skip"); continue; }

        let inputs: Vec<String> = ev
            .get("data").and_then(|d| d.get("context")).and_then(|c| c.get("inputs")).and_then(|a| a.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
            .unwrap_or_default();
        let mut allowed_amounts: Vec<u128> = Vec::new();
        if inputs.len() > 1 { if let Some(v) = hex_to_u128(&inputs[1]) { allowed_amounts.push(v); } }
        if inputs.len() > 2 { if let Some(v) = hex_to_u128(&inputs[2]) { allowed_amounts.push(v); } }
        debug!(allowed_amounts = ?allowed_amounts, "allowed output amounts from inputs");

        let invoke_vout = ev.get("vout").and_then(|v| v.as_i64()).unwrap_or(-1);
        let (expected_b, expected_t) = if has_t0_incoming { (&t1b, &t1t) } else { (&t0b, &t0t) };
        let ret = find_matching_return_strict(i, events, invoke_vout, expected_b, expected_t, &allowed_amounts)
            .or_else(|| find_matching_return(i, events, invoke_vout));
        let Some(ret) = ret else { info!("no matching return; skip"); continue };

        let outgoing: Vec<JsonValue> = ret
            .get("data").and_then(|d| d.get("response")).and_then(|c| c.get("alkanes")).and_then(|a| a.as_array()).cloned().unwrap_or_default();

        let t0_in = calculate_token_total(&incoming, &t0b, &t0t);
        let t0_out = calculate_token_total(&outgoing, &t0b, &t0t);
        let t1_in = calculate_token_total(&incoming, &t1b, &t1t);
        let t1_out = calculate_token_total(&outgoing, &t1b, &t1t);

        info!(t0_in, t0_out, t1_in, t1_out, "token totals");

        let (sold_b, sold_t, bought_b, bought_t, sold_amt, bought_amt) = if t0_out == 0 && t1_in == 0 {
            (t0b.clone(), t0t.clone(), t1b.clone(), t1t.clone(), t0_in, t1_out)
        } else {
            (t1b.clone(), t1t.clone(), t0b.clone(), t0t.clone(), t1_in, t0_out)
        };

        if sold_amt == 0 || bought_amt == 0 {
            info!(sold_amt, bought_amt, "invalid swap amounts; skip");
            continue;
        }

        println!(
            "Decoded swap -> pool=({}:{}) sold=({}:{}) bought=({}:{}) amounts=({}, {})",
            pool_block, pool_tx, sold_b, sold_t, bought_b, bought_t, sold_amt, bought_amt
        );
    }
    Ok(())
}


