use anyhow::Result;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Batch upsert records into "AlkaneTransaction" by unique "transactionId".
/// On conflict, updates mutable fields and refreshes "updatedAt".
pub async fn upsert_alkane_transactions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    // (blockHeight, transactionId, transactionIndex, hasTrace, traceSucceed, transactionData)
    items: &[(i32, String, i32, bool, bool, JsonValue)],
) -> Result<()> {
    if items.is_empty() { return Ok(()); }

    const MAX_PARAMS: usize = 65535;
    const PER_ROW: usize = 6;
    let max_rows = (MAX_PARAMS / PER_ROW).saturating_sub(8).max(1);

    for chunk in items.chunks(max_rows) {
        let mut q = String::from(
            "insert into \"AlkaneTransaction\" (\"blockHeight\", \"transactionId\", \"transactionIndex\", \"hasTrace\", \"traceSucceed\", \"transactionData\") values ",
        );
        for i in 0..chunk.len() {
            if i > 0 { q.push(','); }
            let base = i * PER_ROW;
            q.push_str(&format!("(${}, ${}, ${}, ${}, ${}, ${})", base+1, base+2, base+3, base+4, base+5, base+6));
        }
        q.push_str(
            " on conflict (\"transactionId\") do update set \"blockHeight\" = excluded.\"blockHeight\", \"transactionIndex\" = excluded.\"transactionIndex\", \"hasTrace\" = excluded.\"hasTrace\", \"traceSucceed\" = excluded.\"traceSucceed\", \"transactionData\" = excluded.\"transactionData\", \"updatedAt\" = now()",
        );

        let mut qb = sqlx::query(&q);
        for (bh, txid, idx, has_trace, trace_ok, data) in chunk {
            qb = qb
                .bind(bh)
                .bind(txid)
                .bind(idx)
                .bind(has_trace)
                .bind(trace_ok)
                .bind(data);
        }
        qb.execute(&mut **tx).await?;
    }
    Ok(())
}

/// Replace TraceEvent rows for a set of txids, then bulk insert provided events.
pub async fn replace_trace_events(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    txids: &[String],
    // (transactionId, vout, eventType, data, alkaneAddressBlock, alkaneAddressTx)
    events: &[(String, i32, String, JsonValue, String, String)],
) -> Result<()> {
    if !txids.is_empty() {
        sqlx::query(r#"delete from "TraceEvent" where "transactionId" = any($1)"#)
            .bind(txids)
            .execute(&mut **tx)
            .await?;
    }
    if events.is_empty() { return Ok(()); }

    const MAX_PARAMS: usize = 65535;
    const PER_ROW: usize = 6;
    let max_rows = (MAX_PARAMS / PER_ROW).saturating_sub(8).max(1);

    for chunk in events.chunks(max_rows) {
        let mut q = String::from(
            "insert into \"TraceEvent\" (\"transactionId\", \"vout\", \"eventType\", \"data\", \"alkaneAddressBlock\", \"alkaneAddressTx\") values ",
        );
        for i in 0..chunk.len() {
            if i > 0 { q.push(','); }
            let base = i * PER_ROW;
            q.push_str(&format!("(${}, ${}, ${}, ${}, ${}, ${})", base+1, base+2, base+3, base+4, base+5, base+6));
        }
        let mut qb = sqlx::query(&q);
        for (txid, vout, etype, data, blk, txnum) in chunk {
            qb = qb.bind(txid).bind(vout).bind(etype).bind(data).bind(blk).bind(txnum);
        }
        qb.execute(&mut **tx).await?;
    }
    Ok(())
}

/// Replace DecodedProtostone rows for a set of txids, then bulk insert provided protostones.
pub async fn replace_decoded_protostones(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    txids: &[String],
    // (transactionId, vout, protostoneIndex, decoded_json)
    items: &[(String, i32, i32, JsonValue)],
) -> Result<()> {
    if !txids.is_empty() {
        sqlx::query(r#"delete from "DecodedProtostone" where "transactionId" = any($1)"#)
            .bind(txids)
            .execute(&mut **tx)
            .await?;
    }
    if items.is_empty() { return Ok(()); }

    const MAX_PARAMS: usize = 65535;
    const PER_ROW: usize = 4;
    let max_rows = (MAX_PARAMS / PER_ROW).saturating_sub(8).max(1);

    for chunk in items.chunks(max_rows) {
        let mut q = String::from(
            "insert into \"DecodedProtostone\" (\"transactionId\", \"vout\", \"protostoneIndex\", \"decoded\") values ",
        );
        for i in 0..chunk.len() {
            if i > 0 { q.push(','); }
            let base = i * PER_ROW;
            q.push_str(&format!("(${}, ${}, ${}, ${})", base+1, base+2, base+3, base+4));
        }
        q.push_str(" on conflict (\"transactionId\", \"vout\", \"protostoneIndex\") do update set \"decoded\" = excluded.\"decoded\", \"updatedAt\" = now()");
        let mut qb = sqlx::query(&q);
        for (txid, vout, idx, decoded) in chunk {
            qb = qb.bind(txid).bind(vout).bind(idx).bind(decoded);
        }
        qb.execute(&mut **tx).await?;
    }
    Ok(())
}

/// Fetch decoded protostones for given txids, keyed by (transactionId, vout).
/// Returns a map: txid -> (vout -> Vec<(protostoneIndex, decoded_json)>)
pub async fn get_decoded_protostones_by_txid_vout(
    pool: &sqlx::PgPool,
    txids: &[String],
 ) -> Result<HashMap<String, HashMap<i32, Vec<(i32, JsonValue)>>>> {
    let mut out: HashMap<String, HashMap<i32, Vec<(i32, JsonValue)>>> = HashMap::new();
    if txids.is_empty() { return Ok(out); }

    // Query all rows for provided txids
    let rows = sqlx::query!(
        r#"select "transactionId" as txid, "vout", "protostoneIndex" as idx, "decoded" from "DecodedProtostone" where "transactionId" = any($1) order by "transactionId", "vout", "protostoneIndex""#,
        txids
    )
    .fetch_all(pool)
    .await?;

    for r in rows {
        let txid = r.txid;
        let vout = r.vout;
        let idx = r.idx;
        let decoded: JsonValue = r.decoded;
        out.entry(txid)
            .or_default()
            .entry(vout)
            .or_default()
            .push((idx, decoded));
    }
    Ok(out)
}

/// Replace PoolSwap rows for a set of txids, then bulk insert provided swaps.
pub async fn replace_pool_swaps(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    txids: &[String],
    // (transactionId, blockHeight, transactionIndex, poolBlockId, poolTxId, soldTokenBlockId, soldTokenTxId, boughtTokenBlockId, boughtTokenTxId, soldAmount, boughtAmount, sellerAddress, timestamp)
    swaps: &[(String, i32, i32, String, String, String, String, String, String, f64, f64, Option<String>, chrono::DateTime<chrono::Utc>)],
) -> Result<()> {
    if !txids.is_empty() {
        sqlx::query(r#"delete from "PoolSwap" where "transactionId" = any($1)"#)
            .bind(txids)
            .execute(&mut **tx)
            .await?;
    }
    if swaps.is_empty() { return Ok(()); }

    const MAX_PARAMS: usize = 65535;
    const PER_ROW: usize = 13;
    let max_rows = (MAX_PARAMS / PER_ROW).saturating_sub(8).max(1);

    for chunk in swaps.chunks(max_rows) {
        let mut q = String::from(
            "insert into \"PoolSwap\" (\"transactionId\", \"blockHeight\", \"transactionIndex\", \"poolBlockId\", \"poolTxId\", \"soldTokenBlockId\", \"soldTokenTxId\", \"boughtTokenBlockId\", \"boughtTokenTxId\", \"soldAmount\", \"boughtAmount\", \"sellerAddress\", \"timestamp\") values ",
        );
        for i in 0..chunk.len() {
            if i > 0 { q.push(','); }
            let base = i * PER_ROW;
            q.push_str(&format!("(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})", base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11, base+12, base+13));
        }
        let mut qb = sqlx::query(&q);
        for (txid, bh, idx, pb, pt, sb, st, bb, bt, s_amt, b_amt, seller, ts) in chunk {
            qb = qb
                .bind(txid)
                .bind(bh)
                .bind(idx)
                .bind(pb)
                .bind(pt)
                .bind(sb)
                .bind(st)
                .bind(bb)
                .bind(bt)
                .bind(s_amt)
                .bind(b_amt)
                .bind(seller)
                .bind(ts);
        }
        qb.execute(&mut **tx).await?;
    }
    Ok(())
}


