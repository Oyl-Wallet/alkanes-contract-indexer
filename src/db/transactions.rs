use anyhow::Result;
use serde_json::Value as JsonValue;

/// Batch upsert records into "AlkaneTransaction" by unique "transactionId".
/// On conflict, updates mutable fields and refreshes "updatedAt".
pub async fn upsert_alkane_transactions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    // (blockHeight, transactionId, transactionIndex, hasTrace, traceSucceed, transactionData)
    items: &[(i32, String, i32, bool, bool, JsonValue)],
) -> Result<()> {
    if items.is_empty() { return Ok(()); }

    let mut q = String::from(
        "insert into \"AlkaneTransaction\" (\"blockHeight\", \"transactionId\", \"transactionIndex\", \"hasTrace\", \"traceSucceed\", \"transactionData\") values ",
    );
    for i in 0..items.len() {
        if i > 0 { q.push(','); }
        let base = i * 6;
        q.push_str(&format!("(${}, ${}, ${}, ${}, ${}, ${})", base+1, base+2, base+3, base+4, base+5, base+6));
    }
    q.push_str(
        " on conflict (\"transactionId\") do update set \"blockHeight\" = excluded.\"blockHeight\", \"transactionIndex\" = excluded.\"transactionIndex\", \"hasTrace\" = excluded.\"hasTrace\", \"traceSucceed\" = excluded.\"traceSucceed\", \"transactionData\" = excluded.\"transactionData\", \"updatedAt\" = now()",
    );

    let mut qb = sqlx::query(&q);
    for (bh, txid, idx, has_trace, trace_ok, data) in items {
        qb = qb
            .bind(bh)
            .bind(txid)
            .bind(idx)
            .bind(has_trace)
            .bind(trace_ok)
            .bind(data);
    }
    qb.execute(&mut **tx).await?;
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

    let mut q = String::from(
        "insert into \"TraceEvent\" (\"transactionId\", \"vout\", \"eventType\", \"data\", \"alkaneAddressBlock\", \"alkaneAddressTx\") values ",
    );
    for i in 0..events.len() {
        if i > 0 { q.push(','); }
        let base = i * 6;
        q.push_str(&format!("(${}, ${}, ${}, ${}, ${}, ${})", base+1, base+2, base+3, base+4, base+5, base+6));
    }
    let mut qb = sqlx::query(&q);
    for (txid, vout, etype, data, blk, txnum) in events {
        qb = qb.bind(txid).bind(vout).bind(etype).bind(data).bind(blk).bind(txnum);
    }
    qb.execute(&mut **tx).await?;
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

    let mut q = String::from(
        "insert into \"DecodedProtostone\" (\"transactionId\", \"vout\", \"protostoneIndex\", \"decoded\") values ",
    );
    for i in 0..items.len() {
        if i > 0 { q.push(','); }
        let base = i * 4;
        q.push_str(&format!("(${}, ${}, ${}, ${})", base+1, base+2, base+3, base+4));
    }
    q.push_str(" on conflict (\"transactionId\", \"vout\", \"protostoneIndex\") do update set \"decoded\" = excluded.\"decoded\", \"updatedAt\" = now()");
    let mut qb = sqlx::query(&q);
    for (txid, vout, idx, decoded) in items {
        qb = qb.bind(txid).bind(vout).bind(idx).bind(decoded);
    }
    qb.execute(&mut **tx).await?;
    Ok(())
}


