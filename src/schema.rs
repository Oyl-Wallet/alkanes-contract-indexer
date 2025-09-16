use anyhow::Result;
use sqlx::PgPool;

// This DDL mirrors the provided Prisma models as closely as possible in Postgres.
// Types:
// - Prisma String -> text/uuid
// - Json -> jsonb
// - DateTime -> timestamptz

pub const DDL: &str = r#"
-- Ensure UUID generation is available
create extension if not exists pgcrypto;

create table if not exists "AlkaneTransaction" (
  "id" text primary key default gen_random_uuid()::text,
  "blockHeight" integer not null,
  "transactionId" text not null unique,
  "transactionIndex" integer not null default 0,
  "hasTrace" boolean not null default false,
  "traceSucceed" boolean not null default false,
  "transactionData" jsonb,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now()
);

create index if not exists "idx_AlkaneTransaction_blockHeight" on "AlkaneTransaction"("blockHeight");
create index if not exists "idx_AlkaneTransaction_transactionId" on "AlkaneTransaction"("transactionId");
create index if not exists "idx_AlkaneTransaction_blockHeight_transactionIndex" on "AlkaneTransaction"("blockHeight", "transactionIndex");

create table if not exists "TraceEvent" (
  "id" text primary key default gen_random_uuid()::text,
  "transactionId" text not null,
  "vout" integer not null,
  "alkaneAddressBlock" text not null,
  "alkaneAddressTx" text not null,
  "eventType" text not null,
  "data" jsonb not null,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now(),
  constraint "fk_TraceEvent_transaction" foreign key ("transactionId") references "AlkaneTransaction"("transactionId")
);

create index if not exists "idx_TraceEvent_transactionId" on "TraceEvent"("transactionId");
create index if not exists "idx_TraceEvent_eventType" on "TraceEvent"("eventType");

create table if not exists "DecodedProtostone" (
  "id" text primary key default gen_random_uuid()::text,
  "transactionId" text not null,
  "vout" integer not null,
  "protostoneIndex" integer not null,
  "decoded" jsonb not null,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now(),
  constraint "uq_DecodedProtostone_tx_vout_index" unique ("transactionId", "vout", "protostoneIndex"),
  constraint "fk_DecodedProtostone_transaction" foreign key ("transactionId") references "AlkaneTransaction"("transactionId")
);

create index if not exists "idx_DecodedProtostone_transactionId" on "DecodedProtostone"("transactionId");

create table if not exists "ClockIn" (
  "id" uuid primary key default gen_random_uuid(),
  "transactionId" text not null,
  "blockHeight" integer not null,
  "transactionIndex" integer not null default 0,
  "userAddress" text not null,
  "timestamp" timestamptz not null,
  "oylPayment" boolean not null default false,
  "paymentVout" integer,
  "paymentAmount" integer,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now(),
  constraint "fk_ClockIn_transaction" foreign key ("transactionId") references "AlkaneTransaction"("transactionId")
);

create index if not exists "idx_ClockIn_transactionId" on "ClockIn"("transactionId");
create index if not exists "idx_ClockIn_blockHeight" on "ClockIn"("blockHeight");
create index if not exists "idx_ClockIn_userAddress" on "ClockIn"("userAddress");
create index if not exists "idx_ClockIn_blockHeight_transactionIndex" on "ClockIn"("blockHeight", "transactionIndex");

create table if not exists "ProcessedBlocks" (
  "blockHeight" integer not null unique,
  "blockHash" text not null unique,
  "timestamp" timestamptz not null,
  "isProcessing" boolean not null default false,
  "createdAt" timestamptz not null default now()
);

create index if not exists "idx_ProcessedBlocks_blockHash" on "ProcessedBlocks"("blockHash");

create table if not exists "ClockInBlockSummary" (
  "id" text primary key default gen_random_uuid()::text,
  "blockHeight" integer not null unique,
  "timestamp" timestamptz not null,
  "totalClockIns" integer not null default 0,
  "uniqueUsers" integer not null default 0,
  "isEligibleBlock" boolean not null default false,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now()
);

create index if not exists "idx_ClockInBlockSummary_blockHeight" on "ClockInBlockSummary"("blockHeight");

create table if not exists "ClockInSummary" (
  "userAddress" text primary key,
  "currentStreak" integer not null default 0,
  "maxStreak" integer not null default 0,
  "totalCount" integer not null default 0,
  "lastClockInBlock" integer,
  "lastClockInTimestamp" timestamptz,
  "firstClockInBlock" integer,
  "firstClockInTimestamp" timestamptz,
  "empCount" integer not null default 0,
  "vstrCount" integer not null default 0,
  "empCurrentStreak" integer not null default 0,
  "vstrCurrentStreak" integer not null default 0,
  "empMaxStreak" integer not null default 0,
  "vstrMaxStreak" integer not null default 0,
  "empNumber" integer,
  "vstrNumber" integer,
  "updatedAt" timestamptz not null default now()
);

create index if not exists "idx_ClockInSummary_totalCount" on "ClockInSummary"("totalCount");
create index if not exists "idx_ClockInSummary_currentStreak" on "ClockInSummary"("currentStreak");
create index if not exists "idx_ClockInSummary_maxStreak" on "ClockInSummary"("maxStreak");
create index if not exists "idx_ClockInSummary_empCount" on "ClockInSummary"("empCount");
create index if not exists "idx_ClockInSummary_vstrCount" on "ClockInSummary"("vstrCount");
create index if not exists "idx_ClockInSummary_empCurrentStreak" on "ClockInSummary"("empCurrentStreak");
create index if not exists "idx_ClockInSummary_vstrCurrentStreak" on "ClockInSummary"("vstrCurrentStreak");
create index if not exists "idx_ClockInSummary_empNumber" on "ClockInSummary"("empNumber");
create index if not exists "idx_ClockInSummary_vstrNumber" on "ClockInSummary"("vstrNumber");
create index if not exists "idx_ClockInSummary_lastClockInTimestamp" on "ClockInSummary"("lastClockInTimestamp");

create table if not exists "CorpData" (
  "id" uuid primary key default gen_random_uuid(),
  "empCount" integer not null default 0,
  "vstrCount" integer not null default 0,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now()
);

create table if not exists "Profile" (
  "id" uuid primary key default gen_random_uuid(),
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now(),
  "userAddress" text not null unique,
  "twitterAvatarUrl" text not null default '',
  "twitterUsername" text not null default ''
);
create index if not exists "idx_Profile_userAddress" on "Profile"("userAddress");

create table if not exists "Pool" (
  "id" text primary key default gen_random_uuid()::text,
  "factoryBlockId" text not null,
  "factoryTxId" text not null,
  "poolBlockId" text not null,
  "poolTxId" text not null,
  "token0BlockId" text not null,
  "token0TxId" text not null,
  "token1BlockId" text not null,
  "token1TxId" text not null,
  "poolName" text not null,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now(),
  constraint "uq_Pool_poolBlockId_poolTxId" unique ("poolBlockId", "poolTxId")
);
create index if not exists "idx_Pool_factoryBlockId_factoryTxId" on "Pool"("factoryBlockId", "factoryTxId");

create table if not exists "PoolState" (
  "id" text primary key default gen_random_uuid()::text,
  "poolId" text not null,
  "blockHeight" integer not null,
  "token0Amount" text not null,
  "token1Amount" text not null,
  "tokenSupply" text not null,
  "createdAt" timestamptz not null default now(),
  constraint "fk_PoolState_pool" foreign key ("poolId") references "Pool"("id") on delete cascade,
  constraint "uq_PoolState_poolId_blockHeight" unique ("poolId", "blockHeight")
);
create index if not exists "idx_PoolState_poolId" on "PoolState"("poolId");
create index if not exists "idx_PoolState_blockHeight" on "PoolState"("blockHeight");

create table if not exists "PoolCreation" (
  "id" text primary key default gen_random_uuid()::text,
  "transactionId" text not null,
  "blockHeight" integer not null,
  "transactionIndex" integer not null default 0,
  "poolBlockId" text not null,
  "poolTxId" text not null,
  "token0BlockId" text not null,
  "token0TxId" text not null,
  "token1BlockId" text not null,
  "token1TxId" text not null,
  "token0Amount" text not null,
  "token1Amount" text not null,
  "tokenSupply" text not null,
  "creatorAddress" text,
  "timestamp" timestamptz not null,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now(),
  constraint "fk_PoolCreation_pool" foreign key ("poolBlockId", "poolTxId") references "Pool"("poolBlockId", "poolTxId")
);
create unique index if not exists "uq_PoolCreation_poolBlockId_poolTxId" on "PoolCreation"("poolBlockId", "poolTxId");
create index if not exists "idx_PoolCreation_transactionId" on "PoolCreation"("transactionId");
create index if not exists "idx_PoolCreation_blockHeight" on "PoolCreation"("blockHeight");
create index if not exists "idx_PoolCreation_poolBlockId_poolTxId" on "PoolCreation"("poolBlockId", "poolTxId");
create index if not exists "idx_PoolCreation_blockHeight_transactionIndex" on "PoolCreation"("blockHeight", "transactionIndex");

create table if not exists "PoolSwap" (
  "id" text primary key default gen_random_uuid()::text,
  "transactionId" text not null,
  "blockHeight" integer not null,
  "transactionIndex" integer not null default 0,
  "poolBlockId" text not null,
  "poolTxId" text not null,
  "soldTokenBlockId" text not null,
  "soldTokenTxId" text not null,
  "boughtTokenBlockId" text not null,
  "boughtTokenTxId" text not null,
  "soldAmount" double precision not null,
  "boughtAmount" double precision not null,
  "sellerAddress" text,
  "timestamp" timestamptz not null,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now()
);
create index if not exists "idx_PoolSwap_transactionId" on "PoolSwap"("transactionId");
create index if not exists "idx_PoolSwap_blockHeight" on "PoolSwap"("blockHeight");
create index if not exists "idx_PoolSwap_poolBlockId_poolTxId" on "PoolSwap"("poolBlockId", "poolTxId");
create index if not exists "idx_PoolSwap_blockHeight_transactionIndex" on "PoolSwap"("blockHeight", "transactionIndex");

create table if not exists "PoolBurn" (
  "id" text primary key default gen_random_uuid()::text,
  "transactionId" text not null,
  "blockHeight" integer not null,
  "transactionIndex" integer not null default 0,
  "poolBlockId" text not null,
  "poolTxId" text not null,
  "lpTokenAmount" text not null,
  "token0BlockId" text not null,
  "token0TxId" text not null,
  "token1BlockId" text not null,
  "token1TxId" text not null,
  "token0Amount" text not null,
  "token1Amount" text not null,
  "burnerAddress" text,
  "timestamp" timestamptz not null,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now()
);
create index if not exists "idx_PoolBurn_transactionId" on "PoolBurn"("transactionId");
create index if not exists "idx_PoolBurn_blockHeight" on "PoolBurn"("blockHeight");
create index if not exists "idx_PoolBurn_poolBlockId_poolTxId" on "PoolBurn"("poolBlockId", "poolTxId");
create index if not exists "idx_PoolBurn_blockHeight_transactionIndex" on "PoolBurn"("blockHeight", "transactionIndex");

create table if not exists "PoolMint" (
  "id" text primary key default gen_random_uuid()::text,
  "transactionId" text not null,
  "blockHeight" integer not null,
  "transactionIndex" integer not null default 0,
  "poolBlockId" text not null,
  "poolTxId" text not null,
  "lpTokenAmount" text not null,
  "token0BlockId" text not null,
  "token0TxId" text not null,
  "token1BlockId" text not null,
  "token1TxId" text not null,
  "token0Amount" text not null,
  "token1Amount" text not null,
  "minterAddress" text,
  "timestamp" timestamptz not null,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now()
);
create index if not exists "idx_PoolMint_transactionId" on "PoolMint"("transactionId");
create index if not exists "idx_PoolMint_blockHeight" on "PoolMint"("blockHeight");
create index if not exists "idx_PoolMint_poolBlockId_poolTxId" on "PoolMint"("poolBlockId", "poolTxId");
create index if not exists "idx_PoolMint_blockHeight_transactionIndex" on "PoolMint"("blockHeight", "transactionIndex");

create table if not exists "CuratedPools" (
  "id" text primary key default gen_random_uuid()::text,
  "factoryId" text not null unique,
  "poolIds" text[] not null,
  "createdAt" timestamptz not null default now(),
  "updatedAt" timestamptz not null default now()
);

-- progress KV store (already used by coordinator)
create table if not exists kv_store (
  key text primary key,
  value text not null
);
"#;

const DROP_ALL: &str = r#"
drop table if exists "PoolMint" cascade;
drop table if exists "PoolBurn" cascade;
drop table if exists "PoolSwap" cascade;
drop table if exists "PoolCreation" cascade;
drop table if exists "PoolState" cascade;
drop table if exists "Pool" cascade;
drop table if exists "Profile" cascade;
drop table if exists "CorpData" cascade;
drop table if exists "ClockInSummary" cascade;
drop table if exists "ClockInBlockSummary" cascade;
drop table if exists "ProcessedBlocks" cascade;
drop table if exists "ClockIn" cascade;
drop table if exists "TraceEvent" cascade;
drop table if exists "DecodedProtostone" cascade;
drop table if exists "AlkaneTransaction" cascade;
drop table if exists "CuratedPools" cascade;
drop table if exists kv_store cascade;
"#;

async fn execute_batch(pool: &PgPool, sql: &str) -> Result<()> {
    for stmt in sql.split(';') {
        let s = stmt.trim();
        if s.is_empty() { continue; }
        sqlx::query(s).execute(pool).await?;
    }
    Ok(())
}

pub async fn push_schema(pool: &PgPool) -> Result<()> {
    execute_batch(pool, DDL).await
}

pub async fn reset_schema(pool: &PgPool) -> Result<()> {
    // Drop known tables, then re-push
    execute_batch(pool, DROP_ALL).await?;
    push_schema(pool).await
}

pub async fn drop_all_tables(pool: &PgPool) -> Result<()> {
    execute_batch(pool, DROP_ALL).await
}


