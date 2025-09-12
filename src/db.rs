use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, PgPool};

pub mod pools;
pub mod pool_state;

pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await?;
    Ok(pool)
}


