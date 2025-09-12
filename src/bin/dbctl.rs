use anyhow::Result;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(name = "dbctl", about = "Database management CLI for alkanes-contract-indexer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create or update the DB schema
    Push,
    /// Drop all tables and recreate the schema
    Reset,
    /// Drop all tables without re-pushing the schema
    Drop,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(env_filter).init();

    let cli = Cli::parse();
    let cfg = alkanes_contract_indexer::config::AppConfig::from_env()?;
    let pool = alkanes_contract_indexer::db::connect(&cfg.database_url, 5).await?;
    match cli.command {
        Commands::Push => {
            alkanes_contract_indexer::schema::push_schema(&pool).await?;
            info!("Schema pushed successfully");
        }
        Commands::Reset => {
            alkanes_contract_indexer::schema::reset_schema(&pool).await?;
            info!("Schema reset successfully");
        }
        Commands::Drop => {
            alkanes_contract_indexer::schema::drop_all_tables(&pool).await?;
            info!("All tables dropped successfully");
        }
    }
    Ok(())
}


