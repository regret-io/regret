mod adapter;
mod api;
mod app_state;
mod config;
mod engine;
mod generator;
mod reference;
mod storage;
mod types;

use std::net::SocketAddr;
use std::path::Path;

use anyhow::Result;
use tracing::{info, warn};

use app_state::AppState;
use config::Config;
use engine::{ManagerRegistry, SharedServices};
use storage::files::FileStore;
use storage::rocks::RocksStore;
use storage::sqlite::SqliteStore;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let config = Config::from_env();
    info!(?config, "starting regret-pilot");

    std::fs::create_dir_all(&config.data_dir)?;
    std::fs::create_dir_all(&config.rocksdb_path)?;

    let sqlite = SqliteStore::new(&config.database_url).await?;
    let rocks = RocksStore::new(Path::new(&config.rocksdb_path))?;
    let files = FileStore::new(Path::new(&config.data_dir));

    // Seed built-in generators
    for gen_info in generator::generators::list_generators() {
        let workload = generator::generators::get_generator(gen_info.name);
        let workload_json = serde_json::to_string(&workload)?;
        sqlite.upsert_generator(gen_info.name, gen_info.description, &workload_json, true).await?;
    }
    info!("built-in generators seeded");

    let shared = SharedServices {
        sqlite: sqlite.clone(),
        rocks: rocks.clone(),
        files: files.clone(),
    };

    let managers = ManagerRegistry::new(shared);

    let hypotheses = sqlite.list_hypotheses().await?;
    for h in &hypotheses {
        managers.create_from_hypothesis(&h.id, &h.generator, h.tolerance.clone()).await;
        info!(id = %h.id, name = %h.name, "loaded hypothesis");
    }

    let app_state = AppState {
        sqlite,
        rocks,
        files,
        managers,
    };

    let http_addr: SocketAddr = format!("0.0.0.0:{}", config.http_port).parse()?;
    let app = api::router(app_state);

    info!(%http_addr, "HTTP server starting");
    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
