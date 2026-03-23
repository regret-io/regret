mod adapter;
mod api;
mod app_state;
mod chaos;
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
use chaos::executor::ChaosRegistry;
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
        sqlite.upsert_generator(gen_info.name, gen_info.description, &workload_json, gen_info.rate, true).await?;
    }
    info!("built-in generators seeded");

    // Seed built-in chaos scenarios
    sqlite.upsert_chaos_scenario(
        "builtin-continuous-chaos",
        "oxia-continuous-chaos",
        "regret-system",
        &serde_json::to_string(&serde_json::json!([
            {"type": "pod_kill", "selector": {"match_labels": {"app.kubernetes.io/name": "oxia-cluster"}, "mode": "one"}, "interval": "10m"},
            {"type": "network_delay", "selector": {"match_labels": {"app.kubernetes.io/name": "oxia-cluster"}, "mode": "one"}, "params": {"delay_ms": 200}, "interval": "15m", "duration": "2m"}
        ]))?,
    ).await?;
    sqlite.upsert_chaos_scenario(
        "builtin-upgrade-test",
        "oxia-upgrade-test",
        "regret-system",
        &serde_json::to_string(&serde_json::json!([
            {"type": "upgrade_test", "params": {
                "hypothesis_id": "PLACEHOLDER",
                "resource": "statefulset/oxia",
                "namespace": "regret-system",
                "candidate_image": "PLACEHOLDER",
                "stable_image": "PLACEHOLDER",
                "patch_path": "/spec/template/spec/containers/0/image",
                "checkpoints_per_step": 3,
                "timeout": "600s"
            }}
        ]))?,
    ).await?;
    info!("built-in chaos scenarios seeded");

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

    let chaos = ChaosRegistry::new(files.clone(), sqlite.clone(), managers.clone());

    let kube_client = kube::Client::try_default().await?;
    info!("kubernetes client initialized");

    let app_state = AppState {
        sqlite,
        rocks,
        files,
        managers,
        chaos,
        kube: kube_client,
        namespace: config.namespace.clone(),
    };

    let http_addr: SocketAddr = format!("0.0.0.0:{}", config.http_port).parse()?;
    if config.auth_password.is_some() {
        info!("basic auth enabled for write operations");
    }
    let app = api::router(app_state, config.auth_password);

    info!(%http_addr, "HTTP server starting");
    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
