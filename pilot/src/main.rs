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
        info!(id = %h.id, name = %h.name, status = %h.status, "loaded hypothesis");
    }

    // Auto-resume hypotheses that were running when pilot stopped
    for h in &hypotheses {
        if h.status != "running" {
            continue;
        }
        info!(id = %h.id, name = %h.name, "auto-resuming running hypothesis");

        let duration_secs = h.duration.as_ref().and_then(|d| {
            let s = d.trim();
            if s.ends_with('s') { s[..s.len()-1].parse().ok() }
            else if s.ends_with('m') { s[..s.len()-1].parse::<u64>().ok().map(|m| m * 60) }
            else if s.ends_with('h') { s[..s.len()-1].parse::<u64>().ok().map(|h| h * 3600) }
            else { s.parse().ok() }
        });
        let checkpoint_interval_secs = {
            let s = h.checkpoint_every.trim();
            if s.ends_with('s') { s[..s.len()-1].parse().unwrap_or(600) }
            else if s.ends_with('m') { s[..s.len()-1].parse::<u64>().map(|m| m * 60).unwrap_or(600) }
            else if s.ends_with('h') { s[..s.len()-1].parse::<u64>().map(|h| h * 3600).unwrap_or(600) }
            else { s.parse().unwrap_or(600) }
        };

        let exec_config = engine::executor::ExecutionConfig {
            checkpoint_interval_secs,
            duration_secs,
            ..engine::executor::ExecutionConfig::default()
        };

        let adapter = if let Some(name) = &h.adapter {
            sqlite.get_adapter_by_name(name).await.ok().flatten()
        } else { None };

        let mut gen_params = generator::GenerateParams {
            generator: h.generator.clone(),
            ops: usize::MAX,
            skip_warmup: h.generator.contains("notification"),
            ..generator::GenerateParams::default()
        };
        if let Ok(ks) = serde_json::from_str::<generator::KeySpaceConfig>(&h.key_space) {
            gen_params.key_space = ks;
        }
        gen_params.key_space.prefix = format!("/{}/", h.id);

        if let Some(mgr_arc) = managers.get(&h.id).await {
            let mut mgr = mgr_arc.lock().await;
            // Restore previous run ID if available
            if let Ok(Some(last_result)) = sqlite.get_latest_result(&h.id).await {
                mgr.set_resume_run_id(last_result.run_id.clone());
                info!(id = %h.id, run_id = %last_result.run_id, "restoring run ID");
            }
            match mgr.start_run(exec_config, gen_params, adapter, h.adapter_addr.clone()).await {
                Ok((run_id, _)) => info!(id = %h.id, run_id = %run_id, "hypothesis resumed"),
                Err(e) => warn!(id = %h.id, error = %e, "failed to resume hypothesis"),
            }
        }
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
