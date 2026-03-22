mod adapter;
mod api;
mod app_state;
mod config;
mod engine;
mod generator;
mod grpc;
mod reference;
mod scheduler;
mod storage;
mod types;

use std::net::SocketAddr;
use std::path::Path;

use anyhow::Result;
use tonic::transport::Server as TonicServer;
use tracing::{info, warn};

use regret_proto::regret_v1::pilot_service_server::PilotServiceServer;

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

    // Ensure data directories exist
    std::fs::create_dir_all(&config.data_dir)?;
    std::fs::create_dir_all(&config.rocksdb_path)?;

    // Initialize stores
    let sqlite = SqliteStore::new(&config.database_url).await?;
    let rocks = RocksStore::new(Path::new(&config.rocksdb_path))?;
    let files = FileStore::new(Path::new(&config.data_dir));

    // Try to create K8s scheduler (fails gracefully outside K8s)
    let pilot_grpc_addr = format!("regret-pilot.{}.svc.cluster.local:{}", config.namespace, config.grpc_port);
    let scheduler = match scheduler::k8s::K8sScheduler::try_new(&config.namespace, &pilot_grpc_addr).await {
        Ok(s) => {
            info!("K8s scheduler initialized");
            Some(s)
        }
        Err(e) => {
            warn!("K8s scheduler not available (running outside K8s?): {e}");
            None
        }
    };

    let shared = SharedServices {
        sqlite: sqlite.clone(),
        rocks: rocks.clone(),
        files: files.clone(),
        scheduler: scheduler.clone(),
    };

    let managers = ManagerRegistry::new(shared);

    // Load existing hypotheses and create managers
    let hypotheses = sqlite.list_hypotheses().await?;
    for h in &hypotheses {
        managers
            .create_from_hypothesis(&h.id, &h.profile, h.tolerance.clone())
            .await;
        info!(id = %h.id, name = %h.name, "loaded hypothesis");
    }

    let app_state = AppState {
        sqlite: sqlite.clone(),
        rocks,
        files,
        managers,
        scheduler,
    };

    // Start gRPC server
    let grpc_addr: SocketAddr = format!("0.0.0.0:{}", config.grpc_port).parse()?;
    let pilot_service = grpc::PilotServiceImpl {
        sqlite,
    };

    let grpc_handle = tokio::spawn(async move {
        info!(%grpc_addr, "gRPC server starting");
        TonicServer::builder()
            .add_service(PilotServiceServer::new(pilot_service))
            .serve(grpc_addr)
            .await
            .expect("gRPC server failed");
    });

    // Start HTTP server
    let http_addr: SocketAddr = format!("0.0.0.0:{}", config.http_port).parse()?;
    let app = api::router(app_state);

    info!(%http_addr, "HTTP server starting");
    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    let http_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("HTTP server failed");
    });

    tokio::select! {
        _ = grpc_handle => {},
        _ = http_handle => {},
    }

    Ok(())
}
