use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::reference::{ReferenceModel, Tolerance};
use crate::storage::sqlite::AdapterRecord;
use crate::types::HypothesisStatus;

use super::SharedServices;
use super::executor::{AdapterClient, ExecutionConfig, Executor, ProgressInfo, StopReason};

/// Per-hypothesis lifecycle manager.
pub struct HypothesisManager {
    pub hypothesis_id: String,
    pub generator_name: String,
    pub tolerance: Option<Tolerance>,

    reference: Option<Box<dyn ReferenceModel>>,
    run_state: Option<ActiveRun>,

    shared: SharedServices,
}

struct ActiveRun {
    pub run_id: String,
    pub adapter_name: Option<String>, // for teardown
    pub cancel: CancellationToken,
    pub executor_handle: JoinHandle<(Box<dyn ReferenceModel>, StopReason)>,
    pub progress: Arc<RwLock<ProgressInfo>>,
}

impl HypothesisManager {
    pub fn new(
        hypothesis_id: String,
        generator_name: String,
        tolerance_json: Option<String>,
        reference: Box<dyn ReferenceModel>,
        shared: SharedServices,
    ) -> Self {
        let tolerance = tolerance_json
            .as_ref()
            .and_then(|t| serde_json::from_str(t).ok());

        Self {
            hypothesis_id,
            generator_name,
            tolerance,
            reference: Some(reference),
            run_state: None,
            shared,
        }
    }

    /// Start a new run.
    /// If adapter is provided, deploys it via scheduler and connects via gRPC.
    pub async fn start_run(
        &mut self,
        config: ExecutionConfig,
        adapter: Option<AdapterRecord>,
    ) -> Result<(String, Arc<RwLock<ProgressInfo>>)> {
        if self.run_state.is_some() {
            anyhow::bail!("hypothesis is already running");
        }

        let reference = self
            .reference
            .take()
            .ok_or_else(|| anyhow::anyhow!("reference model not available"))?;

        let run_id = format!(
            "run-{}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
        );
        let cancel = CancellationToken::new();
        let progress = Arc::new(RwLock::new(ProgressInfo::default()));

        // Update hypothesis status
        self.shared
            .sqlite
            .update_hypothesis_status(&self.hypothesis_id, &HypothesisStatus::Running.to_string())
            .await?;
        self.shared
            .sqlite
            .update_last_run_at(
                &self.hypothesis_id,
                &chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            )
            .await?;

        // Deploy adapter and connect
        let mut adapter_name_for_teardown: Option<String> = None;
        let adapter_client: Option<Box<dyn AdapterClient>> = if let Some(adapter_def) = &adapter {
            adapter_name_for_teardown = Some(adapter_def.name.clone());

            if let Some(scheduler) = &self.shared.scheduler {
                // Deploy via K8s scheduler
                info!(
                    hypothesis_id = %self.hypothesis_id,
                    adapter = %adapter_def.name,
                    image = %adapter_def.image,
                    "deploying adapter pod"
                );

                let grpc_addr = scheduler
                    .deploy_adapter(&self.hypothesis_id, adapter_def)
                    .await?;

                // Wait for pod to be ready
                scheduler
                    .wait_for_ready(&self.hypothesis_id, &adapter_def.name, Duration::from_secs(120))
                    .await?;

                // Connect via gRPC
                match crate::adapter::grpc_client::GrpcAdapterClient::connect(&grpc_addr).await {
                    Ok(client) => {
                        info!(adapter = %adapter_def.name, %grpc_addr, "connected to adapter");
                        Some(Box::new(client) as Box<dyn AdapterClient>)
                    }
                    Err(e) => {
                        error!(adapter = %adapter_def.name, %grpc_addr, error = %e, "failed to connect");
                        // Teardown the failed deployment
                        let _ = scheduler.teardown_adapter(&self.hypothesis_id, &adapter_def.name).await;
                        return Err(e.context("failed to connect to deployed adapter"));
                    }
                }
            } else {
                // No scheduler (local dev) — try direct connect via ADAPTER_GRPC_ADDR env
                let env: std::collections::HashMap<String, String> =
                    serde_json::from_str(&adapter_def.env).unwrap_or_default();
                let addr = env
                    .get("ADAPTER_GRPC_ADDR")
                    .cloned()
                    .unwrap_or_else(|| "localhost:9090".to_string());

                info!(adapter = %adapter_def.name, %addr, "connecting to adapter (no scheduler)");
                match crate::adapter::grpc_client::GrpcAdapterClient::connect(&addr).await {
                    Ok(client) => Some(Box::new(client) as Box<dyn AdapterClient>),
                    Err(e) => {
                        warn!(adapter = %adapter_def.name, %addr, error = %e, "failed to connect, running without adapter");
                        None
                    }
                }
            }
        } else {
            info!(hypothesis_id = %self.hypothesis_id, "no adapter specified, running reference model only");
            None
        };

        let executor = Executor {
            hypothesis_id: self.hypothesis_id.clone(),
            run_id: run_id.clone(),
            config,
            tolerance: self.tolerance.clone(),
            reference,
            cancel: cancel.clone(),
            progress: progress.clone(),
            rocks: self.shared.rocks.clone(),
            files: self.shared.files.clone(),
            sqlite: self.shared.sqlite.clone(),
            adapter_client,
        };

        // Capture scheduler + hypothesis_id for teardown after executor finishes
        let scheduler = self.shared.scheduler.clone();
        let hyp_id = self.hypothesis_id.clone();
        let teardown_name = adapter_name_for_teardown.clone();

        let handle = tokio::spawn(async move {
            let result = executor.run().await;

            // Teardown adapter pod after run
            if let (Some(scheduler), Some(name)) = (&scheduler, &teardown_name) {
                info!(adapter = %name, "tearing down adapter pod");
                let _ = scheduler.teardown_adapter(&hyp_id, name).await;
            }

            result
        });

        self.run_state = Some(ActiveRun {
            run_id: run_id.clone(),
            adapter_name: adapter_name_for_teardown,
            cancel,
            executor_handle: handle,
            progress: progress.clone(),
        });

        info!(hypothesis_id = %self.hypothesis_id, run_id = %run_id, "run started");
        Ok((run_id, progress))
    }

    /// Stop the current run.
    pub async fn stop_run(&mut self) -> Result<()> {
        if let Some(run_state) = self.run_state.take() {
            info!(hypothesis_id = %self.hypothesis_id, run_id = %run_state.run_id, "stopping run");
            run_state.cancel.cancel();

            match run_state.executor_handle.await {
                Ok((reference, _reason)) => {
                    self.reference = Some(reference);
                }
                Err(e) => {
                    error!(error = %e, "executor task panicked");
                    self.reference = Some(crate::reference::create_reference(
                        &self.generator_name,
                        self.shared.rocks.clone(),
                        self.hypothesis_id.clone(),
                    ));
                }
            }

            // Teardown adapter if scheduler deployed it
            if let (Some(scheduler), Some(name)) = (&self.shared.scheduler, &run_state.adapter_name) {
                let _ = scheduler.teardown_adapter(&self.hypothesis_id, name).await;
            }
        }
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        self.run_state.is_some()
    }

    pub fn run_id(&self) -> Option<&str> {
        self.run_state.as_ref().map(|r| r.run_id.as_str())
    }

    pub fn progress(&self) -> Option<Arc<RwLock<ProgressInfo>>> {
        self.run_state.as_ref().map(|r| r.progress.clone())
    }
}
