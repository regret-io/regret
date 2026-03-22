use std::sync::Arc;

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
        generate_params: crate::generator::GenerateParams,
        adapter: Option<AdapterRecord>,
        adapter_addr_override: Option<String>,
    ) -> Result<(String, Arc<RwLock<ProgressInfo>>)> {
        // Clean up any finished run before starting a new one
        self.cleanup_finished_run().await;

        if self.is_running() {
            anyhow::bail!("hypothesis is already running");
        }

        let reference = self
            .reference
            .take()
            .ok_or_else(|| anyhow::anyhow!("reference model not available"))?;

        let run_id = format!("run-{}", uuid::Uuid::now_v7());
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

        // Connect to adapter — adapter is user-managed, pilot just connects
        let adapter_client: Option<Box<dyn AdapterClient>> = if let Some(adapter_def) = &adapter {
            // Use override address if provided, otherwise derive from adapter name
            let addr = adapter_addr_override
                .unwrap_or_else(|| format!("{}:9090", adapter_def.name));

            info!(adapter = %adapter_def.name, %addr, "connecting to adapter");
            match crate::adapter::grpc_client::GrpcAdapterClient::connect(&addr).await {
                Ok(client) => Some(Box::new(client) as Box<dyn AdapterClient>),
                Err(e) => {
                    warn!(adapter = %adapter_def.name, %addr, error = %e, "failed to connect, running without adapter");
                    None
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
            generate_params,
            reference,
            cancel: cancel.clone(),
            progress: progress.clone(),
            rocks: self.shared.rocks.clone(),
            files: self.shared.files.clone(),
            sqlite: self.shared.sqlite.clone(),
            adapter_client,
        };

        let handle = tokio::spawn(async move { executor.run().await });

        self.run_state = Some(ActiveRun {
            run_id: run_id.clone(),
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

        }
        Ok(())
    }

    pub fn is_running(&self) -> bool {
        match &self.run_state {
            Some(state) => !state.executor_handle.is_finished(),
            None => false,
        }
    }

    /// Clean up finished run state. Call before start_run.
    pub async fn cleanup_finished_run(&mut self) {
        if let Some(state) = &self.run_state {
            if state.executor_handle.is_finished() {
                let run_state = self.run_state.take().unwrap();
                match run_state.executor_handle.await {
                    Ok((reference, _)) => { self.reference = Some(reference); }
                    Err(_) => {
                        self.reference = Some(crate::reference::create_reference(
                            &self.generator_name, self.shared.rocks.clone(), self.hypothesis_id.clone(),
                        ));
                    }
                }
            }
        }
    }

    pub fn run_id(&self) -> Option<&str> {
        self.run_state.as_ref().map(|r| r.run_id.as_str())
    }

    pub fn progress(&self) -> Option<Arc<RwLock<ProgressInfo>>> {
        self.run_state.as_ref().map(|r| r.progress.clone())
    }
}
