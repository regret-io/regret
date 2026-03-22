use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::reference::{ReferenceModel, Tolerance};

use super::SharedServices;
use super::executor::{AdapterClient, ExecutionConfig, Executor, ProgressInfo, StopReason};

/// Per-hypothesis lifecycle manager.
/// Created when a hypothesis is created, destroyed when deleted.
pub struct HypothesisManager {
    pub hypothesis_id: String,
    pub profile: String,
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
        profile: String,
        tolerance_json: Option<String>,
        reference: Box<dyn ReferenceModel>,
        shared: SharedServices,
    ) -> Self {
        let tolerance = tolerance_json
            .as_ref()
            .and_then(|t| serde_json::from_str(t).ok());

        Self {
            hypothesis_id,
            profile,
            tolerance,
            reference: Some(reference),
            run_state: None,
            shared,
        }
    }

    /// Start a new run. If adapter_addr is provided, connects via gRPC.
    pub async fn start_run(
        &mut self,
        config: ExecutionConfig,
        adapter_addr: Option<String>,
    ) -> Result<(String, Arc<RwLock<ProgressInfo>>)> {
        if self.run_state.is_some() {
            anyhow::bail!("hypothesis is already running");
        }

        let reference = self
            .reference
            .take()
            .ok_or_else(|| anyhow::anyhow!("reference model not available (run in progress?)"))?;

        let run_id = format!(
            "run-{}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ")
        );
        let cancel = CancellationToken::new();
        let progress = Arc::new(RwLock::new(ProgressInfo::default()));

        // Update hypothesis status
        self.shared
            .sqlite
            .update_hypothesis_status(&self.hypothesis_id, "running")
            .await?;
        self.shared
            .sqlite
            .update_last_run_at(
                &self.hypothesis_id,
                &chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            )
            .await?;

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
            adapter_client: {
                if let Some(addr) = &adapter_addr {
                    match crate::adapter::grpc_client::GrpcAdapterClient::connect(addr).await {
                        Ok(client) => {
                            info!(addr, "connected to adapter via gRPC");
                            Some(Box::new(client) as Box<dyn AdapterClient>)
                        }
                        Err(e) => {
                            error!(addr, error = %e, "failed to connect to adapter");
                            None
                        }
                    }
                } else {
                    None
                }
            },
        };

        let handle = tokio::spawn(async move { executor.run().await });

        self.run_state = Some(ActiveRun {
            run_id: run_id.clone(),
            cancel,
            executor_handle: handle,
            progress: progress.clone(),
        });

        info!(
            hypothesis_id = %self.hypothesis_id,
            run_id = %run_id,
            "run started"
        );

        Ok((run_id, progress))
    }

    /// Stop the current run.
    pub async fn stop_run(&mut self) -> Result<()> {
        if let Some(run_state) = self.run_state.take() {
            info!(
                hypothesis_id = %self.hypothesis_id,
                run_id = %run_state.run_id,
                "stopping run"
            );

            run_state.cancel.cancel();

            // Wait for executor to finish and get reference model back
            match run_state.executor_handle.await {
                Ok((reference, _reason)) => {
                    self.reference = Some(reference);
                }
                Err(e) => {
                    error!(error = %e, "executor task panicked");
                    // Re-create reference model
                    self.reference =
                        Some(crate::reference::create_reference(&self.profile));
                }
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
