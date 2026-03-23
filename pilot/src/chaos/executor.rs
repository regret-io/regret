use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::engine::ManagerRegistry;
use crate::storage::files::FileStore;
use crate::storage::sqlite::SqliteStore;

use super::actions;
use super::events::ChaosEvent;
use super::types::{ChaosAction, ChaosScenario};

/// Active chaos injection state.
pub struct ChaosInjection {
    pub injection_id: String,
    pub scenario: ChaosScenario,
    pub cancel: CancellationToken,
    pub handle: JoinHandle<()>,
    pub started_at: String,
}

/// Registry of active chaos injections.
#[derive(Clone)]
pub struct ChaosRegistry {
    injections: Arc<RwLock<HashMap<String, ChaosInjectionHandle>>>,
    files: FileStore,
    sqlite: SqliteStore,
    managers: ManagerRegistry,
}

/// Lightweight handle stored in the registry.
struct ChaosInjectionHandle {
    pub scenario_name: String,
    pub cancel: CancellationToken,
    pub handle: Option<JoinHandle<()>>,
}

impl ChaosRegistry {
    pub fn new(files: FileStore, sqlite: SqliteStore, managers: ManagerRegistry) -> Self {
        Self {
            injections: Arc::new(RwLock::new(HashMap::new())),
            files,
            sqlite,
            managers,
        }
    }

    /// Start a chaos injection from a scenario.
    pub async fn start_injection(&self, scenario: ChaosScenario) -> Result<String> {
        let injection_id = uuid::Uuid::now_v7().to_string();
        let cancel = CancellationToken::new();

        // Record in SQLite
        self.sqlite
            .create_chaos_injection(
                &injection_id,
                &scenario.id,
                &scenario.name,
            )
            .await?;

        // Emit start event
        let event = ChaosEvent::InjectionStarted {
            injection_id: injection_id.clone(),
            scenario_name: scenario.name.clone(),
            timestamp: ChaosEvent::now(),
        };
        self.emit_chaos_event(&event);

        let files = self.files.clone();
        let sqlite = self.sqlite.clone();
        let managers = self.managers.clone();
        let cancel_clone = cancel.clone();
        let inj_id = injection_id.clone();
        let scenario_clone = scenario.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = run_injection(
                &inj_id,
                &scenario_clone,
                &files,
                &sqlite,
                &managers,
                cancel_clone,
            )
            .await
            {
                error!(injection_id = %inj_id, error = %e, "chaos injection failed");
                let event = ChaosEvent::ChaosError {
                    injection_id: inj_id.clone(),
                    scenario_name: scenario_clone.name.clone(),
                    action_type: "injection".to_string(),
                    error: e.to_string(),
                    timestamp: ChaosEvent::now(),
                };
                let _ = files.append_chaos_event(&event.to_json());
                let _ = sqlite
                    .update_chaos_injection_status(&inj_id, "error", Some(&e.to_string()))
                    .await;
            }
        });

        let mut injections = self.injections.write().await;
        injections.insert(
            injection_id.clone(),
            ChaosInjectionHandle {
                scenario_name: scenario.name,
                cancel,
                handle: Some(handle),
            },
        );

        Ok(injection_id)
    }

    /// Stop an active chaos injection.
    pub async fn stop_injection(&self, injection_id: &str) -> Result<()> {
        let mut injections = self.injections.write().await;
        if let Some(handle) = injections.remove(injection_id) {
            handle.cancel.cancel();
            if let Some(jh) = handle.handle {
                let _ = jh.await;
            }

            let event = ChaosEvent::InjectionStopped {
                injection_id: injection_id.to_string(),
                scenario_name: handle.scenario_name,
                reason: "manual".to_string(),
                timestamp: ChaosEvent::now(),
            };
            self.emit_chaos_event(&event);

            self.sqlite
                .update_chaos_injection_status(injection_id, "stopped", None)
                .await?;
        }
        Ok(())
    }

    /// Check if an injection is active.
    pub async fn is_active(&self, injection_id: &str) -> bool {
        let injections = self.injections.read().await;
        injections.contains_key(injection_id)
    }

    /// List active injection IDs.
    pub async fn active_ids(&self) -> Vec<String> {
        let injections = self.injections.read().await;
        injections.keys().cloned().collect()
    }

    fn emit_chaos_event(&self, event: &ChaosEvent) {
        if let Err(e) = self.files.append_chaos_event(&event.to_json()) {
            error!(error = %e, "failed to write chaos event");
        }
    }
}

/// Main injection loop — schedules and executes each action.
async fn run_injection(
    injection_id: &str,
    scenario: &ChaosScenario,
    files: &FileStore,
    sqlite: &SqliteStore,
    managers: &ManagerRegistry,
    cancel: CancellationToken,
) -> Result<()> {
    // Temporarily unset HTTP proxy for K8s client (local proxy breaks K8s API calls)
    let saved_http_proxy = std::env::var("http_proxy").ok();
    let saved_https_proxy = std::env::var("https_proxy").ok();
    // SAFETY: single-threaded at this point in the injection task
    unsafe {
        std::env::remove_var("http_proxy");
        std::env::remove_var("https_proxy");
        std::env::remove_var("HTTP_PROXY");
        std::env::remove_var("HTTPS_PROXY");
    }

    let client_result = kube::Client::try_default().await;

    // Restore proxy env
    unsafe {
        if let Some(v) = saved_http_proxy { std::env::set_var("http_proxy", v); }
        if let Some(v) = saved_https_proxy { std::env::set_var("https_proxy", v); }
    }

    let client = client_result.context("failed to create K8s client")?;

    let namespace = &scenario.namespace;

    // Spawn a task per action based on its timing config
    let mut action_handles = Vec::new();

    for action in &scenario.actions {
        let client = client.clone();
        let ns = namespace.clone();
        let inj_id = injection_id.to_string();
        let scenario_name = scenario.name.clone();
        let action = action.clone();
        let cancel = cancel.clone();
        let files = files.clone();
        let managers = managers.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) =
                run_action_loop(&inj_id, &scenario_name, &client, &ns, &action, &cancel, &files, &managers)
                    .await
            {
                error!(
                    injection_id = %inj_id,
                    action = %action.action_type,
                    error = %e,
                    "action loop failed"
                );
                let event = ChaosEvent::ChaosError {
                    injection_id: inj_id,
                    scenario_name,
                    action_type: action.action_type.clone(),
                    error: e.to_string(),
                    timestamp: ChaosEvent::now(),
                };
                let _ = files.append_chaos_event(&event.to_json());
            }
        });
        action_handles.push(handle);
    }

    // Wait for all actions to finish or cancellation
    for handle in action_handles {
        let _ = handle.await;
    }

    // Mark injection as finished
    sqlite
        .update_chaos_injection_status(injection_id, "finished", None)
        .await?;

    info!(injection_id = %injection_id, "chaos injection finished");
    Ok(())
}

/// Run a single action based on its timing: one-shot (`at`) or repeating (`interval`).
async fn run_action_loop(
    injection_id: &str,
    scenario_name: &str,
    client: &kube::Client,
    namespace: &str,
    action: &ChaosAction,
    cancel: &CancellationToken,
    files: &FileStore,
    managers: &ManagerRegistry,
) -> Result<()> {
    // upgrade_test has its own multi-step lifecycle
    if action.action_type == "upgrade_test" {
        return run_upgrade_test(injection_id, scenario_name, action, cancel, files, managers).await;
    }

    // If "at" is set, wait then execute once
    if let Some(at) = &action.at {
        let delay = parse_duration(at)?;
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
        execute_and_log(injection_id, scenario_name, client, namespace, action, files).await?;

        // If the action has a duration, wait then recover
        if let Some(dur) = &action.duration {
            let duration = parse_duration(dur)?;
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Recover on cancel
                    recover_if_needed(client, namespace, action, injection_id, scenario_name, files).await;
                    return Ok(());
                }
                _ = tokio::time::sleep(duration) => {}
            }
            recover_if_needed(client, namespace, action, injection_id, scenario_name, files).await;
        }
        return Ok(());
    }

    // If "interval" is set, repeat until cancelled
    if let Some(interval_str) = &action.interval {
        let interval = parse_duration(interval_str)?;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    recover_if_needed(client, namespace, action, injection_id, scenario_name, files).await;
                    return Ok(());
                }
                _ = tokio::time::sleep(interval) => {}
            }

            execute_and_log(injection_id, scenario_name, client, namespace, action, files).await?;

            // If duration is set, wait then recover before next cycle
            if let Some(dur) = &action.duration {
                let duration = parse_duration(dur)?;
                tokio::select! {
                    _ = cancel.cancelled() => {
                        recover_if_needed(client, namespace, action, injection_id, scenario_name, files).await;
                        return Ok(());
                    }
                    _ = tokio::time::sleep(duration) => {}
                }
                recover_if_needed(client, namespace, action, injection_id, scenario_name, files).await;
            }
        }
    }

    // No timing — execute once immediately
    execute_and_log(injection_id, scenario_name, client, namespace, action, files).await?;

    if let Some(dur) = &action.duration {
        let duration = parse_duration(dur)?;
        tokio::select! {
            _ = cancel.cancelled() => {
                recover_if_needed(client, namespace, action, injection_id, scenario_name, files).await;
                return Ok(());
            }
            _ = tokio::time::sleep(duration) => {}
        }
        recover_if_needed(client, namespace, action, injection_id, scenario_name, files).await;
    }

    Ok(())
}

async fn execute_and_log(
    injection_id: &str,
    scenario_name: &str,
    client: &kube::Client,
    namespace: &str,
    action: &ChaosAction,
    files: &FileStore,
) -> Result<()> {
    match actions::execute_action(client, namespace, action).await {
        Ok(target_pods) => {
            if !target_pods.is_empty() {
                let event = ChaosEvent::ChaosInjected {
                    injection_id: injection_id.to_string(),
                    scenario_name: scenario_name.to_string(),
                    action_type: action.action_type.clone(),
                    target_pods,
                    namespace: namespace.to_string(),
                    timestamp: ChaosEvent::now(),
                };
                let _ = files.append_chaos_event(&event.to_json());
            }
        }
        Err(e) => {
            warn!(
                injection_id = %injection_id,
                action = %action.action_type,
                error = %e,
                "chaos action failed"
            );
            let event = ChaosEvent::ChaosError {
                injection_id: injection_id.to_string(),
                scenario_name: scenario_name.to_string(),
                action_type: action.action_type.clone(),
                error: e.to_string(),
                timestamp: ChaosEvent::now(),
            };
            let _ = files.append_chaos_event(&event.to_json());
        }
    }
    Ok(())
}

async fn recover_if_needed(
    client: &kube::Client,
    namespace: &str,
    action: &ChaosAction,
    injection_id: &str,
    scenario_name: &str,
    files: &FileStore,
) {
    let needs_recovery = matches!(
        action.action_type.as_str(),
        "network_partition" | "network_delay" | "network_loss"
    );
    if !needs_recovery {
        return;
    }

    // Resolve targets again for recovery
    let target_pods = match actions::execute_action(client, namespace, action).await {
        Ok(pods) => pods,
        Err(_) => return,
    };

    if let Err(e) =
        actions::recover_network(client, namespace, &target_pods, &action.action_type).await
    {
        warn!(error = %e, "failed to recover network chaos");
    } else {
        let event = ChaosEvent::ChaosRecovered {
            injection_id: injection_id.to_string(),
            scenario_name: scenario_name.to_string(),
            action_type: action.action_type.clone(),
            target_pods,
            duration_ms: 0, // duration tracked externally
            timestamp: ChaosEvent::now(),
        };
        let _ = files.append_chaos_event(&event.to_json());
    }
}

/// Run the upgrade_test action: upgrade → wait checkpoints → downgrade → wait → upgrade → wait.
async fn run_upgrade_test(
    injection_id: &str,
    scenario_name: &str,
    action: &ChaosAction,
    cancel: &CancellationToken,
    files: &FileStore,
    managers: &ManagerRegistry,
) -> Result<()> {
    let params = &action.params;
    let hypothesis_id = params.get("hypothesis_id").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("upgrade_test requires 'hypothesis_id' in params"))?;
    let resource = params.get("resource").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("upgrade_test requires 'resource' in params"))?;
    let namespace = params.get("namespace").and_then(|v| v.as_str()).unwrap_or("regret-system");
    let candidate_image = params.get("candidate_image").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("upgrade_test requires 'candidate_image' in params"))?;
    let stable_image = params.get("stable_image").and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("upgrade_test requires 'stable_image' in params"))?;
    let patch_path = params.get("patch_path").and_then(|v| v.as_str())
        .unwrap_or("/spec/template/spec/containers/0/image");
    let checkpoints_per_step = params.get("checkpoints_per_step").and_then(|v| v.as_u64()).unwrap_or(1);
    let timeout = params.get("timeout").and_then(|v| v.as_str()).unwrap_or("600s");

    let steps = [
        ("upgrade", candidate_image),
        ("downgrade", stable_image),
        ("re-upgrade", candidate_image),
    ];

    for (step_name, image) in &steps {
        if cancel.is_cancelled() { return Ok(()); }

        info!(step = %step_name, image = %image, resource = %resource, "upgrade_test: patching");

        // Patch the resource
        let patch_json = serde_json::json!([{"op": "replace", "path": patch_path, "value": image}]);
        let output = tokio::process::Command::new("kubectl")
            .args(["patch", resource, "-n", namespace, "--type=json", "-p", &patch_json.to_string()])
            .output()
            .await
            .context("kubectl patch failed")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("upgrade_test {step_name}: kubectl patch failed: {stderr}");
        }

        // Log the event
        let event = ChaosEvent::ChaosInjected {
            injection_id: injection_id.to_string(),
            scenario_name: scenario_name.to_string(),
            action_type: format!("upgrade_test:{step_name}"),
            target_pods: vec![resource.to_string()],
            namespace: namespace.to_string(),
            timestamp: ChaosEvent::now(),
        };
        let _ = files.append_chaos_event(&event.to_json());

        // Wait for rollout
        info!(step = %step_name, "upgrade_test: waiting for rollout");
        let output = tokio::process::Command::new("kubectl")
            .args(["rollout", "status", resource, "-n", namespace, &format!("--timeout={timeout}")])
            .output()
            .await
            .context("kubectl rollout status failed")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(step = %step_name, stderr = %stderr, "rollout status failed");
        }

        // Wait for N checkpoints to pass
        info!(step = %step_name, checkpoints = checkpoints_per_step, "upgrade_test: waiting for checkpoints");
        let start_checkpoints = get_passed_checkpoints(managers, hypothesis_id).await;

        loop {
            if cancel.is_cancelled() { return Ok(()); }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            let current = get_passed_checkpoints(managers, hypothesis_id).await;
            if current >= start_checkpoints + checkpoints_per_step {
                info!(step = %step_name, passed = current, "upgrade_test: checkpoints passed");
                break;
            }

            // Check if hypothesis is still running
            let mgr = managers.get(hypothesis_id).await;
            if mgr.is_none() {
                anyhow::bail!("upgrade_test: hypothesis {hypothesis_id} not found");
            }
        }
    }

    info!(injection_id = %injection_id, "upgrade_test: full cycle completed");
    Ok(())
}

/// Read passed_checkpoints from a hypothesis manager.
async fn get_passed_checkpoints(managers: &ManagerRegistry, hypothesis_id: &str) -> u64 {
    if let Some(mgr_arc) = managers.get(hypothesis_id).await {
        let mgr = mgr_arc.lock().await;
        if let Some(progress) = mgr.progress() {
            return progress.read().await.passed_checkpoints as u64;
        }
    }
    0
}

/// Parse a duration string like "30s", "5m", "1h", "100ms".
fn parse_duration(s: &str) -> Result<std::time::Duration> {
    let s = s.trim();
    if let Some(ms) = s.strip_suffix("ms") {
        let n: u64 = ms.parse().context("invalid ms duration")?;
        return Ok(std::time::Duration::from_millis(n));
    }
    if let Some(secs) = s.strip_suffix('s') {
        let n: u64 = secs.parse().context("invalid seconds duration")?;
        return Ok(std::time::Duration::from_secs(n));
    }
    if let Some(mins) = s.strip_suffix('m') {
        let n: u64 = mins.parse().context("invalid minutes duration")?;
        return Ok(std::time::Duration::from_secs(n * 60));
    }
    if let Some(hrs) = s.strip_suffix('h') {
        let n: u64 = hrs.parse().context("invalid hours duration")?;
        return Ok(std::time::Duration::from_secs(n * 3600));
    }
    anyhow::bail!("unknown duration format: {s} (use ms, s, m, or h suffix)")
}
