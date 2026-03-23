use anyhow::{Context, Result};
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams};
use kube::Client;
use rand::seq::SliceRandom;
use tracing::{info, warn};

use super::types::{ChaosAction, LabelSelector};

/// Execute a chaos action against the K8s cluster.
pub async fn execute_action(
    client: &Client,
    namespace: &str,
    action: &ChaosAction,
) -> Result<Vec<String>> {
    // Resolve target pods
    let target_pods = resolve_targets(client, namespace, action).await?;
    if target_pods.is_empty() {
        warn!(action = %action.action_type, "no target pods found");
        return Ok(vec![]);
    }

    info!(
        action = %action.action_type,
        targets = ?target_pods,
        "executing chaos action"
    );

    match action.action_type.as_str() {
        "pod_kill" => pod_kill(client, namespace, &target_pods).await?,
        "pod_restart" => pod_restart(client, namespace, &target_pods).await?,
        "network_partition" => network_partition(client, namespace, &target_pods, action).await?,
        "network_delay" => network_delay(client, namespace, &target_pods, action).await?,
        "network_loss" => network_loss(client, namespace, &target_pods, action).await?,
        "rolling_update" => rolling_update(client, namespace, action).await?,
        "custom" => custom_action(client, namespace, &target_pods, action).await?,
        other => anyhow::bail!("unknown chaos action type: {other}"),
    }

    Ok(target_pods)
}

/// Resolve which pods to target using label selector.
async fn resolve_targets(
    client: &Client,
    namespace: &str,
    action: &ChaosAction,
) -> Result<Vec<String>> {
    // If specific pod is named, use it directly
    if let Some(pod_name) = &action.target_pod {
        return Ok(vec![pod_name.clone()]);
    }

    let pods_api: Api<Pod> = Api::namespaced(client.clone(), namespace);

    // Build label selector string from match_labels
    let label_str = action
        .selector
        .match_labels
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");

    if label_str.is_empty() {
        anyhow::bail!("chaos action has no target_pod or selector labels");
    }

    let lp = ListParams::default().labels(&label_str);
    let pod_list = pods_api.list(&lp).await.context("failed to list pods")?;

    let all_pods: Vec<String> = pod_list
        .items
        .iter()
        .filter_map(|p| p.metadata.name.clone())
        .collect();

    if all_pods.is_empty() {
        return Ok(vec![]);
    }

    // Apply selection mode
    select_pods(&all_pods, &action.selector)
}

/// Select pods based on the selector mode.
fn select_pods(pods: &[String], selector: &LabelSelector) -> Result<Vec<String>> {
    let mut rng = rand::thread_rng();
    match selector.mode.as_str() {
        "one" => {
            let pod = pods.choose(&mut rng).cloned();
            Ok(pod.into_iter().collect())
        }
        "all" => Ok(pods.to_vec()),
        "fixed" => {
            let count = selector.count.unwrap_or(1) as usize;
            let mut shuffled = pods.to_vec();
            shuffled.shuffle(&mut rng);
            Ok(shuffled.into_iter().take(count).collect())
        }
        "percentage" => {
            let count = (pods.len() as f64 * selector.percentage as f64 / 100.0).ceil() as usize;
            let mut shuffled = pods.to_vec();
            shuffled.shuffle(&mut rng);
            Ok(shuffled.into_iter().take(count).collect())
        }
        other => anyhow::bail!("unknown selector mode: {other}"),
    }
}

// ── Chaos Actions ──

async fn pod_kill(client: &Client, namespace: &str, pods: &[String]) -> Result<()> {
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    for pod_name in pods {
        info!(pod = %pod_name, "killing pod");
        pods_api
            .delete(pod_name, &DeleteParams::default().grace_period(0))
            .await
            .context(format!("failed to kill pod {pod_name}"))?;
    }
    Ok(())
}

async fn pod_restart(client: &Client, namespace: &str, pods: &[String]) -> Result<()> {
    // Delete pod — StatefulSet controller will recreate it
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    for pod_name in pods {
        info!(pod = %pod_name, "restarting pod (delete + recreate)");
        pods_api
            .delete(pod_name, &DeleteParams::default())
            .await
            .context(format!("failed to restart pod {pod_name}"))?;
    }
    Ok(())
}

async fn network_partition(
    client: &Client,
    namespace: &str,
    pods: &[String],
    _action: &ChaosAction,
) -> Result<()> {
    // Use kubectl exec to add iptables rules that drop all traffic
    for pod_name in pods {
        info!(pod = %pod_name, "injecting network partition");
        exec_in_pod(client, namespace, pod_name, &[
            "sh", "-c",
            "iptables -A INPUT -j DROP && iptables -A OUTPUT -j DROP || true"
        ]).await?;
    }
    Ok(())
}

async fn network_delay(
    client: &Client,
    namespace: &str,
    pods: &[String],
    action: &ChaosAction,
) -> Result<()> {
    let delay_ms = action.params.get("delay_ms").and_then(|v| v.as_u64()).unwrap_or(100);
    for pod_name in pods {
        info!(pod = %pod_name, delay_ms, "injecting network delay");
        exec_in_pod(client, namespace, pod_name, &[
            "sh", "-c",
            &format!("tc qdisc add dev eth0 root netem delay {delay_ms}ms || true"),
        ]).await?;
    }
    Ok(())
}

async fn network_loss(
    client: &Client,
    namespace: &str,
    pods: &[String],
    action: &ChaosAction,
) -> Result<()> {
    let loss_pct = action.params.get("loss_percent").and_then(|v| v.as_u64()).unwrap_or(10);
    for pod_name in pods {
        info!(pod = %pod_name, loss_pct, "injecting network loss");
        exec_in_pod(client, namespace, pod_name, &[
            "sh", "-c",
            &format!("tc qdisc add dev eth0 root netem loss {loss_pct}% || true"),
        ]).await?;
    }
    Ok(())
}

async fn rolling_update(
    client: &Client,
    namespace: &str,
    action: &ChaosAction,
) -> Result<()> {
    let sts_name = action
        .params
        .get("statefulset")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("rolling_update requires 'statefulset' in params"))?;
    let image = action
        .params
        .get("image")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("rolling_update requires 'image' in params"))?;
    let container = action
        .params
        .get("container")
        .and_then(|v| v.as_str())
        .unwrap_or("server");

    info!(
        statefulset = %sts_name,
        image = %image,
        container = %container,
        "initiating rolling update"
    );

    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);

    // Patch the container image
    let patch = serde_json::json!({
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": container,
                        "image": image,
                    }]
                }
            }
        }
    });
    sts_api
        .patch(
            sts_name,
            &PatchParams::apply("regret-chaos"),
            &Patch::Strategic(patch),
        )
        .await
        .context(format!("failed to patch StatefulSet {sts_name}"))?;

    info!(statefulset = %sts_name, "patch applied, waiting for rollout");

    // Wait for rollout to complete via kubectl
    let timeout_secs = action
        .params
        .get("timeout")
        .and_then(|v| v.as_str())
        .unwrap_or("600s");

    let output = tokio::process::Command::new("kubectl")
        .args([
            "rollout", "status", "statefulset", sts_name,
            "-n", namespace,
            &format!("--timeout={timeout_secs}"),
        ])
        .output()
        .await
        .context("failed to wait for rollout")?;

    if output.status.success() {
        info!(statefulset = %sts_name, image = %image, "rolling update completed");
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!(statefulset = %sts_name, stderr = %stderr, "rollout status check failed");
    }

    Ok(())
}

async fn custom_action(
    _client: &Client,
    _namespace: &str,
    pods: &[String],
    action: &ChaosAction,
) -> Result<()> {
    let command = action.params.get("command").and_then(|v| v.as_str()).unwrap_or("");
    if command.is_empty() {
        anyhow::bail!("custom chaos action requires 'command' in params");
    }
    info!(pods = ?pods, command, "executing custom chaos action");
    let output = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .await
        .context("failed to execute custom command")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("custom command failed: {stderr}");
    }
    Ok(())
}

/// Execute a command inside a pod via kubectl exec.
async fn exec_in_pod(
    _client: &Client,
    namespace: &str,
    pod_name: &str,
    command: &[&str],
) -> Result<()> {
    // Use kubectl exec since kube-rs exec is complex
    let mut args = vec!["exec", "-n", namespace, pod_name, "--"];
    args.extend_from_slice(command);

    let output = tokio::process::Command::new("kubectl")
        .args(&args)
        .output()
        .await
        .context(format!("failed to exec in pod {pod_name}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!(pod = %pod_name, stderr = %stderr, "exec command failed");
    }
    Ok(())
}

/// Recover from network chaos by removing tc/iptables rules.
pub async fn recover_network(
    client: &Client,
    namespace: &str,
    pods: &[String],
    action_type: &str,
) -> Result<()> {
    for pod_name in pods {
        info!(pod = %pod_name, action = %action_type, "recovering network");
        match action_type {
            "network_partition" => {
                exec_in_pod(client, namespace, pod_name, &[
                    "sh", "-c", "iptables -F || true"
                ]).await?;
            }
            "network_delay" | "network_loss" => {
                exec_in_pod(client, namespace, pod_name, &[
                    "sh", "-c", "tc qdisc del dev eth0 root || true"
                ]).await?;
            }
            _ => {}
        }
    }
    Ok(())
}
