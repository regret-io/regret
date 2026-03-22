use std::time::Duration;

use anyhow::{Context, Result};
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Service;
use kube::api::{Api, DeleteParams, PostParams};
use kube::Client;
use tracing::{info, warn};

use crate::storage::sqlite::AdapterRecord;

/// Shared service for deploying adapter pods on K8s.
#[derive(Clone)]
pub struct K8sScheduler {
    client: Client,
    namespace: String,
    pilot_grpc_addr: String,
}

impl K8sScheduler {
    pub async fn try_new(namespace: &str, pilot_grpc_addr: &str) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("failed to create K8s client")?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
            pilot_grpc_addr: pilot_grpc_addr.to_string(),
        })
    }

    fn resource_name(hypothesis_id: &str, adapter_name: &str) -> String {
        // Sanitize for K8s naming: lowercase, replace non-alphanumeric with dash
        let raw = format!("{}-{}", hypothesis_id, adapter_name);
        raw.chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c.to_ascii_lowercase() } else { '-' })
            .collect::<String>()
            .trim_matches('-')
            .to_string()
    }

    /// Deploy an adapter pod from its definition.
    /// Auto-injects REGRET_PILOT_ADDR, REGRET_HYPOTHESIS_ID, REGRET_ADAPTER_NAME.
    pub async fn deploy_adapter(
        &self,
        hypothesis_id: &str,
        adapter: &AdapterRecord,
    ) -> Result<String> {
        let name = Self::resource_name(hypothesis_id, &adapter.name);

        // Parse user-defined env vars
        let user_env: std::collections::HashMap<String, String> =
            serde_json::from_str(&adapter.env).unwrap_or_default();

        // Build env vars: auto-injected + user-defined
        let mut env_vars: Vec<serde_json::Value> = vec![
            serde_json::json!({"name": "REGRET_PILOT_ADDR", "value": self.pilot_grpc_addr}),
            serde_json::json!({"name": "REGRET_HYPOTHESIS_ID", "value": hypothesis_id}),
            serde_json::json!({"name": "REGRET_ADAPTER_NAME", "value": adapter.name}),
        ];
        for (k, v) in &user_env {
            // Don't let user override auto-injected vars
            if !k.starts_with("REGRET_") {
                env_vars.push(serde_json::json!({"name": k, "value": v}));
            }
        }

        // Create StatefulSet
        let sts: StatefulSet = serde_json::from_value(serde_json::json!({
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": {
                    "app.kubernetes.io/name": "regret-adapter",
                    "app.kubernetes.io/managed-by": "regret-pilot",
                    "regret.io/hypothesis-id": hypothesis_id,
                    "regret.io/adapter-name": adapter.name,
                }
            },
            "spec": {
                "serviceName": name,
                "replicas": 1,
                "selector": {
                    "matchLabels": {
                        "app.kubernetes.io/name": name
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                            "app.kubernetes.io/name": name,
                            "regret.io/hypothesis-id": hypothesis_id,
                            "regret.io/adapter-name": adapter.name,
                        }
                    },
                    "spec": {
                        "containers": [{
                            "name": "adapter",
                            "image": adapter.image,
                            "ports": [{"containerPort": 9090, "name": "grpc"}],
                            "env": env_vars,
                        }]
                    }
                }
            }
        }))?;

        let sts_api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        sts_api
            .create(&PostParams::default(), &sts)
            .await
            .context(format!("failed to create StatefulSet {name}"))?;

        // Create headless Service
        let svc: Service = serde_json::from_value(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": {
                    "app.kubernetes.io/managed-by": "regret-pilot",
                }
            },
            "spec": {
                "clusterIP": "None",
                "selector": {
                    "app.kubernetes.io/name": name
                },
                "ports": [{
                    "port": 9090,
                    "targetPort": 9090,
                    "name": "grpc"
                }]
            }
        }))?;

        let svc_api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        svc_api
            .create(&PostParams::default(), &svc)
            .await
            .context(format!("failed to create Service {name}"))?;

        let grpc_addr = format!("{name}-0.{name}.{}.svc.cluster.local:9090", self.namespace);
        info!(name, hypothesis_id, adapter = %adapter.name, %grpc_addr, "adapter deployed");
        Ok(grpc_addr)
    }

    /// Tear down an adapter's StatefulSet and Service.
    pub async fn teardown_adapter(
        &self,
        hypothesis_id: &str,
        adapter_name: &str,
    ) -> Result<()> {
        let name = Self::resource_name(hypothesis_id, adapter_name);

        let sts_api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        match sts_api.delete(&name, &DeleteParams::default()).await {
            Ok(_) => info!(name, "StatefulSet deleted"),
            Err(e) => warn!(name, error = %e, "failed to delete StatefulSet"),
        }

        let svc_api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        match svc_api.delete(&name, &DeleteParams::default()).await {
            Ok(_) => info!(name, "Service deleted"),
            Err(e) => warn!(name, error = %e, "failed to delete Service"),
        }

        Ok(())
    }

    /// Wait for the adapter pod to be ready.
    pub async fn wait_for_ready(
        &self,
        hypothesis_id: &str,
        adapter_name: &str,
        timeout: Duration,
    ) -> Result<()> {
        let name = Self::resource_name(hypothesis_id, adapter_name);
        let sts_api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);

        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > timeout {
                anyhow::bail!("timeout waiting for adapter {name} to be ready");
            }

            match sts_api.get(&name).await {
                Ok(sts) => {
                    let ready = sts
                        .status
                        .as_ref()
                        .and_then(|s| s.ready_replicas)
                        .unwrap_or(0);
                    if ready >= 1 {
                        info!(name, "adapter pod ready");
                        return Ok(());
                    }
                }
                Err(e) => {
                    warn!(name, error = %e, "checking StatefulSet status");
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}
