use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result};
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Service;
use kube::api::{Api, DeleteParams, PostParams};
use kube::Client;
use tracing::{info, warn};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AdapterConfig {
    pub name: String,
    pub image: String,
    #[serde(default)]
    pub env: std::collections::HashMap<String, String>,
}

/// Shared service for K8s adapter lifecycle management.
#[derive(Clone)]
pub struct K8sScheduler {
    client: Client,
    namespace: String,
    pilot_grpc_addr: String,
}

impl K8sScheduler {
    pub async fn new(namespace: &str, pilot_grpc_addr: &str) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("failed to create K8s client")?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
            pilot_grpc_addr: pilot_grpc_addr.to_string(),
        })
    }

    pub fn new_with_client(client: Client, namespace: &str, pilot_grpc_addr: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
            pilot_grpc_addr: pilot_grpc_addr.to_string(),
        }
    }

    fn resource_name(hypothesis_id: &str, adapter_name: &str) -> String {
        format!("{hypothesis_id}-{adapter_name}")
    }

    pub async fn deploy_adapter(
        &self,
        hypothesis_id: &str,
        config: &AdapterConfig,
    ) -> Result<()> {
        let name = Self::resource_name(hypothesis_id, &config.name);

        // Build env vars
        let mut env_vars: Vec<serde_json::Value> = vec![
            serde_json::json!({"name": "REGRET_PILOT_ADDR", "value": self.pilot_grpc_addr}),
            serde_json::json!({"name": "REGRET_HYPOTHESIS_ID", "value": hypothesis_id}),
            serde_json::json!({"name": "REGRET_ADAPTER_NAME", "value": config.name}),
        ];
        for (k, v) in &config.env {
            env_vars.push(serde_json::json!({"name": k, "value": v}));
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
                    "regret.io/hypothesis-id": hypothesis_id,
                    "regret.io/adapter-name": config.name,
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
                            "regret.io/adapter-name": config.name,
                        }
                    },
                    "spec": {
                        "containers": [{
                            "name": "adapter",
                            "image": config.image,
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

        info!(name, hypothesis_id, "adapter deployed");
        Ok(())
    }

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
                        info!(name, "adapter ready");
                        return Ok(());
                    }
                }
                Err(e) => {
                    warn!(name, error = %e, "failed to check StatefulSet status");
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}
