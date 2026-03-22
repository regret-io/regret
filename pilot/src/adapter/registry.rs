use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::info;

use regret_proto::regret_v1::adapter_service_client::AdapterServiceClient;

#[derive(Clone)]
pub struct AdapterInfo {
    pub grpc_addr: String,
    pub registered_at: chrono::DateTime<chrono::Utc>,
    client: Option<AdapterServiceClient<Channel>>,
}

/// In-memory registry of adapters registered via gRPC.
#[derive(Clone)]
pub struct AdapterRegistry {
    adapters: Arc<RwLock<HashMap<(String, String), AdapterInfo>>>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self {
            adapters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(
        &self,
        hypothesis_id: &str,
        adapter_name: &str,
        grpc_addr: &str,
    ) {
        info!(
            hypothesis_id,
            adapter_name,
            grpc_addr,
            "adapter registered"
        );
        let mut adapters = self.adapters.write().await;
        adapters.insert(
            (hypothesis_id.to_string(), adapter_name.to_string()),
            AdapterInfo {
                grpc_addr: grpc_addr.to_string(),
                registered_at: chrono::Utc::now(),
                client: None,
            },
        );
    }

    pub async fn get_client(
        &self,
        hypothesis_id: &str,
        adapter_name: &str,
    ) -> Result<Option<AdapterServiceClient<Channel>>> {
        let mut adapters = self.adapters.write().await;
        let key = (hypothesis_id.to_string(), adapter_name.to_string());

        if let Some(info) = adapters.get_mut(&key) {
            if info.client.is_none() {
                let channel = Channel::from_shared(format!("http://{}", info.grpc_addr))?
                    .connect()
                    .await?;
                info.client = Some(AdapterServiceClient::new(channel));
            }
            Ok(info.client.clone())
        } else {
            Ok(None)
        }
    }

    pub async fn remove(&self, hypothesis_id: &str, adapter_name: &str) {
        let mut adapters = self.adapters.write().await;
        adapters.remove(&(hypothesis_id.to_string(), adapter_name.to_string()));
    }

    pub async fn get_adapters_for_hypothesis(
        &self,
        hypothesis_id: &str,
    ) -> Vec<(String, AdapterInfo)> {
        let adapters = self.adapters.read().await;
        adapters
            .iter()
            .filter(|((hid, _), _)| hid == hypothesis_id)
            .map(|((_, name), info)| (name.clone(), info.clone()))
            .collect()
    }
}
