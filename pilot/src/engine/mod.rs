pub mod events;
pub mod executor;
pub mod hypothesis_manager;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::reference;
use crate::scheduler::k8s::K8sScheduler;
use crate::storage::files::FileStore;
use crate::storage::rocks::RocksStore;
use crate::storage::sqlite::SqliteStore;
use hypothesis_manager::HypothesisManager;

/// Shared services passed to each HypothesisManager.
#[derive(Clone)]
pub struct SharedServices {
    pub sqlite: SqliteStore,
    pub rocks: RocksStore,
    pub files: FileStore,
    pub scheduler: Option<K8sScheduler>,
}

/// Global registry of all hypothesis managers.
#[derive(Clone)]
pub struct ManagerRegistry {
    managers: Arc<RwLock<HashMap<String, Arc<tokio::sync::Mutex<HypothesisManager>>>>>,
    shared: SharedServices,
}

impl ManagerRegistry {
    pub fn new(shared: SharedServices) -> Self {
        Self {
            managers: Arc::new(RwLock::new(HashMap::new())),
            shared,
        }
    }

    pub async fn create_from_hypothesis(
        &self,
        id: &str,
        profile: &str,
        tolerance: Option<String>,
    ) {
        let reference = reference::create_reference(profile);
        let manager = HypothesisManager::new(
            id.to_string(),
            profile.to_string(),
            tolerance,
            reference,
            self.shared.clone(),
        );
        let mut managers = self.managers.write().await;
        managers.insert(id.to_string(), Arc::new(tokio::sync::Mutex::new(manager)));
    }

    pub async fn get(
        &self,
        id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<HypothesisManager>>> {
        let managers = self.managers.read().await;
        managers.get(id).cloned()
    }

    pub async fn remove(&self, id: &str) {
        let mut managers = self.managers.write().await;
        managers.remove(id);
    }
}
