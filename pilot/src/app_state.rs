use crate::engine::ManagerRegistry;
use crate::scheduler::k8s::K8sScheduler;
use crate::storage::files::FileStore;
use crate::storage::rocks::RocksStore;
use crate::storage::sqlite::SqliteStore;

#[derive(Clone)]
pub struct AppState {
    pub sqlite: SqliteStore,
    pub rocks: RocksStore,
    pub files: FileStore,
    pub managers: ManagerRegistry,
    pub scheduler: Option<K8sScheduler>,
}
