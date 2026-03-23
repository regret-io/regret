use crate::chaos::executor::ChaosRegistry;
use crate::engine::ManagerRegistry;
use crate::storage::files::FileStore;
use crate::storage::rocks::RocksStore;
use crate::storage::sqlite::SqliteStore;

#[derive(Clone)]
pub struct AppState {
    pub sqlite: SqliteStore,
    pub rocks: RocksStore,
    pub files: FileStore,
    pub managers: ManagerRegistry,
    pub chaos: ChaosRegistry,
    pub kube: kube::Client,
    pub namespace: String,
}
