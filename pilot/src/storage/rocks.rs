use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};

type RocksDB = DBWithThreadMode<MultiThreaded>;

#[derive(Clone)]
pub struct RocksStore {
    db: Arc<Mutex<RocksDB>>,
}

impl RocksStore {
    pub fn new(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = RocksDB::list_cf(&opts, path).unwrap_or_default();
        let cf_descriptors: Vec<ColumnFamilyDescriptor> = if cfs.is_empty() {
            vec![ColumnFamilyDescriptor::new("default", Options::default())]
        } else {
            cfs.into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect()
        };

        let db = RocksDB::open_cf_descriptors(&opts, path, cf_descriptors)
            .context("failed to open RocksDB")?;

        Ok(Self { db: Arc::new(Mutex::new(db)) })
    }

    fn cf_name(hypothesis_id: &str) -> String {
        format!("hypothesis-{hypothesis_id}")
    }

    pub fn create_cf(&self, hypothesis_id: &str) -> Result<()> {
        let name = Self::cf_name(hypothesis_id);
        let db = self.db.lock().unwrap();
        db.create_cf(&name, &Options::default()).context(format!("failed to create CF {name}"))?;
        Ok(())
    }

    pub fn drop_cf(&self, hypothesis_id: &str) -> Result<()> {
        let name = Self::cf_name(hypothesis_id);
        let db = self.db.lock().unwrap();
        db.drop_cf(&name).context(format!("failed to drop CF {name}"))?;
        Ok(())
    }

    pub fn write_state(&self, hypothesis_id: &str, key: &str, data: &[u8]) -> Result<()> {
        let cf_name = Self::cf_name(hypothesis_id);
        let db = self.db.lock().unwrap();
        let cf = db.cf_handle(&cf_name).ok_or_else(|| anyhow::anyhow!("CF {cf_name} not found"))?;
        let rkey = format!("state:{key}");
        db.put_cf(&cf, rkey.as_bytes(), data)?;
        Ok(())
    }

    pub fn read_state(&self, hypothesis_id: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let cf_name = Self::cf_name(hypothesis_id);
        let db = self.db.lock().unwrap();
        let cf = db.cf_handle(&cf_name).ok_or_else(|| anyhow::anyhow!("CF {cf_name} not found"))?;
        let rkey = format!("state:{key}");
        Ok(db.get_cf(&cf, rkey.as_bytes())?)
    }

    pub fn clear_state(&self, hypothesis_id: &str) -> Result<()> {
        let cf_name = Self::cf_name(hypothesis_id);
        let db = self.db.lock().unwrap();
        let cf = db.cf_handle(&cf_name).ok_or_else(|| anyhow::anyhow!("CF {cf_name} not found"))?;

        let prefix = b"state:";
        let keys_to_delete: Vec<Vec<u8>> = {
            let iter = db.prefix_iterator_cf(&cf, prefix);
            let mut keys = Vec::new();
            for item in iter {
                let (key, _) = item?;
                if !key.starts_with(prefix) { break; }
                keys.push(key.to_vec());
            }
            keys
        };
        for key in keys_to_delete {
            db.delete_cf(&cf, &key)?;
        }
        Ok(())
    }
}
