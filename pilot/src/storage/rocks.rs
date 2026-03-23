use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use rocksdb::{DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, ReadOptions};
use serde::{Deserialize, Serialize};

type RocksDB = DBWithThreadMode<MultiThreaded>;

/// Reference entry stored in RocksDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefEntry {
    pub value: String,
    pub version: u64,
    #[serde(default)]
    pub ephemeral: bool,
}

#[derive(Clone)]
pub struct RocksStore {
    db: Arc<Mutex<RocksDB>>,
}

impl RocksStore {
    pub fn new(path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = RocksDB::open(&opts, path).context("failed to open RocksDB")?;
        Ok(Self { db: Arc::new(Mutex::new(db)) })
    }

    // ── Reference state (single default CF, key = full path) ──

    /// Put a reference entry. Key is the full path (e.g. `/ref/hyp-019d/run-019d/000001`).
    pub fn ref_put(&self, key: &str, entry: &RefEntry) -> Result<()> {
        let data = serde_json::to_vec(entry)?;
        let db = self.db.lock().unwrap();
        db.put(key.as_bytes(), &data)?;
        Ok(())
    }

    /// Get a reference entry by exact key.
    pub fn ref_get(&self, key: &str) -> Result<Option<RefEntry>> {
        let db = self.db.lock().unwrap();
        match db.get(key.as_bytes())? {
            Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    /// Delete a reference entry.
    pub fn ref_delete(&self, key: &str) -> Result<()> {
        let db = self.db.lock().unwrap();
        db.delete(key.as_bytes())?;
        Ok(())
    }

    /// Range scan: return all keys in [start, end) within a prefix, sorted.
    pub fn ref_range_keys(&self, prefix: &str, start: &str, end: &str) -> Result<Vec<String>> {
        let db = self.db.lock().unwrap();
        let mut keys = Vec::new();

        let mut opts = ReadOptions::default();
        opts.set_iterate_lower_bound(start.as_bytes());
        opts.set_iterate_upper_bound(end.as_bytes());

        let iter = db.iterator_opt(IteratorMode::From(start.as_bytes(), Direction::Forward), opts);
        for item in iter {
            let (key, _) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                break;
            }
            keys.push(key_str);
        }
        Ok(keys)
    }

    /// Range scan: return all (key, entry) pairs in [start, end) within a prefix, sorted.
    pub fn ref_range_scan(&self, prefix: &str, start: &str, end: &str) -> Result<Vec<(String, RefEntry)>> {
        let db = self.db.lock().unwrap();
        let mut results = Vec::new();

        let mut opts = ReadOptions::default();
        opts.set_iterate_lower_bound(start.as_bytes());
        opts.set_iterate_upper_bound(end.as_bytes());

        let iter = db.iterator_opt(IteratorMode::From(start.as_bytes(), Direction::Forward), opts);
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                break;
            }
            if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                results.push((key_str, entry));
            }
        }
        Ok(results)
    }

    /// Floor: largest key ≤ target within prefix.
    pub fn ref_floor(&self, prefix: &str, target: &str) -> Result<Option<(String, RefEntry)>> {
        let db = self.db.lock().unwrap();
        // Seek forward to find exact match or first key > target
        // Then go back one step if no exact match
        let iter = db.iterator(IteratorMode::From(target.as_bytes(), Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if key_str == target && key_str.starts_with(prefix) {
                // Exact match
                if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                    return Ok(Some((key_str, entry)));
                }
            }
            break; // No exact match, need to go backwards
        }
        // Seek backwards from target
        let iter = db.iterator(IteratorMode::From(target.as_bytes(), Direction::Reverse));
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                return Ok(None);
            }
            if key_str.as_str() <= target {
                if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                    return Ok(Some((key_str, entry)));
                }
            }
        }
        Ok(None)
    }

    /// Ceiling: smallest key ≥ target within prefix.
    pub fn ref_ceiling(&self, prefix: &str, target: &str) -> Result<Option<(String, RefEntry)>> {
        let db = self.db.lock().unwrap();
        let iter = db.iterator(IteratorMode::From(target.as_bytes(), Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                return Ok(None);
            }
            // First key >= target (IteratorMode::From guarantees this)
            if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                return Ok(Some((key_str, entry)));
            }
        }
        Ok(None)
    }

    /// Lower: largest key < target within prefix.
    pub fn ref_lower(&self, prefix: &str, target: &str) -> Result<Option<(String, RefEntry)>> {
        let db = self.db.lock().unwrap();
        // Reverse from target — first key might be == target, skip it
        let iter = db.iterator(IteratorMode::From(target.as_bytes(), Direction::Reverse));
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                return Ok(None);
            }
            if key_str.as_str() < target {
                if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                    return Ok(Some((key_str, entry)));
                }
            }
            // key == target, skip and continue backwards
        }
        Ok(None)
    }

    /// Higher: smallest key > target within prefix.
    pub fn ref_higher(&self, prefix: &str, target: &str) -> Result<Option<(String, RefEntry)>> {
        let db = self.db.lock().unwrap();
        let iter = db.iterator(IteratorMode::From(target.as_bytes(), Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                return Ok(None);
            }
            if key_str.as_str() > target {
                if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                    return Ok(Some((key_str, entry)));
                }
            }
            // key == target, skip and continue forward
        }
        Ok(None)
    }

    /// Delete all keys with a given prefix (e.g. `/ref/hyp-019d/run-019d/`).
    pub fn ref_clear(&self, prefix: &str) -> Result<()> {
        let db = self.db.lock().unwrap();
        let keys_to_delete: Vec<Vec<u8>> = {
            let iter = db.prefix_iterator(prefix.as_bytes());
            let mut keys = Vec::new();
            for item in iter {
                let (key, _) = item?;
                if !key.starts_with(prefix.as_bytes()) {
                    break;
                }
                keys.push(key.to_vec());
            }
            keys
        };
        for key in keys_to_delete {
            db.delete(&key)?;
        }
        Ok(())
    }

    /// Iterate all (key, entry) pairs with a given prefix, sorted.
    pub fn ref_scan_all(&self, prefix: &str) -> Result<Vec<(String, RefEntry)>> {
        let db = self.db.lock().unwrap();
        let mut results = Vec::new();
        let iter = db.prefix_iterator(prefix.as_bytes());
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                break;
            }
            if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                results.push((key_str, entry));
            }
        }
        Ok(results)
    }

    // ── Secondary index reference ──

    fn idx_key(index_name: &str, index_key: &str, primary_key: &str) -> String {
        format!("__idx__\0{}\0{}\0{}", index_name, index_key, primary_key)
    }

    fn idx_prefix(index_name: &str) -> String {
        format!("__idx__\0{}\0", index_name)
    }

    /// Store an index entry: indexName + indexKey → primaryKey.
    pub fn idx_put(&self, index_name: &str, index_key: &str, primary_key: &str) -> Result<()> {
        let key = Self::idx_key(index_name, index_key, primary_key);
        let db = self.db.lock().unwrap();
        db.put(key.as_bytes(), b"")?;
        Ok(())
    }

    /// Remove all index entries for a given primary key across all index keys.
    pub fn idx_delete_primary(&self, index_name: &str, primary_key: &str) -> Result<()> {
        let prefix = Self::idx_prefix(index_name);
        let db = self.db.lock().unwrap();
        let mut to_delete = Vec::new();
        let iter = db.prefix_iterator(prefix.as_bytes());
        for item in iter {
            let (key, _) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(&prefix) {
                break;
            }
            // key format: __idx__\0{indexName}\0{indexKey}\0{primaryKey}
            if key_str.ends_with(&format!("\0{}", primary_key)) {
                to_delete.push(key.to_vec());
            }
        }
        for k in to_delete {
            db.delete(&k)?;
        }
        Ok(())
    }

    /// List primary keys for an index key range [start, end).
    /// Returns primary keys sorted by primary key (matching Oxia's behavior).
    pub fn idx_list(&self, index_name: &str, start: &str, end: &str) -> Result<Vec<String>> {
        let prefix = Self::idx_prefix(index_name);
        let lower = format!("__idx__\0{}\0{}", index_name, start);
        let upper = format!("__idx__\0{}\0{}", index_name, end);
        let db = self.db.lock().unwrap();

        let mut opts = ReadOptions::default();
        opts.set_iterate_lower_bound(lower.as_bytes());
        opts.set_iterate_upper_bound(upper.as_bytes());

        let mut primary_keys = Vec::new();
        let iter = db.iterator_opt(IteratorMode::From(lower.as_bytes(), Direction::Forward), opts);
        for item in iter {
            let (key, _) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(&prefix) {
                break;
            }
            // Extract primary key from: __idx__\0{indexName}\0{indexKey}\0{primaryKey}
            let parts: Vec<&str> = key_str.splitn(4, '\0').collect();
            if parts.len() == 4 {
                primary_keys.push(parts[3].to_string());
            }
        }
        // Sort by primary key to match Oxia's list behavior
        primary_keys.sort();
        Ok(primary_keys)
    }

    /// Clear all index entries with a given index name.
    pub fn idx_clear(&self, index_name: &str) -> Result<()> {
        let prefix = Self::idx_prefix(index_name);
        let db = self.db.lock().unwrap();
        let keys_to_delete: Vec<Vec<u8>> = {
            let iter = db.prefix_iterator(prefix.as_bytes());
            let mut keys = Vec::new();
            for item in iter {
                let (key, _) = item?;
                if !key.starts_with(prefix.as_bytes()) {
                    break;
                }
                keys.push(key.to_vec());
            }
            keys
        };
        for key in keys_to_delete {
            db.delete(&key)?;
        }
        Ok(())
    }
}
