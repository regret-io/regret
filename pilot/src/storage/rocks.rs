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

        // Seek to target
        let iter = db.iterator(IteratorMode::From(target.as_bytes(), Direction::Forward));
        // Check exact match first
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                break;
            }
            if key_str == target {
                if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                    return Ok(Some((key_str, entry)));
                }
            }
            break;
        }

        // No exact match — seek backwards
        let iter = db.iterator(IteratorMode::From(target.as_bytes(), Direction::Reverse));
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                break;
            }
            if key_str.as_str() <= target {
                if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                    return Ok(Some((key_str, entry)));
                }
            }
            break;
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
                break;
            }
            if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                return Ok(Some((key_str, entry)));
            }
            break;
        }
        Ok(None)
    }

    /// Lower: largest key < target within prefix.
    pub fn ref_lower(&self, prefix: &str, target: &str) -> Result<Option<(String, RefEntry)>> {
        let db = self.db.lock().unwrap();
        let iter = db.iterator(IteratorMode::From(target.as_bytes(), Direction::Reverse));
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key).to_string();
            if !key_str.starts_with(prefix) {
                break;
            }
            if key_str.as_str() < target {
                if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                    return Ok(Some((key_str, entry)));
                }
            }
            // Skip exact match, keep going
            continue;
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
                break;
            }
            if key_str.as_str() > target {
                if let Ok(entry) = serde_json::from_slice::<RefEntry>(&value) {
                    return Ok(Some((key_str, entry)));
                }
            }
            // Skip exact match, continue to next
            continue;
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
}
