use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::GenerateParams;
use super::types::OriginOp;

/// Generates origin datasets for all KV-family profiles.
pub struct BasicKvGenerator {
    rng: StdRng,
    params: GenerateParams,
    /// Internal version tracking for CAS ops.
    versions: BTreeMap<String, u64>,
    /// Tracks index keys for secondary index reads.
    index_keys: Vec<String>,
    /// Tracks sequence prefixes for sequence reads.
    sequence_prefixes: Vec<String>,
    op_counter: usize,
}

impl BasicKvGenerator {
    pub fn new(params: &GenerateParams) -> Self {
        Self {
            rng: StdRng::seed_from_u64(params.seed),
            params: params.clone(),
            versions: BTreeMap::new(),
            index_keys: Vec::new(),
            sequence_prefixes: Vec::new(),
            op_counter: 0,
        }
    }

    pub fn generate(mut self) -> Vec<OriginOp> {
        let mut ops = Vec::new();
        while self.op_counter < self.params.ops {
            ops.push(self.gen_op());
        }
        ops
    }

    /// Generate the next N ops. Can be called repeatedly for incremental generation.
    pub fn gen_batch(&mut self, count: usize) -> Vec<OriginOp> {
        let mut ops = Vec::new();
        for _ in 0..count {
            ops.push(self.gen_op());
        }
        ops
    }

    /// Generate a single op based on the full workload weights.
    fn gen_op(&mut self) -> OriginOp {
        let id = self.next_id();
        let roll: f64 = self.rng.r#gen();
        let workload = self.params.resolved_workload();
        let total: f64 = workload.values().sum();
        if total <= 0.0 {
            return self.gen_put(id);
        }

        let mut cumulative = 0.0;
        for (op_type, weight) in &workload {
            cumulative += weight / total;
            if roll < cumulative {
                return match op_type.as_str() {
                    "put" => self.gen_put(id),
                    "get" => self.gen_get(id),
                    "delete" => self.gen_delete(id),
                    "delete_range" => self.gen_delete_range(id),
                    "list" => self.gen_list(id),
                    "range_scan" => self.gen_range_scan(id),
                    "cas" => self.gen_cas(id),
                    "ephemeral_put" => self.gen_ephemeral_put(id),
                    "indexed_put" => self.gen_indexed_put(id),
                    "indexed_get" => self.gen_indexed_get(id),
                    "indexed_list" => self.gen_indexed_list(id),
                    "indexed_range_scan" => self.gen_indexed_range_scan(id),
                    "sequence_put" => self.gen_sequence_put(id),
                    _ => self.gen_put(id),
                };
            }
        }
        self.gen_put(id)
    }

    fn gen_write_op(&mut self) -> OriginOp {
        let id = self.next_id();
        let roll: f64 = self.rng.r#gen();
        let weights = self.params.write_weights();
        let mut cumulative = 0.0;

        for (op_type, weight) in &weights {
            cumulative += weight;
            if roll < cumulative {
                return match op_type.as_str() {
                    "put" => self.gen_put(id),
                    "delete" => self.gen_delete(id),
                    "delete_range" => self.gen_delete_range(id),
                    "cas" => self.gen_cas(id),
                    "ephemeral_put" => self.gen_ephemeral_put(id),
                    "indexed_put" => self.gen_indexed_put(id),
                    "sequence_put" => self.gen_sequence_put(id),
                    _ => self.gen_put(id),
                };
            }
        }
        self.gen_put(id)
    }

    fn gen_put(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        let value = self.random_value();
        let version = self.versions.entry(key.clone()).or_insert(0);
        *version += 1;
        OriginOp::put(id, key, value)
    }

    fn gen_delete(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        self.versions.remove(&key);
        OriginOp::delete(id, key)
    }

    fn gen_delete_range(&mut self, id: String) -> OriginOp {
        let ks = &self.params.key_space;
        let a = self.rng.gen_range(0..ks.count);
        let b = self.rng.gen_range(0..ks.count);
        let lo = a.min(b);
        let hi = a.max(b) + 1;
        let start = self.format_key(lo);
        let end = self.format_key(hi);

        let to_remove: Vec<String> = self.versions
            .range(start.clone()..end.clone())
            .map(|(k, _)| k.clone())
            .collect();
        for k in to_remove { self.versions.remove(&k); }

        OriginOp::delete_range(id, start, end)
    }

    /// Return all keys that this op touches (for conflict detection).
    fn keys_touched_by(&self, op: &OriginOp) -> Vec<String> {
        match op {
            OriginOp::Operation(o) => match &o.fields {
                super::types::OpFields::Put { key, .. } => vec![key.clone()],
                super::types::OpFields::Delete { key } => vec![key.clone()],
                super::types::OpFields::DeleteRange { .. } => {
                    // delete_range conflicts with any key in the segment,
                    // so return all known keys to force a fence
                    self.versions.keys().cloned().collect()
                }
                super::types::OpFields::Cas { key, .. } => vec![key.clone()],
                super::types::OpFields::EphemeralPut { key, .. } => vec![key.clone()],
                super::types::OpFields::IndexedPut { key, .. } => vec![key.clone()],
                super::types::OpFields::SequencePut { prefix, .. } => vec![prefix.clone()],
                _ => vec![],
            },
            _ => vec![],
        }
    }

    fn gen_cas(&mut self, id: String) -> OriginOp {
        if let Some((key, &version)) = self.random_existing_key() {
            let key = key.clone();
            let new_value = self.random_value();
            self.versions.insert(key.clone(), version + 1);
            OriginOp::cas(id, key, version, new_value)
        } else {
            self.gen_put(id)
        }
    }

    fn gen_ephemeral_put(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        let value = self.random_value();
        let version = self.versions.entry(key.clone()).or_insert(0);
        *version += 1;
        OriginOp::ephemeral_put(id, key, value)
    }

    fn gen_indexed_put(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        let value = self.random_value();
        let idx = &self.params.index;
        let index_key_num = self.rng.gen_range(0..idx.key_count);
        let index_key = format!("idx-{index_key_num:04}");

        // Track for later reads
        if !self.index_keys.contains(&index_key) {
            self.index_keys.push(index_key.clone());
        }

        let version = self.versions.entry(key.clone()).or_insert(0);
        *version += 1;
        OriginOp::indexed_put(id, key, value, idx.name.clone(), index_key)
    }

    fn gen_sequence_put(&mut self, id: String) -> OriginOp {
        let ks = &self.params.key_space;
        let prefix = format!("{}{}", ks.prefix, "seq/");
        let value = self.random_value();

        if !self.sequence_prefixes.contains(&prefix) {
            self.sequence_prefixes.push(prefix.clone());
        }

        OriginOp::sequence_put(id, prefix, value, 1)
    }

    // ── Read operations ──

    fn gen_read_op(&mut self) -> OriginOp {
        let id = self.next_id();
        self.gen_read_op_with_id(id)
    }

    fn gen_read_op_with_id(&mut self, id: String) -> OriginOp {
        let roll: f64 = self.rng.r#gen();
        let weights = self.params.read_weights();
        let mut cumulative = 0.0;

        for (op_type, weight) in &weights {
            cumulative += weight;
            if roll < cumulative {
                return match op_type.as_str() {
                    "get" => self.gen_get(id),
                    "range_scan" => self.gen_range_scan(id),
                    "list" => self.gen_list(id),
                    "indexed_get" => self.gen_indexed_get(id),
                    "indexed_list" => self.gen_indexed_list(id),
                    "indexed_range_scan" => self.gen_indexed_range_scan(id),
                    _ => self.gen_get(id),
                };
            }
        }
        self.gen_get(id)
    }

    fn gen_get(&mut self, id: String) -> OriginOp {
        OriginOp::get(id, self.random_key())
    }

    fn gen_range_scan(&mut self, id: String) -> OriginOp {
        let ks = &self.params.key_space;
        let a = self.rng.gen_range(0..ks.count);
        let b = self.rng.gen_range(0..ks.count);
        let lo = a.min(b);
        let hi = a.max(b) + 1;
        OriginOp::range_scan(id, self.format_key(lo), self.format_key(hi))
    }

    fn gen_list(&mut self, id: String) -> OriginOp {
        OriginOp::list(id, self.params.key_space.prefix.clone())
    }

    fn gen_indexed_get(&mut self, id: String) -> OriginOp {
        let idx = &self.params.index;
        if self.index_keys.is_empty() {
            // No indexed keys yet, fall back to regular get
            return self.gen_get(id);
        }
        let i = self.rng.gen_range(0..self.index_keys.len());
        let index_key = self.index_keys[i].clone();
        OriginOp::indexed_get(id, idx.name.clone(), index_key)
    }

    fn gen_indexed_list(&mut self, id: String) -> OriginOp {
        let idx = &self.params.index;
        let a = self.rng.gen_range(0..idx.key_count);
        let b = self.rng.gen_range(0..idx.key_count);
        let lo = a.min(b);
        let hi = a.max(b) + 1;
        OriginOp::indexed_list(id, idx.name.clone(), format!("idx-{lo:04}"), format!("idx-{hi:04}"))
    }

    fn gen_indexed_range_scan(&mut self, id: String) -> OriginOp {
        let idx = &self.params.index;
        let a = self.rng.gen_range(0..idx.key_count);
        let b = self.rng.gen_range(0..idx.key_count);
        let lo = a.min(b);
        let hi = a.max(b) + 1;
        OriginOp::indexed_range_scan(id, idx.name.clone(), format!("idx-{lo:04}"), format!("idx-{hi:04}"))
    }

    // ── Helpers ──

    fn next_id(&mut self) -> String {
        self.op_counter += 1;
        format!("op-{:04}", self.op_counter)
    }

    fn format_key(&self, n: usize) -> String {
        let ks = &self.params.key_space;
        format!("{}{:0width$}", ks.prefix, n, width = ks.padding)
    }

    fn random_key(&mut self) -> String {
        let n = self.rng.gen_range(0..self.params.key_space.count);
        self.format_key(n)
    }

    fn random_value(&mut self) -> String {
        let vc = &self.params.value;
        let charset = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let len = self.rng.gen_range(vc.min_len..=vc.max_len);
        let s: String = (0..len)
            .map(|_| {
                let idx = self.rng.gen_range(0..charset.len());
                charset[idx] as char
            })
            .collect();
        format!("{}{s}", vc.prefix)
    }

    fn random_existing_key(&mut self) -> Option<(&String, &u64)> {
        if self.versions.is_empty() { return None; }
        let keys: Vec<&String> = self.versions.keys().collect();
        let idx = self.rng.gen_range(0..keys.len());
        let key = keys[idx];
        Some((key, self.versions.get(key).unwrap()))
    }
}
