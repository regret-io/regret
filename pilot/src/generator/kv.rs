use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::GenerateParams;
use super::types::OriginOp;

/// Generates a basic-kv origin dataset from a workload profile.
pub struct BasicKvGenerator {
    rng: StdRng,
    params: GenerateParams,
    /// Internal version tracking for valid CAS ops.
    versions: BTreeMap<String, u64>,
    op_counter: usize,
}

impl BasicKvGenerator {
    pub fn new(params: &GenerateParams) -> Self {
        Self {
            rng: StdRng::seed_from_u64(params.seed),
            params: params.clone(),
            versions: BTreeMap::new(),
            op_counter: 0,
        }
    }

    pub fn generate(mut self) -> Vec<OriginOp> {
        let mut ops = Vec::new();
        let mut writes_since_fence = 0;

        while self.op_counter < self.params.ops {
            // Time for a fence? Emit fence + reads
            if writes_since_fence >= self.params.fence_every && self.op_counter < self.params.ops {
                ops.push(OriginOp::fence());
                writes_since_fence = 0;

                let read_count = self.compute_read_count();
                for _ in 0..read_count {
                    if self.op_counter >= self.params.ops {
                        break;
                    }
                    ops.push(self.gen_read_op());
                }
                continue;
            }

            // Emit a write op
            ops.push(self.gen_write_op());
            writes_since_fence += 1;
        }

        // Final fence + trailing reads
        if writes_since_fence > 0 {
            ops.push(OriginOp::fence());

            let read_count = self.compute_read_count().min(5);
            for _ in 0..read_count {
                if self.versions.is_empty() {
                    break;
                }
                let id = self.next_id();
                ops.push(self.gen_read_op_with_id(id));
            }
        }

        ops
    }

    fn compute_read_count(&self) -> usize {
        let read_frac = self.params.read_fraction();
        if read_frac <= 0.0 {
            return 0;
        }
        let total = self.params.fence_every as f64 * read_frac / (1.0 - read_frac);
        (total as usize).max(1)
    }

    // --- Write operations ---

    fn gen_write_op(&mut self) -> OriginOp {
        let id = self.next_id();
        let roll: f64 = self.rng.r#gen();

        let weights = self.params.write_weights();
        let mut cumulative = 0.0;
        for (op_type, weight) in &weights {
            cumulative += weight;
            if roll < cumulative {
                return match *op_type {
                    "put" => self.gen_put(id),
                    "delete" => self.gen_delete(id),
                    "delete_range" => self.gen_delete_range(id),
                    "cas" => self.gen_cas(id),
                    _ => self.gen_put(id),
                };
            }
        }
        // Fallback
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
        let start = format!("{}{:0width$}", ks.prefix, lo, width = ks.padding);
        let end = format!("{}{:0width$}", ks.prefix, hi, width = ks.padding);

        let to_remove: Vec<String> = self
            .versions
            .range(start.clone()..end.clone())
            .map(|(k, _)| k.clone())
            .collect();
        for k in to_remove {
            self.versions.remove(&k);
        }

        OriginOp::delete_range(id, start, end)
    }

    fn gen_cas(&mut self, id: String) -> OriginOp {
        if let Some((key, &version)) = self.random_existing_key() {
            let key = key.clone();
            let new_value = self.random_value();
            self.versions.insert(key.clone(), version + 1);
            OriginOp::cas(id, key, version, new_value)
        } else {
            // No existing keys — fall back to put
            self.gen_put(id)
        }
    }

    // --- Read operations ---

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
                return match *op_type {
                    "get" => self.gen_get(id),
                    "range_scan" => self.gen_range_scan(id),
                    "list" => self.gen_list(id),
                    _ => self.gen_get(id),
                };
            }
        }
        self.gen_get(id)
    }

    fn gen_get(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        OriginOp::get(id, key)
    }

    fn gen_range_scan(&mut self, id: String) -> OriginOp {
        let ks = &self.params.key_space;
        let a = self.rng.gen_range(0..ks.count);
        let b = self.rng.gen_range(0..ks.count);
        let lo = a.min(b);
        let hi = a.max(b) + 1;
        OriginOp::range_scan(
            id,
            format!("{}{:0width$}", ks.prefix, lo, width = ks.padding),
            format!("{}{:0width$}", ks.prefix, hi, width = ks.padding),
        )
    }

    fn gen_list(&mut self, id: String) -> OriginOp {
        OriginOp::list(id, self.params.key_space.prefix.clone())
    }

    // --- Helpers ---

    fn next_id(&mut self) -> String {
        self.op_counter += 1;
        format!("op-{:04}", self.op_counter)
    }

    fn random_key(&mut self) -> String {
        let ks = &self.params.key_space;
        let n = self.rng.gen_range(0..ks.count);
        format!("{}{:0width$}", ks.prefix, n, width = ks.padding)
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
        if self.versions.is_empty() {
            return None;
        }
        let keys: Vec<&String> = self.versions.keys().collect();
        let idx = self.rng.gen_range(0..keys.len());
        let key = keys[idx];
        Some((key, self.versions.get(key).unwrap()))
    }
}
