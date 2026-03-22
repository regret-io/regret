use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::GenerateParams;
use super::types::OriginOp;

/// Generates a basic-kv origin dataset.
pub struct BasicKvGenerator {
    rng: StdRng,
    params: GenerateParams,
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
        let mut ops_since_fence = 0;

        while self.op_counter < self.params.ops {
            if ops_since_fence >= self.params.fence_every && self.op_counter < self.params.ops {
                ops.push(OriginOp::fence());
                ops_since_fence = 0;

                let read_count = self.compute_read_count();
                for _ in 0..read_count {
                    if self.op_counter >= self.params.ops {
                        break;
                    }
                    ops.push(self.gen_read_op());
                    ops_since_fence += 1;
                }
                continue;
            }

            ops.push(self.gen_write_op());
            ops_since_fence += 1;
        }

        if ops_since_fence > 0 {
            ops.push(OriginOp::fence());

            let read_count = (self.params.fence_every as f64 * self.params.read_ratio) as usize;
            let read_count = read_count.max(1).min(5);
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
        let total = self.params.fence_every as f64 * self.params.read_ratio
            / (1.0 - self.params.read_ratio);
        (total as usize).max(1)
    }

    fn gen_write_op(&mut self) -> OriginOp {
        let id = self.next_id();
        let roll: f64 = self.rng.r#gen();

        if roll < self.params.dr_ratio {
            self.gen_delete_range(id)
        } else if roll < self.params.dr_ratio + self.params.cas_ratio {
            self.gen_cas(id)
        } else if roll < self.params.dr_ratio + self.params.cas_ratio + 0.15 {
            self.gen_delete(id)
        } else {
            self.gen_put(id)
        }
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
        let a = self.rng.gen_range(0..self.params.keys);
        let b = self.rng.gen_range(0..self.params.keys);
        let lo = a.min(b);
        let hi = a.max(b) + 1;
        let start = format!("user:{lo:06}");
        let end = format!("user:{hi:06}");

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
            self.gen_put(id)
        }
    }

    fn gen_read_op(&mut self) -> OriginOp {
        let id = self.next_id();
        self.gen_read_op_with_id(id)
    }

    fn gen_read_op_with_id(&mut self, id: String) -> OriginOp {
        let roll: f64 = self.rng.r#gen();
        if roll < 0.5 {
            let key = self.random_key();
            OriginOp::get(id, key)
        } else if roll < 0.8 {
            let a = self.rng.gen_range(0..self.params.keys);
            let b = self.rng.gen_range(0..self.params.keys);
            let lo = a.min(b);
            let hi = a.max(b) + 1;
            OriginOp::range_scan(id, format!("user:{lo:06}"), format!("user:{hi:06}"))
        } else {
            OriginOp::list(id, "user:".to_string())
        }
    }

    fn next_id(&mut self) -> String {
        self.op_counter += 1;
        format!("op-{:04}", self.op_counter)
    }

    fn random_key(&mut self) -> String {
        let n = self.rng.gen_range(0..self.params.keys);
        format!("user:{n:06}")
    }

    fn random_value(&mut self) -> String {
        let charset = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let len = self.rng.gen_range(4..12);
        let s: String = (0..len)
            .map(|_| {
                let idx = self.rng.gen_range(0..charset.len());
                charset[idx] as char
            })
            .collect();
        format!("v-{s}")
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
