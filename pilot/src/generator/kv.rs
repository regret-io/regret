use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::storage::rocks::RocksStore;

use super::GenerateParams;
use super::types::OriginOp;

/// Key padding width — 20 digits for sorted numeric keys.
const KEY_PADDING: usize = 20;

/// Generates origin datasets for all KV-family profiles.
pub struct BasicKvGenerator {
    rng: StdRng,
    params: GenerateParams,
    /// Optional RocksStore for CAS version lookups.
    rocks: Option<RocksStore>,
    /// Tracks index keys for secondary index reads.
    index_keys: Vec<String>,
    /// Tracks sequence prefixes for sequence reads.
    sequence_prefixes: Vec<String>,
    op_counter: usize,
    /// Next key index to put during warmup phase.
    warmup_cursor: usize,
}

impl BasicKvGenerator {
    pub fn new(params: &GenerateParams) -> Self {
        Self {
            rng: StdRng::seed_from_u64(params.seed),
            params: params.clone(),
            rocks: None,
            index_keys: Vec::new(),
            sequence_prefixes: Vec::new(),
            op_counter: 0,
            warmup_cursor: 0,
        }
    }

    pub fn with_rocks(mut self, rocks: RocksStore) -> Self {
        self.rocks = Some(rocks);
        self
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
    /// During warmup, emits puts for all keys in the key space first.
    fn gen_op(&mut self) -> OriginOp {
        // Warmup: put every key in the key space before random ops
        if !self.params.skip_warmup && self.warmup_cursor < self.params.key_space.count {
            let id = self.next_id();
            let key = self.format_key(self.warmup_cursor);
            let value = self.random_value();
            self.warmup_cursor += 1;
            return OriginOp::put(id, key, value);
        }

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
                    "get_floor" => self.gen_get_floor(id),
                    "get_ceiling" => self.gen_get_ceiling(id),
                    "get_lower" => self.gen_get_lower(id),
                    "get_higher" => self.gen_get_higher(id),
                    "delete" => self.gen_delete(id),
                    "delete_range" => self.gen_delete_range(id),
                    "list" => self.gen_list(id),
                    "range_scan" => self.gen_range_scan(id),

                    "ephemeral_put" => self.gen_ephemeral_put(id),
                    "indexed_put" => self.gen_indexed_put(id),
                    "indexed_get" => self.gen_indexed_get(id),
                    "indexed_list" => self.gen_indexed_list(id),
                    "indexed_range_scan" => self.gen_indexed_range_scan(id),
                    "sequence_put" => self.gen_sequence_put(id),
                    "watch_start" => self.gen_watch_start(id),
                    "session_restart" => self.gen_session_restart(id),
                    "get_notifications" => self.gen_get_notifications(id),
                    _ => self.gen_put(id),
                };
            }
        }
        self.gen_put(id)
    }

    // ── Write operations ──

    fn gen_put(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        let value = self.random_value();
        OriginOp::put(id, key, value)
    }

    fn gen_delete(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        OriginOp::delete(id, key)
    }

    fn gen_delete_range(&mut self, id: String) -> OriginOp {
        let ks = &self.params.key_space;
        let lo = self.rng.gen_range(0..ks.count);
        // Cap range to at most 5 keys
        let span = self.rng.gen_range(1..=5.min(ks.count));
        let hi = (lo + span).min(ks.count);
        OriginOp::delete_range(id, self.format_key(lo), self.format_key(hi))
    }

    fn gen_ephemeral_put(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        let value = self.random_value();
        OriginOp::ephemeral_put(id, key, value)
    }

    fn gen_indexed_put(&mut self, id: String) -> OriginOp {
        let key = self.random_key();
        let value = self.random_value();
        let idx = &self.params.index;
        let index_key_num = self.rng.gen_range(0..idx.key_count);
        let index_key = format!("idx-{index_key_num:04}");

        if !self.index_keys.contains(&index_key) {
            self.index_keys.push(index_key.clone());
        }

        OriginOp::indexed_put(id, key, value, idx.name.clone(), index_key)
    }

    fn gen_sequence_put(&mut self, id: String) -> OriginOp {
        let ks = &self.params.key_space;
        let prefix = format!("{}seq/", ks.prefix);
        let value = self.random_value();

        if !self.sequence_prefixes.contains(&prefix) {
            self.sequence_prefixes.push(prefix.clone());
        }

        OriginOp::sequence_put(id, prefix, value, 1)
    }

    // ── Read operations ──

    fn gen_get(&mut self, id: String) -> OriginOp {
        OriginOp::get(id, self.random_key())
    }

    fn gen_get_floor(&mut self, id: String) -> OriginOp {
        OriginOp::get_floor(id, self.random_key())
    }

    fn gen_get_ceiling(&mut self, id: String) -> OriginOp {
        OriginOp::get_ceiling(id, self.random_key())
    }

    fn gen_get_lower(&mut self, id: String) -> OriginOp {
        OriginOp::get_lower(id, self.random_key())
    }

    fn gen_get_higher(&mut self, id: String) -> OriginOp {
        OriginOp::get_higher(id, self.random_key())
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
        let ks = &self.params.key_space;
        // List all keys in the run prefix
        OriginOp::list(id, self.format_key(0), format!("{}~", ks.prefix))
    }

    fn gen_indexed_get(&mut self, id: String) -> OriginOp {
        let idx = &self.params.index;
        if self.index_keys.is_empty() {
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

    // ── Session & notification ops ──

    fn gen_watch_start(&mut self, id: String) -> OriginOp {
        OriginOp::watch_start(id, self.params.key_space.prefix.clone())
    }

    fn gen_session_restart(&mut self, id: String) -> OriginOp {
        OriginOp::session_restart(id)
    }

    fn gen_get_notifications(&mut self, id: String) -> OriginOp {
        OriginOp::get_notifications(id)
    }

    // ── Helpers ──

    fn next_id(&mut self) -> String {
        self.op_counter += 1;
        format!("op-{:04}", self.op_counter)
    }

    /// Format a key with 20-digit padding under the run prefix.
    fn format_key(&self, n: usize) -> String {
        format!("{}{:0width$}", self.params.key_space.prefix, n, width = KEY_PADDING)
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
}
