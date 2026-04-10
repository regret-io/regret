use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::storage::rocks::RocksStore;

use super::GenerateParams;
use super::types::OriginOp;

/// Key padding width — 20 digits for sorted numeric keys.
const KEY_PADDING: usize = 20;

/// Generates CAS (compare-and-swap) correctness test workloads.
///
/// Instead of weighted random operations, this generator produces batches of
/// `cas` (put-with-version) operations with intentional key duplication to
/// create conflicts. The correctness invariant is:
///
///   For N ops targeting the same key+version in one batch,
///   exactly 1 succeeds (Ok) and N-1 fail (VersionMismatch).
///
/// Versions are read from the RocksDB reference store at batch-generation time.
/// The winner's returned version feeds the next batch.
pub struct CasGenerator {
    rng: StdRng,
    params: GenerateParams,
    rocks: Option<RocksStore>,
    op_counter: usize,
    warmup_cursor: usize,
    /// Probability that an op in a batch will reuse a key already in the batch,
    /// creating a conflict.
    conflict_probability: f64,
}

impl CasGenerator {
    pub fn new(params: &GenerateParams) -> Self {
        Self {
            rng: StdRng::seed_from_u64(params.seed),
            params: params.clone(),
            rocks: None,
            op_counter: 0,
            warmup_cursor: 0,
            conflict_probability: 0.4,
        }
    }

    pub fn with_rocks(mut self, rocks: RocksStore) -> Self {
        self.rocks = Some(rocks);
        self
    }

    /// Generate a batch of CAS operations with intentional conflicts.
    ///
    /// During warmup, emits plain puts to populate the key space.
    /// After warmup, all ops are CAS with version. Some keys are intentionally
    /// duplicated within the batch to test the conflict invariant.
    pub fn gen_batch(&mut self, count: usize) -> Vec<OriginOp> {
        let mut ops = Vec::new();
        let mut batch_keys: Vec<String> = Vec::new();

        for _ in 0..count {
            // Warmup: put every key in the key space before CAS ops
            if !self.params.skip_warmup && self.warmup_cursor < self.params.key_space.count {
                let id = self.next_id();
                let key = self.format_key(self.warmup_cursor);
                let value = self.random_value();
                self.warmup_cursor += 1;
                ops.push(OriginOp::put(id, key, value));
                continue;
            }

            let id = self.next_id();

            // Decide whether to create a conflict by reusing a key already in the batch
            let should_conflict = !batch_keys.is_empty()
                && self.rng.gen_bool(self.conflict_probability);

            let key = if should_conflict {
                let idx = self.rng.gen_range(0..batch_keys.len());
                batch_keys[idx].clone()
            } else {
                self.random_key()
            };

            let new_value = self.random_value();

            // Look up current version from RocksDB reference
            let current_version = if let Some(rocks) = &self.rocks {
                rocks.ref_get(&key).ok().flatten().map(|e| e.version).unwrap_or(0)
            } else {
                0
            };

            batch_keys.push(key.clone());
            ops.push(OriginOp::cas(id, key, current_version, new_value));
        }

        ops
    }

    // ── Helpers ──

    fn next_id(&mut self) -> String {
        self.op_counter += 1;
        format!("op-{:04}", self.op_counter)
    }

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
