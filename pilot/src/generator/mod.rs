pub mod kv;
pub mod profiles;
pub mod types;

use std::collections::HashMap;
use std::io::{self, Write};

use serde::{Deserialize, Serialize};
use types::OriginOp;

/// Declarative workload profile for origin dataset generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateParams {
    /// Generation profile name. Use a predefined profile ("basic-kv", "kv-cas",
    /// "kv-ephemeral", "kv-secondary-index", "kv-sequence", "kv-full")
    /// or "custom" with explicit workload weights.
    pub profile: String,

    /// Total number of operations to generate.
    pub ops: usize,

    /// Key space configuration.
    #[serde(default)]
    pub key_space: KeySpaceConfig,

    /// Operation mix — weights per operation type (normalized automatically).
    /// If empty, loaded from the predefined profile.
    #[serde(default)]
    pub workload: HashMap<String, f64>,

    /// Insert a fence every ~N write ops. Reads are emitted after each fence.
    #[serde(default = "default_fence_every")]
    pub fence_every: usize,

    /// RNG seed for reproducibility.
    #[serde(default = "default_seed")]
    pub seed: u64,

    /// Value generation configuration.
    #[serde(default)]
    pub value: ValueConfig,

    /// Secondary index configuration (for kv-secondary-index profile).
    #[serde(default)]
    pub index: IndexConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySpaceConfig {
    #[serde(default = "default_key_prefix")]
    pub prefix: String,
    #[serde(default = "default_key_count")]
    pub count: usize,
    #[serde(default = "default_key_padding")]
    pub padding: usize,
}

impl Default for KeySpaceConfig {
    fn default() -> Self {
        Self {
            prefix: default_key_prefix(),
            count: default_key_count(),
            padding: default_key_padding(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueConfig {
    #[serde(default = "default_value_min_len")]
    pub min_len: usize,
    #[serde(default = "default_value_max_len")]
    pub max_len: usize,
    #[serde(default = "default_value_prefix")]
    pub prefix: String,
}

impl Default for ValueConfig {
    fn default() -> Self {
        Self {
            min_len: default_value_min_len(),
            max_len: default_value_max_len(),
            prefix: default_value_prefix(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    /// Index name for secondary index operations.
    #[serde(default = "default_index_name")]
    pub name: String,
    /// Number of distinct index keys.
    #[serde(default = "default_index_key_count")]
    pub key_count: usize,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            name: default_index_name(),
            key_count: default_index_key_count(),
        }
    }
}

fn default_key_prefix() -> String { "user:".to_string() }
fn default_key_count() -> usize { 100 }
fn default_key_padding() -> usize { 6 }
fn default_value_min_len() -> usize { 4 }
fn default_value_max_len() -> usize { 12 }
fn default_value_prefix() -> String { "v-".to_string() }
fn default_fence_every() -> usize { 50 }
fn default_seed() -> u64 { 42 }
fn default_index_name() -> String { "by-value".to_string() }
fn default_index_key_count() -> usize { 50 }

impl Default for GenerateParams {
    fn default() -> Self {
        Self {
            profile: "basic-kv".to_string(),
            ops: 1000,
            key_space: KeySpaceConfig::default(),
            workload: HashMap::new(), // loaded from profile
            fence_every: default_fence_every(),
            seed: default_seed(),
            value: ValueConfig::default(),
            index: IndexConfig::default(),
        }
    }
}

impl GenerateParams {
    /// Resolve the workload: if empty, load from predefined profile.
    pub fn resolved_workload(&self) -> HashMap<String, f64> {
        if !self.workload.is_empty() {
            return self.workload.clone();
        }
        profiles::get_profile(&self.profile)
    }

    /// Normalized write weights from the resolved workload.
    pub fn write_weights(&self) -> Vec<(String, f64)> {
        let workload = self.resolved_workload();
        let write_ops = [
            "put", "delete", "delete_range", "cas",
            "ephemeral_put", "indexed_put", "sequence_put",
        ];
        let entries: Vec<(String, f64)> = write_ops
            .iter()
            .filter_map(|op| workload.get(*op).map(|w| (op.to_string(), *w)))
            .collect();
        let total: f64 = entries.iter().map(|(_, w)| w).sum();
        if total <= 0.0 {
            return vec![("put".to_string(), 1.0)];
        }
        entries.into_iter().map(|(op, w)| (op, w / total)).collect()
    }

    /// Normalized read weights from the resolved workload.
    pub fn read_weights(&self) -> Vec<(String, f64)> {
        let workload = self.resolved_workload();
        let read_ops = [
            "get", "range_scan", "list",
            "indexed_get", "indexed_list", "indexed_range_scan",
        ];
        let entries: Vec<(String, f64)> = read_ops
            .iter()
            .filter_map(|op| workload.get(*op).map(|w| (op.to_string(), *w)))
            .collect();
        let total: f64 = entries.iter().map(|(_, w)| w).sum();
        if total <= 0.0 {
            return vec![("get".to_string(), 1.0)];
        }
        entries.into_iter().map(|(op, w)| (op, w / total)).collect()
    }

    /// Fraction of total ops that are reads.
    pub fn read_fraction(&self) -> f64 {
        let workload = self.resolved_workload();
        let read_ops = [
            "get", "range_scan", "list",
            "indexed_get", "indexed_list", "indexed_range_scan",
        ];
        let total: f64 = workload.values().sum();
        if total <= 0.0 {
            return 0.0;
        }
        let read_total: f64 = read_ops
            .iter()
            .filter_map(|op| workload.get(*op))
            .sum();
        read_total / total
    }
}

/// Generation statistics.
#[derive(Debug, Clone, Serialize)]
pub struct GenStats {
    pub total_ops: usize,
    pub total_fences: usize,
}

/// Generate origin dataset and return as a Vec.
pub fn generate(params: &GenerateParams) -> Vec<OriginOp> {
    kv::BasicKvGenerator::new(params).generate()
}

/// Generate origin dataset and write JSONL to the provided writer.
pub fn generate_to_writer(params: &GenerateParams, mut writer: impl Write) -> io::Result<GenStats> {
    let ops = generate(params);
    let mut total_ops = 0usize;
    let mut total_fences = 0usize;

    for op in &ops {
        serde_json::to_writer(&mut writer, op)?;
        writer.write_all(b"\n")?;

        match op {
            OriginOp::Fence(_) => total_fences += 1,
            OriginOp::Operation(_) => total_ops += 1,
        }
    }

    Ok(GenStats {
        total_ops,
        total_fences,
    })
}
