pub mod kv;
pub mod types;

use std::collections::HashMap;
use std::io::{self, Write};

use serde::{Deserialize, Serialize};
use types::OriginOp;

/// Declarative workload profile for origin dataset generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateParams {
    /// Generation profile: "basic-kv"
    pub profile: String,

    /// Total number of operations to generate.
    pub ops: usize,

    /// Key space configuration.
    #[serde(default)]
    pub key_space: KeySpaceConfig,

    /// Operation mix — weights per operation type (normalized automatically).
    /// Missing op types default to 0.
    #[serde(default = "default_workload")]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySpaceConfig {
    /// Key prefix.
    #[serde(default = "default_key_prefix")]
    pub prefix: String,

    /// Number of distinct keys.
    #[serde(default = "default_key_count")]
    pub count: usize,

    /// Zero-padding width for numeric suffix.
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
    /// Minimum value length.
    #[serde(default = "default_value_min_len")]
    pub min_len: usize,

    /// Maximum value length.
    #[serde(default = "default_value_max_len")]
    pub max_len: usize,

    /// Value prefix.
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

fn default_key_prefix() -> String { "user:".to_string() }
fn default_key_count() -> usize { 100 }
fn default_key_padding() -> usize { 6 }
fn default_value_min_len() -> usize { 4 }
fn default_value_max_len() -> usize { 12 }
fn default_value_prefix() -> String { "v-".to_string() }
fn default_fence_every() -> usize { 50 }
fn default_seed() -> u64 { 42 }

fn default_workload() -> HashMap<String, f64> {
    HashMap::from([
        ("put".to_string(), 0.55),
        ("delete".to_string(), 0.15),
        ("delete_range".to_string(), 0.05),
        ("get".to_string(), 0.15),
        ("range_scan".to_string(), 0.05),
        ("list".to_string(), 0.05),
    ])
}

impl Default for GenerateParams {
    fn default() -> Self {
        Self {
            profile: "basic-kv".to_string(),
            ops: 1000,
            key_space: KeySpaceConfig::default(),
            workload: default_workload(),
            fence_every: default_fence_every(),
            seed: default_seed(),
            value: ValueConfig::default(),
        }
    }
}

impl GenerateParams {
    /// Resolve the normalized weight for a given op type.
    /// Returns 0.0 if the op type is not in the workload.
    pub fn weight(&self, op_type: &str) -> f64 {
        let total: f64 = self.workload.values().sum();
        if total <= 0.0 {
            return 0.0;
        }
        self.workload.get(op_type).copied().unwrap_or(0.0) / total
    }

    /// Split workload into write weights and read weights (normalized within each group).
    pub fn write_weights(&self) -> Vec<(&str, f64)> {
        let write_ops = ["put", "delete", "delete_range", "cas"];
        let entries: Vec<(&str, f64)> = write_ops
            .iter()
            .filter_map(|op| self.workload.get(*op).map(|w| (*op, *w)))
            .collect();
        let total: f64 = entries.iter().map(|(_, w)| w).sum();
        if total <= 0.0 {
            return vec![("put", 1.0)];
        }
        entries.into_iter().map(|(op, w)| (op, w / total)).collect()
    }

    pub fn read_weights(&self) -> Vec<(&str, f64)> {
        let read_ops = ["get", "range_scan", "list"];
        let entries: Vec<(&str, f64)> = read_ops
            .iter()
            .filter_map(|op| self.workload.get(*op).map(|w| (*op, *w)))
            .collect();
        let total: f64 = entries.iter().map(|(_, w)| w).sum();
        if total <= 0.0 {
            return vec![("get", 1.0)];
        }
        entries.into_iter().map(|(op, w)| (op, w / total)).collect()
    }

    /// Fraction of total ops that are reads.
    pub fn read_fraction(&self) -> f64 {
        let read_ops = ["get", "range_scan", "list"];
        let total: f64 = self.workload.values().sum();
        if total <= 0.0 {
            return 0.0;
        }
        let read_total: f64 = read_ops
            .iter()
            .filter_map(|op| self.workload.get(*op))
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
    match params.profile.as_str() {
        "basic-kv" => kv::BasicKvGenerator::new(params).generate(),
        _ => panic!("unsupported profile: {}", params.profile),
    }
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
