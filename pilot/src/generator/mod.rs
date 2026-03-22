pub mod kv;
pub mod types;

use std::io::{self, Write};

use types::OriginOp;

/// Parameters for origin dataset generation.
#[derive(Debug, Clone)]
pub struct GenerateParams {
    pub profile: String,
    pub ops: usize,
    pub keys: usize,
    pub read_ratio: f64,
    pub cas_ratio: f64,
    pub dr_ratio: f64,
    pub fence_every: usize,
    pub seed: u64,
}

impl Default for GenerateParams {
    fn default() -> Self {
        Self {
            profile: "basic-kv".to_string(),
            ops: 1000,
            keys: 100,
            read_ratio: 0.3,
            cas_ratio: 0.1,
            dr_ratio: 0.05,
            fence_every: 50,
            seed: 42,
        }
    }
}

/// Generation statistics.
#[derive(Debug, Clone)]
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
