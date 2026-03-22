use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
use regret_gen_core::GenerateParams;

#[derive(Parser)]
#[command(name = "regret-gen", about = "Generate origin datasets for regret testing")]
struct Args {
    /// Generation profile
    #[arg(long, default_value = "basic-kv")]
    profile: String,

    /// Total number of operations
    #[arg(long)]
    ops: usize,

    /// Key space size
    #[arg(long, default_value = "100")]
    keys: usize,

    /// Fraction of ops that are reads (0.0–1.0)
    #[arg(long, default_value = "0.3")]
    read_ratio: f64,

    /// Fraction of write ops that are CAS (0.0–1.0)
    #[arg(long, default_value = "0.1")]
    cas_ratio: f64,

    /// Fraction of write ops that are delete_range (0.0–1.0)
    #[arg(long, default_value = "0.05")]
    dr_ratio: f64,

    /// Insert fence approximately every N ops
    #[arg(long, default_value = "50")]
    fence_every: usize,

    /// RNG seed for reproducibility
    #[arg(long, default_value = "42")]
    seed: u64,

    /// Output file (defaults to stdout)
    #[arg(long, short)]
    output: Option<PathBuf>,
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    let params = GenerateParams {
        profile: args.profile,
        ops: args.ops,
        keys: args.keys,
        read_ratio: args.read_ratio,
        cas_ratio: args.cas_ratio,
        dr_ratio: args.dr_ratio,
        fence_every: args.fence_every,
        seed: args.seed,
    };

    let writer: Box<dyn Write> = match &args.output {
        Some(path) => Box::new(BufWriter::new(File::create(path)?)),
        None => Box::new(BufWriter::new(io::stdout().lock())),
    };

    let stats = regret_gen_core::generate_to_writer(&params, writer)?;

    eprintln!(
        "Generated {} ops, {} fences",
        stats.total_ops, stats.total_fences
    );

    Ok(())
}
