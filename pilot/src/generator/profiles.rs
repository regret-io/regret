use std::collections::HashMap;

/// Get a predefined workload profile by name.
/// Returns operation weights (not normalized — the generator normalizes them).
pub fn get_profile(name: &str) -> HashMap<String, f64> {
    match name {
        "basic-kv" => basic_kv(),
        "kv-cas" => kv_cas(),
        "kv-ephemeral" => kv_ephemeral(),
        "kv-secondary-index" => kv_secondary_index(),
        "kv-sequence" => kv_sequence(),
        "kv-full" => kv_full(),
        _ => basic_kv(), // fallback
    }
}

/// List all available profile names.
pub fn list_profiles() -> Vec<ProfileInfo> {
    vec![
        ProfileInfo {
            name: "basic-kv",
            description: "Basic key-value operations: put, get, delete, delete_range, list, range_scan",
        },
        ProfileInfo {
            name: "kv-cas",
            description: "Basic KV + compare-and-swap (versioned conditional writes)",
        },
        ProfileInfo {
            name: "kv-ephemeral",
            description: "Basic KV + ephemeral records (auto-deleted on session expiry)",
        },
        ProfileInfo {
            name: "kv-secondary-index",
            description: "Basic KV + secondary index operations (indexed put/get/list/range_scan)",
        },
        ProfileInfo {
            name: "kv-sequence",
            description: "Sequence key puts with server-assigned monotonic suffixes + reads",
        },
        ProfileInfo {
            name: "kv-full",
            description: "All Oxia operations combined: basic KV + CAS + ephemeral + indexes + sequences",
        },
    ]
}

pub struct ProfileInfo {
    pub name: &'static str,
    pub description: &'static str,
}

// ──────────────────────────────────────────────────
// Profile definitions
// ──────────────────────────────────────────────────

/// Basic KV: put, get, delete, delete_range, list, range_scan
fn basic_kv() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.55),
        ("delete".into(), 0.15),
        ("delete_range".into(), 0.05),
        ("get".into(), 0.15),
        ("range_scan".into(), 0.05),
        ("list".into(), 0.05),
    ])
}

/// Basic KV + CAS (versioned conditional writes)
fn kv_cas() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.35),
        ("cas".into(), 0.20),
        ("delete".into(), 0.10),
        ("delete_range".into(), 0.05),
        ("get".into(), 0.15),
        ("range_scan".into(), 0.05),
        ("list".into(), 0.05),
        // Intentionally no fence weight — fences are inserted by the generator
        // based on fence_every config, not as weighted ops
    ])
}

/// Basic KV + ephemeral records
fn kv_ephemeral() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.30),
        ("ephemeral_put".into(), 0.25),
        ("delete".into(), 0.10),
        ("get".into(), 0.20),
        ("range_scan".into(), 0.05),
        ("list".into(), 0.10),
    ])
}

/// Basic KV + secondary index operations
fn kv_secondary_index() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.15),
        ("indexed_put".into(), 0.25),
        ("delete".into(), 0.05),
        ("get".into(), 0.10),
        ("indexed_get".into(), 0.15),
        ("indexed_list".into(), 0.10),
        ("indexed_range_scan".into(), 0.10),
        ("list".into(), 0.05),
        ("range_scan".into(), 0.05),
    ])
}

/// Sequence key puts + reads
fn kv_sequence() -> HashMap<String, f64> {
    HashMap::from([
        ("sequence_put".into(), 0.50),
        ("put".into(), 0.10),
        ("get".into(), 0.20),
        ("list".into(), 0.10),
        ("range_scan".into(), 0.10),
    ])
}

/// Everything combined
fn kv_full() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.20),
        ("cas".into(), 0.10),
        ("ephemeral_put".into(), 0.10),
        ("indexed_put".into(), 0.10),
        ("sequence_put".into(), 0.05),
        ("delete".into(), 0.08),
        ("delete_range".into(), 0.02),
        ("get".into(), 0.12),
        ("indexed_get".into(), 0.08),
        ("range_scan".into(), 0.05),
        ("indexed_range_scan".into(), 0.03),
        ("list".into(), 0.04),
        ("indexed_list".into(), 0.03),
    ])
}
