use std::collections::HashMap;

/// Get a predefined generator's workload by name.
pub fn get_generator(name: &str) -> HashMap<String, f64> {
    match name {
        "basic-kv" => basic_kv(),
        "kv-cas" => kv_cas(),
        "kv-ephemeral" => kv_ephemeral(), // legacy alias
        "kv-ephemeral-notification" => kv_ephemeral_notification(),
        "kv-secondary-index" => kv_secondary_index(),
        "kv-sequence" => kv_sequence(),
        "kv-full" => kv_full(),
        _ => basic_kv(),
    }
}

/// List all built-in generators.
pub fn list_generators() -> Vec<GeneratorInfo> {
    vec![
        GeneratorInfo {
            name: "basic-kv",
            description: "Basic key-value operations: put, get, delete, delete_range, list, range_scan",
            rate: 100,
        },
        GeneratorInfo {
            name: "kv-cas",
            description: "Basic KV + compare-and-swap (versioned conditional writes)",
            rate: 100,
        },
        GeneratorInfo {
            name: "kv-ephemeral",
            description: "Basic KV + ephemeral records (auto-deleted on session expiry)",
            rate: 100,
        },
        GeneratorInfo {
            name: "kv-ephemeral-notification",
            description: "Ephemeral lifecycle + notification delivery verification",
            rate: 100,
        },
        GeneratorInfo {
            name: "kv-secondary-index",
            description: "Basic KV + secondary index operations (indexed put/get/list/range_scan)",
            rate: 100,
        },
        GeneratorInfo {
            name: "kv-sequence",
            description: "Sequence key puts with server-assigned monotonic suffixes + reads",
            rate: 100,
        },
        GeneratorInfo {
            name: "kv-full",
            description: "All Oxia operations combined: basic KV + CAS + ephemeral + indexes + sequences",
            rate: 100,
        },
    ]
}

pub struct GeneratorInfo {
    pub name: &'static str,
    pub description: &'static str,
    /// Target ops/sec rate. 0 = unlimited.
    pub rate: u32,
}

// ── Generator definitions ──

fn basic_kv() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.20),
        ("get".into(), 0.15),
        ("get_floor".into(), 0.05),
        ("get_ceiling".into(), 0.05),
        ("get_lower".into(), 0.05),
        ("get_higher".into(), 0.05),
        ("delete".into(), 0.08),
        ("delete_range".into(), 0.02),
        ("range_scan".into(), 0.15),
        ("list".into(), 0.10),
    ])
}

fn kv_cas() -> HashMap<String, f64> {
    HashMap::from([
        ("cas".into(), 0.40),
        ("cas_stale".into(), 0.20),
        ("get".into(), 0.20),
        ("put".into(), 0.10),
        ("range_scan".into(), 0.05),
        ("list".into(), 0.05),
    ])
}

fn kv_ephemeral() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.30), ("ephemeral_put".into(), 0.25), ("delete".into(), 0.10),
        ("get".into(), 0.20), ("range_scan".into(), 0.05), ("list".into(), 0.10),
    ])
}

fn kv_ephemeral_notification() -> HashMap<String, f64> {
    // Mutations trigger notifications:
    //   ephemeral_put (new) → KEY_CREATED
    //   ephemeral_put (existing) → KEY_MODIFIED
    //   delete → KEY_DELETED
    //   delete_range → KEY_RANGE_DELETED
    //   session_restart → KEY_DELETED for all ephemeral keys
    // The generator injects watch_start, session_restart, get_notifications
    // at cycle boundaries
    HashMap::from([
        ("ephemeral_put".into(), 0.30),
        ("delete".into(), 0.10),
        ("delete_range".into(), 0.05),
        ("get".into(), 0.20),
        ("list".into(), 0.10),
        ("get_notifications".into(), 0.10),
        ("session_restart".into(), 0.05),
        ("range_scan".into(), 0.10),
    ])
}

fn kv_secondary_index() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.15), ("indexed_put".into(), 0.25), ("delete".into(), 0.05),
        ("get".into(), 0.10), ("indexed_get".into(), 0.15), ("indexed_list".into(), 0.10),
        ("indexed_range_scan".into(), 0.10), ("list".into(), 0.05), ("range_scan".into(), 0.05),
    ])
}

fn kv_sequence() -> HashMap<String, f64> {
    HashMap::from([
        ("sequence_put".into(), 0.50), ("put".into(), 0.10),
        ("get".into(), 0.20), ("list".into(), 0.10), ("range_scan".into(), 0.10),
    ])
}

fn kv_full() -> HashMap<String, f64> {
    HashMap::from([
        ("put".into(), 0.20), ("cas".into(), 0.10), ("ephemeral_put".into(), 0.10),
        ("indexed_put".into(), 0.10), ("sequence_put".into(), 0.05),
        ("delete".into(), 0.08), ("delete_range".into(), 0.02),
        ("get".into(), 0.12), ("indexed_get".into(), 0.08),
        ("range_scan".into(), 0.05), ("indexed_range_scan".into(), 0.03),
        ("list".into(), 0.04), ("indexed_list".into(), 0.03),
    ])
}
