use std::collections::HashMap;
use std::str::FromStr;

use tracing::warn;

use crate::storage::rocks::{RefEntry, RocksStore};
use crate::types::OpType;

use super::{
    AdapterBatchResponse, AdapterOpResult, CheckpointFailure, GetComparison, OpKind, OpStatus,
    Operation, RecordState, ReferenceModel, SafetyViolation, Tolerance,
};

pub struct BasicKvReference {
    rocks: RocksStore,
    hypothesis_id: String,
    run_id: String,
    /// Track last sequence key per prefix for monotonicity verification.
    last_sequence_keys: HashMap<String, String>,
}

impl BasicKvReference {
    pub fn new(rocks: RocksStore, hypothesis_id: String) -> Self {
        Self {
            rocks,
            hypothesis_id,
            run_id: String::new(),
            last_sequence_keys: HashMap::new(),
        }
    }

    /// The key prefix for this hypothesis + run in RocksDB.
    fn run_prefix(&self) -> String {
        format!("/ref/{}/{}/", self.hypothesis_id, self.run_id)
    }

    fn should_ignore_field(field: &str, tolerance: &Option<Tolerance>) -> bool {
        if let Some(t) = tolerance {
            t.structural.iter().any(|s| s.field == field && s.ignore)
        } else {
            false
        }
    }

    /// Apply a successful write to the reference state.
    /// Uses the adapter's returned version_id and key when available.
    fn apply_write(&self, op: &Operation, adapter_version: Option<u64>, adapter_key: Option<&str>) {
        match &op.kind {
            OpKind::Put { key, value } => {
                let version = adapter_version.unwrap_or_else(|| {
                    self.rocks.ref_get(key).ok().flatten().map(|e| e.version + 1).unwrap_or(1)
                });
                let _ = self.rocks.ref_put(key, &RefEntry { value: value.clone(), version, ephemeral: false });
            }
            OpKind::Delete { key } => {
                let _ = self.rocks.ref_delete(key);
            }
            OpKind::DeleteRange { start, end } => {
                let prefix = self.run_prefix();
                if let Ok(keys) = self.rocks.ref_range_keys(&prefix, start, end) {
                    for k in keys {
                        let _ = self.rocks.ref_delete(&k);
                    }
                }
            }
            OpKind::Cas { key, new_value, .. } => {
                let version = adapter_version.unwrap_or_else(|| {
                    self.rocks.ref_get(key).ok().flatten().map(|e| e.version + 1).unwrap_or(1)
                });
                let _ = self.rocks.ref_put(key, &RefEntry { value: new_value.clone(), version, ephemeral: false });
            }
            OpKind::EphemeralPut { key, value } => {
                let version = adapter_version.unwrap_or_else(|| {
                    self.rocks.ref_get(key).ok().flatten().map(|e| e.version + 1).unwrap_or(1)
                });
                let _ = self.rocks.ref_put(key, &RefEntry { value: value.clone(), version, ephemeral: true });
            }
            OpKind::IndexedPut { key, value, index_name, index_key } => {
                let version = adapter_version.unwrap_or_else(|| {
                    self.rocks.ref_get(key).ok().flatten().map(|e| e.version + 1).unwrap_or(1)
                });
                let _ = self.rocks.ref_put(key, &RefEntry { value: value.clone(), version, ephemeral: false });
                // Remove old index entry for this primary key (it may have changed index_key)
                let _ = self.rocks.idx_delete_primary(index_name, key);
                let _ = self.rocks.idx_put(index_name, index_key, key);
            }
            OpKind::SequencePut { .. } => {
                // Sequence keys are stored under a different PartitionKey in Oxia,
                // so they won't appear in regular list/range_scan. Don't store in
                // the main reference — verify monotonicity separately.
            }
            _ => {}
        }
    }

    /// Verify a read result against reference state.
    fn verify_read(
        &self,
        op: &Operation,
        result: &AdapterOpResult,
        tolerance: &Option<Tolerance>,
    ) -> Option<SafetyViolation> {
        let ignore_version = Self::should_ignore_field("version_id", tolerance);
        let parsed_status = OpStatus::from_str(&result.status).ok();

        match &op.kind {
            OpKind::Get { key, comparison } => {
                self.verify_get(op, key, comparison, result, parsed_status.as_ref(), ignore_version)
            }
            OpKind::List { start, end } => {
                self.verify_list(op, start, end, result)
            }
            OpKind::RangeScan { start, end } => {
                self.verify_range_scan(op, start, end, result, ignore_version)
            }
            OpKind::IndexedGet { index_name, index_key } => {
                self.verify_indexed_get(op, index_name, index_key, result, ignore_version)
            }
            OpKind::IndexedList { index_name, start, end } => {
                self.verify_indexed_list(op, index_name, start, end, result)
            }
            OpKind::IndexedRangeScan { index_name, start, end } => {
                self.verify_indexed_range_scan(op, index_name, start, end, result, ignore_version)
            }
            _ => None,
        }
    }

    fn verify_get(
        &self,
        op: &Operation,
        key: &str,
        comparison: &GetComparison,
        result: &AdapterOpResult,
        parsed_status: Option<&OpStatus>,
        ignore_version: bool,
    ) -> Option<SafetyViolation> {
        let prefix = self.run_prefix();

        // Find the expected record based on comparison type
        let expected: Option<(String, RefEntry)> = match comparison {
            GetComparison::Equal => {
                self.rocks.ref_get(key).ok().flatten().map(|e| (key.to_string(), e))
            }
            GetComparison::Floor => {
                self.rocks.ref_floor(&prefix, key).ok().flatten()
            }
            GetComparison::Ceiling => {
                self.rocks.ref_ceiling(&prefix, key).ok().flatten()
            }
            GetComparison::Lower => {
                self.rocks.ref_lower(&prefix, key).ok().flatten()
            }
            GetComparison::Higher => {
                self.rocks.ref_higher(&prefix, key).ok().flatten()
            }
        };

        match expected {
            Some((expected_key, entry)) => {
                // Ephemeral keys may be deleted at any time (session loss)
                // so not_found is always acceptable for them
                if entry.ephemeral && parsed_status == Some(&OpStatus::NotFound) {
                    // Session might have been lost — delete from reference to stay in sync
                    let _ = self.rocks.ref_delete(&expected_key);
                    return None;
                }

                // Should return ok with value
                if parsed_status != Some(&OpStatus::Ok) {
                    return Some(SafetyViolation {
                        op_id: op.id.clone(),
                        op: format!("get_{}", comparison_str(comparison)),
                        expected: format!("ok key={} value={}", expected_key, entry.value),
                        actual: format!("status={}", result.status),
                    });
                }
                // Check value
                if let Some(actual_value) = &result.value {
                    if *actual_value != entry.value {
                        return Some(SafetyViolation {
                            op_id: op.id.clone(),
                            op: format!("get_{}", comparison_str(comparison)),
                            expected: format!("value={}", entry.value),
                            actual: format!("value={}", actual_value),
                        });
                    }
                }
                // Check version (when not ignored, e.g. kv-cas)
                if !ignore_version {
                    if let Some(actual_version) = result.version_id {
                        if actual_version != entry.version {
                            return Some(SafetyViolation {
                                op_id: op.id.clone(),
                                op: format!("get_{}", comparison_str(comparison)),
                                expected: format!("version={}", entry.version),
                                actual: format!("version={}", actual_version),
                            });
                        }
                    }
                }
                None
            }
            None => {
                // Reference found no matching key within our prefix.
                if parsed_status == Some(&OpStatus::Ok) && result.value.is_some() {
                    // For comparison gets, check if the returned key is in our prefix.
                    // If outside prefix, the adapter found a key in a different namespace — not a violation.
                    if *comparison != GetComparison::Equal {
                        if let Some(returned_key) = &result.key {
                            if !returned_key.starts_with(&prefix) {
                                return None; // Key outside our prefix — valid
                            }
                        } else {
                            return None; // No returned key to verify
                        }
                    }
                    return Some(SafetyViolation {
                        op_id: op.id.clone(),
                        op: format!("get_{}", comparison_str(comparison)),
                        expected: "not_found".to_string(),
                        actual: format!("ok key={} value={}", result.key.as_deref().unwrap_or("?"), result.value.as_deref().unwrap_or("")),
                    });
                }
                None
            }
        }
    }

    fn verify_list(
        &self,
        op: &Operation,
        start: &str,
        end: &str,
        result: &AdapterOpResult,
    ) -> Option<SafetyViolation> {
        let prefix = self.run_prefix();
        let expected_keys = self.rocks.ref_range_keys(&prefix, start, end).unwrap_or_default();
        let actual_keys = result.keys.as_deref().unwrap_or(&[]);

        if expected_keys.len() != actual_keys.len() {
            return Some(SafetyViolation {
                op_id: op.id.clone(),
                op: "list".to_string(),
                expected: format!("{} keys", expected_keys.len()),
                actual: format!("{} keys", actual_keys.len()),
            });
        }

        // Verify exact sorted order
        for (i, (exp, act)) in expected_keys.iter().zip(actual_keys.iter()).enumerate() {
            if exp != act {
                return Some(SafetyViolation {
                    op_id: op.id.clone(),
                    op: "list".to_string(),
                    expected: format!("key[{}]={}", i, exp),
                    actual: format!("key[{}]={}", i, act),
                });
            }
        }
        None
    }

    fn verify_range_scan(
        &self,
        op: &Operation,
        start: &str,
        end: &str,
        result: &AdapterOpResult,
        ignore_version: bool,
    ) -> Option<SafetyViolation> {
        let prefix = self.run_prefix();
        let expected = self.rocks.ref_range_scan(&prefix, start, end).unwrap_or_default();
        let actual_records = result.records.as_deref().unwrap_or(&[]);

        if expected.len() != actual_records.len() {
            return Some(SafetyViolation {
                op_id: op.id.clone(),
                op: "range_scan".to_string(),
                expected: format!("{} records", expected.len()),
                actual: format!("{} records", actual_records.len()),
            });
        }

        // Verify exact sorted order + values
        for (i, ((exp_key, exp_entry), act)) in expected.iter().zip(actual_records.iter()).enumerate() {
            if *exp_key != act.key {
                return Some(SafetyViolation {
                    op_id: op.id.clone(),
                    op: "range_scan".to_string(),
                    expected: format!("record[{}].key={}", i, exp_key),
                    actual: format!("record[{}].key={}", i, act.key),
                });
            }
            if exp_entry.value != act.value {
                return Some(SafetyViolation {
                    op_id: op.id.clone(),
                    op: "range_scan".to_string(),
                    expected: format!("record[{}].value={}", i, exp_entry.value),
                    actual: format!("record[{}].value={}", i, act.value),
                });
            }
        }
        None
    }

    fn verify_indexed_get(
        &self,
        op: &Operation,
        index_name: &str,
        index_key: &str,
        result: &AdapterOpResult,
        ignore_version: bool,
    ) -> Option<SafetyViolation> {
        let parsed_status = OpStatus::from_str(&result.status).ok();
        // Look up primary keys from index
        let end_key = format!("{}\0", index_key);
        let primary_keys = self.rocks.idx_list(index_name, index_key, &end_key).unwrap_or_default();

        if primary_keys.is_empty() {
            // Reference has no index entry — adapter should return not_found
            if parsed_status == Some(OpStatus::Ok) && result.value.is_some() {
                return Some(SafetyViolation {
                    op_id: op.id.clone(),
                    op: "indexed_get".to_string(),
                    expected: "not_found".to_string(),
                    actual: format!("ok value={}", result.value.as_deref().unwrap_or("?")),
                });
            }
            return None;
        }

        // Get first primary key's value from reference
        let primary_key = &primary_keys[0];
        let entry = self.rocks.ref_get(primary_key).ok().flatten();

        match entry {
            Some(entry) => {
                if parsed_status != Some(OpStatus::Ok) {
                    return Some(SafetyViolation {
                        op_id: op.id.clone(),
                        op: "indexed_get".to_string(),
                        expected: format!("ok value={}", entry.value),
                        actual: format!("status={}", result.status),
                    });
                }
                if let Some(actual_value) = &result.value {
                    if *actual_value != entry.value {
                        return Some(SafetyViolation {
                            op_id: op.id.clone(),
                            op: "indexed_get".to_string(),
                            expected: format!("value={}", entry.value),
                            actual: format!("value={}", actual_value),
                        });
                    }
                }
                if !ignore_version {
                    if let Some(actual_version) = result.version_id {
                        if actual_version != entry.version {
                            return Some(SafetyViolation {
                                op_id: op.id.clone(),
                                op: "indexed_get".to_string(),
                                expected: format!("version={}", entry.version),
                                actual: format!("version={}", actual_version),
                            });
                        }
                    }
                }
                None
            }
            None => {
                // Primary key deleted but index not cleaned — tolerate not_found
                if parsed_status == Some(OpStatus::NotFound) {
                    return None;
                }
                None
            }
        }
    }

    fn verify_indexed_list(
        &self,
        op: &Operation,
        index_name: &str,
        start: &str,
        end: &str,
        result: &AdapterOpResult,
    ) -> Option<SafetyViolation> {
        let expected_keys = self.rocks.idx_list(index_name, start, end).unwrap_or_default();
        let actual_keys = result.keys.as_deref().unwrap_or(&[]);

        // All expected keys must be present in actual (subset check).
        // Adapter may return extra keys from stale index entries of previous runs.
        let actual_set: std::collections::HashSet<&str> = actual_keys.iter().map(|s| s.as_str()).collect();
        for exp in &expected_keys {
            if !actual_set.contains(exp.as_str()) {
                return Some(SafetyViolation {
                    op_id: op.id.clone(),
                    op: "indexed_list".to_string(),
                    expected: format!("key {} present", exp),
                    actual: format!("key {} missing from {} actual keys", exp, actual_keys.len()),
                });
            }
        }
        None
    }

    fn verify_indexed_range_scan(
        &self,
        op: &Operation,
        index_name: &str,
        start: &str,
        end: &str,
        result: &AdapterOpResult,
        ignore_version: bool,
    ) -> Option<SafetyViolation> {
        // Get primary keys from index, then fetch their values
        let primary_keys = self.rocks.idx_list(index_name, start, end).unwrap_or_default();
        let mut expected: HashMap<String, RefEntry> = HashMap::new();
        for pk in &primary_keys {
            if let Ok(Some(entry)) = self.rocks.ref_get(pk) {
                expected.insert(pk.clone(), entry);
            }
        }
        let actual_records = result.records.as_deref().unwrap_or(&[]);

        // All expected records must be present in actual (subset check).
        let actual_map: HashMap<&str, &crate::reference::RangeRecord> =
            actual_records.iter().map(|r| (r.key.as_str(), r)).collect();

        for (exp_key, exp_entry) in &expected {
            match actual_map.get(exp_key.as_str()) {
                Some(act) => {
                    if exp_entry.value != act.value {
                        return Some(SafetyViolation {
                            op_id: op.id.clone(),
                            op: "indexed_range_scan".to_string(),
                            expected: format!("key={} value={}", exp_key, exp_entry.value),
                            actual: format!("key={} value={}", act.key, act.value),
                        });
                    }
                }
                None => {
                    return Some(SafetyViolation {
                        op_id: op.id.clone(),
                        op: "indexed_range_scan".to_string(),
                        expected: format!("key {} present", exp_key),
                        actual: format!("key {} missing from {} actual records", exp_key, actual_records.len()),
                    });
                }
            }
        }
        None
    }

    /// Verify CAS batch invariant: for ops targeting the same key with the
    /// current version, exactly 1 must succeed (Ok) and the rest must fail
    /// (VersionMismatch). Stale-version ops must all fail.
    fn verify_cas_batch(
        &self,
        ops: &[Operation],
        response: &AdapterBatchResponse,
    ) -> Vec<SafetyViolation> {
        let mut failures = Vec::new();

        // Group CAS ops by key
        let mut cas_groups: HashMap<String, Vec<(usize, &Operation, &AdapterOpResult)>> = HashMap::new();
        for (i, (op, result)) in ops.iter().zip(response.results.iter()).enumerate() {
            if let OpKind::Cas { key, .. } = &op.kind {
                cas_groups.entry(key.clone()).or_default().push((i, op, result));
            }
        }

        for (key, group) in &cas_groups {
            // Read reference version ONCE per key (before applying any writes)
            let ref_version = self.rocks.ref_get(key).ok().flatten().map(|e| e.version).unwrap_or(0);

            // Partition into current-version ops vs stale-version ops
            let mut current_ops: Vec<&(usize, &Operation, &AdapterOpResult)> = Vec::new();
            let mut stale_ops: Vec<&(usize, &Operation, &AdapterOpResult)> = Vec::new();

            for entry in group {
                if let OpKind::Cas { expected_version_id, .. } = &entry.1.kind {
                    if *expected_version_id == ref_version {
                        current_ops.push(entry);
                    } else {
                        stale_ops.push(entry);
                    }
                }
            }

            // Stale ops: ALL must be VersionMismatch
            for (_, op, result) in &stale_ops {
                let status = OpStatus::from_str(&result.status).ok();
                if status != Some(OpStatus::VersionMismatch) {
                    if let OpKind::Cas { expected_version_id, .. } = &op.kind {
                        failures.push(SafetyViolation {
                            op_id: op.id.clone(),
                            op: "cas".to_string(),
                            expected: format!("version_mismatch (stale: expected={} ref={})", expected_version_id, ref_version),
                            actual: format!("status={}", result.status),
                        });
                    }
                }
            }

            // Current-version ops: exactly 1 Ok, rest VersionMismatch
            if !current_ops.is_empty() {
                let ok_count = current_ops.iter()
                    .filter(|(_, _, r)| OpStatus::from_str(&r.status).ok() == Some(OpStatus::Ok))
                    .count();
                let mismatch_count = current_ops.iter()
                    .filter(|(_, _, r)| OpStatus::from_str(&r.status).ok() == Some(OpStatus::VersionMismatch))
                    .count();

                if ok_count != 1 {
                    let first_op = current_ops[0].1;
                    failures.push(SafetyViolation {
                        op_id: first_op.id.clone(),
                        op: "cas".to_string(),
                        expected: format!("exactly 1 ok for key={} version={} ({} ops)", key, ref_version, current_ops.len()),
                        actual: format!("{} ok, {} version_mismatch", ok_count, mismatch_count),
                    });
                }
                if mismatch_count != current_ops.len() - 1 {
                    let first_op = current_ops[0].1;
                    failures.push(SafetyViolation {
                        op_id: first_op.id.clone(),
                        op: "cas".to_string(),
                        expected: format!("{} version_mismatch for key={} version={}", current_ops.len() - 1, key, ref_version),
                        actual: format!("{} ok, {} version_mismatch", ok_count, mismatch_count),
                    });
                }
            }
        }

        failures
    }
}

impl ReferenceModel for BasicKvReference {
    fn process_response(
        &mut self,
        ops: &[Operation],
        response: &AdapterBatchResponse,
        tolerance: &Option<Tolerance>,
    ) -> Vec<SafetyViolation> {
        let mut failures = Vec::new();

        // Check if this batch contains CAS conflict ops (multiple CAS ops for the same key)
        let has_cas_conflicts = {
            let mut cas_keys = std::collections::HashSet::new();
            let mut has_dup = false;
            for op in ops {
                if let OpKind::Cas { key, .. } = &op.kind {
                    if !cas_keys.insert(key.clone()) {
                        has_dup = true;
                        break;
                    }
                }
            }
            has_dup
        };

        if has_cas_conflicts {
            // Batch-level CAS conflict verification
            failures.extend(self.verify_cas_batch(ops, response));

            // Apply winners to reference state
            for (op, result) in ops.iter().zip(response.results.iter()) {
                let parsed_status = OpStatus::from_str(&result.status).ok();
                let is_write = matches!(
                    op.kind,
                    OpKind::Put { .. } | OpKind::Delete { .. } | OpKind::DeleteRange { .. }
                    | OpKind::Cas { .. } | OpKind::EphemeralPut { .. }
                    | OpKind::IndexedPut { .. } | OpKind::SequencePut { .. }
                );
                if is_write && parsed_status == Some(OpStatus::Ok) {
                    self.apply_write(op, result.version_id, result.key.as_deref());
                }
            }

            return failures;
        }

        // Non-conflict path: original per-op verification
        for (op, result) in ops.iter().zip(response.results.iter()) {
            if op.id != result.op_id {
                continue;
            }

            let parsed_status = OpStatus::from_str(&result.status).ok();

            // Single CAS verification (no conflicts in this batch)
            if let OpKind::Cas { key, expected_version_id, .. } = &op.kind {
                let current = self.rocks.ref_get(key).ok().flatten();
                let current_version = current.as_ref().map(|e| e.version).unwrap_or(0);
                let version_matches = current_version == *expected_version_id;

                match parsed_status {
                    Some(OpStatus::Ok) if !version_matches => {
                        failures.push(SafetyViolation {
                            op_id: op.id.clone(),
                            op: "cas".to_string(),
                            expected: format!("version_mismatch (ref_version={} expected={})", current_version, expected_version_id),
                            actual: "ok".to_string(),
                        });
                    }
                    Some(OpStatus::VersionMismatch) if version_matches => {
                        failures.push(SafetyViolation {
                            op_id: op.id.clone(),
                            op: "cas".to_string(),
                            expected: format!("ok (ref_version={} expected={})", current_version, expected_version_id),
                            actual: "version_mismatch".to_string(),
                        });
                    }
                    _ => {}
                }
            }

            let is_write = matches!(
                op.kind,
                OpKind::Put { .. } | OpKind::Delete { .. } | OpKind::DeleteRange { .. }
                | OpKind::Cas { .. } | OpKind::EphemeralPut { .. }
                | OpKind::IndexedPut { .. } | OpKind::SequencePut { .. }
            );

            if is_write && parsed_status == Some(OpStatus::Ok) {
                self.apply_write(op, result.version_id, result.key.as_deref());
            }

            // Sequence put monotonicity verification
            if let OpKind::SequencePut { prefix, .. } = &op.kind {
                if parsed_status == Some(OpStatus::Ok) {
                    if let Some(assigned_key) = &result.key {
                        if let Some(last_key) = self.last_sequence_keys.get(prefix) {
                            if assigned_key <= last_key {
                                failures.push(SafetyViolation {
                                    op_id: op.id.clone(),
                                    op: "sequence_put".to_string(),
                                    expected: format!("key > {}", last_key),
                                    actual: format!("key = {}", assigned_key),
                                });
                            }
                        }
                        self.last_sequence_keys.insert(prefix.clone(), assigned_key.clone());
                    } else {
                        warn!(op_id = %op.id, "sequence_put succeeded but no key returned");
                    }
                }
            }

            let is_read = matches!(
                op.kind,
                OpKind::Get { .. } | OpKind::List { .. } | OpKind::RangeScan { .. }
                | OpKind::IndexedGet { .. } | OpKind::IndexedList { .. } | OpKind::IndexedRangeScan { .. }
            );

            if is_read {
                if let Some(failure) = self.verify_read(op, result, tolerance) {
                    failures.push(failure);
                }
            }

            // Session restart: delete all ephemeral keys from reference
            if matches!(op.kind, OpKind::SessionRestart) && parsed_status == Some(OpStatus::Ok) {
                let prefix = self.run_prefix();
                if let Ok(all) = self.rocks.ref_scan_all(&prefix) {
                    for (key, entry) in &all {
                        if entry.ephemeral {
                            let _ = self.rocks.ref_delete(key);
                        }
                    }
                }
            }
        }

        failures
    }

    fn verify_checkpoint(
        &self,
        actual_state: &HashMap<String, Option<RecordState>>,
        tolerance: &Option<Tolerance>,
    ) -> Vec<CheckpointFailure> {
        let ignore_version = Self::should_ignore_field("version_id", tolerance);
        let prefix = self.run_prefix();
        let expected_all = self.rocks.ref_scan_all(&prefix).unwrap_or_default();

        let mut failures = Vec::new();

        // Build expected map (keep ephemeral flag for tolerance)
        let expected_entries: HashMap<String, &RefEntry> = expected_all
            .iter()
            .map(|(k, e)| (k.clone(), e))
            .collect();

        let expected_map: HashMap<String, RecordState> = expected_all
            .iter()
            .map(|(k, e)| (k.clone(), RecordState { value: Some(e.value.clone()), version_id: e.version }))
            .collect();

        // Check all expected keys exist in actual
        for (key, exp_state) in &expected_map {
            let act = actual_state.get(key).cloned().flatten();
            let is_ephemeral = expected_entries.get(key).map(|e| e.ephemeral).unwrap_or(false);

            match act {
                Some(act_state) => {
                    let value_mismatch = exp_state.value != act_state.value;
                    let version_mismatch = !ignore_version && exp_state.version_id != act_state.version_id;
                    if value_mismatch || version_mismatch {
                        failures.push(CheckpointFailure {
                            key: key.clone(),
                            expected: Some(exp_state.clone()),
                            actual: Some(act_state),
                        });
                    }
                }
                None => {
                    // Ephemeral keys may be gone after session restart — not a failure
                    if !is_ephemeral {
                        failures.push(CheckpointFailure {
                            key: key.clone(),
                            expected: Some(exp_state.clone()),
                            actual: None,
                        });
                    } else {
                        // Clean up reference — key is confirmed gone
                        let _ = self.rocks.ref_delete(key);
                    }
                }
            }
        }

        // Check for extra keys in actual that aren't in expected
        for (key, act) in actual_state {
            if !key.starts_with(&prefix) {
                continue;
            }
            if !expected_map.contains_key(key) {
                if let Some(act_state) = act {
                    failures.push(CheckpointFailure {
                        key: key.clone(),
                        expected: None,
                        actual: Some(act_state.clone()),
                    });
                }
            }
        }

        failures
    }

    fn snapshot_all(&self) -> HashMap<String, Option<RecordState>> {
        let prefix = self.run_prefix();
        let all = self.rocks.ref_scan_all(&prefix).unwrap_or_default();
        all.into_iter()
            .map(|(k, e)| (k, Some(RecordState { value: Some(e.value), version_id: e.version })))
            .collect()
    }

    fn key_prefix(&self) -> String {
        self.run_prefix()
    }

    fn set_run_id(&mut self, run_id: &str) {
        self.run_id = run_id.to_string();
    }

    fn clear(&mut self) {
        let prefix = self.run_prefix();
        if let Err(e) = self.rocks.ref_clear(&prefix) {
            warn!(error = %e, "failed to clear reference state");
        }
    }
}

fn comparison_str(c: &GetComparison) -> &'static str {
    match c {
        GetComparison::Equal => "equal",
        GetComparison::Floor => "floor",
        GetComparison::Ceiling => "ceiling",
        GetComparison::Lower => "lower",
        GetComparison::Higher => "higher",
    }
}
