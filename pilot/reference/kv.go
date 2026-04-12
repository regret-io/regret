package reference

import (
	"fmt"
	"log/slog"
	"strings"
)

// BasicKvReference is the reference model for all KV-family generators.
// It uses an internal pebbleDB to track reference state and verifies adapter responses.
type BasicKvReference struct {
	store            *ReferenceStore
	hypothesisID     string
	runID            string
	lastSequenceKeys map[string]string
}

// NewBasicKvReference creates a new BasicKvReference backed by a ReferenceStore.
func NewBasicKvReference(store *ReferenceStore, hypothesisID string) *BasicKvReference {
	return &BasicKvReference{
		store:            store,
		hypothesisID:     hypothesisID,
		lastSequenceKeys: make(map[string]string),
	}
}

// runPrefix returns the key prefix for this hypothesis + run in PebbleDB.
func (r *BasicKvReference) runPrefix() string {
	return fmt.Sprintf("/ref/%s/%s/", r.hypothesisID, r.runID)
}

// shouldIgnoreField checks if a field should be ignored based on tolerance.
func shouldIgnoreField(field string, tolerance *Tolerance) bool {
	if tolerance == nil {
		return false
	}
	for _, s := range tolerance.Structural {
		if s.Field == field && s.Ignore {
			return true
		}
	}
	return false
}

// applyWrite applies a successful write to the reference state.
// Uses the adapter's returned version_id and key when available.
func (r *BasicKvReference) applyWrite(op *Operation, adapterVersion *uint64, adapterKey *string) {
	switch op.Kind.Type {
	case OpKindPut:
		if op.Kind.Sequence {
			return
		}
		version := r.resolveVersion(op.Kind.Key, adapterVersion)
		_ = r.store.db.refPut(op.Kind.Key, &refEntry{
			Value: op.Kind.Value, Version: version, Ephemeral: op.Kind.Ephemeral,
		}, op.Kind.IndexName, op.Kind.IndexKey)

	case OpKindDelete:
		_ = r.store.db.refDelete(op.Kind.Key)

	case OpKindDeleteRange:
		prefix := r.runPrefix()
		relStart := strings.TrimPrefix(op.Kind.Start, prefix)
		relEnd := strings.TrimPrefix(op.Kind.End, prefix)
		_ = r.store.db.refDeleteRange(prefix, relStart, relEnd)

	case OpKindCas:
		version := r.resolveVersion(op.Kind.Key, adapterVersion)
		_ = r.store.db.refPut(op.Kind.Key, &refEntry{
			Value: op.Kind.NewValue, Version: version, Ephemeral: false,
		}, "", "")
	}
}

// collectRefOp builds a RefOp for batched application.
func (r *BasicKvReference) collectRefOp(op *Operation, adapterVersion *uint64) *refOp {
	switch op.Kind.Type {
	case OpKindPut:
		if op.Kind.Sequence {
			return nil
		}
		version := r.resolveVersion(op.Kind.Key, adapterVersion)
		return &refOp{
			key:       op.Kind.Key,
			entry:     &refEntry{Value: op.Kind.Value, Version: version, Ephemeral: op.Kind.Ephemeral},
			indexName: op.Kind.IndexName,
			indexKey:  op.Kind.IndexKey,
		}
	case OpKindDelete:
		return &refOp{key: op.Kind.Key}
	case OpKindCas:
		version := r.resolveVersion(op.Kind.Key, adapterVersion)
		return &refOp{
			key:   op.Kind.Key,
			entry: &refEntry{Value: op.Kind.NewValue, Version: version},
		}
	default:
		return nil
	}
}

// resolveVersion returns the adapter version if provided, otherwise increments
// the current version in the reference store (or starts at 1).
func (r *BasicKvReference) resolveVersion(key string, adapterVersion *uint64) uint64 {
	if adapterVersion != nil {
		return *adapterVersion
	}
	entry, err := r.store.db.refGet(key)
	if err == nil && entry != nil {
		return entry.Version + 1
	}
	return 1
}

// verifyRead verifies a read result against reference state.
func (r *BasicKvReference) verifyRead(
	op *Operation,
	result *AdapterOpResult,
	tolerance *Tolerance,
) *SafetyViolation {
	ignoreVersion := shouldIgnoreField("version_id", tolerance)

	switch op.Kind.Type {
	case OpKindGet:
		return r.verifyGet(op, op.Kind.Key, op.Kind.Comparison, result, ignoreVersion)
	case OpKindList:
		if op.Kind.IndexName != "" {
			return r.verifyIndexedList(op, op.Kind.IndexName, op.Kind.Start, op.Kind.End, result)
		}
		return r.verifyList(op, op.Kind.Start, op.Kind.End, result)
	case OpKindScan:
		if op.Kind.IndexName != "" {
			return r.verifyIndexedRangeScan(op, op.Kind.IndexName, op.Kind.Start, op.Kind.End, result, ignoreVersion)
		}
		return r.verifyRangeScan(op, op.Kind.Start, op.Kind.End, result, ignoreVersion)
	default:
		return nil
	}
}

func (r *BasicKvReference) verifyGet(
	op *Operation,
	key string,
	comparison GetComparison,
	result *AdapterOpResult,
	ignoreVersion bool,
) *SafetyViolation {
	prefix := r.runPrefix()

	// Strip the prefix from the key for navigational lookups,
	// since RefFloor/Ceiling/Lower/Higher prepend the prefix internally.
	relKey := key
	if strings.HasPrefix(key, prefix) {
		relKey = key[len(prefix):]
	}

	// Find the expected record based on comparison type
	var expected *keyEntry
	switch comparison {
	case ComparisonEqual:
		entry, err := r.store.db.refGet(key)
		if err == nil && entry != nil {
			expected = &keyEntry{key: key, entry: *entry}
		}
	case ComparisonFloor:
		ke, err := r.store.db.refFloor(prefix, relKey)
		if err == nil && ke != nil {
			expected = ke
		}
	case ComparisonCeiling:
		ke, err := r.store.db.refCeiling(prefix, relKey)
		if err == nil && ke != nil {
			expected = ke
		}
	case ComparisonLower:
		ke, err := r.store.db.refLower(prefix, relKey)
		if err == nil && ke != nil {
			expected = ke
		}
	case ComparisonHigher:
		ke, err := r.store.db.refHigher(prefix, relKey)
		if err == nil && ke != nil {
			expected = ke
		}
	}

	compStr := comparison.String()

	if expected != nil {
		entry := &expected.entry
		expectedKey := expected.key

		// Ephemeral keys may be deleted at any time (session loss)
		// so not_found is always acceptable for them
		if entry.Ephemeral && result.Status == OpStatusNotFound {
			// Session might have been lost -- delete from reference to stay in sync
			_ = r.store.db.refDelete(expectedKey)
			return nil
		}

		// Should return ok with value
		if result.Status != OpStatusOk {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       fmt.Sprintf("get_%s", compStr),
				Expected: fmt.Sprintf("ok key=%s value=%s", expectedKey, entry.Value),
				Actual:   fmt.Sprintf("status=%s", result.Status),
			}
		}
		// Check value
		if result.Value != nil && *result.Value != entry.Value {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       fmt.Sprintf("get_%s", compStr),
				Expected: fmt.Sprintf("value=%s", entry.Value),
				Actual:   fmt.Sprintf("value=%s", *result.Value),
			}
		}
		// Check version (when not ignored)
		if !ignoreVersion && result.VersionID != nil {
			if *result.VersionID != entry.Version {
				return &SafetyViolation{
					OpID:     op.ID,
					Op:       fmt.Sprintf("get_%s", compStr),
					Expected: fmt.Sprintf("version=%d", entry.Version),
					Actual:   fmt.Sprintf("version=%d", *result.VersionID),
				}
			}
		}
		return nil
	}

	// Reference found no matching key within our prefix.
	if result.Status == OpStatusOk && result.Value != nil {
		// For comparison gets, check if the returned key is in our prefix.
		// If outside prefix, the adapter found a key in a different namespace -- not a violation.
		if comparison != ComparisonEqual {
			if result.Key != nil {
				if !strings.HasPrefix(*result.Key, prefix) {
					return nil // Key outside our prefix -- valid
				}
			} else {
				return nil // No returned key to verify
			}
		}
		returnedKey := "?"
		if result.Key != nil {
			returnedKey = *result.Key
		}
		returnedValue := ""
		if result.Value != nil {
			returnedValue = *result.Value
		}
		return &SafetyViolation{
			OpID:     op.ID,
			Op:       fmt.Sprintf("get_%s", compStr),
			Expected: "not_found",
			Actual:   fmt.Sprintf("ok key=%s value=%s", returnedKey, returnedValue),
		}
	}
	return nil
}

func (r *BasicKvReference) verifyList(
	op *Operation,
	start, end string,
	result *AdapterOpResult,
) *SafetyViolation {
	prefix := r.runPrefix()
	relStart := strings.TrimPrefix(start, prefix)
	relEnd := strings.TrimPrefix(end, prefix)
	expectedKeys, _ := r.store.db.refRangeKeys(prefix, relStart, relEnd)
	if expectedKeys == nil {
		expectedKeys = []string{}
	}
	actualKeys := result.Keys

	rangeInfo := fmt.Sprintf(" [start=%s, end=%s]", start, end)

	if len(expectedKeys) != len(actualKeys) {
		return &SafetyViolation{
			OpID:     op.ID,
			Op:       "list",
			Expected: fmt.Sprintf("%d keys%s", len(expectedKeys), rangeInfo),
			Actual:   fmt.Sprintf("%d keys", len(actualKeys)),
		}
	}

	// Verify exact sorted order
	for i := range expectedKeys {
		if expectedKeys[i] != actualKeys[i] {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       "list",
				Expected: fmt.Sprintf("key[%d]=%s%s", i, expectedKeys[i], rangeInfo),
				Actual:   fmt.Sprintf("key[%d]=%s", i, actualKeys[i]),
			}
		}
	}
	return nil
}

func (r *BasicKvReference) verifyRangeScan(
	op *Operation,
	start, end string,
	result *AdapterOpResult,
	ignoreVersion bool,
) *SafetyViolation {
	prefix := r.runPrefix()
	relStart := strings.TrimPrefix(start, prefix)
	relEnd := strings.TrimPrefix(end, prefix)
	expectedEntries, _ := r.store.db.refRangeScan(prefix, relStart, relEnd)
	if expectedEntries == nil {
		expectedEntries = []keyEntry{}
	}
	actualRecords := result.Records

	rangeInfo := fmt.Sprintf(" [start=%s, end=%s]", start, end)

	if len(expectedEntries) != len(actualRecords) {
		return &SafetyViolation{
			OpID:     op.ID,
			Op:       "range_scan",
			Expected: fmt.Sprintf("%d records%s", len(expectedEntries), rangeInfo),
			Actual:   fmt.Sprintf("%d records", len(actualRecords)),
		}
	}

	// Verify exact sorted order + values
	for i := range expectedEntries {
		exp := &expectedEntries[i]
		act := &actualRecords[i]
		if exp.key != act.Key {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       "range_scan",
				Expected: fmt.Sprintf("record[%d].key=%s%s", i, exp.key, rangeInfo),
				Actual:   fmt.Sprintf("record[%d].key=%s", i, act.Key),
			}
		}
		if exp.entry.Value != act.Value {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       "range_scan",
				Expected: fmt.Sprintf("record[%d].value=%s%s", i, exp.entry.Value, rangeInfo),
				Actual:   fmt.Sprintf("record[%d].value=%s", i, act.Value),
			}
		}
	}
	return nil
}

func (r *BasicKvReference) verifyIndexedGet(
	op *Operation,
	indexName, indexKey string,
	result *AdapterOpResult,
	ignoreVersion bool,
) *SafetyViolation {
	// Look up primary keys from index
	endKey := indexKey + "\x00"
	primaryKeys, _ := r.store.db.idxList(indexName, indexKey, endKey)

	if len(primaryKeys) == 0 {
		// Reference has no index entry -- adapter should return not_found
		if result.Status == OpStatusOk && result.Value != nil {
			actualValue := "?"
			if result.Value != nil {
				actualValue = *result.Value
			}
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       "indexed_get",
				Expected: "not_found",
				Actual:   fmt.Sprintf("ok value=%s", actualValue),
			}
		}
		return nil
	}

	// Get first primary key's value from reference
	primaryKey := primaryKeys[0]
	entry, err := r.store.db.refGet(primaryKey)

	if err != nil || entry == nil {
		// Primary key deleted but index not cleaned -- tolerate not_found
		return nil
	}

	if result.Status != OpStatusOk {
		return &SafetyViolation{
			OpID:     op.ID,
			Op:       "indexed_get",
			Expected: fmt.Sprintf("ok value=%s", entry.Value),
			Actual:   fmt.Sprintf("status=%s", result.Status),
		}
	}
	if result.Value != nil && *result.Value != entry.Value {
		return &SafetyViolation{
			OpID:     op.ID,
			Op:       "indexed_get",
			Expected: fmt.Sprintf("value=%s", entry.Value),
			Actual:   fmt.Sprintf("value=%s", *result.Value),
		}
	}
	if !ignoreVersion && result.VersionID != nil {
		if *result.VersionID != entry.Version {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       "indexed_get",
				Expected: fmt.Sprintf("version=%d", entry.Version),
				Actual:   fmt.Sprintf("version=%d", *result.VersionID),
			}
		}
	}
	return nil
}

func (r *BasicKvReference) verifyIndexedList(
	op *Operation,
	indexName, start, end string,
	result *AdapterOpResult,
) *SafetyViolation {
	expectedKeys, _ := r.store.db.idxList(indexName, start, end)
	if expectedKeys == nil {
		expectedKeys = []string{}
	}
	actualKeys := result.Keys

	// All expected keys must be present in actual (subset check).
	// Adapter may return extra keys from stale index entries of previous runs.
	actualSet := make(map[string]struct{}, len(actualKeys))
	for _, k := range actualKeys {
		actualSet[k] = struct{}{}
	}
	for _, exp := range expectedKeys {
		if _, ok := actualSet[exp]; !ok {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       "indexed_list",
				Expected: fmt.Sprintf("key %s present", exp),
				Actual:   fmt.Sprintf("key %s missing from %d actual keys", exp, len(actualKeys)),
			}
		}
	}
	return nil
}

func (r *BasicKvReference) verifyIndexedRangeScan(
	op *Operation,
	indexName, start, end string,
	result *AdapterOpResult,
	ignoreVersion bool,
) *SafetyViolation {
	// Get primary keys from index, then fetch their values
	primaryKeys, _ := r.store.db.idxList(indexName, start, end)
	expected := make(map[string]*refEntry)
	for _, pk := range primaryKeys {
		entry, err := r.store.db.refGet(pk)
		if err == nil && entry != nil {
			expected[pk] = entry
		}
	}
	actualRecords := result.Records

	// All expected records must be present in actual (subset check).
	actualMap := make(map[string]*RangeRecord, len(actualRecords))
	for i := range actualRecords {
		actualMap[actualRecords[i].Key] = &actualRecords[i]
	}

	for expKey, expEntry := range expected {
		act, ok := actualMap[expKey]
		if !ok {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       "indexed_range_scan",
				Expected: fmt.Sprintf("key %s present", expKey),
				Actual:   fmt.Sprintf("key %s missing from %d actual records", expKey, len(actualRecords)),
			}
		}
		if expEntry.Value != act.Value {
			return &SafetyViolation{
				OpID:     op.ID,
				Op:       "indexed_range_scan",
				Expected: fmt.Sprintf("key=%s value=%s", expKey, expEntry.Value),
				Actual:   fmt.Sprintf("key=%s value=%s", act.Key, act.Value),
			}
		}
	}
	return nil
}

// verifyCasBatch verifies CAS batch invariant: for ops targeting the same key
// with the current version, exactly 1 must succeed (Ok) and the rest must fail
// (VersionMismatch). Stale-version ops must all fail.
func (r *BasicKvReference) verifyCasBatch(
	ops []Operation,
	response *AdapterBatchResponse,
) []SafetyViolation {
	var failures []SafetyViolation

	type casEntry struct {
		index  int
		op     *Operation
		result *AdapterOpResult
	}

	// Group CAS ops by key
	casGroups := make(map[string][]casEntry)
	for i := range ops {
		if ops[i].Kind.Type == OpKindCas {
			key := ops[i].Kind.Key
			casGroups[key] = append(casGroups[key], casEntry{
				index:  i,
				op:     &ops[i],
				result: &response.Results[i],
			})
		}
	}

	for key, group := range casGroups {
		// Read reference version ONCE per key (before applying any writes)
		var refVersion uint64
		entry, err := r.store.db.refGet(key)
		if err == nil && entry != nil {
			refVersion = entry.Version
		}

		// Partition into current-version ops vs stale-version ops
		var currentOps []casEntry
		var staleOps []casEntry

		for _, e := range group {
			if e.op.Kind.ExpectedVersionID == refVersion {
				currentOps = append(currentOps, e)
			} else {
				staleOps = append(staleOps, e)
			}
		}

		// Stale ops: ALL must be VersionMismatch
		for _, e := range staleOps {
			if e.result.Status != OpStatusVersionMismatch {
				failures = append(failures, SafetyViolation{
					OpID:     e.op.ID,
					Op:       "cas",
					Expected: fmt.Sprintf("version_mismatch (stale: expected=%d ref=%d)", e.op.Kind.ExpectedVersionID, refVersion),
					Actual:   fmt.Sprintf("status=%s", e.result.Status),
				})
			}
		}

		// Current-version ops: exactly 1 Ok, rest VersionMismatch
		if len(currentOps) > 0 {
			okCount := 0
			mismatchCount := 0
			for _, e := range currentOps {
				switch e.result.Status {
				case OpStatusOk:
					okCount++
				case OpStatusVersionMismatch:
					mismatchCount++
				}
			}

			if okCount != 1 {
				failures = append(failures, SafetyViolation{
					OpID:     currentOps[0].op.ID,
					Op:       "cas",
					Expected: fmt.Sprintf("exactly 1 ok for key=%s version=%d (%d ops)", key, refVersion, len(currentOps)),
					Actual:   fmt.Sprintf("%d ok, %d version_mismatch", okCount, mismatchCount),
				})
			}
			if mismatchCount != len(currentOps)-1 {
				failures = append(failures, SafetyViolation{
					OpID:     currentOps[0].op.ID,
					Op:       "cas",
					Expected: fmt.Sprintf("%d version_mismatch for key=%s version=%d", len(currentOps)-1, key, refVersion),
					Actual:   fmt.Sprintf("%d ok, %d version_mismatch", okCount, mismatchCount),
				})
			}
		}
	}

	return failures
}

// isWriteOp returns true if the operation kind is a write.
func isWriteOp(kind OpKindType) bool {
	switch kind {
	case OpKindPut, OpKindDelete, OpKindDeleteRange, OpKindCas:
		return true
	default:
		return false
	}
}

// isReadOp returns true if the operation kind is a read.
func isReadOp(kind OpKindType) bool {
	switch kind {
	case OpKindGet, OpKindList, OpKindScan:
		return true
	default:
		return false
	}
}

// -- ReferenceModel implementation --

func (r *BasicKvReference) SetRunID(runID string) {
	r.runID = runID
}

func (r *BasicKvReference) Clear() {
	prefix := r.runPrefix()
	if err := r.store.db.refClear(prefix); err != nil {
		slog.Warn("failed to clear reference state", slog.Any("error", err))
	}
}

func (r *BasicKvReference) KeyPrefix() string {
	return r.runPrefix()
}

func (r *BasicKvReference) ProcessResponse(
	ops []Operation,
	response *AdapterBatchResponse,
	tolerance *Tolerance,
) []SafetyViolation {
	var failures []SafetyViolation

	// Check if this batch contains CAS conflict ops (multiple CAS ops for the same key)
	hasCasConflicts := false
	casKeys := make(map[string]struct{})
	for i := range ops {
		if ops[i].Kind.Type == OpKindCas {
			key := ops[i].Kind.Key
			if _, exists := casKeys[key]; exists {
				hasCasConflicts = true
				break
			}
			casKeys[key] = struct{}{}
		}
	}

	if hasCasConflicts {
		// Batch-level CAS conflict verification
		failures = append(failures, r.verifyCasBatch(ops, response)...)

		// Apply winners to reference state
		for i := range ops {
			if i >= len(response.Results) {
				break
			}
			result := &response.Results[i]
			if isWriteOp(ops[i].Kind.Type) && result.Status == OpStatusOk {
				r.applyWrite(&ops[i], result.VersionID, result.Key)
			}
		}

		return failures
	}

	// Non-conflict path: verify reads first, then apply writes.
	// The adapter executes ops concurrently, so reads see the state
	// BEFORE any writes in the same batch.

	// Phase 1: Verify reads against current reference state
	for i := range ops {
		if i >= len(response.Results) {
			break
		}
		op := &ops[i]
		result := &response.Results[i]
		if op.ID != result.OpID {
			continue
		}
		if isReadOp(op.Kind.Type) {
			if violation := r.verifyRead(op, result, tolerance); violation != nil {
				failures = append(failures, *violation)
			}
		}

		// Session restart: delete all ephemeral keys from reference
		if op.Kind.Type == OpKindSessionRestart && result.Status == OpStatusOk {
			prefix := r.runPrefix()
			all, err := r.store.db.refScanAll(prefix)
			if err == nil {
				for _, ke := range all {
					if ke.entry.Ephemeral {
						_ = r.store.db.refDelete(ke.key)
					}
				}
			}
		}
	}

	// Phase 2: Verify writes and collect ops for atomic batch commit
	var batchOps []refOp
	for i := range ops {
		if i >= len(response.Results) {
			break
		}
		op := &ops[i]
		result := &response.Results[i]
		if op.ID != result.OpID {
			continue
		}

		// Single CAS verification (no conflicts in this batch)
		if op.Kind.Type == OpKindCas {
			current, err := r.store.db.refGet(op.Kind.Key)
			var currentVersion uint64
			if err == nil && current != nil {
				currentVersion = current.Version
			}
			versionMatches := currentVersion == op.Kind.ExpectedVersionID

			if result.Status == OpStatusOk && !versionMatches {
				failures = append(failures, SafetyViolation{
					OpID:     op.ID,
					Op:       "cas",
					Expected: fmt.Sprintf("version_mismatch (ref_version=%d expected=%d)", currentVersion, op.Kind.ExpectedVersionID),
					Actual:   "ok",
				})
			} else if result.Status == OpStatusVersionMismatch && versionMatches {
				failures = append(failures, SafetyViolation{
					OpID:     op.ID,
					Op:       "cas",
					Expected: fmt.Sprintf("ok (ref_version=%d expected=%d)", currentVersion, op.Kind.ExpectedVersionID),
					Actual:   "version_mismatch",
				})
			}
		}

		// Collect write ops for atomic batch
		if isWriteOp(op.Kind.Type) && result.Status == OpStatusOk {
			// DeleteRange is handled atomically inside PebbleStore
			if op.Kind.Type == OpKindDeleteRange {
				prefix := r.runPrefix()
				relStart := strings.TrimPrefix(op.Kind.Start, prefix)
				relEnd := strings.TrimPrefix(op.Kind.End, prefix)
				_ = r.store.db.refDeleteRange(prefix, relStart, relEnd)
			} else if refOp := r.collectRefOp(op, result.VersionID); refOp != nil {
				batchOps = append(batchOps, *refOp)
			}
		}

		// Sequence put monotonicity verification
		if op.Kind.Type == OpKindPut && op.Kind.Sequence && result.Status == OpStatusOk {
			if result.Key != nil {
				assignedKey := *result.Key
				if lastKey, ok := r.lastSequenceKeys[op.Kind.Prefix]; ok {
					if assignedKey <= lastKey {
						failures = append(failures, SafetyViolation{
							OpID:     op.ID,
							Op:       "sequence_put",
							Expected: fmt.Sprintf("key > %s", lastKey),
							Actual:   fmt.Sprintf("key = %s", assignedKey),
						})
					}
				}
				r.lastSequenceKeys[op.Kind.Prefix] = assignedKey
			} else {
				slog.Warn("sequence_put succeeded but no key returned",
					slog.String("op_id", op.ID))
			}
		}
	}

	// Apply all collected writes atomically
	if len(batchOps) > 0 {
		if err := r.store.db.refApplyBatch(batchOps); err != nil {
			slog.Warn("failed to apply write batch", slog.Any("error", err))
		}
	}

	return failures
}

func (r *BasicKvReference) VerifyCheckpoint(
	actual map[string]*RecordState,
	tolerance *Tolerance,
) []CheckpointFailure {
	ignoreVersion := shouldIgnoreField("version_id", tolerance)
	prefix := r.runPrefix()
	expectedAll, _ := r.store.db.refScanAll(prefix)
	if expectedAll == nil {
		expectedAll = []keyEntry{}
	}

	var failures []CheckpointFailure

	// Build expected maps
	expectedEntries := make(map[string]*refEntry, len(expectedAll))
	expectedMap := make(map[string]*RecordState, len(expectedAll))
	for i := range expectedAll {
		ke := &expectedAll[i]
		expectedEntries[ke.key] = &ke.entry
		val := ke.entry.Value
		expectedMap[ke.key] = &RecordState{Value: &val, VersionID: ke.entry.Version}
	}

	// Check all expected keys exist in actual
	for key, expState := range expectedMap {
		actState := actual[key]
		isEphemeral := false
		if e, ok := expectedEntries[key]; ok {
			isEphemeral = e.Ephemeral
		}

		if actState != nil {
			valueMismatch := !strPtrEqual(expState.Value, actState.Value)
			versionMismatch := !ignoreVersion && expState.VersionID != actState.VersionID
			if valueMismatch || versionMismatch {
				failures = append(failures, CheckpointFailure{
					Key:      key,
					Expected: expState,
					Actual:   actState,
				})
			}
		} else {
			// Ephemeral keys may be gone after session restart -- not a failure
			if !isEphemeral {
				failures = append(failures, CheckpointFailure{
					Key:      key,
					Expected: expState,
					Actual:   nil,
				})
			} else {
				// Clean up reference -- key is confirmed gone
				_ = r.store.db.refDelete(key)
			}
		}
	}

	// Check for extra keys in actual that aren't in expected
	for key, actState := range actual {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, exists := expectedMap[key]; !exists {
			if actState != nil {
				failures = append(failures, CheckpointFailure{
					Key:      key,
					Expected: nil,
					Actual:   actState,
				})
			}
		}
	}

	return failures
}

func (r *BasicKvReference) SnapshotAll() map[string]*RecordState {
	prefix := r.runPrefix()
	all, _ := r.store.db.refScanAll(prefix)
	result := make(map[string]*RecordState, len(all))
	for _, ke := range all {
		val := ke.entry.Value
		result[ke.key] = &RecordState{Value: &val, VersionID: ke.entry.Version}
	}
	return result
}

func (r *BasicKvReference) GetVersion(key string) (uint64, bool) {
	entry, err := r.store.db.refGet(key)
	if err != nil || entry == nil {
		return 0, false
	}
	return entry.Version, true
}

// -- Helpers --

func strPtrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}
