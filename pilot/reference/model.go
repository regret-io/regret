package reference

// No external imports needed -- Pebble is internal to this package.

// RecordState is the unified record state for checkpoint comparison.
type RecordState struct {
	Value     *string `json:"value,omitempty"`
	VersionID uint64  `json:"version_id"`
}

// OpStatus constants.
const (
	OpStatusOk              = "ok"
	OpStatusNotFound        = "not_found"
	OpStatusVersionMismatch = "version_mismatch"
)

// RangeRecord is a record returned in range scan results.
type RangeRecord struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	VersionID uint64 `json:"version_id"`
}

// NotificationRecord is a notification received from the adapter.
type NotificationRecord struct {
	NotificationType string  `json:"type"`
	Key              *string `json:"key,omitempty"`
	KeyStart         *string `json:"key_start,omitempty"`
	KeyEnd           *string `json:"key_end,omitempty"`
}

// SafetyViolation is a failure detected during verification.
type SafetyViolation struct {
	OpID     string `json:"op_id"`
	Op       string `json:"op"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
}

// CheckpointFailure is a failure detected during checkpoint verification.
type CheckpointFailure struct {
	Key      string       `json:"key"`
	Expected *RecordState `json:"expected,omitempty"`
	Actual   *RecordState `json:"actual,omitempty"`
}

// Tolerance configures verification tolerance.
type Tolerance struct {
	Ordering   string                `json:"ordering"`
	Duplicates string                `json:"duplicates"`
	Structural []StructuralTolerance `json:"structural"`
}

// DefaultTolerance returns a Tolerance with default values.
func DefaultTolerance() Tolerance {
	return Tolerance{
		Ordering:   "strict",
		Duplicates: "deny",
	}
}

// StructuralTolerance configures tolerance for a specific field.
type StructuralTolerance struct {
	Field  string `json:"field"`
	Ignore bool   `json:"ignore"`
}

// AdapterBatchResponse is the adapter's response for a batch of operations.
type AdapterBatchResponse struct {
	BatchID string            `json:"batch_id"`
	Results []AdapterOpResult `json:"results"`
}

// AdapterOpResult is the result of a single operation from the adapter.
type AdapterOpResult struct {
	OpID          string                `json:"op_id"`
	Op            string                `json:"op"`
	Status        string                `json:"status"`
	Key           *string               `json:"key,omitempty"`
	Value         *string               `json:"value,omitempty"`
	VersionID     *uint64               `json:"version_id,omitempty"`
	Records       []RangeRecord         `json:"records,omitempty"`
	Keys          []string              `json:"keys,omitempty"`
	DeletedCount  *uint64               `json:"deleted_count,omitempty"`
	Notifications []NotificationRecord  `json:"notifications,omitempty"`
	Message       *string               `json:"message,omitempty"`
}

// GetComparison enumerates get comparison types.
type GetComparison int

const (
	ComparisonEqual GetComparison = iota
	ComparisonFloor
	ComparisonCeiling
	ComparisonLower
	ComparisonHigher
)

// String returns the string representation of a GetComparison.
func (c GetComparison) String() string {
	switch c {
	case ComparisonEqual:
		return "equal"
	case ComparisonFloor:
		return "floor"
	case ComparisonCeiling:
		return "ceiling"
	case ComparisonLower:
		return "lower"
	case ComparisonHigher:
		return "higher"
	default:
		return "equal"
	}
}

// Operation is a parsed operation.
type Operation struct {
	ID   string
	Kind OpKind
}

// OpKindType enumerates the kinds of operations.
type OpKindType int

const (
	OpKindPut OpKindType = iota
	OpKindGet
	OpKindDelete
	OpKindDeleteRange
	OpKindScan
	OpKindList
	OpKindCas
	OpKindFence
	OpKindWatchStart
	OpKindSessionRestart
	OpKindGetNotifications
)

// OpKind represents the data associated with an operation kind.
type OpKind struct {
	Type OpKindType

	// Basic KV / shared
	Key   string
	Value string

	// Get comparison type: "equal" (default), "floor", "ceiling", "lower", "higher"
	Comparison GetComparison

	// Range ops (scan/list/delete_range)
	Start string
	End   string

	// Put modifiers
	Ephemeral bool   // replaces OpKindEphemeralPut
	Sequence  bool   // replaces OpKindSequencePut
	Prefix    string // for sequence put and watch_start
	Delta     int64  // for sequence put

	// Secondary index
	IndexName string // for put (indexed), scan, list
	IndexKey  string // for put (indexed)

	// CAS
	ExpectedVersionID uint64
	NewValue          string
}

// ReferenceModel is the core reference model interface.
// The reference model owns the "truth state" persisted in PebbleDB.
// It processes adapter responses to maintain what the state SHOULD be
// after all successful operations.
type ReferenceModel interface {
	// SetRunID sets the run ID (called before each run).
	SetRunID(runID string)

	// Clear removes all state (start of each run).
	Clear()

	// KeyPrefix returns the key prefix for this run.
	KeyPrefix() string

	// ProcessResponse processes adapter response for a batch of operations.
	// Write succeeded -> update state in PebbleDB.
	// Read succeeded -> verify value against state in PebbleDB.
	// Returns list of read verification failures.
	ProcessResponse(ops []Operation, response *AdapterBatchResponse, tolerance *Tolerance) []SafetyViolation

	// VerifyCheckpoint verifies adapter state snapshot against reference state.
	VerifyCheckpoint(actual map[string]*RecordState, tolerance *Tolerance) []CheckpointFailure

	// SnapshotAll returns all reference state as a map.
	SnapshotAll() map[string]*RecordState

	// GetVersion returns the current version for a key (used by CAS generators).
	GetVersion(key string) (uint64, bool)
}

// CreateReference creates a reference model backed by a ReferenceStore.
func CreateReference(generatorName string, store *ReferenceStore, hypothesisID string) ReferenceModel {
	return NewBasicKvReference(store, hypothesisID)
}
