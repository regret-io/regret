package generator

import "encoding/json"

// OriginOp is a single line in the origin JSONL file.
// It is either a fence or an operation.
type OriginOp struct {
	// IsFence is true if this is a fence operation.
	IsFence bool
	// Operation is populated when IsFence is false.
	Operation *OperationOp
}

// FenceOp represents a synchronization barrier.
type FenceOp struct {
	Type string `json:"type"`
}

// OperationOp represents a single operation with ID, op type, and fields.
type OperationOp struct {
	ID     string   `json:"id"`
	Op     string   `json:"op"`
	Fields OpFields `json:"-"`
}

// OpFieldsType enumerates the kinds of operation fields.
type OpFieldsType int

const (
	OpFieldsPut OpFieldsType = iota
	OpFieldsGet
	OpFieldsDelete
	OpFieldsDeleteRange
	OpFieldsScan
	OpFieldsList
	OpFieldsCas
	OpFieldsFence
	OpFieldsWatchStart
	OpFieldsSessionRestart
	OpFieldsGetNotifications
)

// OpFields holds the variant data for an operation.
type OpFields struct {
	Type OpFieldsType

	// Basic KV / shared
	Key   string
	Value string

	// Get comparison type
	Comparison string

	// Range ops (scan/list/delete_range)
	Start string
	End   string

	// Put modifiers
	Ephemeral bool   // ephemeral put
	Sequence  bool   // sequence put
	Prefix    string // for sequence put, watch_start
	Delta     int64  // for sequence put

	// Secondary index
	IndexName string // for put (indexed), scan, list
	IndexKey  string // for put (indexed)

	// CAS
	ExpectedVersionID uint64
	NewValue          string
}

// MarshalJSON implements custom JSON serialization for OriginOp.
// Fences serialize as {"type":"fence"}. Operations serialize with
// fields flattened into the top-level object.
func (o OriginOp) MarshalJSON() ([]byte, error) {
	if o.IsFence {
		return json.Marshal(map[string]string{"type": "fence"})
	}
	return o.Operation.MarshalJSON()
}

// MarshalJSON implements custom JSON serialization for OperationOp,
// flattening the fields into the top-level map.
func (op OperationOp) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{
		"id": op.ID,
		"op": op.Op,
	}
	f := op.Fields
	switch f.Type {
	case OpFieldsPut:
		if f.Sequence {
			m["prefix"] = f.Prefix
			m["value"] = f.Value
			m["delta"] = f.Delta
			m["sequence"] = true
		} else {
			m["key"] = f.Key
			m["value"] = f.Value
			if f.Ephemeral {
				m["ephemeral"] = true
			}
			if f.IndexName != "" {
				m["index_name"] = f.IndexName
				m["index_key"] = f.IndexKey
			}
		}
	case OpFieldsGet:
		m["key"] = f.Key
		if f.Comparison != "" && f.Comparison != "equal" {
			m["comparison"] = f.Comparison
		}
	case OpFieldsDelete:
		m["key"] = f.Key
	case OpFieldsDeleteRange:
		m["start"] = f.Start
		m["end"] = f.End
	case OpFieldsScan:
		m["start"] = f.Start
		m["end"] = f.End
		if f.IndexName != "" {
			m["index_name"] = f.IndexName
		}
	case OpFieldsList:
		m["start"] = f.Start
		m["end"] = f.End
		if f.IndexName != "" {
			m["index_name"] = f.IndexName
		}
	case OpFieldsCas:
		m["key"] = f.Key
		m["expected_version_id"] = f.ExpectedVersionID
		m["new_value"] = f.NewValue
	case OpFieldsWatchStart:
		m["prefix"] = f.Prefix
	case OpFieldsSessionRestart:
		// no additional fields
	case OpFieldsGetNotifications:
		// no additional fields
	}
	return json.Marshal(m)
}

// -- Constructor helpers --

// Fence creates a fence OriginOp.
func Fence() OriginOp {
	return OriginOp{IsFence: true}
}

// Put creates a put OriginOp.
func Put(id, key, value string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "put",
		Fields: OpFields{Type: OpFieldsPut, Key: key, Value: value},
	}}
}

// Get creates a get OriginOp with equal comparison (default).
func Get(id, key string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get",
		Fields: OpFields{Type: OpFieldsGet, Key: key},
	}}
}

// GetWithComparison creates a get OriginOp with the specified comparison type.
func GetWithComparison(id, key, comparison string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get",
		Fields: OpFields{Type: OpFieldsGet, Key: key, Comparison: comparison},
	}}
}

// Delete creates a delete OriginOp.
func Delete(id, key string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "delete",
		Fields: OpFields{Type: OpFieldsDelete, Key: key},
	}}
}

// DeleteRange creates a delete_range OriginOp.
func DeleteRange(id, start, end string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "delete_range",
		Fields: OpFields{Type: OpFieldsDeleteRange, Start: start, End: end},
	}}
}

// List creates a list OriginOp.
func List(id, start, end string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "list",
		Fields: OpFields{Type: OpFieldsList, Start: start, End: end},
	}}
}

// ListWithIndex creates a list OriginOp with a secondary index.
func ListWithIndex(id, indexName, start, end string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "list",
		Fields: OpFields{Type: OpFieldsList, IndexName: indexName, Start: start, End: end},
	}}
}

// Scan creates a scan OriginOp (replaces range_scan).
func Scan(id, start, end string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "scan",
		Fields: OpFields{Type: OpFieldsScan, Start: start, End: end},
	}}
}

// ScanWithIndex creates a scan OriginOp with a secondary index.
func ScanWithIndex(id, indexName, start, end string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "scan",
		Fields: OpFields{Type: OpFieldsScan, IndexName: indexName, Start: start, End: end},
	}}
}

// Cas creates a cas OriginOp.
func Cas(id, key string, expectedVersionID uint64, newValue string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "cas",
		Fields: OpFields{Type: OpFieldsCas, Key: key, ExpectedVersionID: expectedVersionID, NewValue: newValue},
	}}
}

// EphemeralPut creates an ephemeral put OriginOp.
func EphemeralPut(id, key, value string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "put",
		Fields: OpFields{Type: OpFieldsPut, Key: key, Value: value, Ephemeral: true},
	}}
}

// IndexedPut creates an indexed put OriginOp.
func IndexedPut(id, key, value, indexName, indexKey string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "put",
		Fields: OpFields{Type: OpFieldsPut, Key: key, Value: value, IndexName: indexName, IndexKey: indexKey},
	}}
}

// IndexedGet creates an indexed get OriginOp (list with index_key as both start and end).
func IndexedGet(id, indexName, indexKey string) OriginOp {
	endKey := indexKey + "\x00"
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "list",
		Fields: OpFields{Type: OpFieldsList, IndexName: indexName, Start: indexKey, End: endKey},
	}}
}

// IndexedList creates an indexed list OriginOp.
func IndexedList(id, indexName, start, end string) OriginOp {
	return ListWithIndex(id, indexName, start, end)
}

// IndexedRangeScan creates an indexed scan OriginOp.
func IndexedRangeScan(id, indexName, start, end string) OriginOp {
	return ScanWithIndex(id, indexName, start, end)
}

// SequencePut creates a sequence put OriginOp.
func SequencePut(id, prefix, value string, delta int64) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "put",
		Fields: OpFields{Type: OpFieldsPut, Value: value, Sequence: true, Prefix: prefix, Delta: delta},
	}}
}

// WatchStart creates a watch_start OriginOp.
func WatchStart(id, prefix string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "watch_start",
		Fields: OpFields{Type: OpFieldsWatchStart, Prefix: prefix},
	}}
}

// SessionRestart creates a session_restart OriginOp.
func SessionRestart(id string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "session_restart",
		Fields: OpFields{Type: OpFieldsSessionRestart},
	}}
}

// GetNotifications creates a get_notifications OriginOp.
func GetNotifications(id string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get_notifications",
		Fields: OpFields{Type: OpFieldsGetNotifications},
	}}
}
