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
	OpFieldsList
	OpFieldsRangeScan
	OpFieldsCas
	OpFieldsEphemeralPut
	OpFieldsIndexedPut
	OpFieldsIndexedGet
	OpFieldsIndexedList
	OpFieldsIndexedRangeScan
	OpFieldsSequencePut
)

// OpFields holds the variant data for an operation.
type OpFields struct {
	Type OpFieldsType

	// Basic KV / shared
	Key   string
	Value string

	// Range ops
	Start string
	End   string

	// CAS
	ExpectedVersionID uint64
	NewValue          string

	// Secondary index
	IndexName string
	IndexKey  string

	// Sequence
	Prefix string
	Delta  uint64
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
		m["key"] = f.Key
		m["value"] = f.Value
	case OpFieldsGet:
		m["key"] = f.Key
	case OpFieldsDelete:
		m["key"] = f.Key
	case OpFieldsDeleteRange:
		m["start"] = f.Start
		m["end"] = f.End
	case OpFieldsList:
		m["start"] = f.Start
		m["end"] = f.End
	case OpFieldsRangeScan:
		m["start"] = f.Start
		m["end"] = f.End
	case OpFieldsCas:
		m["key"] = f.Key
		m["expected_version_id"] = f.ExpectedVersionID
		m["new_value"] = f.NewValue
	case OpFieldsEphemeralPut:
		m["key"] = f.Key
		m["value"] = f.Value
	case OpFieldsIndexedPut:
		m["key"] = f.Key
		m["value"] = f.Value
		m["index_name"] = f.IndexName
		m["index_key"] = f.IndexKey
	case OpFieldsIndexedGet:
		m["index_name"] = f.IndexName
		m["index_key"] = f.IndexKey
	case OpFieldsIndexedList:
		m["index_name"] = f.IndexName
		m["start"] = f.Start
		m["end"] = f.End
	case OpFieldsIndexedRangeScan:
		m["index_name"] = f.IndexName
		m["start"] = f.Start
		m["end"] = f.End
	case OpFieldsSequencePut:
		m["prefix"] = f.Prefix
		m["value"] = f.Value
		m["delta"] = f.Delta
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

// Get creates a get OriginOp.
func Get(id, key string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get",
		Fields: OpFields{Type: OpFieldsGet, Key: key},
	}}
}

// GetFloor creates a get_floor OriginOp.
func GetFloor(id, key string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get_floor",
		Fields: OpFields{Type: OpFieldsGet, Key: key},
	}}
}

// GetCeiling creates a get_ceiling OriginOp.
func GetCeiling(id, key string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get_ceiling",
		Fields: OpFields{Type: OpFieldsGet, Key: key},
	}}
}

// GetLower creates a get_lower OriginOp.
func GetLower(id, key string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get_lower",
		Fields: OpFields{Type: OpFieldsGet, Key: key},
	}}
}

// GetHigher creates a get_higher OriginOp.
func GetHigher(id, key string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get_higher",
		Fields: OpFields{Type: OpFieldsGet, Key: key},
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

// RangeScan creates a range_scan OriginOp.
func RangeScan(id, start, end string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "range_scan",
		Fields: OpFields{Type: OpFieldsRangeScan, Start: start, End: end},
	}}
}

// Cas creates a cas OriginOp.
func Cas(id, key string, expectedVersionID uint64, newValue string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "cas",
		Fields: OpFields{Type: OpFieldsCas, Key: key, ExpectedVersionID: expectedVersionID, NewValue: newValue},
	}}
}

// EphemeralPut creates an ephemeral_put OriginOp.
func EphemeralPut(id, key, value string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "ephemeral_put",
		Fields: OpFields{Type: OpFieldsEphemeralPut, Key: key, Value: value},
	}}
}

// IndexedPut creates an indexed_put OriginOp.
func IndexedPut(id, key, value, indexName, indexKey string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "indexed_put",
		Fields: OpFields{Type: OpFieldsIndexedPut, Key: key, Value: value, IndexName: indexName, IndexKey: indexKey},
	}}
}

// IndexedGet creates an indexed_get OriginOp.
func IndexedGet(id, indexName, indexKey string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "indexed_get",
		Fields: OpFields{Type: OpFieldsIndexedGet, IndexName: indexName, IndexKey: indexKey},
	}}
}

// IndexedList creates an indexed_list OriginOp.
func IndexedList(id, indexName, start, end string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "indexed_list",
		Fields: OpFields{Type: OpFieldsIndexedList, IndexName: indexName, Start: start, End: end},
	}}
}

// IndexedRangeScan creates an indexed_range_scan OriginOp.
func IndexedRangeScan(id, indexName, start, end string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "indexed_range_scan",
		Fields: OpFields{Type: OpFieldsIndexedRangeScan, IndexName: indexName, Start: start, End: end},
	}}
}

// SequencePut creates a sequence_put OriginOp.
func SequencePut(id, prefix, value string, delta uint64) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "sequence_put",
		Fields: OpFields{Type: OpFieldsSequencePut, Prefix: prefix, Value: value, Delta: delta},
	}}
}

// WatchStart creates a watch_start OriginOp.
func WatchStart(id, prefix string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "watch_start",
		Fields: OpFields{Type: OpFieldsGet, Key: prefix},
	}}
}

// SessionRestart creates a session_restart OriginOp.
func SessionRestart(id string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "session_restart",
		Fields: OpFields{Type: OpFieldsGet, Key: ""},
	}}
}

// GetNotifications creates a get_notifications OriginOp.
func GetNotifications(id string) OriginOp {
	return OriginOp{Operation: &OperationOp{
		ID: id, Op: "get_notifications",
		Fields: OpFields{Type: OpFieldsGet, Key: ""},
	}}
}
