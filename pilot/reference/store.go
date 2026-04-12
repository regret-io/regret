package reference

import "fmt"

// ReferenceStore wraps a pebbleDB instance and provides the reference storage
// used by ReferenceModel implementations. Callers create one ReferenceStore at
// startup and pass it to every reference model.
type ReferenceStore struct {
	db *pebbleDB
}

// NewReferenceStore opens (or creates) a Pebble database at the given path.
func NewReferenceStore(path string) (*ReferenceStore, error) {
	db, err := openPebble(path)
	if err != nil {
		return nil, fmt.Errorf("open reference store: %w", err)
	}
	return &ReferenceStore{db: db}, nil
}

// Close closes the underlying Pebble database.
func (s *ReferenceStore) Close() error {
	return s.db.close()
}

// GetVersion returns the current version for a key, implementing the
// VersionLookup interface used by generators for CAS operations.
func (s *ReferenceStore) GetVersion(key string) (uint64, bool) {
	entry, err := s.db.refGet(key)
	if err != nil || entry == nil {
		return 0, false
	}
	return entry.Version, true
}

// refEntry is a reference entry stored in the Pebble database.
type refEntry struct {
	Value     string `json:"value"`
	Version   uint64 `json:"version"`
	Ephemeral bool   `json:"ephemeral,omitempty"`
}

// refOp is a single put or delete operation for batch application.
type refOp struct {
	key   string
	entry *refEntry // non-nil = put, nil = delete
	// Optional secondary index fields (only used with put)
	indexName string
	indexKey  string
}

// keyEntry is a key-entry pair returned by range operations.
type keyEntry struct {
	key   string
	entry refEntry
}
