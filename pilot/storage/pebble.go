package storage

import "io"

// RefEntry is a reference entry stored in PebbleDB.
type RefEntry struct {
	Value     string `json:"value"`
	Version   uint64 `json:"version"`
	Ephemeral bool   `json:"ephemeral,omitempty"`
}

// PebbleStore defines the storage interface for reference state backed by PebbleDB.
// This is the Go equivalent of the Rust RocksStore.
// It embeds io.Closer for lifecycle management.
type PebbleStore interface {
	io.Closer

	// RefPut stores a reference entry at the given key.
	RefPut(key string, entry *RefEntry) error

	// RefGet retrieves a reference entry by exact key. Returns nil if not found.
	RefGet(key string) (*RefEntry, error)

	// RefDelete removes a reference entry.
	RefDelete(key string) error

	// RefRangeKeys returns all keys in [start, end) within a prefix, sorted.
	RefRangeKeys(prefix, start, end string) ([]string, error)

	// RefRangeScan returns all (key, entry) pairs in [start, end) within a prefix, sorted.
	RefRangeScan(prefix, start, end string) ([]KeyEntry, error)

	// RefFloor returns the largest key <= target within prefix.
	RefFloor(prefix, target string) (*KeyEntry, error)

	// RefCeiling returns the smallest key >= target within prefix.
	RefCeiling(prefix, target string) (*KeyEntry, error)

	// RefLower returns the largest key < target within prefix.
	RefLower(prefix, target string) (*KeyEntry, error)

	// RefHigher returns the smallest key > target within prefix.
	RefHigher(prefix, target string) (*KeyEntry, error)

	// RefClear deletes all keys with a given prefix.
	RefClear(prefix string) error

	// RefScanAll returns all (key, entry) pairs with a given prefix, sorted.
	RefScanAll(prefix string) ([]KeyEntry, error)

	// IdxPut stores an index entry: indexName + indexKey -> primaryKey.
	IdxPut(indexName, indexKey, primaryKey string) error

	// IdxDeletePrimary removes all index entries for a given primary key.
	IdxDeletePrimary(indexName, primaryKey string) error

	// IdxList lists primary keys for an index key range [start, end).
	IdxList(indexName, start, end string) ([]string, error)

	// IdxClear removes all index entries for a given index name.
	IdxClear(indexName string) error

	// RefBatch creates a new WriteBatch for atomic writes.
	RefBatch() WriteBatch
}

// WriteBatch accumulates writes for atomic commit.
type WriteBatch interface {
	// Put stores a reference entry in the batch.
	Put(key string, entry *RefEntry)
	// Delete removes a reference entry in the batch.
	Delete(key string)
	// Commit atomically applies all batched writes.
	Commit() error
}

// KeyEntry is a key-entry pair returned by range operations.
type KeyEntry struct {
	Key   string
	Entry RefEntry
}
