package storage

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
)

// pebbleDB is the concrete PebbleStore implementation.
type pebbleDB struct {
	db *pebble.DB
}

// Compile-time check that pebbleDB implements PebbleStore.
var _ PebbleStore = (*pebbleDB)(nil)

// NewPebbleStore opens (or creates) a Pebble database at the given path
// and returns a PebbleStore backed by it.
func NewPebbleStore(path string) (PebbleStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}
	return &pebbleDB{db: db}, nil
}

func (p *pebbleDB) Close() error {
	return p.db.Close()
}

// ---------------------------------------------------------------------------
// Internal key helpers
// ---------------------------------------------------------------------------

const refPrefix = "__ref__\x00"

func refKey(key string) []byte {
	return []byte(refPrefix + key)
}

func stripRefPrefix(k []byte) string {
	return string(k[len(refPrefix):])
}

func marshalRef(e *RefEntry) ([]byte, error) {
	return json.Marshal(e)
}

func unmarshalRef(data []byte) (*RefEntry, error) {
	var e RefEntry
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// ---------------------------------------------------------------------------
// Ref state methods
// ---------------------------------------------------------------------------

func (p *pebbleDB) RefPut(key string, entry *RefEntry, indexName, indexKey string) error {
	if indexName == "" {
		// Simple put — no batch needed
		data, err := marshalRef(entry)
		if err != nil {
			return fmt.Errorf("marshal ref: %w", err)
		}
		return p.db.Set(refKey(key), data, pebble.Sync)
	}
	// Atomic: put record + update secondary index
	batch := p.db.NewBatch()
	data, err := marshalRef(entry)
	if err != nil {
		return fmt.Errorf("marshal ref: %w", err)
	}
	if err := batch.Set(refKey(key), data, nil); err != nil {
		return fmt.Errorf("batch set ref: %w", err)
	}
	// Delete old index entries for this primary key, then add new one
	p.batchIdxDeletePrimary(batch, indexName, key)
	idxKey := idxKey(indexName, indexKey, key)
	if err := batch.Set(idxKey, nil, nil); err != nil {
		return fmt.Errorf("batch set idx: %w", err)
	}
	return batch.Commit(pebble.Sync)
}

func (p *pebbleDB) RefGet(key string) (*RefEntry, error) {
	data, closer, err := p.db.Get(refKey(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("ref get: %w", err)
	}
	defer closer.Close()

	return unmarshalRef(data)
}

func (p *pebbleDB) RefDelete(key string) error {
	return p.db.Delete(refKey(key), pebble.Sync)
}

func (p *pebbleDB) RefDeleteRange(prefix, start, end string) error {
	keys, err := p.RefRangeKeys(prefix, start, end)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	batch := p.db.NewBatch()
	for _, k := range keys {
		if err := batch.Delete(refKey(k), nil); err != nil {
			return fmt.Errorf("batch delete: %w", err)
		}
	}
	return batch.Commit(pebble.Sync)
}

func (p *pebbleDB) RefApplyBatch(ops []RefOp) error {
	if len(ops) == 0 {
		return nil
	}
	batch := p.db.NewBatch()
	for _, op := range ops {
		if op.Entry != nil {
			// Put
			data, err := marshalRef(op.Entry)
			if err != nil {
				return fmt.Errorf("marshal ref: %w", err)
			}
			if err := batch.Set(refKey(op.Key), data, nil); err != nil {
				return fmt.Errorf("batch set: %w", err)
			}
			if op.IndexName != "" {
				p.batchIdxDeletePrimary(batch, op.IndexName, op.Key)
				if err := batch.Set(idxKey(op.IndexName, op.IndexKey, op.Key), nil, nil); err != nil {
					return fmt.Errorf("batch set idx: %w", err)
				}
			}
		} else {
			// Delete
			if err := batch.Delete(refKey(op.Key), nil); err != nil {
				return fmt.Errorf("batch delete: %w", err)
			}
		}
	}
	return batch.Commit(pebble.Sync)
}

// batchIdxDeletePrimary scans for and deletes all index entries for a primary key within a batch.
func (p *pebbleDB) batchIdxDeletePrimary(batch *pebble.Batch, indexName, primaryKey string) {
	prefix := []byte(idxPrefix + "\x00" + indexName + "\x00")
	upper := appendByte(prefix, 0xff)
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return
	}
	defer iter.Close()
	suffix := "\x00" + primaryKey
	for iter.First(); iter.Valid(); iter.Next() {
		k := string(iter.Key())
		if len(k) >= len(suffix) && k[len(k)-len(suffix):] == suffix {
			_ = batch.Delete(iter.Key(), nil)
		}
	}
}

func (p *pebbleDB) RefRangeKeys(prefix, start, end string) ([]string, error) {
	lower := refKey(prefix + start)
	upper := refKey(prefix + end)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("ref range keys iter: %w", err)
	}
	defer iter.Close()

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, stripRefPrefix(iter.Key()))
	}
	return keys, iter.Error()
}

func (p *pebbleDB) RefRangeScan(prefix, start, end string) ([]KeyEntry, error) {
	lower := refKey(prefix + start)
	upper := refKey(prefix + end)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("ref range scan iter: %w", err)
	}
	defer iter.Close()

	var out []KeyEntry
	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("ref range scan value: %w", err)
		}
		entry, err := unmarshalRef(val)
		if err != nil {
			return nil, fmt.Errorf("ref range scan unmarshal: %w", err)
		}
		out = append(out, KeyEntry{
			Key:   stripRefPrefix(iter.Key()),
			Entry: *entry,
		})
	}
	return out, iter.Error()
}

func (p *pebbleDB) RefFloor(prefix, target string) (*KeyEntry, error) {
	fullTarget := refKey(prefix + target)
	prefixLower := refKey(prefix)
	prefixUpper := appendByte(refKey(prefix), 0xff)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixLower,
		UpperBound: prefixUpper,
	})
	if err != nil {
		return nil, fmt.Errorf("ref floor iter: %w", err)
	}
	defer iter.Close()

	// SeekGE to target, then check or step back.
	if iter.SeekGE(fullTarget) {
		if string(iter.Key()) == string(fullTarget) {
			return keyEntryFromIter(iter)
		}
		// Current key > target, step back.
		if iter.Prev() {
			return keyEntryFromIter(iter)
		}
	} else {
		// Past all keys — last one is the floor.
		if iter.Last() {
			return keyEntryFromIter(iter)
		}
	}
	return nil, iter.Error()
}

func (p *pebbleDB) RefCeiling(prefix, target string) (*KeyEntry, error) {
	fullTarget := refKey(prefix + target)
	prefixLower := refKey(prefix)
	prefixUpper := appendByte(refKey(prefix), 0xff)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixLower,
		UpperBound: prefixUpper,
	})
	if err != nil {
		return nil, fmt.Errorf("ref ceiling iter: %w", err)
	}
	defer iter.Close()

	if iter.SeekGE(fullTarget) {
		return keyEntryFromIter(iter)
	}
	return nil, iter.Error()
}

func (p *pebbleDB) RefLower(prefix, target string) (*KeyEntry, error) {
	fullTarget := refKey(prefix + target)
	prefixLower := refKey(prefix)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixLower,
		UpperBound: fullTarget, // exclusive upper bound = strictly less than target
	})
	if err != nil {
		return nil, fmt.Errorf("ref lower iter: %w", err)
	}
	defer iter.Close()

	if iter.Last() {
		return keyEntryFromIter(iter)
	}
	return nil, iter.Error()
}

func (p *pebbleDB) RefHigher(prefix, target string) (*KeyEntry, error) {
	fullTarget := refKey(prefix + target)
	prefixLower := refKey(prefix)
	prefixUpper := appendByte(refKey(prefix), 0xff)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefixLower,
		UpperBound: prefixUpper,
	})
	if err != nil {
		return nil, fmt.Errorf("ref higher iter: %w", err)
	}
	defer iter.Close()

	if iter.SeekGE(fullTarget) {
		// If exact match, move forward.
		if string(iter.Key()) == string(fullTarget) {
			if !iter.Next() {
				return nil, iter.Error()
			}
		}
		return keyEntryFromIter(iter)
	}
	return nil, iter.Error()
}

func (p *pebbleDB) RefClear(prefix string) error {
	fullPrefix := refKey(prefix)
	upper := appendByte(fullPrefix, 0xff)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: fullPrefix,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("ref clear iter: %w", err)
	}

	batch := p.db.NewBatch()
	for iter.First(); iter.Valid(); iter.Next() {
		batch.Delete(iter.Key(), nil)
	}
	if err := iter.Error(); err != nil {
		iter.Close()
		batch.Close()
		return fmt.Errorf("ref clear scan: %w", err)
	}
	iter.Close()

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("ref clear commit: %w", err)
	}
	return nil
}

func (p *pebbleDB) RefScanAll(prefix string) ([]KeyEntry, error) {
	fullPrefix := refKey(prefix)
	upper := appendByte(fullPrefix, 0xff)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: fullPrefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("ref scan all iter: %w", err)
	}
	defer iter.Close()

	var out []KeyEntry
	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("ref scan all value: %w", err)
		}
		entry, err := unmarshalRef(val)
		if err != nil {
			return nil, fmt.Errorf("ref scan all unmarshal: %w", err)
		}
		out = append(out, KeyEntry{
			Key:   stripRefPrefix(iter.Key()),
			Entry: *entry,
		})
	}
	return out, iter.Error()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func keyEntryFromIter(iter *pebble.Iterator) (*KeyEntry, error) {
	val, err := iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("iter value: %w", err)
	}
	entry, err := unmarshalRef(val)
	if err != nil {
		return nil, fmt.Errorf("iter unmarshal: %w", err)
	}
	return &KeyEntry{
		Key:   stripRefPrefix(iter.Key()),
		Entry: *entry,
	}, nil
}

func appendByte(b []byte, c byte) []byte {
	out := make([]byte, len(b)+1)
	copy(out, b)
	out[len(b)] = c
	return out
}

// ---------------------------------------------------------------------------
// Secondary index methods
// ---------------------------------------------------------------------------

const idxPrefix = "__idx__\x00"

// idxKey builds: __idx__\0{indexName}\0{indexKey}\0{primaryKey}
func idxKey(indexName, indexKey, primaryKey string) []byte {
	return []byte(idxPrefix + indexName + "\x00" + indexKey + "\x00" + primaryKey)
}

func (p *pebbleDB) IdxPut(indexName, indexKey, primaryKey string) error {
	return p.db.Set(idxKey(indexName, indexKey, primaryKey), []byte{}, pebble.Sync)
}

func (p *pebbleDB) IdxDeletePrimary(indexName, primaryKey string) error {
	prefix := []byte(idxPrefix + indexName + "\x00")
	upper := appendByte(prefix, 0xff)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("idx delete primary iter: %w", err)
	}

	suffix := "\x00" + primaryKey
	batch := p.db.NewBatch()
	for iter.First(); iter.Valid(); iter.Next() {
		k := string(iter.Key())
		if strings.HasSuffix(k, suffix) {
			batch.Delete(iter.Key(), nil)
		}
	}
	if err := iter.Error(); err != nil {
		iter.Close()
		batch.Close()
		return fmt.Errorf("idx delete primary scan: %w", err)
	}
	iter.Close()

	return batch.Commit(pebble.Sync)
}

func (p *pebbleDB) IdxList(indexName, start, end string) ([]string, error) {
	lower := []byte(idxPrefix + indexName + "\x00" + start)
	upper := []byte(idxPrefix + indexName + "\x00" + end)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("idx list iter: %w", err)
	}
	defer iter.Close()

	// Key format: __idx__\0{indexName}\0{indexKey}\0{primaryKey}
	namePrefix := idxPrefix + indexName + "\x00"

	var out []string
	for iter.First(); iter.Valid(); iter.Next() {
		k := string(iter.Key())
		rest := k[len(namePrefix):] // indexKey\0primaryKey
		parts := strings.SplitN(rest, "\x00", 2)
		if len(parts) == 2 {
			out = append(out, parts[1])
		}
	}
	return out, iter.Error()
}

func (p *pebbleDB) IdxClear(indexName string) error {
	prefix := []byte(idxPrefix + indexName + "\x00")
	upper := appendByte(prefix, 0xff)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return fmt.Errorf("idx clear iter: %w", err)
	}

	batch := p.db.NewBatch()
	for iter.First(); iter.Valid(); iter.Next() {
		batch.Delete(iter.Key(), nil)
	}
	if err := iter.Error(); err != nil {
		iter.Close()
		batch.Close()
		return fmt.Errorf("idx clear scan: %w", err)
	}
	iter.Close()

	return batch.Commit(pebble.Sync)
}
