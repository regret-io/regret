package generator

import (
	"fmt"
	"math/rand"
)

// keyPadding is the width for zero-padded numeric keys.
const keyPadding = 20

const (
	secondaryIndexPutRatio       = 0.25 / 0.40
	secondaryIndexGetRatio       = 0.15 / 0.25
	secondaryIndexListRatio      = 0.10 / 0.15
	secondaryIndexRangeScanRatio = 0.10 / 0.15
	sequencePutRatio             = 0.40 / 0.50
)

// BasicKvGenerator generates origin datasets for all KV-family profiles.
type BasicKvGenerator struct {
	rng              *rand.Rand
	params           GenerateParams
	indexKeys        []string
	sequencePrefixes []string
	opCounter        int
	warmupCursor     int
}

// NewBasicKvGenerator creates a new BasicKvGenerator from the given params.
func NewBasicKvGenerator(params *GenerateParams) *BasicKvGenerator {
	p := *params
	// Guard against zero values that would cause Intn(0) panics
	if p.KeySpace.Count <= 0 {
		p.KeySpace.Count = 20_000
	}
	if p.Index.KeyCount <= 0 {
		p.Index.KeyCount = 50
	}
	if p.Value.MinLen <= 0 {
		p.Value.MinLen = 4
	}
	if p.Value.MaxLen <= 0 {
		p.Value.MaxLen = 12
	}
	return &BasicKvGenerator{
		rng:    rand.New(rand.NewSource(int64(p.Seed))),
		params: p,
	}
}

// GenBatch generates the next count operations.
func (g *BasicKvGenerator) GenBatch(count int) []OriginOp {
	ops := make([]OriginOp, 0, count)
	for i := 0; i < count; i++ {
		ops = append(ops, g.genOp())
	}
	return ops
}

// Generate generates all operations up to params.Ops and returns them.
func (g *BasicKvGenerator) Generate() []OriginOp {
	var ops []OriginOp
	for g.opCounter < g.params.Ops {
		ops = append(ops, g.genOp())
	}
	return ops
}

// genOp generates a single op based on the full workload weights.
// During warmup, emits puts for all keys in the key space first.
func (g *BasicKvGenerator) genOp() OriginOp {
	// Warmup: put every key in the key space before random ops
	if !g.params.SkipWarmup && g.warmupCursor < g.params.KeySpace.Count {
		id := g.nextID()
		key := g.formatKey(g.warmupCursor)
		value := g.randomValue()
		g.warmupCursor++
		return Put(id, key, value)
	}

	id := g.nextID()
	roll := g.rng.Float64()
	workload := g.params.ResolvedWorkload()

	total := 0.0
	for _, w := range workload {
		total += w
	}
	if total <= 0.0 {
		return g.genPut(id)
	}

	cumulative := 0.0
	for opType, weight := range workload {
		cumulative += weight / total
		if roll < cumulative {
			return g.dispatchOp(opType, id)
		}
	}
	return g.genPut(id)
}

func (g *BasicKvGenerator) dispatchOp(opType, id string) OriginOp {
	switch opType {
	case "put":
		return g.genProfilePut(id)
	case "get":
		return g.genProfileGet(id)
	case "get_floor":
		return g.genGetWithComparison(id, "floor")
	case "get_ceiling":
		return g.genGetWithComparison(id, "ceiling")
	case "get_lower":
		return g.genGetWithComparison(id, "lower")
	case "get_higher":
		return g.genGetWithComparison(id, "higher")
	case "delete":
		return g.genDelete(id)
	case "delete_range":
		return g.genDeleteRange(id)
	case "list":
		return g.genProfileList(id)
	case "range_scan":
		return g.genProfileRangeScan(id)
	case "watch_start":
		return g.genWatchStart(id)
	case "session_restart":
		return g.genSessionRestart(id)
	case "get_notifications":
		return g.genGetNotifications(id)
	default:
		return g.genPut(id)
	}
}

func (g *BasicKvGenerator) genProfilePut(id string) OriginOp {
	switch g.params.Generator {
	case "kv-ephemeral-notification":
		return g.genEphemeralPut(id)
	case "kv-secondary-index":
		if g.rng.Float64() < secondaryIndexPutRatio {
			return g.genIndexedPut(id)
		}
	case "kv-sequence":
		if g.rng.Float64() < sequencePutRatio {
			return g.genSequencePut(id)
		}
	}
	return g.genPut(id)
}

func (g *BasicKvGenerator) genProfileGet(id string) OriginOp {
	if g.params.Generator == "kv-secondary-index" && g.rng.Float64() < secondaryIndexGetRatio {
		return g.genIndexedGet(id)
	}
	return g.genGet(id)
}

func (g *BasicKvGenerator) genProfileList(id string) OriginOp {
	if g.params.Generator == "kv-secondary-index" && g.rng.Float64() < secondaryIndexListRatio {
		return g.genIndexedList(id)
	}
	return g.genList(id)
}

func (g *BasicKvGenerator) genProfileRangeScan(id string) OriginOp {
	if g.params.Generator == "kv-secondary-index" && g.rng.Float64() < secondaryIndexRangeScanRatio {
		return g.genIndexedRangeScan(id)
	}
	return g.genRangeScan(id)
}

// -- Write operations --

func (g *BasicKvGenerator) genPut(id string) OriginOp {
	return Put(id, g.randomKey(), g.randomValue())
}

func (g *BasicKvGenerator) genDelete(id string) OriginOp {
	return Delete(id, g.randomKey())
}

func (g *BasicKvGenerator) genDeleteRange(id string) OriginOp {
	ks := &g.params.KeySpace
	lo := g.rng.Intn(ks.Count)
	maxSpan := 5
	if ks.Count < maxSpan {
		maxSpan = ks.Count
	}
	span := g.rng.Intn(maxSpan) + 1
	hi := lo + span
	if hi > ks.Count {
		hi = ks.Count
	}
	return DeleteRange(id, g.formatKey(lo), g.formatKey(hi))
}

func (g *BasicKvGenerator) genEphemeralPut(id string) OriginOp {
	return EphemeralPut(id, g.randomKey(), g.randomValue())
}

func (g *BasicKvGenerator) genIndexedPut(id string) OriginOp {
	key := g.randomKey()
	value := g.randomValue()
	idx := &g.params.Index
	indexKeyNum := g.rng.Intn(idx.KeyCount)
	indexKey := fmt.Sprintf("idx-%04d", indexKeyNum)

	found := false
	for _, ik := range g.indexKeys {
		if ik == indexKey {
			found = true
			break
		}
	}
	if !found {
		g.indexKeys = append(g.indexKeys, indexKey)
	}

	return IndexedPut(id, key, value, idx.Name, indexKey)
}

func (g *BasicKvGenerator) genSequencePut(id string) OriginOp {
	prefix := fmt.Sprintf("%sseq/", g.params.KeySpace.Prefix)
	value := g.randomValue()

	found := false
	for _, sp := range g.sequencePrefixes {
		if sp == prefix {
			found = true
			break
		}
	}
	if !found {
		g.sequencePrefixes = append(g.sequencePrefixes, prefix)
	}

	return SequencePut(id, prefix, value, int64(1))
}

// -- Read operations --

func (g *BasicKvGenerator) genGet(id string) OriginOp {
	return Get(id, g.randomKey())
}

func (g *BasicKvGenerator) genGetWithComparison(id, comparison string) OriginOp {
	return GetWithComparison(id, g.randomKey(), comparison)
}

func (g *BasicKvGenerator) genRangeScan(id string) OriginOp {
	const maxRange = 100
	ks := &g.params.KeySpace
	lo := g.rng.Intn(ks.Count)
	span := g.rng.Intn(min(maxRange, ks.Count)) + 1
	hi := lo + span
	if hi > ks.Count {
		hi = ks.Count
	}
	return Scan(id, g.formatKey(lo), g.formatKey(hi))
}

func (g *BasicKvGenerator) genList(id string) OriginOp {
	const maxRange = 100
	ks := &g.params.KeySpace
	lo := g.rng.Intn(ks.Count)
	span := g.rng.Intn(min(maxRange, ks.Count)) + 1
	hi := lo + span
	if hi > ks.Count {
		hi = ks.Count
	}
	return List(id, g.formatKey(lo), g.formatKey(hi))
}

func (g *BasicKvGenerator) genIndexedGet(id string) OriginOp {
	if len(g.indexKeys) == 0 {
		return g.genGet(id)
	}
	i := g.rng.Intn(len(g.indexKeys))
	indexKey := g.indexKeys[i]
	return IndexedGet(id, g.params.Index.Name, indexKey)
}

func (g *BasicKvGenerator) genIndexedList(id string) OriginOp {
	const maxRange = 100
	idx := &g.params.Index
	lo := g.rng.Intn(idx.KeyCount)
	span := g.rng.Intn(min(maxRange, idx.KeyCount)) + 1
	hi := lo + span
	if hi > idx.KeyCount {
		hi = idx.KeyCount
	}
	return IndexedList(id, idx.Name, fmt.Sprintf("idx-%04d", lo), fmt.Sprintf("idx-%04d", hi))
}

func (g *BasicKvGenerator) genIndexedRangeScan(id string) OriginOp {
	const maxRange = 100
	idx := &g.params.Index
	lo := g.rng.Intn(idx.KeyCount)
	span := g.rng.Intn(min(maxRange, idx.KeyCount)) + 1
	hi := lo + span
	if hi > idx.KeyCount {
		hi = idx.KeyCount
	}
	return IndexedRangeScan(id, idx.Name, fmt.Sprintf("idx-%04d", lo), fmt.Sprintf("idx-%04d", hi))
}

// -- Session & notification ops --

func (g *BasicKvGenerator) genWatchStart(id string) OriginOp {
	return WatchStart(id, g.params.KeySpace.Prefix)
}

func (g *BasicKvGenerator) genSessionRestart(id string) OriginOp {
	return SessionRestart(id)
}

func (g *BasicKvGenerator) genGetNotifications(id string) OriginOp {
	return GetNotifications(id)
}

// -- Helpers --

func (g *BasicKvGenerator) nextID() string {
	g.opCounter++
	return fmt.Sprintf("op-%04d", g.opCounter)
}

func (g *BasicKvGenerator) formatKey(n int) string {
	return fmt.Sprintf("%s%0*d", g.params.KeySpace.Prefix, keyPadding, n)
}

func (g *BasicKvGenerator) randomKey() string {
	n := g.rng.Intn(g.params.KeySpace.Count)
	return g.formatKey(n)
}

func (g *BasicKvGenerator) randomValue() string {
	vc := &g.params.Value
	charset := "abcdefghijklmnopqrstuvwxyz0123456789"
	length := vc.MinLen + g.rng.Intn(vc.MaxLen-vc.MinLen+1)
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[g.rng.Intn(len(charset))]
	}
	return vc.Prefix + string(b)
}
