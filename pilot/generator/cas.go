package generator

import (
	"fmt"
	"math/rand"
)

// CasGenerator generates CAS (compare-and-swap) correctness test workloads.
//
// Instead of weighted random operations, this generator produces batches of
// cas (put-with-version) operations with intentional key duplication to
// create conflicts. The correctness invariant is:
//
//	For N ops targeting the same key+version in one batch,
//	exactly 1 succeeds (Ok) and N-1 fail (VersionMismatch).
//
// Versions are read from the reference store via VersionLookup at batch-generation time.
// The winner's returned version feeds the next batch.
type CasGenerator struct {
	rng                 *rand.Rand
	params              GenerateParams
	versionLookup       VersionLookup
	opCounter           int
	warmupCursor        int
	conflictProbability float64
}

// NewCasGenerator creates a new CasGenerator from the given params.
func NewCasGenerator(params *GenerateParams) *CasGenerator {
	p := *params
	if p.KeySpace.Count <= 0 {
		p.KeySpace.Count = 20_000
	}
	if p.Value.MinLen <= 0 {
		p.Value.MinLen = 4
	}
	if p.Value.MaxLen <= 0 {
		p.Value.MaxLen = 12
	}
	return &CasGenerator{
		rng:                 rand.New(rand.NewSource(int64(p.Seed))),
		params:              p,
		conflictProbability: 0.4,
	}
}

// GenBatch generates a batch of CAS operations with intentional conflicts.
//
// During warmup, emits plain puts to populate the key space.
// After warmup, all ops are CAS with version. Some keys are intentionally
// duplicated within the batch to test the conflict invariant.
func (g *CasGenerator) GenBatch(count int) []OriginOp {
	ops := make([]OriginOp, 0, count)
	var batchKeys []string

	for i := 0; i < count; i++ {
		// Warmup: put every key in the key space before CAS ops
		if !g.params.SkipWarmup && g.warmupCursor < g.params.KeySpace.Count {
			id := g.nextID()
			key := g.formatKey(g.warmupCursor)
			value := g.randomValue()
			g.warmupCursor++
			ops = append(ops, Put(id, key, value))
			continue
		}

		id := g.nextID()

		// Decide whether to create a conflict by reusing a key already in the batch
		shouldConflict := len(batchKeys) > 0 && g.rng.Float64() < g.conflictProbability

		var key string
		if shouldConflict {
			idx := g.rng.Intn(len(batchKeys))
			key = batchKeys[idx]
		} else {
			key = g.randomKey()
		}

		newValue := g.randomValue()

		// Look up current version from reference store
		var currentVersion uint64
		if g.versionLookup != nil {
			if v, ok := g.versionLookup.GetVersion(key); ok {
				currentVersion = v
			}
		}

		batchKeys = append(batchKeys, key)
		ops = append(ops, Cas(id, key, currentVersion, newValue))
	}

	return ops
}

// -- Helpers --

func (g *CasGenerator) nextID() string {
	g.opCounter++
	return fmt.Sprintf("op-%04d", g.opCounter)
}

func (g *CasGenerator) formatKey(n int) string {
	return fmt.Sprintf("%s%0*d", g.params.KeySpace.Prefix, keyPadding, n)
}

func (g *CasGenerator) randomKey() string {
	n := g.rng.Intn(g.params.KeySpace.Count)
	return g.formatKey(n)
}

func (g *CasGenerator) randomValue() string {
	vc := &g.params.Value
	charset := "abcdefghijklmnopqrstuvwxyz0123456789"
	length := vc.MinLen + g.rng.Intn(vc.MaxLen-vc.MinLen+1)
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[g.rng.Intn(len(charset))]
	}
	return vc.Prefix + string(b)
}
