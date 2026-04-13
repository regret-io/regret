package generator

// VersionLookup provides version information for CAS operations.
// The reference model implements this interface.
type VersionLookup interface {
	GetVersion(key string) (uint64, bool)
}

// Generator produces batches of origin ops.
type Generator interface {
	// GenBatch generates the next count operations.
	GenBatch(count int) []OriginOp
}

// GenerateParams is a declarative workload profile for origin dataset generation.
type GenerateParams struct {
	// Generator name. Use a predefined generator ("basic-kv", "kv-cas",
	// "kv-ephemeral-notification", "kv-secondary-index", "kv-sequence")
	// or "custom" with explicit workload weights.
	Generator string `json:"generator"`

	// Ops is the total number of operations to generate.
	Ops int `json:"ops"`

	// KeySpace configuration.
	KeySpace KeySpaceConfig `json:"key_space"`

	// Workload is the operation mix -- weights per operation type (normalized automatically).
	// If empty, loaded from the predefined profile.
	Workload map[string]float64 `json:"workload"`

	// FenceEvery inserts a fence every ~N write ops.
	FenceEvery int `json:"fence_every"`

	// Seed is the RNG seed for reproducibility.
	Seed uint64 `json:"seed"`

	// Value generation configuration.
	Value ValueConfig `json:"value"`

	// Index is the secondary index configuration.
	Index IndexConfig `json:"index"`

	// Rate is the target ops/sec rate. 0 = unlimited.
	Rate uint32 `json:"rate"`

	// SkipWarmup skips the warmup phase (pre-populating all keys).
	SkipWarmup bool `json:"skip_warmup"`
}

// KeySpaceConfig configures the key space for generation.
type KeySpaceConfig struct {
	Prefix  string `json:"prefix"`
	Count   int    `json:"count"`
	Padding int    `json:"padding"`
}

// ValueConfig configures value generation.
type ValueConfig struct {
	MinLen int    `json:"min_len"`
	MaxLen int    `json:"max_len"`
	Prefix string `json:"prefix"`
}

// IndexConfig configures secondary index generation.
type IndexConfig struct {
	Name     string `json:"name"`
	KeyCount int    `json:"key_count"`
}

// DefaultGenerateParams returns GenerateParams with default values.
func DefaultGenerateParams() *GenerateParams {
	return &GenerateParams{
		Generator:  "basic-kv",
		Ops:        1000,
		KeySpace:   DefaultKeySpaceConfig(),
		Workload:   map[string]float64{},
		FenceEvery: 50,
		Seed:       42,
		Value:      DefaultValueConfig(),
		Index:      DefaultIndexConfig(),
		Rate:       0,
		SkipWarmup: false,
	}
}

// DefaultKeySpaceConfig returns the default key space configuration.
func DefaultKeySpaceConfig() KeySpaceConfig {
	return KeySpaceConfig{
		Prefix:  "user:",
		Count:   20_000,
		Padding: 6,
	}
}

// DefaultValueConfig returns the default value configuration.
func DefaultValueConfig() ValueConfig {
	return ValueConfig{
		MinLen: 4,
		MaxLen: 12,
		Prefix: "v-",
	}
}

// DefaultIndexConfig returns the default index configuration.
func DefaultIndexConfig() IndexConfig {
	return IndexConfig{
		Name:     "by-value",
		KeyCount: 50,
	}
}

// ResolvedWorkload returns the workload map, loading from the generator
// definition if the params workload is empty.
func (p *GenerateParams) ResolvedWorkload() map[string]float64 {
	if len(p.Workload) > 0 {
		return p.Workload
	}
	return GetGeneratorWorkload(p.Generator)
}

// WriteWeights returns normalized write weights from the resolved workload.
func (p *GenerateParams) WriteWeights() []WeightEntry {
	workload := p.ResolvedWorkload()
	writeOps := []string{
		"put", "delete", "delete_range", "cas",
	}
	var entries []WeightEntry
	for _, op := range writeOps {
		if w, ok := workload[op]; ok {
			entries = append(entries, WeightEntry{Op: op, Weight: w})
		}
	}
	total := 0.0
	for _, e := range entries {
		total += e.Weight
	}
	if total <= 0.0 {
		return []WeightEntry{{Op: "put", Weight: 1.0}}
	}
	for i := range entries {
		entries[i].Weight /= total
	}
	return entries
}

// ReadWeights returns normalized read weights from the resolved workload.
func (p *GenerateParams) ReadWeights() []WeightEntry {
	workload := p.ResolvedWorkload()
	readOps := []string{
		"get", "range_scan", "list",
	}
	var entries []WeightEntry
	for _, op := range readOps {
		if w, ok := workload[op]; ok {
			entries = append(entries, WeightEntry{Op: op, Weight: w})
		}
	}
	total := 0.0
	for _, e := range entries {
		total += e.Weight
	}
	if total <= 0.0 {
		return []WeightEntry{{Op: "get", Weight: 1.0}}
	}
	for i := range entries {
		entries[i].Weight /= total
	}
	return entries
}

// ReadFraction returns the fraction of total ops that are reads.
func (p *GenerateParams) ReadFraction() float64 {
	workload := p.ResolvedWorkload()
	readOps := []string{
		"get", "range_scan", "list",
	}
	total := 0.0
	for _, w := range workload {
		total += w
	}
	if total <= 0.0 {
		return 0.0
	}
	readTotal := 0.0
	for _, op := range readOps {
		if w, ok := workload[op]; ok {
			readTotal += w
		}
	}
	return readTotal / total
}

// WeightEntry is an operation name and its normalized weight.
type WeightEntry struct {
	Op     string
	Weight float64
}

// CreateGenerator creates the appropriate generator for the given params.
func CreateGenerator(params *GenerateParams, vl VersionLookup) Generator {
	switch params.Generator {
	case "kv-cas":
		g := NewCasGenerator(params)
		if vl != nil {
			g.versionLookup = vl
		}
		return g
	default:
		return NewBasicKvGenerator(params)
	}
}
