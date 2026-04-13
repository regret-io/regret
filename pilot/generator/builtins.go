package generator

// GeneratorInfo describes a built-in generator.
type GeneratorInfo struct {
	Name        string
	Description string
	Rate        uint32
}

// ListGenerators returns all built-in generators.
func ListGenerators() []GeneratorInfo {
	return []GeneratorInfo{
		{
			Name:        "basic-kv",
			Description: "Basic key-value operations: put, get, delete, delete_range, list, range_scan",
			Rate:        258,
		},
		{
			Name:        "kv-cas",
			Description: "CAS conflict correctness: duplicate put-with-version ops in each batch, verify exactly-one-wins invariant",
			Rate:        258,
		},
		{
			Name:        "kv-ephemeral-notification",
			Description: "Ephemeral lifecycle + notification delivery verification",
			Rate:        258,
		},
		{
			Name:        "kv-secondary-index",
			Description: "Basic KV + secondary-index access using base put/get/list/range_scan ops with index params",
			Rate:        258,
		},
		{
			Name:        "kv-sequence",
			Description: "Sequence key puts with server-assigned monotonic suffixes + reads + subscription notifications",
			Rate:        258,
		},
	}
}

// GetGeneratorWorkload returns the workload weights for a predefined generator.
// Falls back to basic-kv if the name is unknown.
func GetGeneratorWorkload(name string) map[string]float64 {
	switch name {
	case "basic-kv":
		return basicKV()
	case "kv-cas":
		return kvCas()
	case "kv-ephemeral-notification":
		return kvEphemeralNotification()
	case "kv-secondary-index":
		return kvSecondaryIndex()
	case "kv-sequence":
		return kvSequence()
	default:
		return basicKV()
	}
}

func basicKV() map[string]float64 {
	return map[string]float64{
		"put":          0.20,
		"get":          0.15,
		"get_floor":    0.05,
		"get_ceiling":  0.05,
		"get_lower":    0.05,
		"get_higher":   0.05,
		"delete":       0.08,
		"delete_range": 0.02,
		"range_scan":   0.15,
		"list":         0.10,
	}
}

func kvCas() map[string]float64 {
	return map[string]float64{
		"cas": 1.0,
	}
}

func kvEphemeralNotification() map[string]float64 {
	return map[string]float64{
		"put":               0.30,
		"delete":            0.10,
		"delete_range":      0.05,
		"get":               0.20,
		"list":              0.10,
		"get_notifications": 0.10,
		"session_restart":   0.05,
		"range_scan":        0.10,
	}
}

func kvSecondaryIndex() map[string]float64 {
	return map[string]float64{
		"put":        0.40,
		"delete":     0.05,
		"get":        0.25,
		"list":       0.15,
		"range_scan": 0.15,
	}
}

func kvSequence() map[string]float64 {
	return map[string]float64{
		"put":               0.50,
		"get":               0.15,
		"list":              0.10,
		"range_scan":        0.10,
		"get_notifications": 0.10,
		"session_restart":   0.05,
	}
}
