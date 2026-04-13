package chaos

// ChaosScenario is a reusable template defining what chaos to inject.
type ChaosScenario struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Namespace string        `json:"namespace"`
	Actions   []ChaosAction `json:"actions"`
}

// ChaosAction defines a single chaos action.
type ChaosAction struct {
	// Action type: pod_kill, pod_restart, network_partition, network_delay,
	// network_loss, rolling_update, custom, upgrade_test.
	ActionType string `json:"action_type"`

	// Label selector for target pods.
	Selector LabelSelector `json:"selector"`

	// Specific pod name (overrides selector).
	TargetPod *string `json:"target_pod,omitempty"`

	// Interval between repeated injections (e.g. "30s", "1m").
	Interval *string `json:"interval,omitempty"`

	// One-time injection at this offset from start (e.g. "2m", "5m").
	At *string `json:"at,omitempty"`

	// Duration of the chaos effect (for network chaos).
	Duration *string `json:"duration,omitempty"`

	// Parameters for specific action types.
	Params map[string]any `json:"params,omitempty"`
}

// LabelSelector selects pods by labels with a selection mode.
type LabelSelector struct {
	// Match labels: {"app": "oxia", "component": "server"}.
	MatchLabels map[string]string `json:"match_labels,omitempty"`

	// Percentage of matching pods to target (0-100, default 100).
	Percentage uint32 `json:"percentage,omitempty"`

	// Mode: "one" (random one), "all", "fixed" (exact count), "percentage".
	Mode string `json:"mode,omitempty"`

	// Fixed count when mode="fixed".
	Count *uint32 `json:"count,omitempty"`
}

// DefaultLabelSelector returns a LabelSelector with sensible defaults.
func DefaultLabelSelector() LabelSelector {
	return LabelSelector{
		MatchLabels: make(map[string]string),
		Percentage:  100,
		Mode:        "one",
	}
}
