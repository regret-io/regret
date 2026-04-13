package endpoints

import (
	"encoding/json"

	"github.com/regret-io/regret/pilot-go/engine"
)

// ---------------------------------------------------------------------------
// Hypothesis CRUD
// ---------------------------------------------------------------------------

// CreateHypothesisRequest is the request body for creating a hypothesis.
type CreateHypothesisRequest struct {
	Name            string           `json:"name"`
	Generator       string           `json:"generator"`
	Adapter         *string          `json:"adapter,omitempty"`
	AdapterAddr     *string          `json:"adapter_addr,omitempty"`
	Duration        *string          `json:"duration,omitempty"`
	CheckpointEvery string           `json:"checkpoint_every"`
	Tolerance       *json.RawMessage `json:"tolerance,omitempty"`
	KeySpace        *json.RawMessage `json:"key_space,omitempty"`
}

// HypothesisResponse is the response for a single hypothesis.
type HypothesisResponse struct {
	ID              string           `json:"id"`
	Name            string           `json:"name"`
	Generator       string           `json:"generator"`
	Adapter         *string          `json:"adapter,omitempty"`
	AdapterAddr     *string          `json:"adapter_addr,omitempty"`
	Duration        *string          `json:"duration,omitempty"`
	CheckpointEvery string           `json:"checkpoint_every"`
	Tolerance       *json.RawMessage `json:"tolerance,omitempty"`
	Status          string           `json:"status"`
	CreatedAt       string           `json:"created_at"`
	LastRunAt       *string          `json:"last_run_at,omitempty"`
}

// HypothesisListResponse is the response for listing hypotheses.
type HypothesisListResponse struct {
	Items []HypothesisResponse `json:"items"`
}

// ---------------------------------------------------------------------------
// Run Control
// ---------------------------------------------------------------------------

// StartRunResponse is the response for starting a run.
type StartRunResponse struct {
	RunID        string `json:"run_id"`
	HypothesisID string `json:"hypothesis_id"`
	Status       string `json:"status"`
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

// StatusResponse is the response for hypothesis status.
type StatusResponse struct {
	HypothesisID string               `json:"hypothesis_id"`
	Status       string               `json:"status"`
	RunID        *string              `json:"run_id,omitempty"`
	Progress     *engine.ProgressInfo `json:"progress,omitempty"`
}

// ---------------------------------------------------------------------------
// Events query
// ---------------------------------------------------------------------------

// EventsQuery is the query parameters for the events endpoint.
type EventsQuery struct {
	RunID     *string `json:"run_id"`
	EventType *string `json:"type"`
	Since     *string `json:"since"`
	Last      *int    `json:"last"`
}

// ---------------------------------------------------------------------------
// Bundle query
// ---------------------------------------------------------------------------

// BundleQuery is the query parameters for the bundle endpoint.
type BundleQuery struct {
	RunID *string `json:"run_id"`
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

// CreateAdapterRequest is the request body for creating an adapter.
type CreateAdapterRequest struct {
	Name       string            `json:"name"`
	ConfigYAML string            `json:"config_yaml"`
	Image      string            `json:"image"`
	Env        map[string]string `json:"env"`
}

// AdapterResponse is the response for a single adapter.
type AdapterResponse struct {
	ID         string      `json:"id"`
	Name       string      `json:"name"`
	ConfigYAML string      `json:"config_yaml"`
	Image      string      `json:"image"`
	Env        interface{} `json:"env"`
	CreatedAt  string      `json:"created_at"`
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

// CreateGeneratorRequest is the request body for creating a generator.
type CreateGeneratorRequest struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	Workload    map[string]float64 `json:"workload"`
	Rate        uint32             `json:"rate"`
}

// GeneratorResponse is the response for a single generator.
type GeneratorResponse struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Workload    interface{} `json:"workload"`
	Rate        int64       `json:"rate"`
	Builtin     bool        `json:"builtin"`
	CreatedAt   string      `json:"created_at"`
}

// ---------------------------------------------------------------------------
// Chaos
// ---------------------------------------------------------------------------

// CreateScenarioRequest is the request body for creating a chaos scenario.
type CreateScenarioRequest struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Actions   []json.RawMessage `json:"actions"`
}

// ScenarioResponse is the response for a single chaos scenario.
type ScenarioResponse struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Actions   []json.RawMessage `json:"actions"`
	CreatedAt string            `json:"created_at"`
}

// InjectionResponse is the response for a single chaos injection.
type InjectionResponse struct {
	ID           string  `json:"id"`
	ScenarioID   string  `json:"scenario_id"`
	ScenarioName string  `json:"scenario_name"`
	Status       string  `json:"status"`
	StartedAt    string  `json:"started_at"`
	FinishedAt   *string `json:"finished_at,omitempty"`
	Error        *string `json:"error,omitempty"`
}

// StartInjectionResponse is the response for starting a chaos injection.
type StartInjectionResponse struct {
	InjectionID string `json:"injection_id"`
	ScenarioID  string `json:"scenario_id"`
	Status      string `json:"status"`
}

// InjectRequest is the request body for starting a chaos injection.
type InjectRequest struct {
	Overrides map[string]json.RawMessage `json:"overrides"`
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

// MetricsQuery is the query parameters for the metrics endpoint.
type MetricsQuery struct {
	RunID  *string `json:"run_id"`
	Since  *int64  `json:"since"`
	Metric *string `json:"metric"`
}

// MetricsResponse is the response for the metrics endpoint.
type MetricsResponse struct {
	HypothesisID string        `json:"hypothesis_id"`
	RunID        string        `json:"run_id"`
	Metrics      []MetricGroup `json:"metrics"`
}

// MetricGroup is a group of metric series with the same name.
type MetricGroup struct {
	Name   string         `json:"name"`
	Series []MetricSeries `json:"series"`
}

// MetricSeries is a single metric series with labels and time-ordered points.
type MetricSeries struct {
	Labels interface{}  `json:"labels"`
	Points [][2]float64 `json:"points"`
}
