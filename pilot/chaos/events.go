package chaos

import (
	"encoding/json"
	"time"
)

// ChaosEventType enumerates chaos event variants.
type ChaosEventType string

const (
	EventChaosInjected    ChaosEventType = "ChaosInjected"
	EventChaosRecovered   ChaosEventType = "ChaosRecovered"
	EventChaosError       ChaosEventType = "ChaosError"
	EventInjectionStarted ChaosEventType = "InjectionStarted"
	EventInjectionStopped ChaosEventType = "InjectionStopped"
	EventWorkflowStarted  ChaosEventType = "WorkflowStarted"
	EventWorkflowStopped  ChaosEventType = "WorkflowStopped"
	EventWorkflowStep     ChaosEventType = "WorkflowStep"
)

// ChaosEvent is written to /data/chaos/events.jsonl.
// Only the fields relevant to the event type are populated.
type ChaosEvent struct {
	Type          ChaosEventType `json:"type"`
	InjectionID   string         `json:"injection_id"`
	ScenarioName  string         `json:"scenario_name"`
	WorkflowID    string         `json:"workflow_id,omitempty"`
	WorkflowRunID string         `json:"workflow_run_id,omitempty"`
	StepName      string         `json:"step_name,omitempty"`
	StepIndex     *int           `json:"step_index,omitempty"`
	Timestamp     string         `json:"timestamp"`

	// ChaosInjected / ChaosRecovered fields
	ActionType string   `json:"action_type,omitempty"`
	TargetPods []string `json:"target_pods,omitempty"`
	Namespace  string   `json:"namespace,omitempty"`

	// ChaosRecovered
	DurationMs uint64 `json:"duration_ms,omitempty"`

	// ChaosError
	Error string `json:"error,omitempty"`

	// InjectionStopped
	Reason string `json:"reason,omitempty"`
}

// Now returns the current UTC time formatted as an RFC-3339 timestamp with
// millisecond precision, matching the Rust implementation.
func Now() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// ToJSON serializes the event to a JSON string.
func (e *ChaosEvent) ToJSON() string {
	b, err := json.Marshal(e)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// -- Convenience constructors -------------------------------------------------

// NewChaosInjectedEvent creates a ChaosInjected event.
func NewChaosInjectedEvent(injectionID, scenarioName, actionType, namespace string, targetPods []string) ChaosEvent {
	return ChaosEvent{
		Type:         EventChaosInjected,
		InjectionID:  injectionID,
		ScenarioName: scenarioName,
		ActionType:   actionType,
		TargetPods:   targetPods,
		Namespace:    namespace,
		Timestamp:    Now(),
	}
}

// NewChaosRecoveredEvent creates a ChaosRecovered event.
func NewChaosRecoveredEvent(injectionID, scenarioName, actionType string, targetPods []string, durationMs uint64) ChaosEvent {
	return ChaosEvent{
		Type:         EventChaosRecovered,
		InjectionID:  injectionID,
		ScenarioName: scenarioName,
		ActionType:   actionType,
		TargetPods:   targetPods,
		DurationMs:   durationMs,
		Timestamp:    Now(),
	}
}

// NewChaosErrorEvent creates a ChaosError event.
func NewChaosErrorEvent(injectionID, scenarioName, actionType, errMsg string) ChaosEvent {
	return ChaosEvent{
		Type:         EventChaosError,
		InjectionID:  injectionID,
		ScenarioName: scenarioName,
		ActionType:   actionType,
		Error:        errMsg,
		Timestamp:    Now(),
	}
}

// NewInjectionStartedEvent creates an InjectionStarted event.
func NewInjectionStartedEvent(injectionID, scenarioName string) ChaosEvent {
	return ChaosEvent{
		Type:         EventInjectionStarted,
		InjectionID:  injectionID,
		ScenarioName: scenarioName,
		Timestamp:    Now(),
	}
}

// NewInjectionStoppedEvent creates an InjectionStopped event.
func NewInjectionStoppedEvent(injectionID, scenarioName, reason string) ChaosEvent {
	return ChaosEvent{
		Type:         EventInjectionStopped,
		InjectionID:  injectionID,
		ScenarioName: scenarioName,
		Reason:       reason,
		Timestamp:    Now(),
	}
}

func NewWorkflowStartedEvent(workflowID, workflowRunID, workflowName string) ChaosEvent {
	return ChaosEvent{
		Type:          EventWorkflowStarted,
		WorkflowID:    workflowID,
		WorkflowRunID: workflowRunID,
		ScenarioName:  workflowName,
		Timestamp:     Now(),
	}
}

func NewWorkflowStoppedEvent(workflowID, workflowRunID, workflowName, reason string) ChaosEvent {
	return ChaosEvent{
		Type:          EventWorkflowStopped,
		WorkflowID:    workflowID,
		WorkflowRunID: workflowRunID,
		ScenarioName:  workflowName,
		Reason:        reason,
		Timestamp:     Now(),
	}
}

func NewWorkflowStepEvent(workflowID, workflowRunID, workflowName, stepName, reason string, stepIndex int) ChaosEvent {
	return ChaosEvent{
		Type:          EventWorkflowStep,
		WorkflowID:    workflowID,
		WorkflowRunID: workflowRunID,
		ScenarioName:  workflowName,
		StepName:      stepName,
		StepIndex:     &stepIndex,
		Reason:        reason,
		Timestamp:     Now(),
	}
}
