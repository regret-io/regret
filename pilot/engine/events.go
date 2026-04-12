package engine

import (
	"encoding/json"
	"fmt"
	"time"
)

// OpRecord captures the result of a single operation within a batch.
type OpRecord struct {
	OpID     string      `json:"op_id"`
	OpType   string      `json:"op_type"`
	Payload  interface{} `json:"payload"`
	Status   string      `json:"status"`
	Response interface{} `json:"response"`
	Expected interface{} `json:"expected,omitempty"`
	Actual   interface{} `json:"actual,omitempty"`
	Verified *bool       `json:"verified,omitempty"`
}

// CheckpointDetail records the comparison result for a single key.
type CheckpointDetail struct {
	Key      string      `json:"key"`
	Matched  bool        `json:"matched"`
	Expected interface{} `json:"expected"`
	Actual   interface{} `json:"actual"`
}

// EventType enumerates the kinds of events.
type EventType string

const (
	EventTypeRunStarted      EventType = "RunStarted"
	EventTypeOperationBatch  EventType = "OperationBatch"
	EventTypeBatchFailed     EventType = "BatchFailed"
	EventTypeSafetyViolation EventType = "SafetyViolation"
	EventTypeCheckpoint      EventType = "Checkpoint"
	EventTypeRunCompleted    EventType = "RunCompleted"
	EventTypeRunStopped      EventType = "RunStopped"
)

// Event represents a single event in the events.jsonl stream.
type Event struct {
	Type         EventType          `json:"type"`
	RunID        string             `json:"run_id"`
	Timestamp    string             `json:"timestamp"`
	HypothesisID string             `json:"hypothesis_id,omitempty"`
	BatchID      string             `json:"batch_id,omitempty"`
	Offset       int                `json:"offset,omitempty"`
	Size         int                `json:"size,omitempty"`
	DurationMs   uint64             `json:"duration_ms,omitempty"`
	Ops          []OpRecord         `json:"ops,omitempty"`
	Attempts     uint32             `json:"attempts,omitempty"`
	Error        string             `json:"error,omitempty"`
	OpID         string             `json:"op_id,omitempty"`
	Op           string             `json:"op,omitempty"`
	Expected     string             `json:"expected,omitempty"`
	Actual       string             `json:"actual,omitempty"`
	CheckpointID string             `json:"checkpoint_id,omitempty"`
	Keys         int                `json:"keys,omitempty"`
	Passed       *bool              `json:"passed,omitempty"`
	Details      []CheckpointDetail `json:"details,omitempty"`
	StopReason   string             `json:"stop_reason,omitempty"`
	PassRate     *float64           `json:"pass_rate,omitempty"`
}

// Now returns the current UTC time in ISO8601 format with milliseconds.
func Now() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// NewRunStartedEvent creates a RunStarted event.
func NewRunStartedEvent(runID, hypothesisID string) Event {
	return Event{
		Type:         EventTypeRunStarted,
		RunID:        runID,
		HypothesisID: hypothesisID,
		Timestamp:    Now(),
	}
}

// NewOperationBatchEvent creates an OperationBatch event.
func NewOperationBatchEvent(runID, batchID string, offset int, durationMs uint64, ops []OpRecord) Event {
	return Event{
		Type:       EventTypeOperationBatch,
		RunID:      runID,
		BatchID:    batchID,
		Offset:     offset,
		Size:       len(ops),
		DurationMs: durationMs,
		Ops:        ops,
		Timestamp:  Now(),
	}
}

// NewBatchFailedEvent creates a BatchFailed event.
func NewBatchFailedEvent(runID, batchID string, attempts uint32, errMsg string) Event {
	return Event{
		Type:      EventTypeBatchFailed,
		RunID:     runID,
		BatchID:   batchID,
		Attempts:  attempts,
		Error:     errMsg,
		Timestamp: Now(),
	}
}

// NewSafetyViolationEvent creates a SafetyViolation event.
func NewSafetyViolationEvent(runID, batchID, opID, op, expected, actual string) Event {
	return Event{
		Type:      EventTypeSafetyViolation,
		RunID:     runID,
		BatchID:   batchID,
		OpID:      opID,
		Op:        op,
		Expected:  expected,
		Actual:    actual,
		Timestamp: Now(),
	}
}

// NewCheckpointEvent creates a Checkpoint event.
func NewCheckpointEvent(runID, checkpointID string, durationMs uint64, passed bool, details []CheckpointDetail) Event {
	keys := len(details)
	return Event{
		Type:         EventTypeCheckpoint,
		RunID:        runID,
		CheckpointID: checkpointID,
		Keys:         keys,
		Passed:       &passed,
		DurationMs:   durationMs,
		Details:      details,
		Timestamp:    Now(),
	}
}

// NewRunCompletedEvent creates a RunCompleted event.
func NewRunCompletedEvent(runID, stopReason string, passRate float64) Event {
	return Event{
		Type:       EventTypeRunCompleted,
		RunID:      runID,
		StopReason: stopReason,
		PassRate:   &passRate,
		Timestamp:  Now(),
	}
}

// NewRunStoppedEvent creates a RunStopped event.
func NewRunStoppedEvent(runID, stopReason string) Event {
	return Event{
		Type:       EventTypeRunStopped,
		RunID:      runID,
		StopReason: stopReason,
		Timestamp:  Now(),
	}
}

// ToJSON serializes the event to a JSON string.
func (e *Event) ToJSON() string {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf(`{"type":"error","message":"%s"}`, err.Error())
	}
	return string(data)
}
