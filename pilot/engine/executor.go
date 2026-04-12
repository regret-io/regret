package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/regret-io/regret/pilot-go/generator"
	"github.com/regret-io/regret/pilot-go/reference"
	"github.com/regret-io/regret/pilot-go/storage"
)

// ExecutionConfig configures the executor.
type ExecutionConfig struct {
	BatchSize             int     `json:"batch_size"`
	CheckpointIntervalSecs uint64 `json:"checkpoint_interval_secs"`
	FailFast              bool    `json:"fail_fast"`
	DurationSecs          *uint64 `json:"duration_secs,omitempty"`
}

// DefaultExecutionConfig returns the default execution config.
func DefaultExecutionConfig() ExecutionConfig {
	return ExecutionConfig{
		BatchSize:             100,
		CheckpointIntervalSecs: 600,
		FailFast:              true,
	}
}

// ProgressInfo tracks the progress of a running execution.
type ProgressInfo struct {
	TotalOps          int     `json:"total_ops"`
	CompletedOps      int     `json:"completed_ops"`
	TotalBatches      int     `json:"total_batches"`
	CompletedBatches  int     `json:"completed_batches"`
	TotalCheckpoints  int     `json:"total_checkpoints"`
	PassedCheckpoints int     `json:"passed_checkpoints"`
	FailedCheckpoints int     `json:"failed_checkpoints"`
	SafetyViolations  int     `json:"safety_violations"`
	ElapsedSecs       uint64  `json:"elapsed_secs"`
	OpsPerSec         float64 `json:"ops_per_sec"`
}

// StopReason indicates why a run stopped.
type StopReason string

const (
	StopCompleted        StopReason = "completed"
	StopSafetyViolation  StopReason = "safety_violation"
	StopCheckpointFailed StopReason = "checkpoint_failed"
	StopStopped          StopReason = "stopped"
	StopError            StopReason = "error"
)

// AdapterClient is the interface the executor uses to talk to adapters.
type AdapterClient interface {
	ExecuteBatch(ctx context.Context, batchID string, ops []reference.Operation) ([]reference.AdapterOpResult, error)
	ReadState(ctx context.Context, keyPrefix string) (map[string]*reference.RecordState, error)
	Cleanup(ctx context.Context, keyPrefix string) error
}

// Executor is the main execution loop.
type Executor struct {
	HypothesisID  string
	RunID         string
	Config        ExecutionConfig
	Tolerance     *reference.Tolerance
	GenerateParams *generator.GenerateParams
	Reference     reference.ReferenceModel
	Ctx           context.Context
	Cancel        context.CancelFunc
	Progress      *ProgressInfo
	ProgressMu    sync.RWMutex
	Files         *storage.FileStore
	Sqlite        *storage.SqliteStore
	Pebble        storage.PebbleStore
	AdapterClient AdapterClient
}

// Run executes the main loop and returns the stop reason.
func (e *Executor) Run() (reference.ReferenceModel, StopReason) {
	result := e.runInner()

	e.ProgressMu.RLock()
	progress := *e.Progress
	e.ProgressMu.RUnlock()

	var passRate float64
	if progress.TotalCheckpoints > 0 {
		passRate = float64(progress.PassedCheckpoints) / float64(progress.TotalCheckpoints)
	} else if progress.SafetyViolations == 0 {
		passRate = 1.0
	}

	// Store result
	now := Now()
	stopReason := string(result)
	finishedAt := now
	resultRecord := &storage.HypothesisResult{
		ID:                fmt.Sprintf("res-%d", time.Now().UnixNano()),
		HypothesisID:      e.HypothesisID,
		RunID:             e.RunID,
		TotalBatches:      int64(progress.CompletedBatches),
		TotalCheckpoints:  int64(progress.TotalCheckpoints),
		PassedCheckpoints: int64(progress.PassedCheckpoints),
		FailedCheckpoints: int64(progress.FailedCheckpoints),
		TotalResponseOps:  int64(progress.CompletedOps),
		SafetyViolations:  int64(progress.SafetyViolations),
		StopReason:        &stopReason,
		FinishedAt:        &finishedAt,
		CreatedAt:         now,
	}
	if err := e.Sqlite.CreateResult(context.Background(), resultRecord); err != nil {
		slog.Error("failed to store result", slog.Any("error", err))
	}

	// Update hypothesis status
	status := "failed"
	if result == StopCompleted && progress.SafetyViolations == 0 && progress.FailedCheckpoints == 0 {
		status = "passed"
	} else if result == StopStopped {
		status = "stopped"
	}
	if err := e.Sqlite.UpdateHypothesisStatus(context.Background(), e.HypothesisID, status); err != nil {
		slog.Error("failed to update hypothesis status", slog.Any("error", err))
	}

	// Emit final event
	var event Event
	if result == StopStopped {
		event = NewRunStoppedEvent(e.RunID, "manual")
	} else {
		event = NewRunCompletedEvent(e.RunID, string(result), passRate)
	}
	e.emitEvent(event)

	slog.Info("run finished",
		slog.String("hypothesis_id", e.HypothesisID),
		slog.String("run_id", e.RunID),
		slog.String("stop_reason", string(result)),
		slog.Float64("pass_rate", passRate),
	)

	return e.Reference, result
}

func (e *Executor) runInner() StopReason {
	slog.Info("starting execution",
		slog.String("hypothesis_id", e.HypothesisID),
		slog.String("run_id", e.RunID),
	)

	e.emitEvent(NewRunStartedEvent(e.RunID, e.HypothesisID))
	e.Reference.SetRunID(e.RunID)
	e.Reference.Clear()

	// Clean adapter data from previous run
	prefix := e.Reference.KeyPrefix()
	if e.AdapterClient != nil {
		if err := e.AdapterClient.Cleanup(e.Ctx, prefix); err != nil {
			slog.Warn("failed to cleanup adapter, continuing", slog.Any("error", err))
		}
	}

	// Handle watch_start precondition for notification generators
	workload := e.GenerateParams.ResolvedWorkload()
	_, hasNotifications := workload["get_notifications"]
	_, hasSessionRestart := workload["session_restart"]
	if (hasNotifications || hasSessionRestart) && e.AdapterClient != nil {
		watchOp := reference.Operation{
			ID: "precondition-watch",
			Kind: reference.OpKind{
				Type:   reference.OpKindWatchStart,
				Prefix: prefix,
			},
		}
		results, err := e.AdapterClient.ExecuteBatch(e.Ctx, "precondition", []reference.Operation{watchOp})
		if err != nil {
			slog.Warn("watch_start precondition failed", slog.Any("error", err))
		} else {
			for _, r := range results {
				slog.Info("watch_start result", slog.String("op_id", r.OpID), slog.String("status", r.Status))
			}
		}
	}

	// Update key prefix to include run_id, then create generator
	e.GenerateParams.KeySpace.Prefix = fmt.Sprintf("/ref/%s/%s/", e.HypothesisID, e.RunID)
	gen := generator.CreateGenerator(e.GenerateParams, e.Pebble)

	runStart := time.Now()
	var durationSecs uint64
	if e.Config.DurationSecs != nil {
		durationSecs = *e.Config.DurationSecs
	}

	var totalOps int
	batchCounter := 0
	checkpointCounter := 0
	lastCheckpoint := time.Now()
	checkpointInterval := time.Duration(e.Config.CheckpointIntervalSecs) * time.Second

	// Rate limiting
	var batchInterval *time.Duration
	if e.GenerateParams.Rate > 0 {
		interval := time.Duration(float64(time.Second) * float64(e.Config.BatchSize) / float64(e.GenerateParams.Rate))
		batchInterval = &interval
		slog.Info("rate limiting enabled",
			slog.String("rate_ops_sec", fmt.Sprintf("%d", e.GenerateParams.Rate)),
			slog.Int("batch_size", e.Config.BatchSize),
			slog.Int64("batch_interval_ms", interval.Milliseconds()),
		)
	}

	for {
		// Check stop conditions
		select {
		case <-e.Ctx.Done():
			return StopStopped
		default:
		}

		if durationSecs > 0 && uint64(time.Since(runStart).Seconds()) >= durationSecs {
			break
		}

		rawOps := gen.GenBatch(e.Config.BatchSize)
		ops := parseOriginOps(rawOps)

		if len(ops) == 0 {
			break
		}

		// Split into conflict-free sub-batches
		batches := e.splitIntoBatches(ops)

		for _, batch := range batches {
			select {
			case <-e.Ctx.Done():
				return StopStopped
			default:
			}

			batchID := fmt.Sprintf("batch-%04d", batchCounter)
			start := time.Now()

			var failures []reference.SafetyViolation
			if e.AdapterClient != nil {
				var lastErr string
				var results []reference.AdapterOpResult
				for attempt := uint32(1); attempt <= 3; attempt++ {
					var err error
					results, err = e.AdapterClient.ExecuteBatch(e.Ctx, batchID, batch)
					if err == nil {
						break
					}
					lastErr = err.Error()
					if attempt < 3 {
						time.Sleep(time.Duration(500*attempt) * time.Millisecond)
					}
				}
				if results == nil {
					e.emitEvent(NewBatchFailedEvent(e.RunID, batchID, 3, lastErr))
					return StopError
				}

				durationMs := uint64(time.Since(start).Milliseconds())
				response := &reference.AdapterBatchResponse{BatchID: batchID, Results: results}
				failures = e.Reference.ProcessResponse(batch, response, e.Tolerance)

				// Build op records
				failedOps := make(map[string]bool)
				for _, f := range failures {
					failedOps[f.OpID] = true
				}
				opRecords := make([]OpRecord, 0, len(batch))
				for i, op := range batch {
					if i >= len(response.Results) {
						break
					}
					res := response.Results[i]
					isFailed := failedOps[op.ID]
					var resp interface{}
					if isFailed {
						resp = opResponse(&res)
					} else {
						resp = opResponseBrief(&res)
					}
					var expected, actual interface{}
					if isFailed {
						for _, f := range failures {
							if f.OpID == op.ID {
								expected = f.Expected
								actual = f.Actual
								break
							}
						}
					}
					verified := !isFailed
					opRecords = append(opRecords, OpRecord{
						OpID:     op.ID,
						OpType:   opTypeStr(op),
						Payload:  opPayload(op),
						Status:   res.Status,
						Response: resp,
						Expected: expected,
						Actual:   actual,
						Verified: &verified,
					})
				}
				e.emitEvent(NewOperationBatchEvent(e.RunID, batchID, batchCounter, durationMs, opRecords))
			} else {
				// No adapter: reference-only mode
				mockResults := e.buildMockResults(batch)
				durationMs := uint64(time.Since(start).Milliseconds())
				response := &reference.AdapterBatchResponse{BatchID: batchID, Results: mockResults}
				e.Reference.ProcessResponse(batch, response, e.Tolerance)
				opRecords := make([]OpRecord, 0, len(batch))
				for i, op := range batch {
					if i >= len(response.Results) {
						break
					}
					res := response.Results[i]
					opRecords = append(opRecords, OpRecord{
						OpID:     op.ID,
						OpType:   opTypeStr(op),
						Payload:  opPayload(op),
						Status:   res.Status,
						Response: opResponseBrief(&res),
					})
				}
				e.emitEvent(NewOperationBatchEvent(e.RunID, batchID, batchCounter, durationMs, opRecords))
				failures = nil
			}

			if len(failures) > 0 {
				e.ProgressMu.Lock()
				e.Progress.SafetyViolations += len(failures)
				e.ProgressMu.Unlock()
				for _, f := range failures {
					e.emitEvent(NewSafetyViolationEvent(e.RunID, batchID, f.OpID, f.Op, f.Expected, f.Actual))
				}
				if e.Config.FailFast {
					return StopSafetyViolation
				}
			}

			e.ProgressMu.Lock()
			e.Progress.CompletedOps += len(batch)
			e.Progress.CompletedBatches++
			elapsed := uint64(time.Since(runStart).Seconds())
			e.Progress.ElapsedSecs = elapsed
			if elapsed > 0 {
				e.Progress.OpsPerSec = float64(e.Progress.CompletedOps) / float64(elapsed)
			}
			e.ProgressMu.Unlock()

			batchCounter++

			// Rate limiting
			if batchInterval != nil {
				elapsed := time.Since(start)
				if elapsed < *batchInterval {
					time.Sleep(*batchInterval - elapsed)
				}
			}

			// Checkpoint check
			if time.Since(lastCheckpoint) >= checkpointInterval {
				checkpointCounter++
				lastCheckpoint = time.Now()
				r := e.runCheckpoint(checkpointCounter)
				if r == StopCheckpointFailed && e.Config.FailFast {
					return StopCheckpointFailed
				}
			}
		}

		totalOps += len(ops)
		e.ProgressMu.Lock()
		e.Progress.TotalOps = totalOps
		e.ProgressMu.Unlock()
	}

	// Final checkpoint
	checkpointCounter++
	e.runCheckpoint(checkpointCounter)

	return StopCompleted
}

func (e *Executor) splitIntoBatches(ops []reference.Operation) [][]reference.Operation {
	allowKeyConflicts := e.GenerateParams.Generator == "kv-cas"
	batchSize := e.Config.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	var batches [][]reference.Operation
	var writes []reference.Operation
	var reads []reference.Operation
	writeKeys := make(map[string]bool)

	for _, op := range ops {
		isRead := op.Kind.Type == reference.OpKindGet ||
			op.Kind.Type == reference.OpKindList ||
			op.Kind.Type == reference.OpKindScan

		isSolo := op.Kind.Type == reference.OpKindDeleteRange ||
			op.Kind.Type == reference.OpKindSessionRestart ||
			op.Kind.Type == reference.OpKindWatchStart

		if isRead {
			if len(writes) > 0 {
				batches = append(batches, writes)
				writes = nil
				writeKeys = make(map[string]bool)
			}
			reads = append(reads, op)
			if len(reads) >= batchSize {
				batches = append(batches, reads)
				reads = nil
			}
		} else if isSolo {
			if len(writes) > 0 {
				batches = append(batches, writes)
				writes = nil
				writeKeys = make(map[string]bool)
			}
			if len(reads) > 0 {
				batches = append(batches, reads)
				reads = nil
			}
			batches = append(batches, []reference.Operation{op})
		} else {
			// Write op
			if len(reads) > 0 {
				batches = append(batches, reads)
				reads = nil
			}

			if !allowKeyConflicts {
				key := opKey(op)
				if key != "" && writeKeys[key] {
					batches = append(batches, writes)
					writes = nil
					writeKeys = make(map[string]bool)
				}
				if key != "" {
					writeKeys[key] = true
				}
			}

			writes = append(writes, op)
			if len(writes) >= batchSize {
				batches = append(batches, writes)
				writes = nil
				writeKeys = make(map[string]bool)
			}
		}
	}
	if len(writes) > 0 {
		batches = append(batches, writes)
	}
	if len(reads) > 0 {
		batches = append(batches, reads)
	}
	return batches
}

func (e *Executor) runCheckpoint(num int) StopReason {
	id := fmt.Sprintf("ckpt-%04d", num)
	prefix := e.Reference.KeyPrefix()

	e.ProgressMu.Lock()
	e.Progress.TotalCheckpoints++
	e.ProgressMu.Unlock()

	start := time.Now()
	var actual map[string]*reference.RecordState
	if e.AdapterClient != nil {
		var err error
		actual, err = e.AdapterClient.ReadState(e.Ctx, prefix)
		if err != nil {
			slog.Error("checkpoint read_state failed", slog.Any("error", err))
			return StopError
		}
	} else {
		actual = e.Reference.SnapshotAll()
	}

	failures := e.Reference.VerifyCheckpoint(actual, e.Tolerance)
	durationMs := uint64(time.Since(start).Milliseconds())
	expect := e.Reference.SnapshotAll()

	passed := len(failures) == 0

	// Build checkpoint details
	allKeys := make(map[string]bool)
	for k := range expect {
		allKeys[k] = true
	}
	for k := range actual {
		allKeys[k] = true
	}

	failedKeys := make(map[string]bool)
	for _, f := range failures {
		failedKeys[f.Key] = true
	}

	var details []CheckpointDetail
	for key := range allKeys {
		exp := expect[key]
		act := actual[key]
		details = append(details, CheckpointDetail{
			Key:      key,
			Matched:  !failedKeys[key],
			Expected: exp,
			Actual:   act,
		})
	}

	e.emitEvent(NewCheckpointEvent(e.RunID, id, durationMs, passed, details))

	if passed {
		e.ProgressMu.Lock()
		e.Progress.PassedCheckpoints++
		e.ProgressMu.Unlock()
		return StopCompleted
	}

	e.ProgressMu.Lock()
	e.Progress.FailedCheckpoints++
	e.ProgressMu.Unlock()

	_ = e.Files.WriteCheckpoint(e.HypothesisID, expect, actual)
	if e.Config.FailFast {
		return StopCheckpointFailed
	}
	return StopCompleted
}

func (e *Executor) buildMockResults(ops []reference.Operation) []reference.AdapterOpResult {
	var results []reference.AdapterOpResult
	for _, op := range ops {
		if op.ID == "" {
			continue
		}
		status := reference.OpStatusOk
		switch op.Kind.Type {
		case reference.OpKindGet:
			status = reference.OpStatusNotFound
		case reference.OpKindFence:
			continue
		}

		r := reference.AdapterOpResult{
			OpID:   op.ID,
			Status: status,
		}
		if op.Kind.Type == reference.OpKindScan {
			r.Records = []reference.RangeRecord{}
		}
		if op.Kind.Type == reference.OpKindList {
			r.Keys = []string{}
		}
		results = append(results, r)
	}
	return results
}

func (e *Executor) emitEvent(event Event) {
	if err := e.Files.AppendEvent(e.HypothesisID, event.ToJSON()); err != nil {
		slog.Error("failed to write event", slog.Any("error", err))
	}
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func opTypeStr(op reference.Operation) string {
	switch op.Kind.Type {
	case reference.OpKindPut:
		if op.Kind.Sequence {
			return "sequence_put"
		}
		if op.Kind.Ephemeral {
			return "ephemeral_put"
		}
		if op.Kind.IndexName != "" {
			return "indexed_put"
		}
		return "put"
	case reference.OpKindGet:
		switch op.Kind.Comparison {
		case reference.ComparisonFloor:
			return "get_floor"
		case reference.ComparisonCeiling:
			return "get_ceiling"
		case reference.ComparisonLower:
			return "get_lower"
		case reference.ComparisonHigher:
			return "get_higher"
		default:
			return "get"
		}
	case reference.OpKindDelete:
		return "delete"
	case reference.OpKindDeleteRange:
		return "delete_range"
	case reference.OpKindList:
		if op.Kind.IndexName != "" {
			return "indexed_list"
		}
		return "list"
	case reference.OpKindScan:
		if op.Kind.IndexName != "" {
			return "indexed_range_scan"
		}
		return "range_scan"
	case reference.OpKindCas:
		return "cas"
	case reference.OpKindWatchStart:
		return "watch_start"
	case reference.OpKindSessionRestart:
		return "session_restart"
	case reference.OpKindGetNotifications:
		return "get_notifications"
	case reference.OpKindFence:
		return "fence"
	default:
		return "unknown"
	}
}

func opPayload(op reference.Operation) interface{} {
	k := op.Kind
	switch k.Type {
	case reference.OpKindPut:
		if k.Sequence {
			return map[string]interface{}{"prefix": k.Prefix, "value": k.Value, "delta": k.Delta, "sequence": true}
		}
		m := map[string]interface{}{"key": k.Key, "value": k.Value}
		if k.Ephemeral {
			m["ephemeral"] = true
		}
		if k.IndexName != "" {
			m["index_name"] = k.IndexName
			m["index_key"] = k.IndexKey
		}
		return m
	case reference.OpKindGet:
		return map[string]interface{}{"key": k.Key}
	case reference.OpKindDelete:
		return map[string]interface{}{"key": k.Key}
	case reference.OpKindDeleteRange:
		return map[string]interface{}{"start": k.Start, "end": k.End}
	case reference.OpKindList:
		m := map[string]interface{}{"start": k.Start, "end": k.End}
		if k.IndexName != "" {
			m["index_name"] = k.IndexName
		}
		return m
	case reference.OpKindScan:
		m := map[string]interface{}{"start": k.Start, "end": k.End}
		if k.IndexName != "" {
			m["index_name"] = k.IndexName
		}
		return m
	case reference.OpKindCas:
		return map[string]interface{}{"key": k.Key, "expected_version_id": k.ExpectedVersionID, "new_value": k.NewValue}
	case reference.OpKindWatchStart:
		return map[string]interface{}{"prefix": k.Prefix}
	default:
		return map[string]interface{}{}
	}
}

func opKey(op reference.Operation) string {
	switch op.Kind.Type {
	case reference.OpKindPut:
		if op.Kind.Sequence {
			return op.Kind.Prefix
		}
		return op.Kind.Key
	case reference.OpKindGet, reference.OpKindDelete, reference.OpKindCas:
		return op.Kind.Key
	default:
		return ""
	}
}

func opResponse(res *reference.AdapterOpResult) interface{} {
	m := make(map[string]interface{})
	if res.Value != nil {
		m["value"] = *res.Value
	}
	if res.VersionID != nil {
		m["version_id"] = *res.VersionID
	}
	if res.Records != nil {
		m["records"] = res.Records
	}
	if res.Keys != nil {
		m["keys"] = res.Keys
	}
	if res.DeletedCount != nil {
		m["deleted_count"] = *res.DeletedCount
	}
	if res.Message != nil {
		m["message"] = *res.Message
	}
	return m
}

func opResponseBrief(res *reference.AdapterOpResult) interface{} {
	m := make(map[string]interface{})
	if res.Value != nil {
		m["value"] = *res.Value
	}
	if res.VersionID != nil {
		m["version_id"] = *res.VersionID
	}
	if res.DeletedCount != nil {
		m["deleted_count"] = *res.DeletedCount
	}
	if res.Message != nil {
		m["message"] = *res.Message
	}
	return m
}

// parseOriginOps converts generator OriginOps to reference Operations.
func parseOriginOps(rawOps []generator.OriginOp) []reference.Operation {
	var ops []reference.Operation
	for _, raw := range rawOps {
		if raw.IsFence || raw.Operation == nil {
			continue
		}
		op := parseOriginOp(raw)
		if op != nil {
			ops = append(ops, *op)
		}
	}
	return ops
}

func parseOriginOp(raw generator.OriginOp) *reference.Operation {
	if raw.IsFence || raw.Operation == nil {
		return nil
	}
	// Marshal to JSON and parse, matching the Rust approach
	data, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return parseOriginOpFromJSON(m)
}

func parseOriginOpFromJSON(m map[string]interface{}) *reference.Operation {
	id, _ := m["id"].(string)
	opType, _ := m["op"].(string)
	if id == "" || opType == "" {
		return nil
	}

	getString := func(key string) string {
		s, _ := m[key].(string)
		return s
	}
	getUint64 := func(key string) uint64 {
		if f, ok := m[key].(float64); ok {
			return uint64(f)
		}
		return 0
	}

	getBool := func(key string) bool {
		b, _ := m[key].(bool)
		return b
	}
	getInt64 := func(key string) int64 {
		if f, ok := m[key].(float64); ok {
			return int64(f)
		}
		return 0
	}

	parseComparison := func(s string) reference.GetComparison {
		switch s {
		case "floor":
			return reference.ComparisonFloor
		case "ceiling":
			return reference.ComparisonCeiling
		case "lower":
			return reference.ComparisonLower
		case "higher":
			return reference.ComparisonHigher
		default:
			return reference.ComparisonEqual
		}
	}

	op := &reference.Operation{ID: id}
	switch opType {
	case "put":
		if getBool("sequence") {
			delta := getInt64("delta")
			if delta == 0 {
				delta = 1
			}
			op.Kind = reference.OpKind{Type: reference.OpKindPut, Value: getString("value"), Sequence: true, Prefix: getString("prefix"), Delta: delta}
		} else {
			op.Kind = reference.OpKind{
				Type:      reference.OpKindPut,
				Key:       getString("key"),
				Value:     getString("value"),
				Ephemeral: getBool("ephemeral"),
				IndexName: getString("index_name"),
				IndexKey:  getString("index_key"),
			}
		}
	case "get":
		comp := parseComparison(getString("comparison"))
		op.Kind = reference.OpKind{Type: reference.OpKindGet, Key: getString("key"), Comparison: comp}
	// Support legacy op names for backwards compatibility with existing JSONL files
	case "get_floor":
		op.Kind = reference.OpKind{Type: reference.OpKindGet, Key: getString("key"), Comparison: reference.ComparisonFloor}
	case "get_ceiling":
		op.Kind = reference.OpKind{Type: reference.OpKindGet, Key: getString("key"), Comparison: reference.ComparisonCeiling}
	case "get_lower":
		op.Kind = reference.OpKind{Type: reference.OpKindGet, Key: getString("key"), Comparison: reference.ComparisonLower}
	case "get_higher":
		op.Kind = reference.OpKind{Type: reference.OpKindGet, Key: getString("key"), Comparison: reference.ComparisonHigher}
	case "delete":
		op.Kind = reference.OpKind{Type: reference.OpKindDelete, Key: getString("key")}
	case "delete_range":
		op.Kind = reference.OpKind{Type: reference.OpKindDeleteRange, Start: getString("start"), End: getString("end")}
	case "list":
		op.Kind = reference.OpKind{Type: reference.OpKindList, Start: getString("start"), End: getString("end"), IndexName: getString("index_name")}
	case "scan", "range_scan":
		op.Kind = reference.OpKind{Type: reference.OpKindScan, Start: getString("start"), End: getString("end"), IndexName: getString("index_name")}
	case "cas":
		op.Kind = reference.OpKind{Type: reference.OpKindCas, Key: getString("key"), ExpectedVersionID: getUint64("expected_version_id"), NewValue: getString("new_value")}
	// Legacy op names
	case "ephemeral_put":
		op.Kind = reference.OpKind{Type: reference.OpKindPut, Key: getString("key"), Value: getString("value"), Ephemeral: true}
	case "indexed_put":
		op.Kind = reference.OpKind{Type: reference.OpKindPut, Key: getString("key"), Value: getString("value"), IndexName: getString("index_name"), IndexKey: getString("index_key")}
	case "indexed_get":
		indexKey := getString("index_key")
		endKey := indexKey + "\x00"
		op.Kind = reference.OpKind{Type: reference.OpKindList, IndexName: getString("index_name"), Start: indexKey, End: endKey}
	case "indexed_list":
		op.Kind = reference.OpKind{Type: reference.OpKindList, IndexName: getString("index_name"), Start: getString("start"), End: getString("end")}
	case "indexed_range_scan":
		op.Kind = reference.OpKind{Type: reference.OpKindScan, IndexName: getString("index_name"), Start: getString("start"), End: getString("end")}
	case "sequence_put":
		delta := getInt64("delta")
		if delta == 0 {
			delta = 1
		}
		op.Kind = reference.OpKind{Type: reference.OpKindPut, Value: getString("value"), Sequence: true, Prefix: getString("prefix"), Delta: delta}
	case "watch_start":
		op.Kind = reference.OpKind{Type: reference.OpKindWatchStart, Prefix: getString("prefix")}
		// Also check legacy "key" field
		if op.Kind.Prefix == "" {
			op.Kind.Prefix = getString("key")
		}
	case "session_restart":
		op.Kind = reference.OpKind{Type: reference.OpKindSessionRestart}
	case "get_notifications":
		op.Kind = reference.OpKind{Type: reference.OpKindGetNotifications}
	default:
		return nil
	}
	return op
}
