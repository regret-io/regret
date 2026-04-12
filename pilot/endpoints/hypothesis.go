package endpoints

import (
	"context"
	"log/slog"

	"github.com/regret-io/regret/pilot-go/ext"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/regret-io/regret/pilot-go/adapter"
	"github.com/regret-io/regret/pilot-go/database"
	"github.com/regret-io/regret/pilot-go/engine"
	"github.com/regret-io/regret/pilot-go/generator"
)

type hypothesisHandlers struct {
	state *AppState
}

// Create handles POST /api/hypothesis.
func (h *hypothesisHandlers) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateHypothesisRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, BadRequest("invalid JSON: "+err.Error()))
		return
	}
	if req.CheckpointEvery == "" {
		req.CheckpointEvery = "10m"
	}

	ctx := r.Context()

	// Check name uniqueness
	existing, _ := h.state.Sqlite.GetHypothesisByName(ctx, req.Name)
	if existing != nil {
		WriteError(w, Conflict(fmt.Sprintf("hypothesis '%s' already exists", req.Name)))
		return
	}

	// Validate generator
	gen, err := h.state.Sqlite.GetGenerator(ctx, req.Generator)
	if err != nil || gen == nil {
		WriteError(w, BadRequest(fmt.Sprintf("unknown generator: %s", req.Generator)))
		return
	}

	// Validate adapter
	if req.Adapter != nil {
		a, err := h.state.Sqlite.GetAdapterByName(ctx, *req.Adapter)
		if err != nil || a == nil {
			WriteError(w, BadRequest(fmt.Sprintf("unknown adapter: %s", *req.Adapter)))
			return
		}
	}

	id := fmt.Sprintf("hyp-%d", ext.NowUnixMilli())

	var toleranceJSON string
	if req.Tolerance != nil {
		toleranceJSON = string(*req.Tolerance)
	}

	var keySpaceJSON string
	if req.KeySpace != nil {
		keySpaceJSON = string(*req.KeySpace)
	} else {
		keySpaceJSON = "{}"
	}

	adapterStr := ""
	if req.Adapter != nil {
		adapterStr = *req.Adapter
	}
	adapterAddrStr := ""
	if req.AdapterAddr != nil {
		adapterAddrStr = *req.AdapterAddr
	}
	durationStr := ""
	if req.Duration != nil {
		durationStr = *req.Duration
	}

	hyp, err := h.state.Sqlite.CreateHypothesis(ctx, id, req.Name, req.Generator,
		adapterStr, adapterAddrStr, durationStr, toleranceJSON, req.CheckpointEvery, keySpaceJSON, "{}")
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	_ = h.state.Files.CreateHypothesisDir(id)

	tolPtr := &toleranceJSON
	if toleranceJSON == "" {
		tolPtr = nil
	}
	h.state.Managers.CreateFromHypothesis(id, req.Generator, tolPtr)

	WriteJSON(w, http.StatusCreated, toHypothesisResponse(hyp))
}

// List handles GET /api/hypothesis.
func (h *hypothesisHandlers) List(w http.ResponseWriter, r *http.Request) {
	items, err := h.state.Sqlite.ListHypotheses(r.Context())
	if err != nil {
		slog.Error("ListHypotheses failed", "error", err)
		WriteError(w, InternalError(err))
		return
	}
	slog.Debug("ListHypotheses", "count", len(items))

	resp := make([]HypothesisResponse, 0, len(items))
	for i := range items {
		resp = append(resp, toHypothesisResponse(&items[i]))
	}
	WriteJSON(w, http.StatusOK, HypothesisListResponse{Items: resp})
}

// GetOne handles GET /api/hypothesis/{id}.
func (h *hypothesisHandlers) GetOne(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	hyp, err := h.state.Sqlite.GetHypothesis(r.Context(), id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}
	WriteJSON(w, http.StatusOK, toHypothesisResponse(hyp))
}

// Update handles PUT /api/hypothesis/{id}.
func (h *hypothesisHandlers) Update(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	_, err := h.state.Sqlite.GetHypothesis(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}

	var req CreateHypothesisRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, BadRequest("invalid JSON: "+err.Error()))
		return
	}

	gen, err := h.state.Sqlite.GetGenerator(ctx, req.Generator)
	if err != nil || gen == nil {
		WriteError(w, BadRequest(fmt.Sprintf("unknown generator: %s", req.Generator)))
		return
	}

	var toleranceJSON string
	if req.Tolerance != nil {
		toleranceJSON = string(*req.Tolerance)
	}

	adapterStr := ""
	if req.Adapter != nil {
		adapterStr = *req.Adapter
	}
	adapterAddrStr := ""
	if req.AdapterAddr != nil {
		adapterAddrStr = *req.AdapterAddr
	}
	durationStr := ""
	if req.Duration != nil {
		durationStr = *req.Duration
	}
	checkpointEvery := req.CheckpointEvery
	if checkpointEvery == "" {
		checkpointEvery = "10m"
	}
	keySpaceJSON := "{}"
	if req.KeySpace != nil {
		keySpaceJSON = string(*req.KeySpace)
	}

	if err := h.state.Sqlite.UpdateHypothesis(ctx, id, req.Name, req.Generator,
		adapterStr, adapterAddrStr, durationStr, toleranceJSON, checkpointEvery, keySpaceJSON); err != nil {
		WriteError(w, InternalError(err))
		return
	}

	hyp, err := h.state.Sqlite.GetHypothesis(ctx, id)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}
	WriteJSON(w, http.StatusOK, toHypothesisResponse(hyp))
}

// Delete handles DELETE /api/hypothesis/{id}.
func (h *hypothesisHandlers) Delete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	mgr := h.state.Managers.Get(id)
	if mgr != nil {
		if mgr.IsRunning() {
			_ = mgr.StopRun()
		}
	}
	h.state.Managers.Remove(id)
	_ = h.state.Sqlite.DeleteHypothesis(ctx, id)
	_ = h.state.Files.DeleteHypothesisDir(id)

	// Best-effort adapter cleanup in background
	adapters, _ := h.state.Sqlite.ListAdapters(ctx)
	keyPrefix := fmt.Sprintf("/%s/", id)
	go func() {
		for _, a := range adapters {
			addr := fmt.Sprintf("http://adapter-%s:9090", a.Name)
			_ = adapter.CleanupPrefix(addr, keyPrefix)
		}
	}()

	w.WriteHeader(http.StatusNoContent)
}

// StartRun handles POST /api/hypothesis/{id}/run.
func (h *hypothesisHandlers) StartRun(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	hyp, err := h.state.Sqlite.GetHypothesis(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}

	mgr := h.state.Managers.Get(id)
	if mgr == nil {
		WriteError(w, NotFound(fmt.Sprintf("manager for %s not found", id)))
		return
	}

	if mgr.IsRunning() {
		WriteError(w, Conflict("hypothesis is already running"))
		return
	}

	durationSecs := ext.ParseDuration(hyp.Duration)
	checkpointIntervalSecs := ext.ParseDurationSecs(hyp.CheckpointEvery)
	if checkpointIntervalSecs == 0 {
		checkpointIntervalSecs = 600
	}

	config := engine.ExecutionConfig{
		BatchSize:              100,
		CheckpointIntervalSecs: checkpointIntervalSecs,
		FailFast:               true,
		DurationSecs:           durationSecs,
	}

	var adapterRecord *storageAdapterRecord
	if hyp.Adapter != nil {
		a, err := h.state.Sqlite.GetAdapterByName(ctx, *hyp.Adapter)
		if err != nil {
			WriteError(w, NotFound(fmt.Sprintf("adapter '%s' not found", *hyp.Adapter)))
			return
		}
		adapterRecord = a
	}

	genRecord, _ := h.state.Sqlite.GetGenerator(ctx, hyp.Generator)
	var rate uint32
	if genRecord != nil {
		rate = uint32(genRecord.Rate)
	}
	skipWarmup := strings.Contains(hyp.Generator, "notification")

	genParams := generator.DefaultGenerateParams()
	genParams.Generator = hyp.Generator
	genParams.Ops = math.MaxInt32
	genParams.SkipWarmup = skipWarmup
	genParams.Rate = rate

	// Apply stored key_space config (preserve defaults for zero values)
	if hyp.KeySpace != "" {
		var ks generator.KeySpaceConfig
		if json.Unmarshal([]byte(hyp.KeySpace), &ks) == nil {
			if ks.Count > 0 {
				genParams.KeySpace.Count = ks.Count
			}
			if ks.Prefix != "" {
				genParams.KeySpace.Prefix = ks.Prefix
			}
		}
	}
	genParams.KeySpace.Prefix = fmt.Sprintf("/%s/", id)

	// Connect to adapter if specified
	var adapterClient engine.AdapterClient
	if adapterRecord != nil {
		addr := ""
		if hyp.AdapterAddr != nil && *hyp.AdapterAddr != "" {
			addr = *hyp.AdapterAddr
		} else {
			addr = fmt.Sprintf("http://adapter-%s:9090", adapterRecord.Name)
		}
		client, err := adapter.Connect(addr)
		if err == nil {
			adapterClient = client
		}
	}

	runID, _, err := mgr.StartRun(context.Background(), config, genParams, adapterRecord, hyp.AdapterAddr, adapterClient)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	WriteJSON(w, http.StatusAccepted, StartRunResponse{
		RunID:        runID,
		HypothesisID: id,
		Status:       "running",
	})
}

// StopRun handles DELETE /api/hypothesis/{id}/run.
func (h *hypothesisHandlers) StopRun(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	mgr := h.state.Managers.Get(id)
	if mgr == nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}
	if err := mgr.StopRun(); err != nil {
		WriteError(w, InternalError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetStatus handles GET /api/hypothesis/{id}/status.
func (h *hypothesisHandlers) GetStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	hyp, err := h.state.Sqlite.GetHypothesis(r.Context(), id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}

	resp := StatusResponse{
		HypothesisID: id,
		Status:       hyp.Status,
	}

	mgr := h.state.Managers.Get(id)
	if mgr != nil {
		resp.RunID = mgr.RunID()
		resp.Progress = mgr.Progress()
	}

	WriteJSON(w, http.StatusOK, resp)
}

// GetEvents handles GET /api/hypothesis/{id}/events.
func (h *hypothesisHandlers) GetEvents(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	_, err := h.state.Sqlite.GetHypothesis(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}

	q := r.URL.Query()
	runID := queryPtr(q, "run_id")
	eventType := queryPtr(q, "type")
	since := queryPtr(q, "since")
	last := queryIntPtr(q, "last")

	events, err := h.state.Files.ReadMergedEvents(id, runID, eventType, since, last)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	for _, event := range events {
		data, _ := json.Marshal(event)
		w.Write(data)
		w.Write([]byte("\n"))
	}
}

// GetResults handles GET /api/hypothesis/{id}/results.
func (h *hypothesisHandlers) GetResults(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	_, err := h.state.Sqlite.GetHypothesis(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}

	results, err := h.state.Sqlite.GetResults(ctx, id)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	type resultItem struct {
		ID                string  `json:"id"`
		RunID             string  `json:"run_id"`
		TotalBatches      int64   `json:"total_batches"`
		TotalCheckpoints  int64   `json:"total_checkpoints"`
		PassedCheckpoints int64   `json:"passed_checkpoints"`
		FailedCheckpoints int64   `json:"failed_checkpoints"`
		TotalResponseOps  int64   `json:"total_response_ops"`
		SafetyViolations  int64   `json:"safety_violations"`
		StopReason        *string `json:"stop_reason,omitempty"`
		StartedAt         *string `json:"started_at,omitempty"`
		FinishedAt        *string `json:"finished_at,omitempty"`
	}

	items := make([]resultItem, 0, len(results))
	for _, r := range results {
		items = append(items, resultItem{
			ID: r.ID, RunID: r.RunID, TotalBatches: r.TotalBatches,
			TotalCheckpoints: r.TotalCheckpoints, PassedCheckpoints: r.PassedCheckpoints,
			FailedCheckpoints: r.FailedCheckpoints, TotalResponseOps: r.TotalResponseOps,
			SafetyViolations: r.SafetyViolations, StopReason: r.StopReason,
			StartedAt: r.StartedAt, FinishedAt: r.FinishedAt,
		})
	}

	WriteJSON(w, http.StatusOK, map[string]interface{}{"items": items})
}

// DeleteResult handles DELETE /api/hypothesis/{id}/results/{resultID}.
func (h *hypothesisHandlers) DeleteResult(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	resultID := chi.URLParam(r, "resultID")
	ctx := r.Context()

	_, err := h.state.Sqlite.GetHypothesis(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}

	deleted, err := h.state.Sqlite.DeleteResult(ctx, resultID)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}
	if !deleted {
		WriteError(w, NotFound(fmt.Sprintf("result %s not found", resultID)))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GetBundle handles GET /api/hypothesis/{id}/bundle.
func (h *hypothesisHandlers) GetBundle(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	_, err := h.state.Sqlite.GetHypothesis(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}

	runID := queryPtr(r.URL.Query(), "run_id")
	data, err := h.state.Files.CreateBundle(id, runID)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="hypothesis-%s.zip"`, id))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// storageAdapterRecord is a type alias to avoid import cycles.
type storageAdapterRecord = database.AdapterRecord

func toHypothesisResponse(hyp *database.Hypothesis) HypothesisResponse {
	resp := HypothesisResponse{
		ID:              hyp.ID,
		Name:            hyp.Name,
		Generator:       hyp.Generator,
		Adapter:         hyp.Adapter,
		AdapterAddr:     hyp.AdapterAddr,
		Duration:        hyp.Duration,
		CheckpointEvery: hyp.CheckpointEvery,
		Status:          hyp.Status,
		CreatedAt:       hyp.CreatedAt,
		LastRunAt:       hyp.LastRunAt,
	}
	if hyp.Tolerance != nil && *hyp.Tolerance != "" {
		raw := json.RawMessage(*hyp.Tolerance)
		resp.Tolerance = &raw
	}
	return resp
}

func queryPtr(q map[string][]string, key string) *string {
	if vals, ok := q[key]; ok && len(vals) > 0 && vals[0] != "" {
		return &vals[0]
	}
	return nil
}

func queryIntPtr(q map[string][]string, key string) *int {
	if vals, ok := q[key]; ok && len(vals) > 0 {
		v, err := strconv.Atoi(vals[0])
		if err == nil {
			return &v
		}
	}
	return nil
}


// Ensure time import is used.
var _ = time.Now
