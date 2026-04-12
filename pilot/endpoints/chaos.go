package endpoints

import (
	"github.com/regret-io/regret/pilot-go/ext"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	chaospkg "github.com/regret-io/regret/pilot-go/chaos"
	"github.com/regret-io/regret/pilot-go/database"
)

type chaosHandlers struct {
	state *AppState
}

// CreateScenario handles POST /api/chaos/scenarios.
func (c *chaosHandlers) CreateScenario(w http.ResponseWriter, r *http.Request) {
	var req CreateScenarioRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, BadRequest("invalid JSON: "+err.Error()))
		return
	}

	if req.Namespace == "" {
		req.Namespace = "regret-system"
	}
	if len(req.Actions) == 0 {
		WriteError(w, BadRequest("actions must not be empty"))
		return
	}

	id := fmt.Sprintf("cs-%d", ext.NowUnixMilli())
	actionsJSON, _ := json.Marshal(req.Actions)

	record, err := c.state.Sqlite.CreateChaosScenario(r.Context(), id, req.Name, req.Namespace, string(actionsJSON))
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	WriteJSON(w, http.StatusCreated, scenarioToResponse(record))
}

// ListScenarios handles GET /api/chaos/scenarios.
func (c *chaosHandlers) ListScenarios(w http.ResponseWriter, r *http.Request) {
	records, err := c.state.Sqlite.ListChaosScenarios(r.Context())
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}
	var items []ScenarioResponse
	for i := range records {
		items = append(items, scenarioToResponse(&records[i]))
	}
	WriteJSON(w, http.StatusOK, map[string]interface{}{"items": items})
}

// GetScenario handles GET /api/chaos/scenarios/{id}.
func (c *chaosHandlers) GetScenario(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	record, err := c.state.Sqlite.GetChaosScenario(r.Context(), id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("chaos scenario %s not found", id)))
		return
	}
	WriteJSON(w, http.StatusOK, scenarioToResponse(record))
}

// UpdateScenario handles PUT /api/chaos/scenarios/{id}.
func (c *chaosHandlers) UpdateScenario(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	_, err := c.state.Sqlite.GetChaosScenario(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("chaos scenario %s not found", id)))
		return
	}

	var req CreateScenarioRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, BadRequest("invalid JSON: "+err.Error()))
		return
	}

	if len(req.Actions) == 0 {
		WriteError(w, BadRequest("actions must not be empty"))
		return
	}

	actionsJSON, _ := json.Marshal(req.Actions)

	if err := c.state.Sqlite.UpdateChaosScenario(ctx, id, req.Name, req.Namespace, string(actionsJSON)); err != nil {
		WriteError(w, InternalError(err))
		return
	}

	record, err := c.state.Sqlite.GetChaosScenario(ctx, id)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}
	WriteJSON(w, http.StatusOK, scenarioToResponse(record))
}

// DeleteScenario handles DELETE /api/chaos/scenarios/{id}.
func (c *chaosHandlers) DeleteScenario(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	deleted, err := c.state.Sqlite.DeleteChaosScenario(r.Context(), id)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}
	if !deleted {
		WriteError(w, NotFound(fmt.Sprintf("chaos scenario %s not found", id)))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// StartInjection handles POST /api/chaos/scenarios/{id}/inject.
func (c *chaosHandlers) StartInjection(w http.ResponseWriter, r *http.Request) {
	scenarioID := chi.URLParam(r, "id")
	ctx := r.Context()

	record, err := c.state.Sqlite.GetChaosScenario(ctx, scenarioID)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("chaos scenario %s not found", scenarioID)))
		return
	}

	var actions []chaospkg.ChaosAction
	if err := json.Unmarshal([]byte(record.Actions), &actions); err != nil {
		WriteError(w, InternalError(fmt.Errorf("invalid actions JSON: %w", err)))
		return
	}

	// Apply overrides if provided
	var req InjectRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if len(req.Overrides) > 0 {
		for i := range actions {
			if override, ok := req.Overrides[actions[i].ActionType]; ok {
				var overrideObj map[string]interface{}
				if json.Unmarshal(override, &overrideObj) == nil {
					if actions[i].Params == nil {
						actions[i].Params = make(map[string]interface{})
					}
					for k, v := range overrideObj {
						actions[i].Params[k] = v
					}
				}
			}
		}
	}

	scenario := chaospkg.ChaosScenario{
		ID:        record.ID,
		Name:      record.Name,
		Namespace: record.Namespace,
		Actions:   actions,
	}

	injectionID, err := c.state.Chaos.StartInjection(ctx, scenario)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	WriteJSON(w, http.StatusAccepted, StartInjectionResponse{
		InjectionID: injectionID,
		ScenarioID:  record.ID,
		Status:      "running",
	})
}

// StopInjection handles POST /api/chaos/injections/{id}/stop.
func (c *chaosHandlers) StopInjection(w http.ResponseWriter, r *http.Request) {
	injectionID := chi.URLParam(r, "id")
	if err := c.state.Chaos.StopInjection(r.Context(), injectionID); err != nil {
		WriteError(w, InternalError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListInjections handles GET /api/chaos/injections.
func (c *chaosHandlers) ListInjections(w http.ResponseWriter, r *http.Request) {
	records, err := c.state.Sqlite.ListChaosInjections(r.Context())
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}
	var items []InjectionResponse
	for _, rec := range records {
		items = append(items, InjectionResponse{
			ID:           rec.ID,
			ScenarioID:   rec.ScenarioID,
			ScenarioName: rec.ScenarioName,
			Status:       rec.Status,
			StartedAt:    rec.StartedAt,
			FinishedAt:   rec.FinishedAt,
			Error:        rec.Error,
		})
	}
	WriteJSON(w, http.StatusOK, map[string]interface{}{"items": items})
}

// GetInjection handles GET /api/chaos/injections/{id}.
func (c *chaosHandlers) GetInjection(w http.ResponseWriter, r *http.Request) {
	injectionID := chi.URLParam(r, "id")
	record, err := c.state.Sqlite.GetChaosInjection(r.Context(), injectionID)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("chaos injection %s not found", injectionID)))
		return
	}
	WriteJSON(w, http.StatusOK, InjectionResponse{
		ID:           record.ID,
		ScenarioID:   record.ScenarioID,
		ScenarioName: record.ScenarioName,
		Status:       record.Status,
		StartedAt:    record.StartedAt,
		FinishedAt:   record.FinishedAt,
		Error:        record.Error,
	})
}

// DeleteInjection handles DELETE /api/chaos/injections/{id}.
func (c *chaosHandlers) DeleteInjection(w http.ResponseWriter, r *http.Request) {
	injectionID := chi.URLParam(r, "id")
	ctx := r.Context()

	// Stop if still running
	if c.state.Chaos.IsActive(injectionID) {
		_ = c.state.Chaos.StopInjection(ctx, injectionID)
	}

	deleted, err := c.state.Sqlite.DeleteChaosInjection(ctx, injectionID)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}
	if !deleted {
		WriteError(w, NotFound(fmt.Sprintf("chaos injection %s not found", injectionID)))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ChaosEvents handles GET /api/chaos/events.
func (c *chaosHandlers) ChaosEvents(w http.ResponseWriter, r *http.Request) {
	events, err := c.state.Files.ReadChaosEvents(nil)
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func scenarioToResponse(r *database.ChaosScenarioRecord) ScenarioResponse {
	var actions []json.RawMessage
	_ = json.Unmarshal([]byte(r.Actions), &actions)
	return ScenarioResponse{
		ID:        r.ID,
		Name:      r.Name,
		Namespace: r.Namespace,
		Actions:   actions,
		CreatedAt: r.CreatedAt,
	}
}
