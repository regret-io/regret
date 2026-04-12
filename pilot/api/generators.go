package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/regret-io/regret/pilot-go/storage"
)

type generatorHandlers struct {
	state *AppState
}

// List handles GET /api/generators.
func (g *generatorHandlers) List(w http.ResponseWriter, r *http.Request) {
	generators, err := g.state.Sqlite.ListGenerators(r.Context())
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	items := make([]GeneratorResponse, 0, len(generators))
	for i := range generators {
		items = append(items, toGeneratorResponse(&generators[i]))
	}
	WriteJSON(w, http.StatusOK, map[string]interface{}{"items": items})
}

// GetOne handles GET /api/generators/{name}.
func (g *generatorHandlers) GetOne(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	record, err := g.state.Sqlite.GetGenerator(r.Context(), name)
	if err != nil || record == nil {
		WriteError(w, NotFound(fmt.Sprintf("generator '%s' not found", name)))
		return
	}
	WriteJSON(w, http.StatusOK, toGeneratorResponse(record))
}

// Create handles POST /api/generators.
func (g *generatorHandlers) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateGeneratorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, BadRequest("invalid JSON: "+err.Error()))
		return
	}

	ctx := r.Context()

	existing, _ := g.state.Sqlite.GetGenerator(ctx, req.Name)
	if existing != nil {
		WriteError(w, Conflict(fmt.Sprintf("generator '%s' already exists", req.Name)))
		return
	}

	workloadJSON, err := json.Marshal(req.Workload)
	if err != nil {
		WriteError(w, BadRequest(err.Error()))
		return
	}

	if err := g.state.Sqlite.UpsertGenerator(ctx, req.Name, req.Description, string(workloadJSON), int64(req.Rate), 0); err != nil {
		WriteError(w, InternalError(err))
		return
	}

	record, err := g.state.Sqlite.GetGenerator(ctx, req.Name)
	if err != nil || record == nil {
		WriteError(w, InternalError(fmt.Errorf("failed to read just-created generator")))
		return
	}

	WriteJSON(w, http.StatusCreated, toGeneratorResponse(record))
}

// Update handles PUT /api/generators/{name}.
func (g *generatorHandlers) Update(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	ctx := r.Context()

	existing, err := g.state.Sqlite.GetGenerator(ctx, name)
	if err != nil || existing == nil {
		WriteError(w, NotFound(fmt.Sprintf("generator '%s' not found", name)))
		return
	}

	var req CreateGeneratorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, BadRequest("invalid JSON: "+err.Error()))
		return
	}

	workloadJSON, err := json.Marshal(req.Workload)
	if err != nil {
		WriteError(w, BadRequest(err.Error()))
		return
	}

	builtin := 0
	if existing.Builtin != 0 {
		builtin = 1
	}

	if err := g.state.Sqlite.UpsertGenerator(ctx, name, req.Description, string(workloadJSON), int64(req.Rate), builtin); err != nil {
		WriteError(w, InternalError(err))
		return
	}

	record, err := g.state.Sqlite.GetGenerator(ctx, name)
	if err != nil || record == nil {
		WriteError(w, InternalError(fmt.Errorf("failed to read updated generator")))
		return
	}

	WriteJSON(w, http.StatusOK, toGeneratorResponse(record))
}

// Delete handles DELETE /api/generators/{name}.
func (g *generatorHandlers) Delete(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	ctx := r.Context()

	record, err := g.state.Sqlite.GetGenerator(ctx, name)
	if err != nil || record == nil {
		WriteError(w, NotFound(fmt.Sprintf("generator '%s' not found", name)))
		return
	}

	if record.Builtin != 0 {
		WriteError(w, Conflict("cannot delete built-in generator"))
		return
	}

	if _, err := g.state.Sqlite.DeleteGenerator(ctx, name); err != nil {
		WriteError(w, InternalError(err))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func toGeneratorResponse(g *storage.GeneratorRecord) GeneratorResponse {
	var workload interface{}
	if err := json.Unmarshal([]byte(g.Workload), &workload); err != nil {
		workload = map[string]interface{}{}
	}
	return GeneratorResponse{
		Name:        g.Name,
		Description: g.Description,
		Workload:    workload,
		Rate:        g.Rate,
		Builtin:     g.Builtin != 0,
		CreatedAt:   g.CreatedAt,
	}
}
