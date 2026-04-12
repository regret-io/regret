package endpoints

import (
	"github.com/regret-io/regret/pilot-go/ext"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/regret-io/regret/pilot-go/database"
)

type adapterHandlers struct {
	state *AppState
}

// Create handles POST /api/adapters.
func (a *adapterHandlers) Create(w http.ResponseWriter, r *http.Request) {
	var req CreateAdapterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, BadRequest("invalid JSON: "+err.Error()))
		return
	}

	ctx := r.Context()

	// Check name uniqueness
	existing, _ := a.state.Sqlite.GetAdapterByName(ctx, req.Name)
	if existing != nil {
		WriteError(w, Conflict(fmt.Sprintf("adapter '%s' already exists", req.Name)))
		return
	}

	id := fmt.Sprintf("adp-%d", ext.NowUnixMilli())
	envJSON, _ := json.Marshal(req.Env)

	// Step 1: Save to DB
	adapter, err := a.state.Sqlite.CreateAdapter(ctx, id, req.Name, req.Image, string(envJSON))
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	// Step 2: Deploy Pod + Service to K8s (if kube client is available)
	if a.state.KubeClient != nil {
		if err := a.deployAdapterToK8s(req, id); err != nil {
			// Rollback DB record
			slog.Warn("failed to deploy adapter, rolling back", slog.Any("error", err))
			_, _ = a.state.Sqlite.DeleteAdapter(ctx, id)
			WriteError(w, InternalError(fmt.Errorf("failed to deploy adapter: %w", err)))
			return
		}
	}

	WriteJSON(w, http.StatusCreated, toAdapterResponse(adapter))
}

// List handles GET /api/adapters.
func (a *adapterHandlers) List(w http.ResponseWriter, r *http.Request) {
	adapters, err := a.state.Sqlite.ListAdapters(r.Context())
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	items := make([]AdapterResponse, 0, len(adapters))
	for i := range adapters {
		items = append(items, toAdapterResponse(&adapters[i]))
	}
	WriteJSON(w, http.StatusOK, map[string]interface{}{"items": items})
}

// GetOne handles GET /api/adapters/{id}.
func (a *adapterHandlers) GetOne(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	adapter, err := a.state.Sqlite.GetAdapter(r.Context(), id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("adapter %s not found", id)))
		return
	}
	WriteJSON(w, http.StatusOK, toAdapterResponse(adapter))
}

// Delete handles DELETE /api/adapters/{id}.
func (a *adapterHandlers) Delete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	adapterRec, err := a.state.Sqlite.GetAdapter(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("adapter %s not found", id)))
		return
	}

	// Delete K8s resources (best-effort)
	if a.state.KubeClient != nil {
		a.deleteAdapterFromK8s(adapterRec.Name)
	}

	deleted, err := a.state.Sqlite.DeleteAdapter(ctx, id)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}
	if !deleted {
		WriteError(w, NotFound(fmt.Sprintf("adapter %s not found", id)))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// deployAdapterToK8s deploys the adapter Pod + Service.
// This is a placeholder that will be wired to client-go later.
func (a *adapterHandlers) deployAdapterToK8s(req CreateAdapterRequest, id string) error {
	// Pod spec:
	// - name: adapter-{req.Name}
	// - namespace: a.state.Namespace
	// - container: "adapter" with image req.Image
	// - ports: 9090, 9091
	// - env: req.Env
	// - imagePullPolicy: Always
	//
	// Service spec:
	// - name: adapter-{req.Name}
	// - ClusterIP
	// - ports: 9090, 9091
	//
	// Labels: app=adapter-{req.Name}, regret.io/adapter={req.Name}, regret.io/adapter-id={id}

	// TODO: implement with client-go when KubeClient is wired
	return nil
}

// deleteAdapterFromK8s removes the adapter Pod + Service.
func (a *adapterHandlers) deleteAdapterFromK8s(name string) {
	// podName := fmt.Sprintf("adapter-%s", name)
	// Delete service, then pod (best-effort)
	// TODO: implement with client-go when KubeClient is wired
}

func toAdapterResponse(a *database.AdapterRecord) AdapterResponse {
	var env interface{}
	if err := json.Unmarshal([]byte(a.Env), &env); err != nil {
		env = map[string]interface{}{}
	}
	return AdapterResponse{
		ID:        a.ID,
		Name:      a.Name,
		Image:     a.Image,
		Env:       env,
		CreatedAt: a.CreatedAt,
	}
}
