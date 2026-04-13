package endpoints

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/regret-io/regret/pilot-go/database"
	"github.com/regret-io/regret/pilot-go/ext"
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

	cfg, configYAML, err := adapterConfigFromRequest(&req)
	if err != nil {
		WriteError(w, BadRequest(err.Error()))
		return
	}
	envJSON, _ := json.Marshal(adapterEnvJSONMap(cfg))

	// Check name uniqueness
	existing, _ := a.state.Sqlite.GetAdapterByName(ctx, req.Name)
	if existing != nil {
		WriteError(w, Conflict(fmt.Sprintf("adapter '%s' already exists", req.Name)))
		return
	}

	id := fmt.Sprintf("adp-%d", ext.NowUnixMilli())

	// Step 1: Save to DB
	adapter, err := a.state.Sqlite.CreateAdapter(ctx, id, req.Name, cfg.Image, string(envJSON), configYAML)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	// Step 2: Deploy Pod + Service to K8s (if kube client is available)
	if a.state.KubeClient != nil {
		if err := a.deployAdapterToK8s(req.Name, id, cfg); err != nil {
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

func (a *adapterHandlers) deployAdapterToK8s(adapterName, id string, cfg *AdapterConfig) error {
	service := buildAdapterService(a.state.Namespace, id, adapterName)
	deployment := buildAdapterDeployment(a.state.Namespace, id, adapterName, cfg)

	if _, err := a.state.KubeClient.CoreV1().Services(a.state.Namespace).Create(context.Background(), service, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	if _, err := a.state.KubeClient.AppsV1().Deployments(a.state.Namespace).Create(context.Background(), deployment, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		if _, err := a.state.KubeClient.AppsV1().Deployments(a.state.Namespace).Update(context.Background(), deployment, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

// deleteAdapterFromK8s removes the adapter Pod + Service.
func (a *adapterHandlers) deleteAdapterFromK8s(name string) {
	ctx := context.Background()
	_ = a.state.KubeClient.AppsV1().Deployments(a.state.Namespace).Delete(ctx, adapterWorkloadName(name), metav1.DeleteOptions{})
	_ = a.state.KubeClient.CoreV1().Services(a.state.Namespace).Delete(ctx, adapterWorkloadName(name), metav1.DeleteOptions{})
}

func toAdapterResponse(a *database.AdapterRecord) AdapterResponse {
	var env interface{}
	if err := json.Unmarshal([]byte(a.Env), &env); err != nil {
		env = map[string]interface{}{}
	}
	configYAML := a.ConfigYAML
	if configYAML == "" {
		envMap := map[string]string{}
		_ = json.Unmarshal([]byte(a.Env), &envMap)
		if generated, err := legacyAdapterConfigYAML(a.Image, envMap); err == nil {
			configYAML = generated
		}
	}
	return AdapterResponse{
		ID:         a.ID,
		Name:       a.Name,
		ConfigYAML: configYAML,
		Image:      a.Image,
		Env:        env,
		CreatedAt:  a.CreatedAt,
	}
}
