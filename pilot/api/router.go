package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/regret-io/regret/pilot-go/chaos"
	"github.com/regret-io/regret/pilot-go/engine"
	"github.com/regret-io/regret/pilot-go/storage"
)

// AppState holds all shared application state.
type AppState struct {
	Sqlite     *storage.SqliteStore
	Pebble     storage.PebbleStore
	Files      *storage.FileStore
	Managers   *engine.ManagerRegistry
	Chaos      *chaos.ChaosRegistry
	KubeClient interface{} // placeholder for client-go, wire later
	Namespace  string
}

// NewRouter creates the chi router with all routes, CORS, and optional auth.
func NewRouter(state *AppState, authPassword *string) http.Handler {
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	if authPassword != nil {
		r.Use(BasicAuth(*authPassword))
	}

	// Health
	r.Get("/health", HealthHandler)
	r.Get("/metrics", MetricsPlaceholderHandler)

	// Hypothesis CRUD
	h := &hypothesisHandlers{state: state}
	r.Post("/api/hypothesis", h.Create)
	r.Get("/api/hypothesis", h.List)
	r.Get("/api/hypothesis/{id}", h.GetOne)
	r.Put("/api/hypothesis/{id}", h.Update)
	r.Delete("/api/hypothesis/{id}", h.Delete)

	// Run control
	r.Post("/api/hypothesis/{id}/run", h.StartRun)
	r.Delete("/api/hypothesis/{id}/run", h.StopRun)

	// Observability
	r.Get("/api/hypothesis/{id}/status", h.GetStatus)
	r.Get("/api/hypothesis/{id}/events", h.GetEvents)
	r.Get("/api/hypothesis/{id}/results", h.GetResults)
	r.Delete("/api/hypothesis/{id}/results/{resultID}", h.DeleteResult)
	r.Get("/api/hypothesis/{id}/bundle", h.GetBundle)
	r.Get("/api/hypothesis/{id}/metrics", (&metricsHandlers{state: state}).GetMetrics)

	// Adapters
	a := &adapterHandlers{state: state}
	r.Post("/api/adapters", a.Create)
	r.Get("/api/adapters", a.List)
	r.Get("/api/adapters/{id}", a.GetOne)
	r.Delete("/api/adapters/{id}", a.Delete)

	// Generators
	g := &generatorHandlers{state: state}
	r.Get("/api/generators", g.List)
	r.Post("/api/generators", g.Create)
	r.Get("/api/generators/{name}", g.GetOne)
	r.Put("/api/generators/{name}", g.Update)
	r.Delete("/api/generators/{name}", g.Delete)

	// Chaos -- Scenarios
	c := &chaosHandlers{state: state}
	r.Post("/api/chaos/scenarios", c.CreateScenario)
	r.Get("/api/chaos/scenarios", c.ListScenarios)
	r.Get("/api/chaos/scenarios/{id}", c.GetScenario)
	r.Put("/api/chaos/scenarios/{id}", c.UpdateScenario)
	r.Delete("/api/chaos/scenarios/{id}", c.DeleteScenario)

	// Chaos -- Injections
	r.Post("/api/chaos/scenarios/{id}/inject", c.StartInjection)
	r.Get("/api/chaos/injections", c.ListInjections)
	r.Get("/api/chaos/injections/{id}", c.GetInjection)
	r.Delete("/api/chaos/injections/{id}", c.DeleteInjection)
	r.Post("/api/chaos/injections/{id}/stop", c.StopInjection)

	// Chaos -- Events
	r.Get("/api/chaos/events", c.ChaosEvents)

	return r
}
