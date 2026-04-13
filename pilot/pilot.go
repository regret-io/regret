package pilot

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	adapterPkg "github.com/regret-io/regret/pilot-go/adapter"
	"github.com/regret-io/regret/pilot-go/chaos"
	"github.com/regret-io/regret/pilot-go/database"
	"github.com/regret-io/regret/pilot-go/endpoints"
	"github.com/regret-io/regret/pilot-go/engine"
	"github.com/regret-io/regret/pilot-go/eventlog"
	"github.com/regret-io/regret/pilot-go/ext"
	"github.com/regret-io/regret/pilot-go/generator"
	"github.com/regret-io/regret/pilot-go/reference"
)

// Pilot is the main application struct that owns all components.
type Pilot struct {
	dataDir  string
	sqlite   *database.SqliteStore
	refStore *reference.ReferenceStore
	events   *eventlog.EventLog
	managers *engine.ManagerRegistry
	chaos    *chaos.ChaosRegistry
	server   *http.Server

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// chaosManagerAdapter wraps engine.ManagerRegistry to satisfy chaos.ManagerRegistry interface.
type chaosManagerAdapter struct {
	managers *engine.ManagerRegistry
}

func (a *chaosManagerAdapter) Get(id string) interface{} {
	return a.managers.Get(id)
}

// New creates a new Pilot instance, initializing all storage and components.
func New(dataDir string, httpPort uint16, namespace string, authPassword *string) (*Pilot, error) {
	dbPath := filepath.Join(dataDir, "regret.db")
	pebblePath := filepath.Join(dataDir, "pebble")

	sqlite, err := database.NewSqliteStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("init sqlite: %w", err)
	}

	refStore, err := reference.NewReferenceStore(pebblePath)
	if err != nil {
		return nil, fmt.Errorf("init reference store: %w", err)
	}

	events := eventlog.NewEventLog(dataDir)

	shared := engine.SharedServices{
		Sqlite:   sqlite,
		RefStore: refStore,
		Events:   events,
	}
	managers := engine.NewManagerRegistry(shared)

	chaosRegistry := chaos.NewChaosRegistry(events, sqlite, &chaosManagerAdapter{managers: managers})
	kubeClient := buildKubeClient()

	state := &endpoints.AppState{
		Sqlite:     sqlite,
		Files:      events,
		Managers:   managers,
		Chaos:      chaosRegistry,
		KubeClient: kubeClient,
		Namespace:  namespace,
	}

	router := endpoints.NewRouter(state, authPassword)
	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", httpPort),
		Handler: router,
	}

	return &Pilot{
		dataDir:  dataDir,
		sqlite:   sqlite,
		refStore: refStore,
		events:   events,
		managers: managers,
		chaos:    chaosRegistry,
		server:   server,
	}, nil
}

func buildKubeClient() kubernetes.Interface {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		slog.Warn("kubernetes client unavailable", "error", err)
		return nil
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		slog.Warn("kubernetes client init failed", "error", err)
		return nil
	}
	return client
}

// Start seeds data, resumes running hypotheses, and starts the HTTP server.
// It blocks until ctx is cancelled, then shuts down gracefully.
func (p *Pilot) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	// Seed built-in data
	p.seedGenerators(ctx)
	p.seedChaosScenarios(ctx)

	// Load and resume hypotheses
	if err := p.loadHypotheses(ctx); err != nil {
		return fmt.Errorf("load hypotheses: %w", err)
	}
	if err := p.resumeChaosInjections(ctx); err != nil {
		return fmt.Errorf("resume chaos injections: %w", err)
	}

	// Start HTTP server
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		slog.Info("HTTP server starting", "addr", p.server.Addr)
		if err := p.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	slog.Info("shutting down")

	return p.shutdown()
}

// shutdown gracefully stops all components.
func (p *Pilot) shutdown() error {
	// Stop HTTP server
	if err := p.server.Close(); err != nil {
		slog.Warn("HTTP server close error", "error", err)
	}

	// Wait for HTTP server goroutine to finish
	p.wg.Wait()

	// Close storage
	if err := p.refStore.Close(); err != nil {
		slog.Warn("reference store close error", "error", err)
	}

	slog.Info("shutdown complete")
	return nil
}

func (p *Pilot) seedGenerators(ctx context.Context) {
	for _, gen := range generator.ListGenerators() {
		workload := generator.GetGeneratorWorkload(gen.Name)
		workloadJSON, _ := json.Marshal(workload)
		_ = p.sqlite.UpsertGenerator(ctx, gen.Name, gen.Description, string(workloadJSON), int64(gen.Rate), 1)
	}
	slog.Info("built-in generators seeded")
}

func (p *Pilot) seedChaosScenarios(ctx context.Context) {
	scenarios := []struct {
		Name, Namespace string
		Actions         []map[string]any
	}{
		{
			Name: "builtin-continuous-chaos", Namespace: "regret",
			Actions: []map[string]any{
				{"action_type": "pod_kill", "interval": "10m", "selector": map[string]any{"match_labels": map[string]string{}, "mode": "one", "percentage": 100}},
				{"action_type": "network_delay", "interval": "15m", "duration": "2m", "selector": map[string]any{"match_labels": map[string]string{}, "mode": "one", "percentage": 100}, "params": map[string]any{"delay_ms": 200}},
			},
		},
		{
			Name: "builtin-upgrade-test", Namespace: "regret",
			Actions: []map[string]any{
				{"action_type": "upgrade_test", "selector": map[string]any{"match_labels": map[string]string{}, "mode": "all", "percentage": 100}, "params": map[string]any{"resource": "statefulset/oxia", "namespace": "regret", "candidate_image": "", "stable_image": "", "checkpoints_per_step": 1, "timeout": "600s"}},
			},
		},
	}
	for _, s := range scenarios {
		if existing, _ := p.sqlite.GetChaosScenarioByName(ctx, s.Name); existing != nil {
			continue
		}
		actionsJSON, _ := json.Marshal(s.Actions)
		id := fmt.Sprintf("scn-%s", uuid.Must(uuid.NewV7()).String())
		_, _ = p.sqlite.CreateChaosScenario(ctx, id, s.Name, s.Namespace, string(actionsJSON))
	}
	slog.Info("built-in chaos scenarios seeded")
}

func (p *Pilot) loadHypotheses(ctx context.Context) error {
	hypotheses, err := p.sqlite.ListHypotheses(ctx)
	if err != nil {
		return err
	}
	for _, h := range hypotheses {
		slog.Info("loaded hypothesis", "id", h.ID, "name", h.Name, "status", h.Status)
		p.managers.CreateFromHypothesis(h.ID, h.Generator, h.Tolerance)
	}
	for _, h := range hypotheses {
		if h.Status != "running" {
			continue
		}
		p.resumeHypothesis(ctx, &h)
	}
	return nil
}

func (p *Pilot) resumeHypothesis(ctx context.Context, h *database.Hypothesis) {
	slog.Info("auto-resuming hypothesis", "id", h.ID, "name", h.Name)

	durationSecs := ext.ParseDuration(ext.PtrOr(h.Duration, ""))
	checkpointSecs := ext.ParseDuration(&h.CheckpointEvery)
	if checkpointSecs == nil {
		v := uint64(600)
		checkpointSecs = &v
	}

	execConfig := engine.ExecutionConfig{
		BatchSize:              100,
		CheckpointIntervalSecs: *checkpointSecs,
		FailFast:               true,
		DurationSecs:           durationSecs,
	}

	var adapter *database.AdapterRecord
	if h.Adapter != nil {
		a, _ := p.sqlite.GetAdapterByName(ctx, *h.Adapter)
		adapter = a
	}

	genRecord, _ := p.sqlite.GetGenerator(ctx, h.Generator)
	var rate uint32
	if genRecord != nil {
		rate = uint32(genRecord.Rate)
	}

	genParams := generator.DefaultGenerateParams()
	genParams.Generator = h.Generator
	genParams.Ops = int(^uint(0) >> 1)
	genParams.SkipWarmup = strings.Contains(h.Generator, "notification")
	genParams.Rate = rate
	if h.KeySpace != "" {
		var ks generator.KeySpaceConfig
		if json.Unmarshal([]byte(h.KeySpace), &ks) == nil {
			if ks.Count > 0 {
				genParams.KeySpace.Count = ks.Count
			}
		}
	}
	genParams.KeySpace.Prefix = fmt.Sprintf("/%s/", h.ID)

	mgr := p.managers.Get(h.ID)
	if mgr == nil {
		return
	}

	if lastResult, err := p.sqlite.GetLatestResult(ctx, h.ID); err == nil && lastResult != nil {
		mgr.SetResumeRunID(lastResult.RunID)
	}

	var adapterClient engine.AdapterClient
	if adapter != nil {
		addr := fmt.Sprintf("http://adapter-%s:9090", adapter.Name)
		if h.AdapterAddr != nil && *h.AdapterAddr != "" {
			addr = *h.AdapterAddr
		}
		if client, err := adapterPkg.Connect(addr); err == nil {
			adapterClient = client
		}
	}

	if runID, _, err := mgr.StartRun(ctx, execConfig, genParams, adapter, h.AdapterAddr, adapterClient); err != nil {
		slog.Warn("failed to resume", "id", h.ID, "error", err)
	} else {
		slog.Info("hypothesis resumed", "id", h.ID, "run_id", runID)
	}
}

func (p *Pilot) resumeChaosInjections(ctx context.Context) error {
	injections, err := p.sqlite.ListChaosInjections(ctx)
	if err != nil {
		return err
	}

	for _, inj := range injections {
		if inj.Status != "running" {
			continue
		}

		record, err := p.sqlite.GetChaosScenario(ctx, inj.ScenarioID)
		if err != nil {
			slog.Error("failed to load scenario for running chaos injection",
				slog.String("injection_id", inj.ID),
				slog.String("scenario_id", inj.ScenarioID),
				slog.Any("error", err),
			)
			continue
		}

		var actions []chaos.ChaosAction
		if err := json.Unmarshal([]byte(record.Actions), &actions); err != nil {
			slog.Error("failed to decode actions for running chaos injection",
				slog.String("injection_id", inj.ID),
				slog.String("scenario_id", inj.ScenarioID),
				slog.Any("error", err),
			)
			continue
		}

		p.chaos.ResumeInjection(inj.ID, chaos.ChaosScenario{
			ID:        record.ID,
			Name:      record.Name,
			Namespace: record.Namespace,
			Actions:   actions,
		})

		slog.Info("chaos injection resumed",
			slog.String("injection_id", inj.ID),
			slog.String("scenario_id", inj.ScenarioID),
			slog.String("scenario_name", inj.ScenarioName),
		)
	}

	return nil
}
