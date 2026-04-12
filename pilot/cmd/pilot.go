package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"
	"github.com/spf13/cobra"

	adapterPkg "github.com/regret-io/regret/pilot-go/adapter"
	"github.com/regret-io/regret/pilot-go/chaos"
	"github.com/regret-io/regret/pilot-go/database"
	"github.com/regret-io/regret/pilot-go/endpoints"
	"github.com/regret-io/regret/pilot-go/engine"
	"github.com/regret-io/regret/pilot-go/eventlog"
	"github.com/regret-io/regret/pilot-go/generator"
	"github.com/regret-io/regret/pilot-go/reference"
)

type chaosManagerAdapter struct {
	managers *engine.ManagerRegistry
}

func (a *chaosManagerAdapter) Get(id string) interface{} {
	return a.managers.Get(id)
}

var rootCmd = &cobra.Command{
	Use:   "regret-pilot",
	Short: "Regret chaos engineering pilot server",
	RunE:  runServe,
}

func init() {
	rootCmd.Flags().String("data-dir", envOr("DATA_DIR", "/data"), "Base data directory (all state is stored under this path)")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServe(cmd *cobra.Command, _ []string) error {
	zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	slog.SetDefault(slog.New(slogzerolog.Option{Logger: &zerologLogger}.NewZerologHandler()))

	dataDir, _ := cmd.Flags().GetString("data-dir")

	// Derive all paths from data-dir
	dbPath := filepath.Join(dataDir, "regret.db")
	pebblePath := filepath.Join(dataDir, "pebble")
	httpPort := envUint16("HTTP_PORT", 8080)
	grpcPort := envUint16("GRPC_PORT", 9090)
	namespace := envOr("NAMESPACE", "regret")
	authPassword := os.Getenv("AUTH_PASSWORD")

	slog.Info("starting regret-pilot",
		"data_dir", dataDir,
		"http_port", httpPort,
		"grpc_port", grpcPort,
		"namespace", namespace,
		"auth", authPassword != "",
	)

	ctx := context.Background()

	// Init storage
	sqlite, err := database.NewSqliteStore(dbPath)
	if err != nil {
		return fmt.Errorf("init sqlite: %w", err)
	}

	refStore, err := reference.NewReferenceStore(pebblePath)
	if err != nil {
		return fmt.Errorf("init reference store: %w", err)
	}
	defer refStore.Close()

	events := eventlog.NewEventLog(dataDir)

	// Seed built-in generators
	for _, gen := range generator.ListGenerators() {
		workload := generator.GetGeneratorWorkload(gen.Name)
		workloadJSON, _ := json.Marshal(workload)
		_ = sqlite.UpsertGenerator(ctx, gen.Name, gen.Description, string(workloadJSON), int64(gen.Rate), 1)
	}
	slog.Info("built-in generators seeded")

	seedBuiltinChaosScenarios(ctx, sqlite)

	// Create manager registry
	shared := engine.SharedServices{
		Sqlite:   sqlite,
		RefStore: refStore,
		Events:   events,
	}
	managers := engine.NewManagerRegistry(shared)

	// Load hypotheses
	hypotheses, err := sqlite.ListHypotheses(ctx)
	if err != nil {
		return fmt.Errorf("list hypotheses: %w", err)
	}
	for _, h := range hypotheses {
		slog.Info("loaded hypothesis", "id", h.ID, "name", h.Name, "status", h.Status)
		managers.CreateFromHypothesis(h.ID, h.Generator, h.Tolerance)
	}

	// Auto-resume running hypotheses
	for _, h := range hypotheses {
		if h.Status != "running" {
			continue
		}
		resumeHypothesis(ctx, &h, sqlite, managers)
	}

	// Build and start server
	chaosRegistry := chaos.NewChaosRegistry(events, sqlite, &chaosManagerAdapter{managers: managers})

	var authPtr *string
	if authPassword != "" {
		authPtr = &authPassword
	}

	state := &endpoints.AppState{
		Sqlite:    sqlite,
		Files:     events,
		Managers:  managers,
		Chaos:     chaosRegistry,
		Namespace: namespace,
	}

	router := endpoints.NewRouter(state, authPtr)
	addr := fmt.Sprintf("0.0.0.0:%d", httpPort)
	srv := &http.Server{Addr: addr, Handler: router}

	go func() {
		slog.Info("HTTP server starting", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func resumeHypothesis(ctx context.Context, h *database.Hypothesis, sqlite *database.SqliteStore, managers *engine.ManagerRegistry) {
	slog.Info("auto-resuming hypothesis", "id", h.ID, "name", h.Name)

	durationSecs := parseDuration(ptrOr(h.Duration, ""))
	checkpointSecs := parseDuration(&h.CheckpointEvery)
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
		a, _ := sqlite.GetAdapterByName(ctx, *h.Adapter)
		adapter = a
	}

	genRecord, _ := sqlite.GetGenerator(ctx, h.Generator)
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

	mgr := managers.Get(h.ID)
	if mgr == nil {
		return
	}

	if lastResult, err := sqlite.GetLatestResult(ctx, h.ID); err == nil && lastResult != nil {
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

func seedBuiltinChaosScenarios(ctx context.Context, sqlite *database.SqliteStore) {
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
		if existing, _ := sqlite.GetChaosScenarioByName(ctx, s.Name); existing != nil {
			continue
		}
		actionsJSON, _ := json.Marshal(s.Actions)
		id := fmt.Sprintf("scn-%s", uuid.Must(uuid.NewV7()).String())
		_, _ = sqlite.CreateChaosScenario(ctx, id, s.Name, s.Namespace, string(actionsJSON))
	}
	slog.Info("built-in chaos scenarios seeded")
}

func parseDuration(s *string) *uint64 {
	if s == nil || *s == "" {
		return nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(*s))
	if err == nil {
		v := uint64(d.Seconds())
		return &v
	}
	var v uint64
	if _, err := fmt.Sscanf(*s, "%d", &v); err == nil {
		return &v
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envUint16(key string, fallback uint16) uint16 {
	var n uint16
	if _, err := fmt.Sscanf(os.Getenv(key), "%d", &n); err == nil {
		return n
	}
	return fallback
}

func ptrOr(s *string, fallback string) *string {
	if s != nil {
		return s
	}
	return &fallback
}

func main() {
	Execute()
}
