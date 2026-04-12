package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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

// chaosManagerAdapter wraps engine.ManagerRegistry to satisfy chaos.ManagerRegistry.
type chaosManagerAdapter struct {
	managers *engine.ManagerRegistry
}

func (a *chaosManagerAdapter) Get(id string) interface{} {
	return a.managers.Get(id)
}

var rootCmd = &cobra.Command{
	Use:   "regret-pilot",
	Short: "Regret pilot server",
	RunE:  runServe,
}

func init() {
	rootCmd.Flags().String("data-dir", envOr("DATA_DIR", "/data"), "Data directory")
	rootCmd.Flags().Uint16("http-port", envUint16("HTTP_PORT", 8080), "HTTP port")
	rootCmd.Flags().Uint16("grpc-port", envUint16("GRPC_PORT", 9090), "gRPC port")
	rootCmd.Flags().String("namespace", envOr("NAMESPACE", "regret-system"), "Kubernetes namespace")
	rootCmd.Flags().String("database-url", envOr("DATABASE_URL", "sqlite:///data/regret.db"), "Database URL")
	rootCmd.Flags().String("pebble-path", envOr("PEBBLE_PATH", "/data/pebble"), "Pebble storage path")
	rootCmd.Flags().String("auth-password", os.Getenv("AUTH_PASSWORD"), "Basic auth password (empty = disabled)")
}

// Execute is the entry point for the CLI.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func runServe(cmd *cobra.Command, args []string) error {
	zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	handler := slogzerolog.Option{Logger: &zerologLogger}.NewZerologHandler()
	slog.SetDefault(slog.New(handler))

	dataDir, _ := cmd.Flags().GetString("data-dir")
	httpPort, _ := cmd.Flags().GetUint16("http-port")
	grpcPort, _ := cmd.Flags().GetUint16("grpc-port")
	namespace, _ := cmd.Flags().GetString("namespace")
	databaseURL, _ := cmd.Flags().GetString("database-url")
	pebblePath, _ := cmd.Flags().GetString("pebble-path")
	authPasswordStr, _ := cmd.Flags().GetString("auth-password")

	var authPassword *string
	if authPasswordStr != "" {
		authPassword = &authPasswordStr
	}

	// Mask password in log
	pwDisplay := "None"
	if authPassword != nil {
		pwDisplay = "Some(\"***\")"
	}
	slog.Info("starting regret-pilot",
		slog.String("data_dir", dataDir),
		slog.String("http_port", fmt.Sprintf("%d", httpPort)),
		slog.String("grpc_port", fmt.Sprintf("%d", grpcPort)),
		slog.String("namespace", namespace),
		slog.String("database_url", databaseURL),
		slog.String("pebble_path", pebblePath),
		slog.String("auth_password", pwDisplay),
	)

	// Parse SQLite path from URL
	dbPath := databaseURL
	if strings.HasPrefix(dbPath, "sqlite://") {
		dbPath = strings.TrimPrefix(dbPath, "sqlite://")
	}
	if strings.HasPrefix(dbPath, "sqlite:") {
		dbPath = strings.TrimPrefix(dbPath, "sqlite:")
	}

	// Init storage
	ctx := context.Background()

	sqlite, err := database.NewSqliteStore(dbPath)
	if err != nil {
		slog.Error("failed to init sqlite", slog.Any("error", err))
		return err
	}

	refStore, err := reference.NewReferenceStore(pebblePath)
	if err != nil {
		slog.Error("failed to init reference store", slog.Any("error", err))
		return err
	}
	defer refStore.Close()

	events := eventlog.NewEventLog(dataDir)

	// Seed built-in generators
	for _, gen := range generator.ListGenerators() {
		workload := generator.GetGeneratorWorkload(gen.Name)
		workloadJSON, _ := json.Marshal(workload)
		if err := sqlite.UpsertGenerator(ctx, gen.Name, gen.Description, string(workloadJSON), int64(gen.Rate), 1); err != nil {
			slog.Warn("failed to seed generator", slog.String("name", gen.Name), slog.Any("error", err))
		}
	}
	slog.Info("built-in generators seeded")

	// Seed built-in chaos scenarios
	seedBuiltinChaosScenarios(ctx, sqlite)

	// Create manager registry
	shared := engine.SharedServices{
		Sqlite:   sqlite,
		RefStore: refStore,
		Events:   events,
	}
	managers := engine.NewManagerRegistry(shared)

	// Load existing hypotheses and create managers
	hypotheses, err := sqlite.ListHypotheses(ctx)
	if err != nil {
		slog.Error("failed to list hypotheses", slog.Any("error", err))
		return err
	}
	for _, h := range hypotheses {
		slog.Info("loaded hypothesis", slog.String("id", h.ID), slog.String("name", h.Name), slog.String("status", h.Status))
		managers.CreateFromHypothesis(h.ID, h.Generator, h.Tolerance)
	}

	// Auto-resume running hypotheses
	for _, h := range hypotheses {
		if h.Status != "running" {
			continue
		}
		slog.Info("auto-resuming running hypothesis", slog.String("id", h.ID), slog.String("name", h.Name))

		durationSecs := parseDuration(ptrOr(h.Duration, ""))
		checkpointSecs := parseDuration(&h.CheckpointEvery)
		if checkpointSecs == nil {
			defaultVal := uint64(600)
			checkpointSecs = &defaultVal
		}

		execConfig := engine.ExecutionConfig{
			BatchSize:              100,
			CheckpointIntervalSecs: *checkpointSecs,
			FailFast:               true,
			DurationSecs:           durationSecs,
		}

		var adapter *database.AdapterRecord
		if h.Adapter != nil {
			a, err := sqlite.GetAdapterByName(ctx, *h.Adapter)
			if err == nil && a != nil {
				adapter = a
			}
		}

		genRecord, _ := sqlite.GetGenerator(ctx, h.Generator)
		var rate uint32
		if genRecord != nil {
			rate = uint32(genRecord.Rate)
		}

		genParams := generator.DefaultGenerateParams()
		genParams.Generator = h.Generator
		genParams.Ops = int(^uint(0) >> 1) // MaxInt
		genParams.SkipWarmup = strings.Contains(h.Generator, "notification")
		genParams.Rate = rate
		if h.KeySpace != "" {
			var ks generator.KeySpaceConfig
			if err := json.Unmarshal([]byte(h.KeySpace), &ks); err == nil {
				if ks.Count > 0 {
					genParams.KeySpace.Count = ks.Count
				}
				if ks.Prefix != "" {
					genParams.KeySpace.Prefix = ks.Prefix
				}
			}
		}
		genParams.KeySpace.Prefix = fmt.Sprintf("/%s/", h.ID)

		mgr := managers.Get(h.ID)
		if mgr == nil {
			continue
		}
		// Restore previous run ID
		if lastResult, err := sqlite.GetLatestResult(ctx, h.ID); err == nil && lastResult != nil {
			mgr.SetResumeRunID(lastResult.RunID)
			slog.Info("restoring run ID", slog.String("id", h.ID), slog.String("run_id", lastResult.RunID))
		}
		// Connect to adapter gRPC client
		var adapterClient engine.AdapterClient
		if adapter != nil {
			addr := ""
			if h.AdapterAddr != nil && *h.AdapterAddr != "" {
				addr = *h.AdapterAddr
			} else {
				addr = fmt.Sprintf("http://adapter-%s:9090", adapter.Name)
			}
			client, err := adapterPkg.Connect(addr)
			if err != nil {
				slog.Warn("failed to connect to adapter", slog.String("id", h.ID), slog.Any("error", err))
			} else {
				adapterClient = client
				slog.Info("connected to adapter", slog.String("adapter", adapter.Name), slog.String("addr", addr))
			}
		}
		runID, _, err := mgr.StartRun(ctx, execConfig, genParams, adapter, h.AdapterAddr, adapterClient)
		if err != nil {
			slog.Warn("failed to resume hypothesis", slog.String("id", h.ID), slog.Any("error", err))
		} else {
			slog.Info("hypothesis resumed", slog.String("id", h.ID), slog.String("run_id", runID))
		}
	}

	// Create chaos registry
	chaosRegistry := chaos.NewChaosRegistry(events, sqlite, &chaosManagerAdapter{managers: managers})

	// Build app state
	state := &endpoints.AppState{
		Sqlite:    sqlite,
		Files:     events,
		Managers:  managers,
		Chaos:     chaosRegistry,
		Namespace: namespace,
	}

	slog.Info("basic auth enabled for write operations")

	// Start HTTP server
	router := endpoints.NewRouter(state, authPassword)
	addr := fmt.Sprintf("0.0.0.0:%d", httpPort)
	srv := &http.Server{Addr: addr, Handler: router}

	go func() {
		slog.Info("HTTP server starting", slog.String("http_addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server failed", slog.Any("error", err))
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	return nil
}

func seedBuiltinChaosScenarios(ctx context.Context, sqlite *database.SqliteStore) {
	type scenario struct {
		Name      string
		Namespace string
		Actions   []map[string]any
	}
	scenarios := []scenario{
		{
			Name:      "builtin-continuous-chaos",
			Namespace: "regret-system",
			Actions: []map[string]any{
				{
					"action_type": "pod_kill",
					"interval":    "10m",
					"selector": map[string]any{
						"match_labels": map[string]string{},
						"mode":         "one",
						"percentage":   100,
					},
				},
				{
					"action_type": "network_delay",
					"interval":    "15m",
					"duration":    "2m",
					"selector": map[string]any{
						"match_labels": map[string]string{},
						"mode":         "one",
						"percentage":   100,
					},
					"params": map[string]any{
						"delay_ms": 200,
					},
				},
			},
		},
		{
			Name:      "builtin-upgrade-test",
			Namespace: "regret-system",
			Actions: []map[string]any{
				{
					"action_type": "upgrade_test",
					"selector": map[string]any{
						"match_labels": map[string]string{},
						"mode":         "all",
						"percentage":   100,
					},
					"params": map[string]any{
						"resource":             "statefulset/oxia",
						"namespace":            "regret",
						"candidate_image":      "",
						"stable_image":         "",
						"checkpoints_per_step": 1,
						"timeout":              "600s",
					},
				},
			},
		},
	}

	for _, s := range scenarios {
		actionsJSON, _ := json.Marshal(s.Actions)
		existing, _ := sqlite.GetChaosScenarioByName(ctx, s.Name)
		if existing != nil {
			continue
		}
		id := fmt.Sprintf("scn-%s", uuid.Must(uuid.NewV7()).String())
		if _, err := sqlite.CreateChaosScenario(ctx, id, s.Name, s.Namespace, string(actionsJSON)); err != nil {
			slog.Warn("failed to seed chaos scenario", slog.String("name", s.Name), slog.Any("error", err))
		}
	}
	slog.Info("built-in chaos scenarios seeded")
}

func parseDuration(s *string) *uint64 {
	if s == nil || *s == "" {
		return nil
	}
	str := strings.TrimSpace(*s)
	d, err := time.ParseDuration(str)
	if err == nil {
		val := uint64(d.Seconds())
		return &val
	}
	// Fallback: try plain number as seconds
	var val uint64
	if _, err := fmt.Sscanf(str, "%d", &val); err == nil {
		return &val
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
	if v := os.Getenv(key); v != "" {
		var n uint16
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func ptrOr(s *string, fallback string) *string {
	if s != nil {
		return s
	}
	return &fallback
}
