package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	slogzerolog "github.com/samber/slog-zerolog/v2"

	adapterPkg "github.com/regret-io/regret/pilot-go/adapter"
	"github.com/regret-io/regret/pilot-go/api"
	"github.com/regret-io/regret/pilot-go/chaos"
	"github.com/regret-io/regret/pilot-go/engine"
	"github.com/regret-io/regret/pilot-go/generator"
	"github.com/regret-io/regret/pilot-go/storage"
)

// chaosManagerAdapter wraps engine.ManagerRegistry to satisfy chaos.ManagerRegistry.
type chaosManagerAdapter struct {
	managers *engine.ManagerRegistry
}

func (a *chaosManagerAdapter) Get(id string) interface{} {
	return a.managers.Get(id)
}

type Config struct {
	DataDir      string
	HTTPPort     uint16
	GRPCPort     uint16
	Namespace    string
	DatabaseURL  string
	PebblePath   string
	AuthPassword *string
}

func configFromEnv() Config {
	c := Config{
		DataDir:     envOr("DATA_DIR", "/data"),
		HTTPPort:    envUint16("HTTP_PORT", 8080),
		GRPCPort:    envUint16("GRPC_PORT", 9090),
		Namespace:   envOr("NAMESPACE", "regret-system"),
		DatabaseURL: envOr("DATABASE_URL", "sqlite:///data/regret.db"),
		PebblePath:  envOr("PEBBLE_PATH", "/data/pebble"),
	}
	if pw := os.Getenv("AUTH_PASSWORD"); pw != "" {
		c.AuthPassword = &pw
	}
	return c
}

func main() {
	zerologLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	handler := slogzerolog.Option{Logger: &zerologLogger}.NewZerologHandler()
	slog.SetDefault(slog.New(handler))

	cfg := configFromEnv()

	// Mask password in log
	pwDisplay := "None"
	if cfg.AuthPassword != nil {
		pwDisplay = "Some(\"***\")"
	}
	slog.Info("starting regret-pilot",
		slog.String("data_dir", cfg.DataDir),
		slog.String("http_port", fmt.Sprintf("%d", cfg.HTTPPort)),
		slog.String("grpc_port", fmt.Sprintf("%d", cfg.GRPCPort)),
		slog.String("namespace", cfg.Namespace),
		slog.String("database_url", cfg.DatabaseURL),
		slog.String("pebble_path", cfg.PebblePath),
		slog.String("auth_password", pwDisplay),
	)

	// Parse SQLite path from URL
	dbPath := cfg.DatabaseURL
	if strings.HasPrefix(dbPath, "sqlite://") {
		dbPath = strings.TrimPrefix(dbPath, "sqlite://")
	}
	if strings.HasPrefix(dbPath, "sqlite:") {
		dbPath = strings.TrimPrefix(dbPath, "sqlite:")
	}

	// Init storage
	ctx := context.Background()

	sqlite, err := storage.NewSqliteStore(dbPath)
	if err != nil {
		slog.Error("failed to init sqlite", slog.Any("error", err))
		os.Exit(1)
	}

	pebbleStore, err := storage.NewPebbleStore(cfg.PebblePath)
	if err != nil {
		slog.Error("failed to init pebble", slog.Any("error", err))
		os.Exit(1)
	}
	defer pebbleStore.Close()

	files := storage.NewFileStore(cfg.DataDir)

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
		Sqlite: sqlite,
		Pebble: pebbleStore,
		Files:  files,
	}
	managers := engine.NewManagerRegistry(shared)

	// Load existing hypotheses and create managers
	hypotheses, err := sqlite.ListHypotheses(ctx)
	if err != nil {
		slog.Error("failed to list hypotheses", slog.Any("error", err))
		os.Exit(1)
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

		var adapter *storage.AdapterRecord
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
	chaosRegistry := chaos.NewChaosRegistry(files, sqlite, &chaosManagerAdapter{managers: managers})

	// Build app state
	state := &api.AppState{
		Sqlite:    sqlite,
		Pebble:    pebbleStore,
		Files:     files,
		Managers:  managers,
		Chaos:     chaosRegistry,
		Namespace: cfg.Namespace,
	}

	slog.Info("basic auth enabled for write operations")

	// Start HTTP server
	router := api.NewRouter(state, cfg.AuthPassword)
	addr := fmt.Sprintf("0.0.0.0:%d", cfg.HTTPPort)
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
}

func seedBuiltinChaosScenarios(ctx context.Context, sqlite *storage.SqliteStore) {
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
		id := fmt.Sprintf("scn-%s", newUUID())
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
	var val uint64
	if strings.HasSuffix(str, "h") {
		n, err := strconv.ParseUint(strings.TrimSuffix(str, "h"), 10, 64)
		if err != nil {
			return nil
		}
		val = n * 3600
	} else if strings.HasSuffix(str, "m") {
		n, err := strconv.ParseUint(strings.TrimSuffix(str, "m"), 10, 64)
		if err != nil {
			return nil
		}
		val = n * 60
	} else if strings.HasSuffix(str, "s") {
		n, err := strconv.ParseUint(strings.TrimSuffix(str, "s"), 10, 64)
		if err != nil {
			return nil
		}
		val = n
	} else {
		n, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return nil
		}
		val = n
	}
	return &val
}

func newUUID() string {
	// UUID v7 (time-ordered)
	// Simple implementation using timestamp + random
	now := time.Now().UnixMilli()
	b := make([]byte, 16)
	b[0] = byte(now >> 40)
	b[1] = byte(now >> 32)
	b[2] = byte(now >> 24)
	b[3] = byte(now >> 16)
	b[4] = byte(now >> 8)
	b[5] = byte(now)
	b[6] = 0x70 | byte(now>>4)&0x0f // version 7
	b[7] = byte(now) & 0x3f | 0x80  // variant
	// Fill rest with random
	r := time.Now().UnixNano()
	for i := 8; i < 16; i++ {
		b[i] = byte(r >> (uint(i-8) * 8))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uint32(b[0])<<24|uint32(b[1])<<16|uint32(b[2])<<8|uint32(b[3]),
		uint16(b[4])<<8|uint16(b[5]),
		uint16(b[6])<<8|uint16(b[7]),
		uint16(b[8])<<8|uint16(b[9]),
		uint64(b[10])<<40|uint64(b[11])<<32|uint64(b[12])<<24|uint64(b[13])<<16|uint64(b[14])<<8|uint64(b[15]),
	)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envUint16(key string, fallback uint16) uint16 {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.ParseUint(v, 10, 16)
		if err == nil {
			return uint16(n)
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
