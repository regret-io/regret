package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/regret-io/regret/pilot-go/database"
	"github.com/regret-io/regret/pilot-go/eventlog"
	"github.com/regret-io/regret/pilot-go/generator"
	"github.com/regret-io/regret/pilot-go/reference"
)

// SharedServices holds shared infrastructure passed to each HypothesisManager.
type SharedServices struct {
	Sqlite   *database.SqliteStore
	RefStore *reference.ReferenceStore
	Events   *eventlog.EventLog
}

// ManagerRegistry is the global registry of all hypothesis managers.
type ManagerRegistry struct {
	mu       sync.RWMutex
	managers map[string]*HypothesisManager
	shared   SharedServices
}

// NewManagerRegistry creates a new ManagerRegistry.
func NewManagerRegistry(shared SharedServices) *ManagerRegistry {
	return &ManagerRegistry{
		managers: make(map[string]*HypothesisManager),
		shared:   shared,
	}
}

// CreateFromHypothesis creates and registers a new manager for the given hypothesis.
func (r *ManagerRegistry) CreateFromHypothesis(id, generatorName string, tolerance *string) {
	ref := reference.CreateReference(generatorName, r.shared.RefStore, id)
	var tol *reference.Tolerance
	if tolerance != nil {
		var t reference.Tolerance
		if err := jsonUnmarshalString(*tolerance, &t); err == nil {
			tol = &t
		}
	}
	mgr := NewHypothesisManager(id, generatorName, tol, ref, r.shared)
	r.mu.Lock()
	r.managers[id] = mgr
	r.mu.Unlock()
}

// Get returns the manager for the given hypothesis ID, or nil.
func (r *ManagerRegistry) Get(id string) *HypothesisManager {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.managers[id]
}

// Remove removes and returns the manager for the given hypothesis ID.
func (r *ManagerRegistry) Remove(id string) {
	r.mu.Lock()
	delete(r.managers, id)
	r.mu.Unlock()
}

// ---------------------------------------------------------------------------
// HypothesisManager
// ---------------------------------------------------------------------------

// activeRun holds the state of a running execution.
type activeRun struct {
	RunID    string
	cancel   context.CancelFunc
	progress *ProgressInfo
	mu       sync.RWMutex
	done     chan struct{}
}

// HypothesisManager manages the lifecycle of a single hypothesis.
type HypothesisManager struct {
	mu            sync.Mutex
	HypothesisID  string
	GeneratorName string
	tolerance     *reference.Tolerance
	reference     reference.ReferenceModel
	activeRun     *activeRun
	resumeRunID   *string
	shared        SharedServices
}

// NewHypothesisManager creates a new HypothesisManager.
func NewHypothesisManager(id, generatorName string, tolerance *reference.Tolerance, ref reference.ReferenceModel, shared SharedServices) *HypothesisManager {
	return &HypothesisManager{
		HypothesisID:  id,
		GeneratorName: generatorName,
		tolerance:     tolerance,
		reference:     ref,
		shared:        shared,
	}
}

// StartRun starts a new execution run.
func (m *HypothesisManager) StartRun(
	ctx context.Context,
	config ExecutionConfig,
	genParams *generator.GenerateParams,
	adapter *database.AdapterRecord,
	adapterAddrOverride *string,
	adapterClient AdapterClient,
) (string, *ProgressInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cleanupFinishedRunLocked()

	if m.isRunningLocked() {
		return "", nil, fmt.Errorf("hypothesis is already running")
	}

	if m.reference == nil {
		m.reference = reference.CreateReference(m.GeneratorName, m.shared.RefStore, m.HypothesisID)
	}

	var runID string
	if m.resumeRunID != nil {
		runID = *m.resumeRunID
		m.resumeRunID = nil
	} else {
		runID = fmt.Sprintf("run-%d", timeNowUnixMilli())
	}

	progress := &ProgressInfo{}

	// Update hypothesis status
	bgCtx := context.Background()
	if err := m.shared.Sqlite.UpdateHypothesisStatus(bgCtx, m.HypothesisID, "running"); err != nil {
		return "", nil, fmt.Errorf("update status: %w", err)
	}
	if err := m.shared.Sqlite.UpdateLastRunAt(bgCtx, m.HypothesisID); err != nil {
		return "", nil, fmt.Errorf("update last_run_at: %w", err)
	}

	// Log if adapter is set but no client provided
	if adapter != nil && adapterClient == nil {
		addr := ""
		if adapterAddrOverride != nil && *adapterAddrOverride != "" {
			addr = *adapterAddrOverride
		} else {
			addr = fmt.Sprintf("http://adapter-%s:9090", adapter.Name)
		}
		slog.Warn("adapter configured but no client provided, running in mock mode",
			slog.String("adapter", adapter.Name),
			slog.String("addr", addr),
		)
	}

	// Start metrics scraper if we have an adapter address
	runCtx, cancel := context.WithCancel(ctx)
	if adapter != nil {
		addr := ""
		if adapterAddrOverride != nil && *adapterAddrOverride != "" {
			addr = *adapterAddrOverride
		} else {
			addr = fmt.Sprintf("http://adapter-%s:9090", adapter.Name)
		}
		if metricsURL := MetricsURLFromAdapterAddr(addr); metricsURL != "" {
			scraperCfg := ScraperConfig{
				HypothesisID:   m.HypothesisID,
				RunID:          runID,
				MetricsURL:     metricsURL,
				Interval:       DefaultScraperInterval,
				RequestTimeout: DefaultScraperTimeout,
			}
			go RunScraper(runCtx, scraperCfg, m.shared.Sqlite)
		}
	}

	done := make(chan struct{})
	executor := &Executor{
		HypothesisID:   m.HypothesisID,
		RunID:          runID,
		Config:         config,
		Tolerance:      m.tolerance,
		GenerateParams: genParams,
		Reference:      m.reference,
		Ctx:            runCtx,
		Cancel:         cancel,
		Progress:       progress,
		Files:          m.shared.Events,
		Sqlite:         m.shared.Sqlite,
		AdapterClient:  adapterClient,
	}

	m.reference = nil // ownership transferred to executor

	go func() {
		defer close(done)
		ref, _ := executor.Run()
		// Store reference back
		m.mu.Lock()
		if m.reference == nil {
			m.reference = ref
		}
		m.mu.Unlock()
		cancel()
	}()

	m.activeRun = &activeRun{
		RunID:    runID,
		cancel:   cancel,
		progress: progress,
		done:     done,
	}

	slog.Info("run started",
		slog.String("hypothesis_id", m.HypothesisID),
		slog.String("run_id", runID),
	)
	return runID, progress, nil
}

// StopRun stops the current run.
func (m *HypothesisManager) StopRun() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeRun == nil {
		return nil
	}

	slog.Info("stopping run",
		slog.String("hypothesis_id", m.HypothesisID),
		slog.String("run_id", m.activeRun.RunID),
	)

	m.activeRun.cancel()
	<-m.activeRun.done
	m.activeRun = nil
	return nil
}

// IsRunning returns true if a run is currently active.
func (m *HypothesisManager) IsRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isRunningLocked()
}

func (m *HypothesisManager) isRunningLocked() bool {
	if m.activeRun == nil {
		return false
	}
	select {
	case <-m.activeRun.done:
		return false
	default:
		return true
	}
}

// CleanupFinishedRun cleans up a finished run if any.
func (m *HypothesisManager) CleanupFinishedRun() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupFinishedRunLocked()
}

func (m *HypothesisManager) cleanupFinishedRunLocked() {
	if m.activeRun == nil {
		return
	}
	select {
	case <-m.activeRun.done:
		m.activeRun = nil
	default:
	}
}

// SetResumeRunID sets a run ID to use for the next run.
func (m *HypothesisManager) SetResumeRunID(runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resumeRunID = &runID
}

// RunID returns the current run ID, if any.
func (m *HypothesisManager) RunID() *string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeRun == nil {
		return nil
	}
	return &m.activeRun.RunID
}

// Progress returns the current progress, if running.
func (m *HypothesisManager) Progress() *ProgressInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeRun == nil {
		return nil
	}
	return m.activeRun.progress
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func jsonUnmarshalString(s string, v interface{}) error {
	return json.Unmarshal([]byte(s), v)
}

func timeNowUnixMilli() int64 {
	return time.Now().UnixMilli()
}
