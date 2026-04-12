package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// ---------------------------------------------------------------------------
// Dependency interfaces
// ---------------------------------------------------------------------------

// EventLog persists chaos events to a JSONL file.
type EventLog interface {
	AppendChaosEvent(eventJSON string) error
}

// ChaosStore persists chaos injection metadata.
// Implementations must handle creation and status updates.
type SqliteStore interface {
	ChaosCreateInjection(ctx context.Context, id, scenarioID, scenarioName string) error
	UpdateChaosInjectionStatus(ctx context.Context, id, status string, errMsg *string) error
}

// ManagerRegistry provides access to running hypothesis managers.
type ManagerRegistry interface {
	// Get returns a manager by hypothesis ID, or nil if not found.
	// The returned interface should support PassedCheckpoints() uint64.
	Get(id string) interface{}
}

// ---------------------------------------------------------------------------
// ChaosRegistry
// ---------------------------------------------------------------------------

// chaosInjectionHandle is the internal bookkeeping for an active injection.
type chaosInjectionHandle struct {
	scenarioName string
	cancel       context.CancelFunc
	done         <-chan struct{}
}

// ChaosRegistry manages active chaos injections.
type ChaosRegistry struct {
	mu         sync.RWMutex
	injections map[string]*chaosInjectionHandle
	files      EventLog
	sqlite     SqliteStore
	managers   ManagerRegistry
}

// NewChaosRegistry creates a new ChaosRegistry.
func NewChaosRegistry(files EventLog, sqlite SqliteStore, managers ManagerRegistry) *ChaosRegistry {
	return &ChaosRegistry{
		injections: make(map[string]*chaosInjectionHandle),
		files:      files,
		sqlite:     sqlite,
		managers:   managers,
	}
}

// StartInjection begins a chaos injection from the given scenario.
// It returns the injection ID.
func (r *ChaosRegistry) StartInjection(ctx context.Context, scenario ChaosScenario) (string, error) {
	injectionID := uuid.New().String()

	// Record in SQLite.
	if err := r.sqlite.ChaosCreateInjection(ctx, injectionID, scenario.ID, scenario.Name); err != nil {
		return "", fmt.Errorf("create chaos injection record: %w", err)
	}

	// Emit start event.
	event := NewInjectionStartedEvent(injectionID, scenario.Name)
	r.emitChaosEvent(&event)

	// Create a cancellable context for the injection goroutine.
	injCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		if err := runInjection(injCtx, injectionID, &scenario, r.files, r.sqlite, r.managers); err != nil {
			slog.Error("chaos injection failed",
				slog.String("injection_id", injectionID),
				slog.Any("error", err),
			)
			errEvent := NewChaosErrorEvent(injectionID, scenario.Name, "injection", err.Error())
			r.files.AppendChaosEvent(errEvent.ToJSON()) //nolint:errcheck
			errMsg := err.Error()
			r.sqlite.UpdateChaosInjectionStatus(context.Background(), injectionID, "error", &errMsg) //nolint:errcheck
		}
	}()

	r.mu.Lock()
	r.injections[injectionID] = &chaosInjectionHandle{
		scenarioName: scenario.Name,
		cancel:       cancel,
		done:         done,
	}
	r.mu.Unlock()

	return injectionID, nil
}

// StopInjection cancels an active chaos injection and waits for it to finish.
func (r *ChaosRegistry) StopInjection(ctx context.Context, injectionID string) error {
	r.mu.Lock()
	handle, ok := r.injections[injectionID]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	delete(r.injections, injectionID)
	r.mu.Unlock()

	// Cancel the injection goroutine and wait for it to finish.
	handle.cancel()
	<-handle.done

	event := NewInjectionStoppedEvent(injectionID, handle.scenarioName, "manual")
	r.emitChaosEvent(&event)

	if err := r.sqlite.UpdateChaosInjectionStatus(ctx, injectionID, "stopped", nil); err != nil {
		return fmt.Errorf("update injection status: %w", err)
	}
	return nil
}

// IsActive returns true if the given injection is currently active.
func (r *ChaosRegistry) IsActive(injectionID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.injections[injectionID]
	return ok
}

// ActiveIDs returns the IDs of all active injections.
func (r *ChaosRegistry) ActiveIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.injections))
	for id := range r.injections {
		ids = append(ids, id)
	}
	return ids
}

func (r *ChaosRegistry) emitChaosEvent(event *ChaosEvent) {
	if err := r.files.AppendChaosEvent(event.ToJSON()); err != nil {
		slog.Error("failed to write chaos event", slog.Any("error", err))
	}
}

// ---------------------------------------------------------------------------
// Injection loop
// ---------------------------------------------------------------------------

// runInjection is the main injection goroutine. It creates a k8s client and
// spawns one goroutine per action.
func runInjection(
	ctx context.Context,
	injectionID string,
	scenario *ChaosScenario,
	files EventLog,
	sqlite SqliteStore,
	managers ManagerRegistry,
) error {
	clientset, err := buildK8sClient()
	if err != nil {
		return fmt.Errorf("failed to create K8s client: %w", err)
	}

	namespace := scenario.Namespace

	var wg sync.WaitGroup
	for i := range scenario.Actions {
		action := &scenario.Actions[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runActionLoop(ctx, injectionID, scenario.Name, clientset, namespace, action, files, managers); err != nil {
				slog.Error("action loop failed",
					slog.String("injection_id", injectionID),
					slog.String("action", action.ActionType),
					slog.Any("error", err),
				)
				errEvent := NewChaosErrorEvent(injectionID, scenario.Name, action.ActionType, err.Error())
				files.AppendChaosEvent(errEvent.ToJSON()) //nolint:errcheck
			}
		}()
	}

	wg.Wait()

	// Mark injection as finished.
	if err := sqlite.UpdateChaosInjectionStatus(context.Background(), injectionID, "finished", nil); err != nil {
		return fmt.Errorf("update injection status: %w", err)
	}

	slog.Info("chaos injection finished", slog.String("injection_id", injectionID))
	return nil
}

// buildK8sClient creates a kubernetes.Interface from in-cluster config.
func buildK8sClient() (kubernetes.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

// ---------------------------------------------------------------------------
// Action loop
// ---------------------------------------------------------------------------

// runActionLoop runs a single action based on its timing configuration:
// one-shot ("at"), repeating ("interval"), or immediate.
func runActionLoop(
	ctx context.Context,
	injectionID, scenarioName string,
	clientset kubernetes.Interface,
	namespace string,
	action *ChaosAction,
	files EventLog,
	managers ManagerRegistry,
) error {
	// upgrade_test has its own multi-step lifecycle.
	if action.ActionType == "upgrade_test" {
		return runUpgradeTest(ctx, injectionID, scenarioName, action, files, managers)
	}

	// If "at" is set, wait then execute once.
	if action.At != nil {
		delay, err := ParseDuration(*action.At)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		executeAndLog(ctx, injectionID, scenarioName, clientset, namespace, action, files)

		if action.Duration != nil {
			dur, err := ParseDuration(*action.Duration)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				recoverIfNeeded(clientset, namespace, action, injectionID, scenarioName, files)
				return nil
			case <-time.After(dur):
			}
			recoverIfNeeded(clientset, namespace, action, injectionID, scenarioName, files)
		}
		return nil
	}

	// If "interval" is set, repeat until cancelled.
	if action.Interval != nil {
		interval, err := ParseDuration(*action.Interval)
		if err != nil {
			return err
		}
		for {
			select {
			case <-ctx.Done():
				recoverIfNeeded(clientset, namespace, action, injectionID, scenarioName, files)
				return nil
			case <-time.After(interval):
			}

			executeAndLog(ctx, injectionID, scenarioName, clientset, namespace, action, files)

			if action.Duration != nil {
				dur, err := ParseDuration(*action.Duration)
				if err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					recoverIfNeeded(clientset, namespace, action, injectionID, scenarioName, files)
					return nil
				case <-time.After(dur):
				}
				recoverIfNeeded(clientset, namespace, action, injectionID, scenarioName, files)
			}
		}
	}

	// No timing -- execute once immediately.
	executeAndLog(ctx, injectionID, scenarioName, clientset, namespace, action, files)

	if action.Duration != nil {
		dur, err := ParseDuration(*action.Duration)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			recoverIfNeeded(clientset, namespace, action, injectionID, scenarioName, files)
			return nil
		case <-time.After(dur):
		}
		recoverIfNeeded(clientset, namespace, action, injectionID, scenarioName, files)
	}

	return nil
}

func executeAndLog(
	ctx context.Context,
	injectionID, scenarioName string,
	clientset kubernetes.Interface,
	namespace string,
	action *ChaosAction,
	files EventLog,
) {
	targetPods, err := ExecuteAction(ctx, clientset, namespace, action)
	if err != nil {
		slog.Warn("chaos action failed",
			slog.String("injection_id", injectionID),
			slog.String("action", action.ActionType),
			slog.Any("error", err),
		)
		errEvent := NewChaosErrorEvent(injectionID, scenarioName, action.ActionType, err.Error())
		files.AppendChaosEvent(errEvent.ToJSON()) //nolint:errcheck
		return
	}
	if len(targetPods) > 0 {
		event := NewChaosInjectedEvent(injectionID, scenarioName, action.ActionType, namespace, targetPods)
		files.AppendChaosEvent(event.ToJSON()) //nolint:errcheck
	}
}

func recoverIfNeeded(
	clientset kubernetes.Interface,
	namespace string,
	action *ChaosAction,
	injectionID, scenarioName string,
	files EventLog,
) {
	switch action.ActionType {
	case "network_partition", "network_delay", "network_loss":
	default:
		return
	}

	// Resolve targets again for recovery.
	targetPods, err := ExecuteAction(context.Background(), clientset, namespace, action)
	if err != nil {
		return
	}

	if err := RecoverNetwork(namespace, targetPods, action.ActionType); err != nil {
		slog.Warn("failed to recover network chaos", slog.Any("error", err))
	} else {
		event := NewChaosRecoveredEvent(injectionID, scenarioName, action.ActionType, targetPods, 0)
		files.AppendChaosEvent(event.ToJSON()) //nolint:errcheck
	}
}

// ---------------------------------------------------------------------------
// Upgrade test
// ---------------------------------------------------------------------------

// runUpgradeTest performs the upgrade -> checkpoint wait -> downgrade -> checkpoint wait
// -> re-upgrade -> checkpoint wait cycle.
func runUpgradeTest(
	ctx context.Context,
	injectionID, scenarioName string,
	action *ChaosAction,
	files EventLog,
	managers ManagerRegistry,
) error {
	hypothesisID, _ := paramString(action.Params, "hypothesis_id")
	if hypothesisID == "" {
		return fmt.Errorf("upgrade_test requires 'hypothesis_id' in params")
	}
	resource, _ := paramString(action.Params, "resource")
	if resource == "" {
		return fmt.Errorf("upgrade_test requires 'resource' in params")
	}
	namespace, _ := paramString(action.Params, "namespace")
	if namespace == "" {
		namespace = "regret-system"
	}
	candidateImage, _ := paramString(action.Params, "candidate_image")
	if candidateImage == "" {
		return fmt.Errorf("upgrade_test requires 'candidate_image' in params")
	}
	stableImage, _ := paramString(action.Params, "stable_image")
	if stableImage == "" {
		return fmt.Errorf("upgrade_test requires 'stable_image' in params")
	}
	patchPath, _ := paramString(action.Params, "patch_path")
	if patchPath == "" {
		patchPath = "/spec/template/spec/containers/0/image"
	}
	checkpointsPerStep := uint64(1)
	if v, ok := paramUint64(action.Params, "checkpoints_per_step"); ok {
		checkpointsPerStep = v
	}
	timeout, _ := paramString(action.Params, "timeout")
	if timeout == "" {
		timeout = "600s"
	}

	type step struct {
		name  string
		image string
	}
	steps := []step{
		{"upgrade", candidateImage},
		{"downgrade", stableImage},
		{"re-upgrade", candidateImage},
	}

	for _, s := range steps {
		if ctx.Err() != nil {
			return nil
		}

		slog.Info("upgrade_test: patching",
			slog.String("step", s.name),
			slog.String("image", s.image),
			slog.String("resource", resource),
		)

		// Patch the resource.
		patchJSON := fmt.Sprintf(`[{"op":"replace","path":"%s","value":"%s"}]`, patchPath, s.image)
		cmd := exec.CommandContext(ctx, "kubectl",
			"patch", resource, "-n", namespace,
			"--type=json", "-p", patchJSON,
		)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("upgrade_test %s: kubectl patch failed: %s", s.name, string(output))
		}

		// Log the event.
		event := NewChaosInjectedEvent(
			injectionID, scenarioName,
			fmt.Sprintf("upgrade_test:%s", s.name),
			namespace,
			[]string{resource},
		)
		files.AppendChaosEvent(event.ToJSON()) //nolint:errcheck

		// Wait for rollout.
		slog.Info("upgrade_test: waiting for rollout", slog.String("step", s.name))
		rolloutCmd := exec.CommandContext(ctx, "kubectl",
			"rollout", "status", resource,
			"-n", namespace,
			fmt.Sprintf("--timeout=%s", timeout),
		)
		rolloutOutput, err := rolloutCmd.CombinedOutput()
		if err != nil {
			slog.Warn("rollout status failed",
				slog.String("step", s.name),
				slog.String("output", string(rolloutOutput)),
			)
		}

		// Wait for N checkpoints to pass.
		slog.Info("upgrade_test: waiting for checkpoints",
			slog.String("step", s.name),
			slog.Uint64("checkpoints", checkpointsPerStep),
		)
		startCheckpoints := getPassedCheckpoints(managers, hypothesisID)

		for {
			if ctx.Err() != nil {
				return nil
			}

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}

			current := getPassedCheckpoints(managers, hypothesisID)
			if current >= startCheckpoints+checkpointsPerStep {
				slog.Info("upgrade_test: checkpoints passed",
					slog.String("step", s.name),
					slog.Uint64("passed", current),
				)
				break
			}

			// Check if hypothesis is still running.
			mgr := managers.Get(hypothesisID)
			if mgr == nil {
				return fmt.Errorf("upgrade_test: hypothesis %s not found", hypothesisID)
			}
		}
	}

	slog.Info("upgrade_test: full cycle completed", slog.String("injection_id", injectionID))
	return nil
}

// getPassedCheckpoints reads the passed checkpoint count from a hypothesis manager.
// If the manager supports a PassedCheckpoints() method, it calls it; otherwise returns 0.
func getPassedCheckpoints(managers ManagerRegistry, hypothesisID string) uint64 {
	mgr := managers.Get(hypothesisID)
	if mgr == nil {
		return 0
	}
	type checkpointer interface {
		PassedCheckpoints() uint64
	}
	if cp, ok := mgr.(checkpointer); ok {
		return cp.PassedCheckpoints()
	}
	return 0
}

// ---------------------------------------------------------------------------
// Duration parser
// ---------------------------------------------------------------------------

// ParseDuration parses a duration string like "30s", "5m", "1h", "100ms".
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)

	if strings.HasSuffix(s, "ms") {
		n, err := strconv.ParseUint(strings.TrimSuffix(s, "ms"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid ms duration: %w", err)
		}
		return time.Duration(n) * time.Millisecond, nil
	}
	if strings.HasSuffix(s, "s") {
		n, err := strconv.ParseUint(strings.TrimSuffix(s, "s"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid seconds duration: %w", err)
		}
		return time.Duration(n) * time.Second, nil
	}
	if strings.HasSuffix(s, "m") {
		n, err := strconv.ParseUint(strings.TrimSuffix(s, "m"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid minutes duration: %w", err)
		}
		return time.Duration(n) * time.Minute, nil
	}
	if strings.HasSuffix(s, "h") {
		n, err := strconv.ParseUint(strings.TrimSuffix(s, "h"), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid hours duration: %w", err)
		}
		return time.Duration(n) * time.Hour, nil
	}

	return 0, fmt.Errorf("unknown duration format: %s (use ms, s, m, or h suffix)", s)
}

// jsonMarshal is a helper for JSON encoding (used internally by upgrade test).
func jsonMarshal(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
