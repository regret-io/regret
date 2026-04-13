package chaos

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"
)

func runWorkflow(
	ctx context.Context,
	runID string,
	workflow ChaosWorkflow,
	files EventLog,
	_ SqliteStore,
	managers ManagerRegistry,
) (string, error) {
	clientset, err := buildK8sClient()
	if err != nil {
		return "", fmt.Errorf("failed to create K8s client: %w", err)
	}

	for i, step := range workflow.Steps {
		select {
		case <-ctx.Done():
			return "stopped", nil
		default:
		}

		event := NewWorkflowStepEvent(workflow.ID, runID, workflow.Name, step.Name, "started", i)
		files.AppendChaosEvent(event.ToJSON()) //nolint:errcheck

		if err := runWorkflowStep(ctx, runID, workflow, step, clientset, files, managers); err != nil {
			failEvent := NewWorkflowStepEvent(workflow.ID, runID, workflow.Name, step.Name, "failed", i)
			files.AppendChaosEvent(failEvent.ToJSON()) //nolint:errcheck
			return "error", fmt.Errorf("workflow step %q failed: %w", step.Name, err)
		}

		doneEvent := NewWorkflowStepEvent(workflow.ID, runID, workflow.Name, step.Name, "completed", i)
		files.AppendChaosEvent(doneEvent.ToJSON()) //nolint:errcheck
	}

	return "finished", nil
}

func runWorkflowStep(
	ctx context.Context,
	runID string,
	workflow ChaosWorkflow,
	step ChaosWorkflowStep,
	clientset kubernetes.Interface,
	files EventLog,
	managers ManagerRegistry,
) error {
	if step.Action == nil {
		if step.Duration == nil {
			return fmt.Errorf("workflow wait step requires duration")
		}
		return waitDuration(ctx, *step.Duration)
	}

	action := *step.Action

	if step.Duration != nil && (action.Interval != nil || action.At != nil) {
		dur, err := ParseDuration(*step.Duration)
		if err != nil {
			return err
		}
		stepCtx, cancel := context.WithTimeout(ctx, dur)
		defer cancel()
		if err := runActionLoop(stepCtx, runID, workflow.Name, clientset, workflow.Namespace, &action, files, managers); err != nil {
			if stepCtx.Err() == context.DeadlineExceeded {
				return nil
			}
			return err
		}
		return nil
	}

	if err := runActionLoop(ctx, runID, workflow.Name, clientset, workflow.Namespace, &action, files, managers); err != nil {
		return err
	}

	if step.Duration != nil {
		return waitDuration(ctx, *step.Duration)
	}
	return nil
}

func waitDuration(ctx context.Context, value string) error {
	dur, err := ParseDuration(value)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(dur):
		return nil
	}
}
