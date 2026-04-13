package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os/exec"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// ExecuteAction runs a single chaos action against the cluster and returns the
// names of pods that were targeted.
func ExecuteAction(ctx context.Context, clientset kubernetes.Interface, namespace string, action *ChaosAction) ([]string, error) {
	switch action.ActionType {
	case "rolling_update":
		return nil, rollingUpdate(ctx, clientset, namespace, action)
	case "custom":
		return nil, customAction(action)
	case "custom_patch":
		return nil, customPatch(ctx, namespace, action)
	}

	targetPods, err := resolveTargets(ctx, clientset, namespace, action)
	if err != nil {
		return nil, fmt.Errorf("resolve targets: %w", err)
	}
	if len(targetPods) == 0 {
		slog.Warn("no target pods found", slog.String("action", action.ActionType))
		return nil, nil
	}

	slog.Info("executing chaos action",
		slog.String("action", action.ActionType),
		slog.Any("targets", targetPods),
	)

	switch action.ActionType {
	case "pod_kill":
		err = podKill(ctx, clientset, namespace, targetPods)
	case "pod_restart":
		err = podRestart(ctx, clientset, namespace, targetPods)
	case "network_partition":
		err = networkPartition(namespace, targetPods)
	case "network_delay":
		err = networkDelay(namespace, targetPods, action)
	case "network_loss":
		err = networkLoss(namespace, targetPods, action)
	default:
		return nil, fmt.Errorf("unknown chaos action type: %s", action.ActionType)
	}

	if err != nil {
		return nil, err
	}
	return targetPods, nil
}

// resolveTargets determines which pods to target for the given action.
func resolveTargets(ctx context.Context, clientset kubernetes.Interface, namespace string, action *ChaosAction) ([]string, error) {
	// If a specific pod is named, use it directly.
	if action.TargetPod != nil && *action.TargetPod != "" {
		return []string{*action.TargetPod}, nil
	}

	// Build label selector string from match_labels.
	var parts []string
	for k, v := range action.Selector.MatchLabels {
		parts = append(parts, fmt.Sprintf("%s=%s", k, v))
	}
	labelStr := strings.Join(parts, ",")
	if labelStr == "" {
		return nil, fmt.Errorf("chaos action has no target_pod or selector labels")
	}

	podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelStr,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	var allPods []string
	for _, p := range podList.Items {
		if p.Name != "" {
			allPods = append(allPods, p.Name)
		}
	}
	if len(allPods) == 0 {
		return nil, nil
	}

	return selectPods(allPods, &action.Selector)
}

// selectPods applies the selection mode to filter pods.
func selectPods(pods []string, selector *LabelSelector) ([]string, error) {
	mode := selector.Mode
	if mode == "" {
		mode = "one"
	}

	switch mode {
	case "one":
		idx := rand.Intn(len(pods))
		return []string{pods[idx]}, nil

	case "all":
		result := make([]string, len(pods))
		copy(result, pods)
		return result, nil

	case "fixed":
		count := 1
		if selector.Count != nil {
			count = int(*selector.Count)
		}
		shuffled := make([]string, len(pods))
		copy(shuffled, pods)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		if count > len(shuffled) {
			count = len(shuffled)
		}
		return shuffled[:count], nil

	case "percentage":
		count := int(math.Ceil(float64(len(pods)) * float64(selector.Percentage) / 100.0))
		shuffled := make([]string, len(pods))
		copy(shuffled, pods)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		if count > len(shuffled) {
			count = len(shuffled)
		}
		return shuffled[:count], nil

	default:
		return nil, fmt.Errorf("unknown selector mode: %s", mode)
	}
}

// ---------------------------------------------------------------------------
// Chaos action implementations
// ---------------------------------------------------------------------------

func podKill(ctx context.Context, clientset kubernetes.Interface, namespace string, pods []string) error {
	gracePeriod := int64(0)
	for _, podName := range pods {
		slog.Info("killing pod", slog.String("pod", podName))
		err := clientset.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
			GracePeriodSeconds: &gracePeriod,
		})
		if err != nil {
			return fmt.Errorf("failed to kill pod %s: %w", podName, err)
		}
	}
	return nil
}

func podRestart(ctx context.Context, clientset kubernetes.Interface, namespace string, pods []string) error {
	for _, podName := range pods {
		slog.Info("restarting pod (delete + recreate)", slog.String("pod", podName))
		err := clientset.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
		if err != nil {
			return fmt.Errorf("failed to restart pod %s: %w", podName, err)
		}
	}
	return nil
}

func networkPartition(namespace string, pods []string) error {
	for _, podName := range pods {
		slog.Info("injecting network partition", slog.String("pod", podName))
		err := execInPod(namespace, podName,
			"sh", "-c", "iptables -A INPUT -j DROP && iptables -A OUTPUT -j DROP || true",
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func networkDelay(namespace string, pods []string, action *ChaosAction) error {
	delayMs := uint64(100)
	if v, ok := action.Params["delay_ms"]; ok {
		if f, ok := v.(float64); ok {
			delayMs = uint64(f)
		}
	}
	for _, podName := range pods {
		slog.Info("injecting network delay", slog.String("pod", podName), slog.Uint64("delay_ms", delayMs))
		err := execInPod(namespace, podName,
			"sh", "-c", fmt.Sprintf("tc qdisc add dev eth0 root netem delay %dms || true", delayMs),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func networkLoss(namespace string, pods []string, action *ChaosAction) error {
	lossPct := uint64(10)
	if v, ok := action.Params["loss_percent"]; ok {
		if f, ok := v.(float64); ok {
			lossPct = uint64(f)
		}
	}
	for _, podName := range pods {
		slog.Info("injecting network loss", slog.String("pod", podName), slog.Uint64("loss_pct", lossPct))
		err := execInPod(namespace, podName,
			"sh", "-c", fmt.Sprintf("tc qdisc add dev eth0 root netem loss %d%% || true", lossPct),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func rollingUpdate(ctx context.Context, clientset kubernetes.Interface, namespace string, action *ChaosAction) error {
	stsName, _ := paramString(action.Params, "statefulset")
	if stsName == "" {
		return fmt.Errorf("rolling_update requires 'statefulset' in params")
	}
	image, _ := paramString(action.Params, "image")
	if image == "" {
		return fmt.Errorf("rolling_update requires 'image' in params")
	}
	container, _ := paramString(action.Params, "container")
	if container == "" {
		container = "server"
	}

	slog.Info("initiating rolling update",
		slog.String("statefulset", stsName),
		slog.String("image", image),
		slog.String("container", container),
	)

	// Patch the container image using strategic merge patch.
	patchJSON := fmt.Sprintf(
		`{"spec":{"template":{"spec":{"containers":[{"name":"%s","image":"%s"}]}}}}`,
		container, image,
	)
	_, err := clientset.AppsV1().StatefulSets(namespace).Patch(
		ctx, stsName, k8stypes.StrategicMergePatchType, []byte(patchJSON), metav1.PatchOptions{
			FieldManager: "regret-chaos",
		},
	)
	if err != nil {
		return fmt.Errorf("failed to patch StatefulSet %s: %w", stsName, err)
	}

	slog.Info("patch applied, waiting for rollout", slog.String("statefulset", stsName))

	timeoutSecs, _ := paramString(action.Params, "timeout")
	if timeoutSecs == "" {
		timeoutSecs = "600s"
	}

	cmd := exec.CommandContext(ctx, "kubectl",
		"rollout", "status", "statefulset", stsName,
		"-n", namespace,
		fmt.Sprintf("--timeout=%s", timeoutSecs),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		slog.Warn("rollout status check failed",
			slog.String("statefulset", stsName),
			slog.String("output", string(output)),
		)
	} else {
		slog.Info("rolling update completed",
			slog.String("statefulset", stsName),
			slog.String("image", image),
		)
	}

	return nil
}

func customAction(action *ChaosAction) error {
	command, _ := paramString(action.Params, "command")
	if command == "" {
		return fmt.Errorf("custom chaos action requires 'command' in params")
	}

	slog.Info("executing custom chaos action", slog.String("command", command))
	cmd := exec.Command("sh", "-c", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("custom command failed: %s", string(output))
	}
	return nil
}

func customPatch(ctx context.Context, namespace string, action *ChaosAction) error {
	resourceName, _ := paramString(action.Params, "resource")
	if resourceName == "" {
		return fmt.Errorf("custom_patch requires 'resource' in params")
	}

	patchType := "merge"
	if v, ok := paramString(action.Params, "patch_type"); ok && v != "" {
		patchType = v
	}

	patchBytes, err := patchBytesFromParams(action.Params)
	if err != nil {
		return err
	}

	args := []string{"patch", resourceName, "-n", namespace, "--type", patchType, "--patch", string(patchBytes)}
	slog.Info("executing custom patch",
		slog.String("resource", resourceName),
		slog.String("patch_type", patchType),
	)
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("custom patch failed: %s", string(output))
	}
	return nil
}

// execInPod executes a command inside a pod via kubectl exec.
func execInPod(namespace, podName string, command ...string) error {
	args := []string{"exec", "-n", namespace, podName, "--"}
	args = append(args, command...)

	cmd := exec.Command("kubectl", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		slog.Warn("exec command failed",
			slog.String("pod", podName),
			slog.String("output", string(output)),
		)
	}
	return nil
}

// RecoverNetwork removes tc/iptables rules injected by network chaos.
func RecoverNetwork(namespace string, pods []string, actionType string) error {
	for _, podName := range pods {
		slog.Info("recovering network", slog.String("pod", podName), slog.String("action", actionType))
		switch actionType {
		case "network_partition":
			if err := execInPod(namespace, podName, "sh", "-c", "iptables -F || true"); err != nil {
				return err
			}
		case "network_delay", "network_loss":
			if err := execInPod(namespace, podName, "sh", "-c", "tc qdisc del dev eth0 root || true"); err != nil {
				return err
			}
		}
	}
	return nil
}

// paramString extracts a string value from a params map.
func paramString(params map[string]any, key string) (string, bool) {
	v, ok := params[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// paramUint64 extracts a uint64 value from a params map (JSON numbers decode as float64).
func paramUint64(params map[string]any, key string) (uint64, bool) {
	v, ok := params[key]
	if !ok {
		return 0, false
	}
	f, ok := v.(float64)
	if !ok {
		return 0, false
	}
	return uint64(f), true
}

func patchBytesFromParams(params map[string]any) ([]byte, error) {
	v, ok := params["patch"]
	if !ok {
		return nil, fmt.Errorf("custom_patch requires 'patch' in params")
	}
	if s, ok := v.(string); ok {
		return []byte(s), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal patch: %w", err)
	}
	return b, nil
}
