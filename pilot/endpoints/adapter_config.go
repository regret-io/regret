package endpoints

import (
	"bytes"
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type AdapterConfig struct {
	Image  string            `yaml:"image" json:"image"`
	Env    map[string]string `yaml:"env,omitempty" json:"env,omitempty"`
	CPU    *string           `yaml:"cpu,omitempty" json:"cpu,omitempty"`
	Memory *string           `yaml:"memory,omitempty" json:"memory,omitempty"`
}

func parseAdapterConfigYAML(configYAML string) (*AdapterConfig, error) {
	var cfg AdapterConfig
	dec := yaml.NewDecoder(bytes.NewBufferString(configYAML))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse adapter config yaml: %w", err)
	}
	if cfg.Image == "" {
		return nil, fmt.Errorf("adapter config requires image")
	}
	if cfg.CPU != nil && *cfg.CPU != "" {
		if _, err := resource.ParseQuantity(*cfg.CPU); err != nil {
			return nil, fmt.Errorf("invalid cpu quantity: %w", err)
		}
	}
	if cfg.Memory != nil && *cfg.Memory != "" {
		if _, err := resource.ParseQuantity(*cfg.Memory); err != nil {
			return nil, fmt.Errorf("invalid memory quantity: %w", err)
		}
	}
	if cfg.Env == nil {
		cfg.Env = map[string]string{}
	}
	return &cfg, nil
}

func legacyAdapterConfigYAML(image string, env map[string]string) (string, error) {
	cfg := AdapterConfig{
		Image: image,
		Env:   env,
	}
	return normalizeAdapterConfigYAML(&cfg)
}

func adapterConfigFromRequest(req *CreateAdapterRequest) (*AdapterConfig, string, error) {
	if req.ConfigYAML != "" {
		cfg, err := parseAdapterConfigYAML(req.ConfigYAML)
		if err != nil {
			return nil, "", err
		}
		configYAML, err := normalizeAdapterConfigYAML(cfg)
		if err != nil {
			return nil, "", err
		}
		return cfg, configYAML, nil
	}

	if req.Image == "" {
		return nil, "", fmt.Errorf("adapter config_yaml or image is required")
	}
	configYAML, err := legacyAdapterConfigYAML(req.Image, req.Env)
	if err != nil {
		return nil, "", err
	}
	cfg, err := parseAdapterConfigYAML(configYAML)
	if err != nil {
		return nil, "", err
	}
	return cfg, configYAML, nil
}

func normalizeAdapterConfigYAML(cfg *AdapterConfig) (string, error) {
	if cfg.Env == nil {
		cfg.Env = map[string]string{}
	}
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal adapter config yaml: %w", err)
	}
	return string(b), nil
}

func adapterEnvJSONMap(cfg *AdapterConfig) map[string]string {
	if cfg.Env == nil {
		return map[string]string{}
	}
	return cfg.Env
}

func buildAdapterDeployment(namespace, adapterID, adapterName string, cfg *AdapterConfig) *appsv1.Deployment {
	labels := adapterLabels(adapterID, adapterName)

	container := corev1.Container{
		Name:  "adapter",
		Image: cfg.Image,
		Ports: []corev1.ContainerPort{
			{Name: "grpc", ContainerPort: 9090},
			{Name: "metrics", ContainerPort: 9091},
		},
	}

	if len(cfg.Env) > 0 {
		keys := make([]string, 0, len(cfg.Env))
		for k := range cfg.Env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		envVars := make([]corev1.EnvVar, 0, len(keys))
		for _, k := range keys {
			envVars = append(envVars, corev1.EnvVar{Name: k, Value: cfg.Env[k]})
		}
		container.Env = envVars
	}

	if cfg.CPU != nil || cfg.Memory != nil {
		reqs := corev1.ResourceList{}
		limits := corev1.ResourceList{}
		if cfg.CPU != nil && *cfg.CPU != "" {
			q, err := resource.ParseQuantity(*cfg.CPU)
			if err == nil {
				reqs[corev1.ResourceCPU] = q
				limits[corev1.ResourceCPU] = q
			}
		}
		if cfg.Memory != nil && *cfg.Memory != "" {
			q, err := resource.ParseQuantity(*cfg.Memory)
			if err == nil {
				reqs[corev1.ResourceMemory] = q
				limits[corev1.ResourceMemory] = q
			}
		}
		container.Resources = corev1.ResourceRequirements{
			Requests: reqs,
			Limits:   limits,
		}
	}

	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      adapterWorkloadName(adapterName),
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{container},
				},
			},
		},
	}
}

func buildAdapterService(namespace, adapterID, adapterName string) *corev1.Service {
	labels := adapterLabels(adapterID, adapterName)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      adapterWorkloadName(adapterName),
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{Name: "grpc", Port: 9090, TargetPort: intstrFromInt(9090)},
				{Name: "metrics", Port: 9091, TargetPort: intstrFromInt(9091)},
			},
		},
	}
}

func adapterLabels(adapterID, adapterName string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name": fmt.Sprintf("adapter-%s", adapterName),
		"regret.io/adapter":      adapterName,
		"regret.io/adapter-id":   adapterID,
	}
}

func adapterWorkloadName(adapterName string) string {
	return fmt.Sprintf("adapter-%s", adapterName)
}

func intstrFromInt(v int) intstr.IntOrString {
	return intstr.FromInt(v)
}
