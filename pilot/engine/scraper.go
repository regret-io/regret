package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/regret-io/regret/pilot-go/storage"
)

const (
	DefaultMetricsPort    = 9091
	DefaultScraperIntervalSecs = 15
	DefaultScraperTimeoutSecs  = 5
)

var (
	DefaultScraperInterval = time.Duration(DefaultScraperIntervalSecs) * time.Second
	DefaultScraperTimeout  = time.Duration(DefaultScraperTimeoutSecs) * time.Second
)

// ScraperConfig configures the per-run metrics scraper.
type ScraperConfig struct {
	HypothesisID   string
	RunID          string
	MetricsURL     string
	Interval       time.Duration
	RequestTimeout time.Duration
}

// MetricsURLFromAdapterAddr derives a metrics URL from an adapter gRPC address.
// Returns empty string if the address cannot be parsed.
func MetricsURLFromAdapterAddr(addr string) string {
	stripped := addr
	stripped = strings.TrimPrefix(stripped, "http://")
	stripped = strings.TrimPrefix(stripped, "https://")

	// Everything before the path segment
	hostPort := stripped
	if idx := strings.IndexByte(stripped, '/'); idx >= 0 {
		hostPort = stripped[:idx]
	}

	host := hostPort
	if idx := strings.IndexByte(hostPort, ':'); idx >= 0 {
		host = hostPort[:idx]
	}

	if host == "" {
		return ""
	}
	return fmt.Sprintf("http://%s:%d/metrics", host, DefaultMetricsPort)
}

// RunScraper runs the scraper loop until ctx is cancelled. Errors from
// individual scrapes are logged and ignored.
func RunScraper(ctx context.Context, cfg ScraperConfig, sqlite *storage.SqliteStore) {
	client := &http.Client{Timeout: cfg.RequestTimeout}

	slog.Info("starting adapter metrics scraper",
		slog.String("hypothesis_id", cfg.HypothesisID),
		slog.String("run_id", cfg.RunID),
		slog.String("url", cfg.MetricsURL),
		slog.Int64("interval_secs", int64(cfg.Interval.Seconds())),
	)

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("metrics scraper stopping (run cancelled)",
				slog.String("run_id", cfg.RunID))
			return
		case <-ticker.C:
			if err := scrapeOnce(ctx, client, &cfg, sqlite); err != nil {
				slog.Debug("scrape failed",
					slog.String("run_id", cfg.RunID),
					slog.Any("error", err))
			}
		}
	}
}

func scrapeOnce(ctx context.Context, client *http.Client, cfg *ScraperConfig, sqlite *storage.SqliteStore) error {
	ts := time.Now().UnixMilli()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.MetricsURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("metrics endpoint returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	parsed := ParsePrometheusText(string(body))
	if len(parsed) == 0 {
		return nil
	}

	var samples []storage.MetricSample
	for _, p := range parsed {
		if !strings.HasPrefix(p.Metric, "regret_adapter_") {
			continue
		}
		samples = append(samples, storage.MetricSample{
			HypothesisID: cfg.HypothesisID,
			RunID:        cfg.RunID,
			Ts:           ts,
			Metric:       p.Metric,
			Labels:       p.Labels,
			Value:        p.Value,
		})
	}

	if len(samples) == 0 {
		return nil
	}

	return sqlite.InsertMetricSamples(ctx, samples)
}

// ParsedMetric is a single parsed metric line.
type ParsedMetric struct {
	Metric string
	Labels string
	Value  float64
}

// ParsePrometheusText is a minimal Prometheus text-format parser.
func ParsePrometheusText(body string) []ParsedMetric {
	var out []ParsedMetric
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		var metricName, labelStr, rest string
		if lbrace := strings.IndexByte(line, '{'); lbrace >= 0 {
			rbrace := strings.IndexByte(line[lbrace:], '}')
			if rbrace < 0 {
				continue
			}
			metricName = line[:lbrace]
			labelStr = line[lbrace+1 : lbrace+rbrace]
			rest = strings.TrimSpace(line[lbrace+rbrace+1:])
		} else {
			parts := strings.SplitN(line, " ", 2)
			if len(parts) < 2 {
				parts = strings.SplitN(line, "\t", 2)
			}
			metricName = parts[0]
			if len(parts) > 1 {
				rest = strings.TrimSpace(parts[1])
			}
		}

		// Value is the first whitespace-delimited token in rest
		valueStr := rest
		if idx := strings.IndexAny(rest, " \t"); idx >= 0 {
			valueStr = rest[:idx]
		}

		var value float64
		if _, err := fmt.Sscanf(valueStr, "%g", &value); err != nil {
			continue
		}
		if math.IsInf(value, 0) || math.IsNaN(value) {
			continue
		}

		labelsJSON := LabelsToJSON(labelStr)
		out = append(out, ParsedMetric{
			Metric: metricName,
			Labels: labelsJSON,
			Value:  value,
		})
	}
	return out
}

// LabelsToJSON parses a Prometheus label set like `op_type="put",status="ok"`
// into a canonical JSON object with sorted keys.
func LabelsToJSON(labelStr string) string {
	labelStr = strings.TrimSpace(labelStr)
	if labelStr == "" {
		return "{}"
	}

	m := make(map[string]string)
	bytes := []byte(labelStr)
	i := 0
	for i < len(bytes) {
		// Skip whitespace and commas
		for i < len(bytes) && (bytes[i] == ',' || bytes[i] == ' ' || bytes[i] == '\t') {
			i++
		}
		if i >= len(bytes) {
			break
		}

		// Parse key
		keyStart := i
		for i < len(bytes) && bytes[i] != '=' {
			i++
		}
		if i >= len(bytes) {
			break
		}
		key := strings.TrimSpace(string(bytes[keyStart:i]))
		i++ // skip '='

		// Parse quoted value
		if i >= len(bytes) || bytes[i] != '"' {
			break
		}
		i++ // skip opening quote
		var value strings.Builder
		for i < len(bytes) {
			switch bytes[i] {
			case '"':
				i++
				goto doneValue
			case '\\':
				if i+1 < len(bytes) {
					switch bytes[i+1] {
					case '"':
						value.WriteByte('"')
					case '\\':
						value.WriteByte('\\')
					case 'n':
						value.WriteByte('\n')
					default:
						value.WriteByte(bytes[i+1])
					}
					i += 2
				} else {
					i++
				}
			default:
				value.WriteByte(bytes[i])
				i++
			}
		}
	doneValue:
		if key != "" {
			m[key] = value.String()
		}
	}

	// Sort keys for canonical output
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	data, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(data)
}
