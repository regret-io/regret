package endpoints

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
)

type metricsHandlers struct {
	state *AppState
}

// GetMetrics handles GET /api/hypothesis/{id}/metrics.
func (m *metricsHandlers) GetMetrics(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx := r.Context()

	hyp, err := m.state.Sqlite.GetHypothesis(ctx, id)
	if err != nil {
		WriteError(w, NotFound(fmt.Sprintf("hypothesis %s not found", id)))
		return
	}

	q := r.URL.Query()

	// Resolve run_id
	var runID string
	if rid := q.Get("run_id"); rid != "" {
		runID = rid
	} else {
		// Try active run
		mgr := m.state.Managers.Get(id)
		if mgr != nil {
			if rid := mgr.RunID(); rid != nil {
				runID = *rid
			}
		}
		// Fall back to latest result
		if runID == "" {
			latest, err := m.state.Sqlite.GetLatestResult(ctx, id)
			if err != nil || latest == nil {
				WriteError(w, NotFound(fmt.Sprintf("no runs for hypothesis %s", id)))
				return
			}
			runID = latest.RunID
		}
	}

	// Parse optional filters
	var metricPrefix *string
	if mp := q.Get("metric"); mp != "" {
		metricPrefix = &mp
	}
	var since *int64
	if s := q.Get("since"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			since = &v
		}
	}

	rows, err := m.state.Sqlite.QueryMetricSamples(ctx, runID, metricPrefix, since)
	if err != nil {
		WriteError(w, InternalError(err))
		return
	}

	// Group by metric name, then by labels, preserving time order
	type seriesKey struct {
		metric string
		labels string
	}
	grouped := make(map[string]map[string][][2]float64)
	for _, row := range rows {
		if _, ok := grouped[row.Metric]; !ok {
			grouped[row.Metric] = make(map[string][][2]float64)
		}
		grouped[row.Metric][row.Labels] = append(grouped[row.Metric][row.Labels], [2]float64{float64(row.Ts), row.Value})
	}

	// Build sorted MetricGroups
	metrics := make([]MetricGroup, 0)
	metricNames := make([]string, 0, len(grouped))
	for name := range grouped {
		metricNames = append(metricNames, name)
	}
	sort.Strings(metricNames)

	for _, name := range metricNames {
		seriesByLabels := grouped[name]
		series := make([]MetricSeries, 0)
		for labels, points := range seriesByLabels {
			var labelsObj interface{}
			if err := json.Unmarshal([]byte(labels), &labelsObj); err != nil {
				labelsObj = map[string]interface{}{}
			}
			series = append(series, MetricSeries{
				Labels: labelsObj,
				Points: points,
			})
		}
		metrics = append(metrics, MetricGroup{Name: name, Series: series})
	}

	// Derive percentile groups from histogram bucket groups
	derived := computePercentileGroups(metrics)
	metrics = append(metrics, derived...)
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].Name < metrics[j].Name
	})

	WriteJSON(w, http.StatusOK, MetricsResponse{
		HypothesisID: hyp.ID,
		RunID:        runID,
		Metrics:      metrics,
	})
}

// computePercentileGroups derives p50/p95/p99 groups from *_bucket histogram groups.
func computePercentileGroups(metrics []MetricGroup) []MetricGroup {
	var out []MetricGroup

	for _, group := range metrics {
		baseName := strings.TrimSuffix(group.Name, "_bucket")
		if baseName == group.Name {
			continue // not a _bucket group
		}

		// Key: (non-le labels JSON, ts_ms) -> Vec<(le, count)>
		type bucketKey struct {
			labels string
			ts     int64
		}
		bucketsByKey := make(map[bucketKey][]struct{ Le, Count float64 })

		for _, series := range group.Series {
			labelsObj, ok := series.Labels.(map[string]interface{})
			if !ok {
				continue
			}
			leStr, _ := labelsObj["le"].(string)
			var le float64
			switch leStr {
			case "+Inf", "+inf", "Inf":
				le = math.Inf(1)
			default:
				var err error
				le, err = strconv.ParseFloat(leStr, 64)
				if err != nil {
					continue
				}
			}

			// Build non-le labels key
			nonLe := make(map[string]interface{})
			for k, v := range labelsObj {
				if k != "le" && k != "otel_scope_name" {
					nonLe[k] = v
				}
			}
			nonLeJSON, _ := json.Marshal(nonLe)
			nonLeStr := string(nonLeJSON)

			for _, point := range series.Points {
				ts := int64(point[0])
				count := point[1]
				key := bucketKey{labels: nonLeStr, ts: ts}
				bucketsByKey[key] = append(bucketsByKey[key], struct{ Le, Count float64 }{le, count})
			}
		}

		quantiles := [3]float64{0.5, 0.95, 0.99}
		suffixes := [3]string{"_p50", "_p95", "_p99"}
		pSeries := [3]map[string][][2]float64{}
		for i := range pSeries {
			pSeries[i] = make(map[string][][2]float64)
		}

		for key, buckets := range bucketsByKey {
			// Sort by le
			sort.Slice(buckets, func(i, j int) bool {
				return buckets[i].Le < buckets[j].Le
			})

			pairs := make([]struct{ Le, Count float64 }, len(buckets))
			copy(pairs, buckets)

			for i, q := range quantiles {
				if v := histogramQuantile(q, pairs); v != nil {
					pSeries[i][key.labels] = append(pSeries[i][key.labels], [2]float64{float64(key.ts), *v})
				}
			}
		}

		for i, suffix := range suffixes {
			if len(pSeries[i]) == 0 {
				continue
			}
			name := baseName + suffix
			series := make([]MetricSeries, 0)
			for labelsJSON, points := range pSeries[i] {
				var labelsObj interface{}
				if json.Unmarshal([]byte(labelsJSON), &labelsObj) != nil {
					labelsObj = map[string]interface{}{}
				}
				series = append(series, MetricSeries{
					Labels: labelsObj,
					Points: points,
				})
			}
			out = append(out, MetricGroup{Name: name, Series: series})
		}
	}

	return out
}

// histogramQuantile computes a Prometheus-style histogram quantile using
// linear interpolation between adjacent cumulative buckets.
func histogramQuantile(q float64, buckets []struct{ Le, Count float64 }) *float64 {
	if len(buckets) == 0 {
		return nil
	}
	total := buckets[len(buckets)-1].Count
	if total <= 0 {
		return nil
	}
	target := q * total

	var prevLe, prevCount float64

	for _, b := range buckets {
		if b.Count >= target {
			if math.IsInf(b.Le, 1) {
				return &prevLe
			}
			deltaC := b.Count - prevCount
			if deltaC <= 0 {
				return &b.Le
			}
			deltaLe := b.Le - prevLe
			result := prevLe + deltaLe*(target-prevCount)/deltaC
			return &result
		}
		if !math.IsInf(b.Le, 0) {
			prevLe = b.Le
		}
		prevCount = b.Count
	}

	// Fallback: everything in the +Inf bucket
	return &prevLe
}
