//! `GET /api/hypothesis/{id}/metrics` — returns adapter metric samples
//! scraped by the pilot during a run, grouped into series that Studio can
//! render directly as charts.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::app_state::AppState;

use super::error::ApiError;

#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    /// Specific run_id to fetch samples for. If omitted, uses the latest
    /// result for the hypothesis (and falls back to the currently running
    /// run, if any).
    pub run_id: Option<String>,
    /// Unix epoch milliseconds. Only samples with `ts >= since` are returned.
    pub since: Option<i64>,
    /// Optional metric-name prefix filter (matches SQL `LIKE '<prefix>%'`).
    pub metric: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub hypothesis_id: String,
    pub run_id: String,
    pub metrics: Vec<MetricGroup>,
}

#[derive(Debug, Serialize)]
pub struct MetricGroup {
    pub name: String,
    pub series: Vec<MetricSeries>,
}

#[derive(Debug, Serialize)]
pub struct MetricSeries {
    pub labels: serde_json::Value,
    /// Time-ordered `[ts_ms, value]` pairs.
    pub points: Vec<[f64; 2]>,
}

pub async fn get_metrics(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<MetricsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    // Resolve run_id: explicit query param wins, otherwise fall back to the
    // live run, otherwise the most recent stored result.
    let hypothesis = state
        .sqlite
        .get_hypothesis(&id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("hypothesis {id} not found")))?;

    let run_id = if let Some(run_id) = q.run_id {
        run_id
    } else {
        let live = if let Some(manager) = state.managers.get(&id).await {
            let mgr = manager.lock().await;
            mgr.run_id().map(|s| s.to_string())
        } else {
            None
        };
        if let Some(r) = live {
            r
        } else {
            state
                .sqlite
                .get_latest_result(&id)
                .await?
                .map(|r| r.run_id)
                .ok_or_else(|| ApiError::NotFound(format!("no runs for hypothesis {id}")))?
        }
    };

    let rows = state
        .sqlite
        .query_metric_samples(&run_id, q.metric.as_deref(), q.since)
        .await?;

    // Group by metric name, then by labels, preserving time order.
    let mut grouped: BTreeMap<String, BTreeMap<String, Vec<[f64; 2]>>> = BTreeMap::new();
    for row in rows {
        grouped
            .entry(row.metric)
            .or_default()
            .entry(row.labels)
            .or_default()
            .push([row.ts as f64, row.value]);
    }

    let mut metrics: Vec<MetricGroup> = grouped
        .into_iter()
        .map(|(name, series_by_labels)| MetricGroup {
            name,
            series: series_by_labels
                .into_iter()
                .map(|(labels, points)| MetricSeries {
                    labels: serde_json::from_str(&labels).unwrap_or(serde_json::json!({})),
                    points,
                })
                .collect(),
        })
        .collect();

    // Derive p50/p95/p99 series from any histogram `_bucket` groups so the
    // frontend doesn't have to do the quantile math itself.
    let mut derived = compute_percentile_groups(&metrics);
    metrics.append(&mut derived);
    metrics.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Json(MetricsResponse {
        hypothesis_id: hypothesis.id,
        run_id,
        metrics,
    }))
}

/// For every `*_bucket` group we find in `metrics`, build derived groups
/// `*_p50` / `*_p95` / `*_p99` using classic histogram-quantile linear
/// interpolation within the matching bucket. One series is emitted per
/// distinct non-`le` label set (e.g. per `op_type`).
fn compute_percentile_groups(metrics: &[MetricGroup]) -> Vec<MetricGroup> {
    let mut out: Vec<MetricGroup> = Vec::new();

    for group in metrics {
        let Some(base_name) = group.name.strip_suffix("_bucket") else {
            continue;
        };

        // Key: (non-le labels JSON, ts_ms) → Vec<(le, cumulative_count)>
        let mut buckets_by_key: BTreeMap<(String, i64), Vec<(f64, f64)>> = BTreeMap::new();

        for series in &group.series {
            let labels_obj = series
                .labels
                .as_object()
                .cloned()
                .unwrap_or_default();

            let le = match labels_obj.get("le").and_then(|v| v.as_str()) {
                Some("+Inf") | Some("+inf") | Some("Inf") => f64::INFINITY,
                Some(s) => match s.parse::<f64>() {
                    Ok(v) => v,
                    Err(_) => continue,
                },
                None => continue,
            };

            // Strip `le` for the non-bucket-specific labels key.
            let mut non_le = labels_obj.clone();
            non_le.remove("le");
            // Also strip `otel_scope_name` — it's the same everywhere and just
            // clutters the rendered legend.
            non_le.remove("otel_scope_name");
            let non_le_json = serde_json::to_string(&non_le).unwrap_or_else(|_| "{}".to_string());

            for point in &series.points {
                let ts = point[0] as i64;
                let count = point[1];
                buckets_by_key
                    .entry((non_le_json.clone(), ts))
                    .or_default()
                    .push((le, count));
            }
        }

        // Compute each quantile per (label-set, ts) and fan out into series.
        let mut p_series: [BTreeMap<String, Vec<[f64; 2]>>; 3] = Default::default();
        let quantiles = [0.5_f64, 0.95, 0.99];

        for ((non_le_json, ts), mut buckets) in buckets_by_key {
            buckets.sort_by(|a, b| {
                a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
            });
            for (i, q) in quantiles.iter().enumerate() {
                if let Some(v) = histogram_quantile(*q, &buckets) {
                    p_series[i]
                        .entry(non_le_json.clone())
                        .or_default()
                        .push([ts as f64, v]);
                }
            }
        }

        let suffixes = ["_p50", "_p95", "_p99"];
        for (i, suffix) in suffixes.iter().enumerate() {
            if p_series[i].is_empty() {
                continue;
            }
            let name = format!("{base_name}{suffix}");
            let series: Vec<MetricSeries> = std::mem::take(&mut p_series[i])
                .into_iter()
                .map(|(labels_json, points)| MetricSeries {
                    labels: serde_json::from_str(&labels_json)
                        .unwrap_or_else(|_| serde_json::json!({})),
                    points,
                })
                .collect();
            out.push(MetricGroup { name, series });
        }
    }

    out
}

/// Classic Prometheus-style histogram_quantile: linear interpolation between
/// the two adjacent cumulative buckets that bracket the target count. Returns
/// `None` if the histogram has no observations.
fn histogram_quantile(q: f64, buckets: &[(f64, f64)]) -> Option<f64> {
    if buckets.is_empty() {
        return None;
    }
    let total = buckets.last().map(|(_, c)| *c).unwrap_or(0.0);
    if !(total > 0.0) {
        return None;
    }
    let target = q * total;

    let mut prev_le = 0.0_f64;
    let mut prev_count = 0.0_f64;

    for (le, count) in buckets {
        if *count >= target {
            if le.is_infinite() {
                // Target lies in the +Inf bucket — return the previous finite le.
                return Some(prev_le);
            }
            let delta_c = count - prev_count;
            if delta_c <= 0.0 {
                return Some(*le);
            }
            let delta_le = le - prev_le;
            return Some(prev_le + delta_le * (target - prev_count) / delta_c);
        }
        if !le.is_infinite() {
            prev_le = *le;
        }
        prev_count = *count;
    }

    // Fallback: everything in the +Inf bucket.
    Some(prev_le)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_linear_interpolation() {
        // 10 observations split 4 / 3 / 3 across buckets le=1, le=2, le=+Inf (cumulative: 4,7,10)
        let buckets = vec![(1.0, 4.0), (2.0, 7.0), (f64::INFINITY, 10.0)];
        // p50: target=5 → lands in the 1..2 bucket, fraction (5-4)/(7-4)=1/3
        let p50 = histogram_quantile(0.5, &buckets).unwrap();
        assert!((p50 - (1.0 + (2.0 - 1.0) * (1.0 / 3.0))).abs() < 1e-9);
        // p99: target=9.9 → lands in the +Inf bucket → returns last finite le
        assert_eq!(histogram_quantile(0.99, &buckets).unwrap(), 2.0);
    }

    #[test]
    fn quantile_empty_histogram() {
        assert!(histogram_quantile(0.5, &[]).is_none());
        assert!(histogram_quantile(0.5, &[(1.0, 0.0)]).is_none());
    }

    #[test]
    fn derived_percentile_groups_from_bucket_group() {
        // Build one _bucket group with two label sets (op_type=put, op_type=get)
        // at the same timestamp.
        let bucket_group = MetricGroup {
            name: "foo_bucket".to_string(),
            series: vec![
                // op_type=put: 2/4/4 → p50 ≈ interpolate in (1, 2]
                MetricSeries {
                    labels: serde_json::json!({"op_type":"put","le":"1"}),
                    points: vec![[0.0, 2.0]],
                },
                MetricSeries {
                    labels: serde_json::json!({"op_type":"put","le":"2"}),
                    points: vec![[0.0, 4.0]],
                },
                MetricSeries {
                    labels: serde_json::json!({"op_type":"put","le":"+Inf"}),
                    points: vec![[0.0, 4.0]],
                },
                // op_type=get: 0/0/1 → p50 is in the +Inf bucket
                MetricSeries {
                    labels: serde_json::json!({"op_type":"get","le":"1"}),
                    points: vec![[0.0, 0.0]],
                },
                MetricSeries {
                    labels: serde_json::json!({"op_type":"get","le":"2"}),
                    points: vec![[0.0, 0.0]],
                },
                MetricSeries {
                    labels: serde_json::json!({"op_type":"get","le":"+Inf"}),
                    points: vec![[0.0, 1.0]],
                },
            ],
        };

        let derived = compute_percentile_groups(&[bucket_group]);
        assert_eq!(derived.len(), 3, "expect p50, p95, p99 groups");
        let p50 = derived.iter().find(|g| g.name == "foo_p50").unwrap();
        assert_eq!(p50.series.len(), 2, "expect one series per op_type");
    }
}
