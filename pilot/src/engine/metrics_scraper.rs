//! Background task that scrapes a Java adapter's Prometheus `/metrics`
//! endpoint and stores samples in SQLite so the Studio can chart them.
//!
//! The adapter SDK exposes metrics via the OpenTelemetry Prometheus exporter
//! (see `sdk/java/.../AdapterMetrics.java`), which speaks the standard
//! Prometheus text exposition format.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::storage::sqlite::{MetricSample, SqliteStore};

/// Config for the per-run scraper.
#[derive(Debug, Clone)]
pub struct ScraperConfig {
    pub hypothesis_id: String,
    pub run_id: String,
    /// Full URL like `http://oxia-adapter:9091/metrics`.
    pub metrics_url: String,
    pub interval: Duration,
    pub request_timeout: Duration,
}

impl ScraperConfig {
    pub const DEFAULT_PORT: u16 = 9091;
    pub const DEFAULT_INTERVAL_SECS: u64 = 15;
    pub const DEFAULT_TIMEOUT_SECS: u64 = 5;

    /// Derive a metrics URL from an adapter gRPC address such as
    /// `http://oxia-adapter:9090` or `oxia-adapter:9090`. Returns `None` if
    /// the address cannot be parsed.
    pub fn metrics_url_from_adapter_addr(addr: &str) -> Option<String> {
        let stripped = addr
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        // Everything before the path segment.
        let host_port = stripped.split('/').next().unwrap_or(stripped);
        let host = host_port.split(':').next().unwrap_or(host_port);
        if host.is_empty() {
            return None;
        }
        Some(format!("http://{host}:{}/metrics", Self::DEFAULT_PORT))
    }
}

/// Run the scraper loop until `cancel` fires. Any error from a single scrape
/// is logged and ignored so transient adapter unavailability doesn't kill
/// the task.
pub async fn run(cfg: ScraperConfig, sqlite: SqliteStore, cancel: CancellationToken) {
    let client = match reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "failed to build metrics HTTP client, scraper disabled");
            return;
        }
    };

    info!(
        hypothesis_id = %cfg.hypothesis_id,
        run_id = %cfg.run_id,
        url = %cfg.metrics_url,
        interval_secs = cfg.interval.as_secs(),
        "starting adapter metrics scraper"
    );

    let mut ticker = interval(cfg.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!(run_id = %cfg.run_id, "metrics scraper stopping (run cancelled)");
                return;
            }
            _ = ticker.tick() => {
                if let Err(e) = scrape_once(&client, &cfg, &sqlite).await {
                    debug!(run_id = %cfg.run_id, error = %e, "scrape failed");
                }
            }
        }
    }
}

async fn scrape_once(
    client: &reqwest::Client,
    cfg: &ScraperConfig,
    sqlite: &SqliteStore,
) -> anyhow::Result<()> {
    let ts = chrono::Utc::now().timestamp_millis();
    let resp = client.get(&cfg.metrics_url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("metrics endpoint returned {}", resp.status());
    }
    let body = resp.text().await?;

    let parsed = parse_prometheus_text(&body);
    if parsed.is_empty() {
        return Ok(());
    }

    let samples: Vec<MetricSample> = parsed
        .into_iter()
        .filter(|(name, _, _)| name.starts_with("regret_adapter_"))
        .map(|(metric, labels, value)| MetricSample {
            hypothesis_id: cfg.hypothesis_id.clone(),
            run_id: cfg.run_id.clone(),
            ts,
            metric,
            labels,
            value,
        })
        .collect();

    if samples.is_empty() {
        return Ok(());
    }

    sqlite.insert_metric_samples(&samples).await?;
    debug!(run_id = %cfg.run_id, count = samples.len(), "stored metric samples");
    Ok(())
}

/// Minimal Prometheus text-format parser. Returns `(metric_name, labels_json, value)`
/// tuples for each non-comment line. Lines that fail to parse are skipped.
///
/// Label values containing `"`, `\`, or `\n` follow the standard escaping
/// rules from <https://prometheus.io/docs/instrumenting/exposition_formats>.
fn parse_prometheus_text(body: &str) -> Vec<(String, String, f64)> {
    let mut out = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Split metric name / labels / value.
        let (metric_name, label_str, rest) = match line.find('{') {
            Some(lbrace) => {
                let Some(rbrace) = line[lbrace..].find('}') else { continue };
                let name = &line[..lbrace];
                let labels = &line[lbrace + 1..lbrace + rbrace];
                let rest = line[lbrace + rbrace + 1..].trim();
                (name, labels, rest)
            }
            None => {
                let mut parts = line.splitn(2, char::is_whitespace);
                let name = parts.next().unwrap_or("");
                let rest = parts.next().unwrap_or("").trim();
                (name, "", rest)
            }
        };

        // value is the first whitespace-delimited token in rest; anything
        // after (e.g. a scrape timestamp) is ignored.
        let value_str = rest.split_whitespace().next().unwrap_or("");
        let Ok(value) = value_str.parse::<f64>() else { continue };
        if !value.is_finite() {
            continue;
        }

        let labels_json = labels_to_json(label_str);
        out.push((metric_name.to_string(), labels_json, value));
    }
    out
}

/// Parse a Prometheus label set like `op_type="put",status="ok"` into a
/// canonical JSON object with keys sorted (so identical label sets produce
/// identical JSON strings, which matters for downstream aggregation).
fn labels_to_json(label_str: &str) -> String {
    if label_str.trim().is_empty() {
        return "{}".to_string();
    }

    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let bytes = label_str.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Skip whitespace and commas.
        while i < bytes.len() && (bytes[i] == b',' || bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        // Parse key.
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key = label_str[key_start..i].trim().to_string();
        i += 1; // skip '='

        // Parse quoted value.
        if i >= bytes.len() || bytes[i] != b'"' {
            break;
        }
        i += 1; // skip opening quote
        let mut value = String::new();
        while i < bytes.len() {
            match bytes[i] {
                b'"' => {
                    i += 1;
                    break;
                }
                b'\\' if i + 1 < bytes.len() => {
                    match bytes[i + 1] {
                        b'"' => value.push('"'),
                        b'\\' => value.push('\\'),
                        b'n' => value.push('\n'),
                        other => value.push(other as char),
                    }
                    i += 2;
                }
                other => {
                    value.push(other as char);
                    i += 1;
                }
            }
        }
        if !key.is_empty() {
            map.insert(key, value);
        }
    }

    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_counter() {
        let body = "\
# HELP regret_adapter_op_total Total ops
# TYPE regret_adapter_op_total counter
regret_adapter_op_total{op_type=\"put\",status=\"ok\"} 42
regret_adapter_op_total{op_type=\"get\",status=\"ok\"} 17
";
        let parsed = parse_prometheus_text(body);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, "regret_adapter_op_total");
        assert_eq!(parsed[0].1, r#"{"op_type":"put","status":"ok"}"#);
        assert_eq!(parsed[0].2, 42.0);
    }

    #[test]
    fn parses_no_labels() {
        let body = "regret_adapter_batch_size_count 9\n";
        let parsed = parse_prometheus_text(body);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0, "regret_adapter_batch_size_count");
        assert_eq!(parsed[0].1, "{}");
        assert_eq!(parsed[0].2, 9.0);
    }

    #[test]
    fn labels_sorted_canonically() {
        // Input order should not matter.
        let a = labels_to_json("status=\"ok\",op_type=\"put\"");
        let b = labels_to_json("op_type=\"put\",status=\"ok\"");
        assert_eq!(a, b);
        assert_eq!(a, r#"{"op_type":"put","status":"ok"}"#);
    }

    #[test]
    fn metrics_url_derivation() {
        assert_eq!(
            ScraperConfig::metrics_url_from_adapter_addr("http://oxia-adapter:9090"),
            Some("http://oxia-adapter:9091/metrics".to_string())
        );
        assert_eq!(
            ScraperConfig::metrics_url_from_adapter_addr("oxia-adapter:9090"),
            Some("http://oxia-adapter:9091/metrics".to_string())
        );
    }
}
