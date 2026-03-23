use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};

#[derive(Clone)]
pub struct FileStore {
    base_path: Arc<PathBuf>,
}

impl FileStore {
    pub fn new(base_path: &Path) -> Self {
        Self {
            base_path: Arc::new(base_path.to_path_buf()),
        }
    }

    fn hypothesis_dir(&self, id: &str) -> PathBuf {
        self.base_path.join("hypothesis").join(id)
    }

    pub fn create_hypothesis_dir(&self, id: &str) -> Result<()> {
        let dir = self.hypothesis_dir(id);
        fs::create_dir_all(&dir).context(format!("failed to create dir {}", dir.display()))?;
        Ok(())
    }

    pub fn delete_hypothesis_dir(&self, id: &str) -> Result<()> {
        let dir = self.hypothesis_dir(id);
        if dir.exists() {
            fs::remove_dir_all(&dir)
                .context(format!("failed to remove dir {}", dir.display()))?;
        }
        Ok(())
    }

    pub fn append_event(&self, id: &str, event_json: &str) -> Result<()> {
        let path = self.hypothesis_dir(id).join("events.jsonl");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .context(format!("failed to open {}", path.display()))?;
        writeln!(file, "{event_json}")?;
        Ok(())
    }

    pub fn read_events(
        &self,
        id: &str,
        run_id: Option<&str>,
        event_type: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<serde_json::Value>> {
        let path = self.hypothesis_dir(id).join("events.jsonl");
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = fs::File::open(&path)?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: serde_json::Value = serde_json::from_str(&line)?;

            if let Some(rid) = run_id {
                if event.get("run_id").and_then(|v| v.as_str()) != Some(rid) {
                    continue;
                }
            }
            if let Some(et) = event_type {
                if event.get("type").and_then(|v| v.as_str()) != Some(et) {
                    continue;
                }
            }
            if let Some(s) = since {
                if let Some(ts) = event.get("timestamp").and_then(|v| v.as_str()) {
                    if ts < s {
                        continue;
                    }
                }
            }

            events.push(event);
        }

        Ok(events)
    }

    // --- Chaos event log (global) ---

    fn chaos_events_path(&self) -> PathBuf {
        self.base_path.join("chaos").join("events.jsonl")
    }

    pub fn append_chaos_event(&self, event_json: &str) -> Result<()> {
        let path = self.chaos_events_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .context(format!("failed to open {}", path.display()))?;
        writeln!(file, "{event_json}")?;
        Ok(())
    }

    pub fn read_chaos_events(
        &self,
        since: Option<&str>,
    ) -> Result<Vec<serde_json::Value>> {
        let path = self.chaos_events_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&path)?;
        let reader = BufReader::new(file);
        let mut events = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: serde_json::Value = serde_json::from_str(&line)?;
            if let Some(s) = since {
                if let Some(ts) = event.get("timestamp").and_then(|v| v.as_str()) {
                    if ts < s {
                        continue;
                    }
                }
            }
            events.push(event);
        }
        Ok(events)
    }

    /// Read hypothesis events merged with chaos events, sorted by timestamp.
    pub fn read_merged_events(
        &self,
        hypothesis_id: &str,
        run_id: Option<&str>,
        event_type: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<serde_json::Value>> {
        let mut hypothesis_events = self.read_events(hypothesis_id, run_id, event_type, since)?;

        // Only merge chaos events if no specific event_type filter excludes them
        let include_chaos = event_type.is_none()
            || event_type == Some("ChaosInjected")
            || event_type == Some("ChaosRecovered")
            || event_type == Some("ChaosError")
            || event_type == Some("InjectionStarted")
            || event_type == Some("InjectionStopped");

        if include_chaos {
            let chaos_events = self.read_chaos_events(since)?;
            // Filter chaos events by type if specified
            for ce in chaos_events {
                if let Some(et) = event_type {
                    if ce.get("type").and_then(|v| v.as_str()) != Some(et) {
                        continue;
                    }
                }
                hypothesis_events.push(ce);
            }
        }

        // Sort all events by timestamp
        hypothesis_events.sort_by(|a, b| {
            let ts_a = a.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
            let ts_b = b.get("timestamp").and_then(|v| v.as_str()).unwrap_or("");
            ts_a.cmp(ts_b)
        });

        Ok(hypothesis_events)
    }

    pub fn write_checkpoint(
        &self,
        id: &str,
        expect: &serde_json::Value,
        actual: &serde_json::Value,
    ) -> Result<()> {
        let dir = self.hypothesis_dir(id).join("checkpoint");
        fs::create_dir_all(&dir)?;

        let expect_path = dir.join("expect.json");
        let actual_path = dir.join("actual.json");

        fs::write(&expect_path, serde_json::to_string_pretty(expect)?)?;
        fs::write(&actual_path, serde_json::to_string_pretty(actual)?)?;

        Ok(())
    }

    pub fn create_bundle(&self, id: &str, _run_id_filter: Option<&str>) -> Result<Vec<u8>> {
        use zip::write::SimpleFileOptions;
        use zip::ZipWriter;

        let dir = self.hypothesis_dir(id);
        let buf = Vec::new();
        let mut zip = ZipWriter::new(std::io::Cursor::new(buf));

        let prefix = format!("hypothesis-{id}/");
        let options = SimpleFileOptions::default();

        // origin.jsonl
        let origin_path = dir.join("origin.jsonl");
        if origin_path.exists() {
            zip.start_file(format!("{prefix}origin.jsonl"), options)?;
            let data = fs::read(&origin_path)?;
            zip.write_all(&data)?;
        }

        // events.jsonl
        let events_path = dir.join("events.jsonl");
        if events_path.exists() {
            zip.start_file(format!("{prefix}events.jsonl"), options)?;
            let data = fs::read(&events_path)?;
            zip.write_all(&data)?;
        }

        // checkpoint/expect.json
        let expect_path = dir.join("checkpoint").join("expect.json");
        if expect_path.exists() {
            zip.start_file(format!("{prefix}checkpoint/expect.json"), options)?;
            let data = fs::read(&expect_path)?;
            zip.write_all(&data)?;
        }

        // checkpoint/actual.json
        let actual_path = dir.join("checkpoint").join("actual.json");
        if actual_path.exists() {
            zip.start_file(format!("{prefix}checkpoint/actual.json"), options)?;
            let data = fs::read(&actual_path)?;
            zip.write_all(&data)?;
        }

        // chaos/events.jsonl (global chaos events)
        let chaos_path = self.chaos_events_path();
        if chaos_path.exists() {
            zip.start_file(format!("{prefix}chaos-events.jsonl"), options)?;
            let data = fs::read(&chaos_path)?;
            zip.write_all(&data)?;
        }

        // merged-timeline.jsonl — merged hypothesis + chaos events sorted by timestamp
        let merged = self.read_merged_events(id, None, None, None)?;
        if !merged.is_empty() {
            zip.start_file(format!("{prefix}merged-timeline.jsonl"), options)?;
            for event in &merged {
                let line = serde_json::to_string(event).unwrap_or_default();
                zip.write_all(line.as_bytes())?;
                zip.write_all(b"\n")?;
            }
        }

        let cursor = zip.finish()?;
        Ok(cursor.into_inner())
    }
}
