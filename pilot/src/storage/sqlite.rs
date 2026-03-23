use anyhow::Result;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{FromRow, SqlitePool};

#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

#[derive(Debug, Clone, FromRow)]
pub struct Hypothesis {
    pub id: String,
    pub name: String,
    pub generator: String,
    pub adapter: Option<String>,
    pub adapter_addr: Option<String>,
    pub duration: Option<String>,
    pub tolerance: Option<String>,
    pub checkpoint_every: String,
    pub key_space: String,
    pub config: String,
    pub status: String,
    pub created_at: String,
    pub last_run_at: Option<String>,
}

/// Adapter definition — reusable, not tied to a hypothesis.
#[derive(Debug, Clone, FromRow)]
pub struct AdapterRecord {
    pub id: String,
    pub name: String,
    pub image: String,
    pub env: String, // JSON object
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct GeneratorRecord {
    pub name: String,
    pub description: String,
    pub workload: String, // JSON object
    pub rate: i64,        // target ops/sec, 0 = unlimited
    pub builtin: i32,
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct ChaosScenarioRecord {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub actions: String, // JSON array
    pub created_at: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct ChaosInjectionRecord {
    pub id: String,
    pub scenario_id: String,
    pub scenario_name: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
pub struct HypothesisResult {
    pub id: String,
    pub hypothesis_id: String,
    pub run_id: String,
    pub total_batches: i64,
    pub total_checkpoints: i64,
    pub passed_checkpoints: i64,
    pub failed_checkpoints: i64,
    pub total_response_ops: i64,
    pub safety_violations: i64,
    pub stop_reason: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub created_at: String,
}

impl SqliteStore {
    pub async fn new(database_url: &str) -> Result<Self> {
        let options: SqliteConnectOptions =
            database_url.parse::<SqliteConnectOptions>()?.create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;

        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&pool)
            .await?;

        let migration = include_str!("../../migrations/001_init.sql");
        for statement in migration.split(';') {
            let trimmed = statement.trim();
            if !trimmed.is_empty() {
                sqlx::query(trimmed).execute(&pool).await?;
            }
        }

        Ok(Self { pool })
    }

    // --- Hypothesis CRUD ---

    pub async fn create_hypothesis(
        &self,
        id: &str,
        name: &str,
        generator: &str,
        adapter: Option<&str>,
        adapter_addr: Option<&str>,
        duration: Option<&str>,
        tolerance: Option<&str>,
        checkpoint_every: &str,
        key_space: &str,
        config: &str,
    ) -> Result<Hypothesis> {
        sqlx::query(
            "INSERT INTO hypotheses (id, name, generator, adapter, adapter_addr, duration, tolerance, checkpoint_every, key_space, config) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(generator)
        .bind(adapter)
        .bind(adapter_addr)
        .bind(duration)
        .bind(tolerance)
        .bind(checkpoint_every)
        .bind(key_space)
        .bind(config)
        .execute(&self.pool)
        .await?;

        self.get_hypothesis(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed to read just-created hypothesis"))
    }

    pub async fn get_hypothesis(&self, id: &str) -> Result<Option<Hypothesis>> {
        Ok(sqlx::query_as::<_, Hypothesis>("SELECT * FROM hypotheses WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn get_hypothesis_by_name(&self, name: &str) -> Result<Option<Hypothesis>> {
        Ok(sqlx::query_as::<_, Hypothesis>("SELECT * FROM hypotheses WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn list_hypotheses(&self) -> Result<Vec<Hypothesis>> {
        Ok(sqlx::query_as::<_, Hypothesis>("SELECT * FROM hypotheses ORDER BY created_at DESC")
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn delete_hypothesis(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM hypotheses WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn update_hypothesis(
        &self,
        id: &str,
        name: &str,
        generator: &str,
        adapter: Option<&str>,
        adapter_addr: Option<&str>,
        duration: Option<&str>,
        tolerance: Option<&str>,
    ) -> Result<Option<Hypothesis>> {
        sqlx::query(
            "UPDATE hypotheses SET name = ?, generator = ?, adapter = ?, adapter_addr = ?, duration = ?, tolerance = ? WHERE id = ?",
        )
        .bind(name).bind(generator).bind(adapter).bind(adapter_addr).bind(duration).bind(tolerance).bind(id)
        .execute(&self.pool).await?;
        self.get_hypothesis(id).await
    }

    pub async fn update_hypothesis_status(&self, id: &str, status: &str) -> Result<()> {
        sqlx::query("UPDATE hypotheses SET status = ? WHERE id = ?")
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_last_run_at(&self, id: &str, timestamp: &str) -> Result<()> {
        sqlx::query("UPDATE hypotheses SET last_run_at = ? WHERE id = ?")
            .bind(timestamp)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // --- Adapter CRUD (standalone definitions) ---

    pub async fn create_adapter(
        &self,
        id: &str,
        name: &str,
        image: &str,
        env_json: &str,
    ) -> Result<AdapterRecord> {
        sqlx::query(
            "INSERT INTO adapters (id, name, image, env) VALUES (?, ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(image)
        .bind(env_json)
        .execute(&self.pool)
        .await?;

        self.get_adapter_by_name(name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed to read just-created adapter"))
    }

    pub async fn get_adapter(&self, id: &str) -> Result<Option<AdapterRecord>> {
        Ok(sqlx::query_as::<_, AdapterRecord>("SELECT * FROM adapters WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn get_adapter_by_name(&self, name: &str) -> Result<Option<AdapterRecord>> {
        Ok(sqlx::query_as::<_, AdapterRecord>("SELECT * FROM adapters WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn list_adapters_all(&self) -> Result<Vec<AdapterRecord>> {
        Ok(sqlx::query_as::<_, AdapterRecord>("SELECT * FROM adapters ORDER BY created_at DESC")
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn delete_adapter(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM adapters WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // --- Profiles ---

    pub async fn upsert_generator(
        &self,
        name: &str,
        description: &str,
        workload_json: &str,
        rate: u32,
        builtin: bool,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO generators (name, description, workload, rate, builtin) VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(name) DO UPDATE SET workload = excluded.workload, description = excluded.description WHERE builtin = 1",
        )
        .bind(name)
        .bind(description)
        .bind(workload_json)
        .bind(rate as i32)
        .bind(builtin as i32)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_generator(&self, name: &str) -> Result<Option<GeneratorRecord>> {
        Ok(sqlx::query_as::<_, GeneratorRecord>("SELECT * FROM generators WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?)
    }

    pub async fn list_generators(&self) -> Result<Vec<GeneratorRecord>> {
        Ok(sqlx::query_as::<_, GeneratorRecord>("SELECT * FROM generators ORDER BY builtin DESC, name")
            .fetch_all(&self.pool)
            .await?)
    }

    pub async fn delete_generator(&self, name: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM generators WHERE name = ? AND builtin = 0")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // --- Results ---

    pub async fn create_result(&self, result: &HypothesisResult) -> Result<()> {
        sqlx::query(
            "INSERT INTO hypothesis_results (id, hypothesis_id, run_id, total_batches, total_checkpoints, passed_checkpoints, failed_checkpoints, total_response_ops, safety_violations, stop_reason, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&result.id)
        .bind(&result.hypothesis_id)
        .bind(&result.run_id)
        .bind(result.total_batches)
        .bind(result.total_checkpoints)
        .bind(result.passed_checkpoints)
        .bind(result.failed_checkpoints)
        .bind(result.total_response_ops)
        .bind(result.safety_violations)
        .bind(&result.stop_reason)
        .bind(&result.started_at)
        .bind(&result.finished_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_results(&self, hypothesis_id: &str) -> Result<Vec<HypothesisResult>> {
        Ok(sqlx::query_as::<_, HypothesisResult>(
            "SELECT * FROM hypothesis_results WHERE hypothesis_id = ? ORDER BY created_at DESC",
        )
        .bind(hypothesis_id)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_latest_result(
        &self,
        hypothesis_id: &str,
    ) -> Result<Option<HypothesisResult>> {
        Ok(sqlx::query_as::<_, HypothesisResult>(
            "SELECT * FROM hypothesis_results WHERE hypothesis_id = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(hypothesis_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn delete_result(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM hypothesis_results WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // --- Chaos Scenarios ---

    pub async fn create_chaos_scenario(
        &self,
        id: &str,
        name: &str,
        namespace: &str,
        actions_json: &str,
    ) -> Result<ChaosScenarioRecord> {
        sqlx::query(
            "INSERT INTO chaos_scenarios (id, name, namespace, actions) VALUES (?, ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(namespace)
        .bind(actions_json)
        .execute(&self.pool)
        .await?;

        self.get_chaos_scenario(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed to read just-created chaos scenario"))
    }

    pub async fn upsert_chaos_scenario(
        &self,
        id: &str,
        name: &str,
        namespace: &str,
        actions_json: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO chaos_scenarios (id, name, namespace, actions) VALUES (?, ?, ?, ?)
             ON CONFLICT(name) DO NOTHING",
        )
        .bind(id)
        .bind(name)
        .bind(namespace)
        .bind(actions_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_chaos_scenario(&self, id: &str) -> Result<Option<ChaosScenarioRecord>> {
        Ok(
            sqlx::query_as::<_, ChaosScenarioRecord>(
                "SELECT * FROM chaos_scenarios WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?,
        )
    }

    pub async fn list_chaos_scenarios(&self) -> Result<Vec<ChaosScenarioRecord>> {
        Ok(sqlx::query_as::<_, ChaosScenarioRecord>(
            "SELECT * FROM chaos_scenarios ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn update_chaos_scenario(
        &self,
        id: &str,
        name: &str,
        namespace: &str,
        actions_json: &str,
    ) -> Result<Option<ChaosScenarioRecord>> {
        sqlx::query(
            "UPDATE chaos_scenarios SET name = ?, namespace = ?, actions = ? WHERE id = ?",
        )
        .bind(name)
        .bind(namespace)
        .bind(actions_json)
        .bind(id)
        .execute(&self.pool)
        .await?;
        self.get_chaos_scenario(id).await
    }

    pub async fn delete_chaos_scenario(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM chaos_scenarios WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // --- Chaos Injections ---

    pub async fn create_chaos_injection(
        &self,
        id: &str,
        scenario_id: &str,
        scenario_name: &str,
    ) -> Result<ChaosInjectionRecord> {
        sqlx::query(
            "INSERT INTO chaos_injections (id, scenario_id, scenario_name) VALUES (?, ?, ?)",
        )
        .bind(id)
        .bind(scenario_id)
        .bind(scenario_name)
        .execute(&self.pool)
        .await?;

        self.get_chaos_injection(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed to read just-created chaos injection"))
    }

    pub async fn get_chaos_injection(&self, id: &str) -> Result<Option<ChaosInjectionRecord>> {
        Ok(
            sqlx::query_as::<_, ChaosInjectionRecord>(
                "SELECT * FROM chaos_injections WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?,
        )
    }

    pub async fn list_chaos_injections(&self) -> Result<Vec<ChaosInjectionRecord>> {
        Ok(sqlx::query_as::<_, ChaosInjectionRecord>(
            "SELECT * FROM chaos_injections ORDER BY started_at DESC",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn update_chaos_injection_status(
        &self,
        id: &str,
        status: &str,
        error: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE chaos_injections SET status = ?, finished_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), error = ? WHERE id = ?",
        )
        .bind(status)
        .bind(error)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_chaos_injection(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM chaos_injections WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
