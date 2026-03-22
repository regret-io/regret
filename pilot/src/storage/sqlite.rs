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
    pub profile: String,
    pub state_machine: String, // JSON string
    pub tolerance: Option<String>, // JSON string
    pub status: String,
    pub created_at: String,
    pub last_run_at: Option<String>,
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
    pub failed_response_ops: i64,
    pub stop_reason: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub created_at: String,
}

impl SqliteStore {
    pub async fn new(database_url: &str) -> Result<Self> {
        let options: SqliteConnectOptions = database_url.parse::<SqliteConnectOptions>()?.create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;

        // Run migrations
        sqlx::query(include_str!("../../migrations/001_init.sql"))
            .execute(&pool)
            .await?;

        // Enable WAL mode and foreign keys
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&pool)
            .await?;

        Ok(Self { pool })
    }

    pub async fn create_hypothesis(
        &self,
        id: &str,
        name: &str,
        profile: &str,
        state_machine: &str,
        tolerance: Option<&str>,
    ) -> Result<Hypothesis> {
        sqlx::query(
            "INSERT INTO hypotheses (id, name, profile, state_machine, tolerance) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind(name)
        .bind(profile)
        .bind(state_machine)
        .bind(tolerance)
        .execute(&self.pool)
        .await?;

        self.get_hypothesis(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("failed to read just-created hypothesis"))
    }

    pub async fn get_hypothesis(&self, id: &str) -> Result<Option<Hypothesis>> {
        let row = sqlx::query_as::<_, Hypothesis>("SELECT * FROM hypotheses WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    pub async fn get_hypothesis_by_name(&self, name: &str) -> Result<Option<Hypothesis>> {
        let row = sqlx::query_as::<_, Hypothesis>("SELECT * FROM hypotheses WHERE name = ?")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    pub async fn list_hypotheses(&self) -> Result<Vec<Hypothesis>> {
        let rows =
            sqlx::query_as::<_, Hypothesis>("SELECT * FROM hypotheses ORDER BY created_at DESC")
                .fetch_all(&self.pool)
                .await?;
        Ok(rows)
    }

    pub async fn delete_hypothesis(&self, id: &str) -> Result<bool> {
        let result = sqlx::query("DELETE FROM hypotheses WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
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

    pub async fn create_result(&self, result: &HypothesisResult) -> Result<()> {
        sqlx::query(
            "INSERT INTO hypothesis_results (id, hypothesis_id, run_id, total_batches, total_checkpoints, passed_checkpoints, failed_checkpoints, total_response_ops, failed_response_ops, stop_reason, started_at, finished_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&result.id)
        .bind(&result.hypothesis_id)
        .bind(&result.run_id)
        .bind(result.total_batches)
        .bind(result.total_checkpoints)
        .bind(result.passed_checkpoints)
        .bind(result.failed_checkpoints)
        .bind(result.total_response_ops)
        .bind(result.failed_response_ops)
        .bind(&result.stop_reason)
        .bind(&result.started_at)
        .bind(&result.finished_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_results(&self, hypothesis_id: &str) -> Result<Vec<HypothesisResult>> {
        let rows = sqlx::query_as::<_, HypothesisResult>(
            "SELECT * FROM hypothesis_results WHERE hypothesis_id = ? ORDER BY created_at DESC",
        )
        .bind(hypothesis_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_latest_result(
        &self,
        hypothesis_id: &str,
    ) -> Result<Option<HypothesisResult>> {
        let row = sqlx::query_as::<_, HypothesisResult>(
            "SELECT * FROM hypothesis_results WHERE hypothesis_id = ? ORDER BY created_at DESC LIMIT 1",
        )
        .bind(hypothesis_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }
}
