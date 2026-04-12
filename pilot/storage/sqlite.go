package storage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------

type Hypothesis struct {
	ID              string  `json:"id"`
	Name            string  `json:"name"`
	Generator       string  `json:"generator"`
	Adapter         *string `json:"adapter,omitempty"`
	AdapterAddr     *string `json:"adapter_addr,omitempty"`
	Duration        *string `json:"duration,omitempty"`
	Tolerance       *string `json:"tolerance,omitempty"`
	KeySpace        string  `json:"key_space"`
	CheckpointEvery string  `json:"checkpoint_every"`
	Config          string  `json:"config"`
	Status          string  `json:"status"`
	CreatedAt       string  `json:"created_at"`
	LastRunAt       *string `json:"last_run_at,omitempty"`
}

type AdapterRecord struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Image     string `json:"image"`
	Env       string `json:"env"`
	CreatedAt string `json:"created_at"`
}

type GeneratorRecord struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Workload    string `json:"workload"`
	Rate        int64  `json:"rate"`
	Builtin     int    `json:"builtin"`
	CreatedAt   string `json:"created_at"`
}

type ChaosScenarioRecord struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Actions   string `json:"actions"`
	CreatedAt string `json:"created_at"`
}

type ChaosInjectionRecord struct {
	ID           string  `json:"id"`
	ScenarioID   string  `json:"scenario_id"`
	ScenarioName string  `json:"scenario_name"`
	Status       string  `json:"status"`
	StartedAt    string  `json:"started_at"`
	FinishedAt   *string `json:"finished_at,omitempty"`
	Error        *string `json:"error,omitempty"`
}

type MetricSample struct {
	HypothesisID string  `json:"hypothesis_id"`
	RunID        string  `json:"run_id"`
	Ts           int64   `json:"ts"`
	Metric       string  `json:"metric"`
	Labels       string  `json:"labels"`
	Value        float64 `json:"value"`
}

type HypothesisResult struct {
	ID                string  `json:"id"`
	HypothesisID      string  `json:"hypothesis_id"`
	RunID             string  `json:"run_id"`
	TotalBatches      int64   `json:"total_batches"`
	TotalCheckpoints  int64   `json:"total_checkpoints"`
	PassedCheckpoints int64   `json:"passed_checkpoints"`
	FailedCheckpoints int64   `json:"failed_checkpoints"`
	TotalResponseOps  int64   `json:"total_response_ops"`
	SafetyViolations  int64   `json:"safety_violations"`
	StopReason        *string `json:"stop_reason,omitempty"`
	StartedAt         *string `json:"started_at,omitempty"`
	FinishedAt        *string `json:"finished_at,omitempty"`
	CreatedAt         string  `json:"created_at"`
}

// ---------------------------------------------------------------------------
// SqliteStore
// ---------------------------------------------------------------------------

type SqliteStore struct {
	db *sql.DB
}

func NewSqliteStore(dbURL string) (*SqliteStore, error) {
	db, err := sql.Open("sqlite", dbURL)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// WAL mode + foreign keys
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	s := &SqliteStore{db: db}
	if err := s.runMigrations(); err != nil {
		db.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}
	return s, nil
}

func (s *SqliteStore) Close() error {
	return s.db.Close()
}

func (s *SqliteStore) DB() *sql.DB {
	return s.db
}

func (s *SqliteStore) runMigrations() error {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	// Sort by filename to ensure order.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		data, err := migrationsFS.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		if _, err := s.db.Exec(string(data)); err != nil {
			return fmt.Errorf("exec migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func nullStr(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}

func strPtr(ns sql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	s := ns.String
	return &s
}

func strToPtr(s string) *string {
	return &s
}

// ---------------------------------------------------------------------------
// Hypothesis CRUD
// ---------------------------------------------------------------------------

func (s *SqliteStore) CreateHypothesis(ctx context.Context, id, name, generator, adapter, adapterAddr, duration, tolerance, checkpointEvery, keySpace, config string) (*Hypothesis, error) {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO hypotheses (id, name, generator, adapter, adapter_addr, duration, tolerance, checkpoint_every, key_space, config)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, name, generator, adapter, adapterAddr, duration, tolerance, checkpointEvery, keySpace, config,
	)
	if err != nil {
		return nil, fmt.Errorf("insert hypothesis: %w", err)
	}
	return s.GetHypothesis(ctx, id)
}

func (s *SqliteStore) GetHypothesis(ctx context.Context, id string) (*Hypothesis, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, generator, adapter, adapter_addr, duration, tolerance, key_space, checkpoint_every, config, status, created_at, last_run_at
		 FROM hypotheses WHERE id = ?`, id)
	return scanHypothesis(row)
}

func (s *SqliteStore) GetHypothesisByName(ctx context.Context, name string) (*Hypothesis, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, generator, adapter, adapter_addr, duration, tolerance, key_space, checkpoint_every, config, status, created_at, last_run_at
		 FROM hypotheses WHERE name = ?`, name)
	return scanHypothesis(row)
}

func (s *SqliteStore) ListHypotheses(ctx context.Context) ([]Hypothesis, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, generator, adapter, adapter_addr, duration, tolerance, key_space, checkpoint_every, config, status, created_at, last_run_at
		 FROM hypotheses ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list hypotheses: %w", err)
	}
	defer rows.Close()

	var out []Hypothesis
	for rows.Next() {
		h, err := scanHypothesisRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *h)
	}
	return out, rows.Err()
}

func (s *SqliteStore) UpdateHypothesis(ctx context.Context, id, name, generator, adapter, adapterAddr, duration, tolerance, checkpointEvery, keySpace string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE hypotheses SET name=?, generator=?, adapter=?, adapter_addr=?, duration=?, tolerance=?, checkpoint_every=?, key_space=?
		 WHERE id=?`,
		name, generator, adapter, adapterAddr, duration, tolerance, checkpointEvery, keySpace, id,
	)
	if err != nil {
		return fmt.Errorf("update hypothesis: %w", err)
	}
	return nil
}

func (s *SqliteStore) DeleteHypothesis(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM hypotheses WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete hypothesis: %w", err)
	}
	return nil
}

func (s *SqliteStore) UpdateHypothesisStatus(ctx context.Context, id, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE hypotheses SET status = ? WHERE id = ?`, status, id)
	if err != nil {
		return fmt.Errorf("update hypothesis status: %w", err)
	}
	return nil
}

func (s *SqliteStore) UpdateLastRunAt(ctx context.Context, id string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx, `UPDATE hypotheses SET last_run_at = ? WHERE id = ?`, now, id)
	if err != nil {
		return fmt.Errorf("update last_run_at: %w", err)
	}
	return nil
}

type hypothesisScanner interface {
	Scan(dest ...any) error
}

func scanHypothesis(row hypothesisScanner) (*Hypothesis, error) {
	var h Hypothesis
	var adapter, adapterAddr, duration, tolerance, lastRunAt sql.NullString
	err := row.Scan(&h.ID, &h.Name, &h.Generator, &adapter, &adapterAddr, &duration, &tolerance,
		&h.KeySpace, &h.CheckpointEvery, &h.Config, &h.Status, &h.CreatedAt, &lastRunAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("not found")
		}
		return nil, fmt.Errorf("scan hypothesis: %w", err)
	}
	h.Adapter = strPtr(adapter)
	h.AdapterAddr = strPtr(adapterAddr)
	h.Duration = strPtr(duration)
	h.Tolerance = strPtr(tolerance)
	h.LastRunAt = strPtr(lastRunAt)
	return &h, nil
}

func scanHypothesisRows(rows *sql.Rows) (*Hypothesis, error) {
	return scanHypothesis(rows)
}

// ---------------------------------------------------------------------------
// Adapter CRUD
// ---------------------------------------------------------------------------

func (s *SqliteStore) CreateAdapter(ctx context.Context, id, name, image, envJSON string) (*AdapterRecord, error) {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO adapters (id, name, image, env) VALUES (?, ?, ?, ?)`,
		id, name, image, envJSON,
	)
	if err != nil {
		return nil, fmt.Errorf("insert adapter: %w", err)
	}
	return s.GetAdapter(ctx, id)
}

func (s *SqliteStore) GetAdapter(ctx context.Context, id string) (*AdapterRecord, error) {
	var a AdapterRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, image, env, created_at FROM adapters WHERE id = ?`, id).
		Scan(&a.ID, &a.Name, &a.Image, &a.Env, &a.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("adapter not found")
		}
		return nil, fmt.Errorf("get adapter: %w", err)
	}
	return &a, nil
}

func (s *SqliteStore) GetAdapterByName(ctx context.Context, name string) (*AdapterRecord, error) {
	var a AdapterRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, image, env, created_at FROM adapters WHERE name = ?`, name).
		Scan(&a.ID, &a.Name, &a.Image, &a.Env, &a.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("adapter not found")
		}
		return nil, fmt.Errorf("get adapter by name: %w", err)
	}
	return &a, nil
}

func (s *SqliteStore) ListAdapters(ctx context.Context) ([]AdapterRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, image, env, created_at FROM adapters ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list adapters: %w", err)
	}
	defer rows.Close()

	var out []AdapterRecord
	for rows.Next() {
		var a AdapterRecord
		if err := rows.Scan(&a.ID, &a.Name, &a.Image, &a.Env, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan adapter: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *SqliteStore) DeleteAdapter(ctx context.Context, id string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM adapters WHERE id = ?`, id)
	if err != nil {
		return false, fmt.Errorf("delete adapter: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---------------------------------------------------------------------------
// Generator CRUD
// ---------------------------------------------------------------------------

func (s *SqliteStore) UpsertGenerator(ctx context.Context, name, description, workloadJSON string, rate int64, builtin int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO generators (name, description, workload, rate, builtin)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET description=excluded.description, workload=excluded.workload, rate=excluded.rate, builtin=excluded.builtin`,
		name, description, workloadJSON, rate, builtin,
	)
	if err != nil {
		return fmt.Errorf("upsert generator: %w", err)
	}
	return nil
}

func (s *SqliteStore) GetGenerator(ctx context.Context, name string) (*GeneratorRecord, error) {
	var g GeneratorRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT name, description, workload, rate, builtin, created_at FROM generators WHERE name = ?`, name).
		Scan(&g.Name, &g.Description, &g.Workload, &g.Rate, &g.Builtin, &g.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("generator not found")
		}
		return nil, fmt.Errorf("get generator: %w", err)
	}
	return &g, nil
}

func (s *SqliteStore) ListGenerators(ctx context.Context) ([]GeneratorRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, description, workload, rate, builtin, created_at FROM generators ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list generators: %w", err)
	}
	defer rows.Close()

	var out []GeneratorRecord
	for rows.Next() {
		var g GeneratorRecord
		if err := rows.Scan(&g.Name, &g.Description, &g.Workload, &g.Rate, &g.Builtin, &g.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan generator: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (s *SqliteStore) DeleteGenerator(ctx context.Context, name string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM generators WHERE name = ?`, name)
	if err != nil {
		return false, fmt.Errorf("delete generator: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---------------------------------------------------------------------------
// HypothesisResult CRUD
// ---------------------------------------------------------------------------

func (s *SqliteStore) CreateResult(ctx context.Context, r *HypothesisResult) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO hypothesis_results (id, hypothesis_id, run_id, total_batches, total_checkpoints, passed_checkpoints, failed_checkpoints, total_response_ops, safety_violations, stop_reason, started_at, finished_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.HypothesisID, r.RunID, r.TotalBatches, r.TotalCheckpoints,
		r.PassedCheckpoints, r.FailedCheckpoints, r.TotalResponseOps, r.SafetyViolations,
		nullStr(r.StopReason), nullStr(r.StartedAt), nullStr(r.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("insert result: %w", err)
	}
	return nil
}

func (s *SqliteStore) GetResults(ctx context.Context, hypothesisID string) ([]HypothesisResult, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, hypothesis_id, run_id, total_batches, total_checkpoints, passed_checkpoints, failed_checkpoints, total_response_ops, safety_violations, stop_reason, started_at, finished_at, created_at
		 FROM hypothesis_results WHERE hypothesis_id = ? ORDER BY created_at DESC`, hypothesisID)
	if err != nil {
		return nil, fmt.Errorf("get results: %w", err)
	}
	defer rows.Close()

	var out []HypothesisResult
	for rows.Next() {
		r, err := scanResult(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

func (s *SqliteStore) GetLatestResult(ctx context.Context, hypothesisID string) (*HypothesisResult, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, hypothesis_id, run_id, total_batches, total_checkpoints, passed_checkpoints, failed_checkpoints, total_response_ops, safety_violations, stop_reason, started_at, finished_at, created_at
		 FROM hypothesis_results WHERE hypothesis_id = ? ORDER BY created_at DESC LIMIT 1`, hypothesisID)
	return scanResult(row)
}

func (s *SqliteStore) DeleteResult(ctx context.Context, resultID string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM hypothesis_results WHERE id = ?`, resultID)
	if err != nil {
		return false, fmt.Errorf("delete result: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func scanResult(row hypothesisScanner) (*HypothesisResult, error) {
	var r HypothesisResult
	var stopReason, startedAt, finishedAt sql.NullString
	err := row.Scan(&r.ID, &r.HypothesisID, &r.RunID, &r.TotalBatches, &r.TotalCheckpoints,
		&r.PassedCheckpoints, &r.FailedCheckpoints, &r.TotalResponseOps, &r.SafetyViolations,
		&stopReason, &startedAt, &finishedAt, &r.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("result not found")
		}
		return nil, fmt.Errorf("scan result: %w", err)
	}
	r.StopReason = strPtr(stopReason)
	r.StartedAt = strPtr(startedAt)
	r.FinishedAt = strPtr(finishedAt)
	return &r, nil
}

// ---------------------------------------------------------------------------
// ChaosScenario CRUD
// ---------------------------------------------------------------------------

func (s *SqliteStore) CreateChaosScenario(ctx context.Context, id, name, namespace, actionsJSON string) (*ChaosScenarioRecord, error) {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO chaos_scenarios (id, name, namespace, actions) VALUES (?, ?, ?, ?)`,
		id, name, namespace, actionsJSON,
	)
	if err != nil {
		return nil, fmt.Errorf("insert chaos scenario: %w", err)
	}
	return s.GetChaosScenario(ctx, id)
}

func (s *SqliteStore) GetChaosScenario(ctx context.Context, id string) (*ChaosScenarioRecord, error) {
	var c ChaosScenarioRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, namespace, actions, created_at FROM chaos_scenarios WHERE id = ?`, id).
		Scan(&c.ID, &c.Name, &c.Namespace, &c.Actions, &c.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("chaos scenario not found")
		}
		return nil, fmt.Errorf("get chaos scenario: %w", err)
	}
	return &c, nil
}

func (s *SqliteStore) GetChaosScenarioByName(ctx context.Context, name string) (*ChaosScenarioRecord, error) {
	var c ChaosScenarioRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT id, name, namespace, actions, created_at FROM chaos_scenarios WHERE name = ?`, name).
		Scan(&c.ID, &c.Name, &c.Namespace, &c.Actions, &c.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get chaos scenario by name: %w", err)
	}
	return &c, nil
}

func (s *SqliteStore) ListChaosScenarios(ctx context.Context) ([]ChaosScenarioRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, namespace, actions, created_at FROM chaos_scenarios ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list chaos scenarios: %w", err)
	}
	defer rows.Close()

	var out []ChaosScenarioRecord
	for rows.Next() {
		var c ChaosScenarioRecord
		if err := rows.Scan(&c.ID, &c.Name, &c.Namespace, &c.Actions, &c.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan chaos scenario: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *SqliteStore) UpdateChaosScenario(ctx context.Context, id, name, namespace, actionsJSON string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE chaos_scenarios SET name=?, namespace=?, actions=? WHERE id=?`,
		name, namespace, actionsJSON, id,
	)
	if err != nil {
		return fmt.Errorf("update chaos scenario: %w", err)
	}
	return nil
}

func (s *SqliteStore) DeleteChaosScenario(ctx context.Context, id string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM chaos_scenarios WHERE id = ?`, id)
	if err != nil {
		return false, fmt.Errorf("delete chaos scenario: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---------------------------------------------------------------------------
// ChaosInjection CRUD
// ---------------------------------------------------------------------------

// ChaosCreateInjection creates a chaos injection record, returning only an error.
// This satisfies the chaos.SqliteStore interface without exposing the concrete type.
func (s *SqliteStore) ChaosCreateInjection(ctx context.Context, id, scenarioID, scenarioName string) error {
	_, err := s.CreateChaosInjection(ctx, id, scenarioID, scenarioName)
	return err
}

func (s *SqliteStore) CreateChaosInjection(ctx context.Context, id, scenarioID, scenarioName string) (*ChaosInjectionRecord, error) {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO chaos_injections (id, scenario_id, scenario_name) VALUES (?, ?, ?)`,
		id, scenarioID, scenarioName,
	)
	if err != nil {
		return nil, fmt.Errorf("insert chaos injection: %w", err)
	}
	return s.GetChaosInjection(ctx, id)
}

func (s *SqliteStore) GetChaosInjection(ctx context.Context, id string) (*ChaosInjectionRecord, error) {
	var c ChaosInjectionRecord
	var finishedAt, errMsg sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT id, scenario_id, scenario_name, status, started_at, finished_at, error FROM chaos_injections WHERE id = ?`, id).
		Scan(&c.ID, &c.ScenarioID, &c.ScenarioName, &c.Status, &c.StartedAt, &finishedAt, &errMsg)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("chaos injection not found")
		}
		return nil, fmt.Errorf("get chaos injection: %w", err)
	}
	c.FinishedAt = strPtr(finishedAt)
	c.Error = strPtr(errMsg)
	return &c, nil
}

func (s *SqliteStore) ListChaosInjections(ctx context.Context) ([]ChaosInjectionRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, scenario_id, scenario_name, status, started_at, finished_at, error FROM chaos_injections ORDER BY started_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list chaos injections: %w", err)
	}
	defer rows.Close()

	var out []ChaosInjectionRecord
	for rows.Next() {
		var c ChaosInjectionRecord
		var finishedAt, errMsg sql.NullString
		if err := rows.Scan(&c.ID, &c.ScenarioID, &c.ScenarioName, &c.Status, &c.StartedAt, &finishedAt, &errMsg); err != nil {
			return nil, fmt.Errorf("scan chaos injection: %w", err)
		}
		c.FinishedAt = strPtr(finishedAt)
		c.Error = strPtr(errMsg)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *SqliteStore) UpdateChaosInjectionStatus(ctx context.Context, id, status string, errorMsg *string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE chaos_injections SET status=?, finished_at=?, error=? WHERE id=?`,
		status, now, nullStr(errorMsg), id,
	)
	if err != nil {
		return fmt.Errorf("update chaos injection status: %w", err)
	}
	return nil
}

func (s *SqliteStore) DeleteChaosInjection(ctx context.Context, id string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM chaos_injections WHERE id = ?`, id)
	if err != nil {
		return false, fmt.Errorf("delete chaos injection: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---------------------------------------------------------------------------
// MetricSamples
// ---------------------------------------------------------------------------

func (s *SqliteStore) InsertMetricSamples(ctx context.Context, samples []MetricSample) error {
	if len(samples) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO adapter_metric_samples (hypothesis_id, run_id, ts, metric, labels, value) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare insert metric: %w", err)
	}
	defer stmt.Close()

	for _, m := range samples {
		if _, err := stmt.ExecContext(ctx, m.HypothesisID, m.RunID, m.Ts, m.Metric, m.Labels, m.Value); err != nil {
			return fmt.Errorf("insert metric sample: %w", err)
		}
	}

	return tx.Commit()
}

func (s *SqliteStore) QueryMetricSamples(ctx context.Context, runID string, metricPrefix *string, since *int64) ([]MetricSample, error) {
	var clauses []string
	var args []any

	clauses = append(clauses, "run_id = ?")
	args = append(args, runID)

	if metricPrefix != nil {
		clauses = append(clauses, "metric LIKE ?")
		args = append(args, *metricPrefix+"%")
	}
	if since != nil {
		clauses = append(clauses, "ts >= ?")
		args = append(args, *since)
	}

	query := `SELECT hypothesis_id, run_id, ts, metric, labels, value FROM adapter_metric_samples WHERE ` +
		strings.Join(clauses, " AND ") + ` ORDER BY ts`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query metric samples: %w", err)
	}
	defer rows.Close()

	var out []MetricSample
	for rows.Next() {
		var m MetricSample
		if err := rows.Scan(&m.HypothesisID, &m.RunID, &m.Ts, &m.Metric, &m.Labels, &m.Value); err != nil {
			return nil, fmt.Errorf("scan metric sample: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *SqliteStore) DeleteMetricSamplesForRun(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM adapter_metric_samples WHERE run_id = ?`, runID)
	if err != nil {
		return fmt.Errorf("delete metric samples: %w", err)
	}
	return nil
}
