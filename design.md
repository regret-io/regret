# Regret — Implementation Design (v2)

> Full project design. Read alongside `spec.md` for behavior details.
> Changes from v1: Reference module (replaces verifier + state_machine), HypothesisManager,
> streaming support, Java SDK + Oxia adapter, no Rust SDK.

---

## 1. Workspace Structure

```
regret/
├── Cargo.toml                     workspace root (4 Rust crates)
│
├── proto/                          regret-proto (library)
│   ├── Cargo.toml
│   ├── build.rs                    tonic-build codegen
│   └── src/lib.rs                  re-exports generated gRPC types
│
├── gen/
│   ├── core/                       regret-gen-core (library)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs              public API: GenerateParams, generate(), generate_to_writer()
│   │       ├── types.rs            OriginOp, OpKind, serde serialization
│   │       ├── kv.rs               basic-kv profile generator
│   │       └── stats.rs            GenStats
│   │
│   └── cli/                        regret-gen (binary)
│       ├── Cargo.toml
│       └── src/main.rs             clap CLI wrapper
│
├── pilot/                          regret-pilot (binary)
│   ├── Cargo.toml
│   ├── proto/regret.proto          gRPC service definitions
│   ├── migrations/001_init.sql     SQLite schema
│   └── src/
│       ├── main.rs                 entrypoint
│       ├── config.rs               env-based configuration
│       ├── app_state.rs            AppState (shared services)
│       ├── metrics.rs              Prometheus metrics
│       ├── grpc.rs                 PilotService gRPC impl
│       ├── api/
│       │   ├── mod.rs              axum router
│       │   ├── hypothesis.rs       CRUD + origin + generate + run control
│       │   ├── health.rs           health + metrics endpoints
│       │   ├── error.rs            ApiError → HTTP status
│       │   └── models.rs           request/response structs
│       ├── storage/
│       │   ├── mod.rs
│       │   ├── sqlite.rs           SqliteStore (shared)
│       │   ├── rocksdb.rs          RocksStore (shared)
│       │   └── files.rs            FileStore (shared)
│       ├── reference/
│       │   ├── mod.rs              ReferenceModel trait
│       │   ├── kv.rs               BasicKvReference (basic-kv)
│       │   └── streaming.rs        BasicStreamingReference (basic-streaming)
│       ├── engine/
│       │   ├── mod.rs
│       │   ├── executor.rs         main execution loop
│       │   ├── events.rs           event types + serialization
│       │   └── hypothesis_manager.rs  per-hypothesis lifecycle manager
│       ├── scheduler/
│       │   ├── mod.rs
│       │   └── k8s.rs              K8sScheduler (shared service)
│       └── adapter/
│           ├── mod.rs
│           └── registry.rs         in-memory adapter registration
│
├── sdk/
│   └── java/                       regret-adapter-sdk-java (Maven)
│       ├── pom.xml
│       └── src/main/java/io/regret/sdk/
│           ├── Adapter.java
│           ├── Batch.java
│           ├── BatchResponse.java
│           ├── Item.java
│           ├── Operation.java
│           ├── OpResult.java
│           ├── Record.java
│           ├── RegretAdapterServer.java
│           └── payload/            PutPayload, DeletePayload, ...
│
├── adapters/
│   └── oxia-java/                  Oxia KV reference adapter (Maven)
│       ├── pom.xml
│       └── src/main/java/io/regret/adapter/oxia/
│           └── OxiaKVAdapter.java
│
├── charts/regret/                  Helm chart
└── examples/oxia-kv/
```

---

## 2. Dependency Graph

```
regret-gen (CLI)
  └── regret-gen-core

regret-pilot
  ├── regret-gen-core          (for generate endpoint)
  └── regret-proto             (gRPC types)

sdk/java → regret.proto        (via protobuf-maven-plugin)
adapters/oxia-java → sdk/java
```

---

## 3. Key Architectural Concepts

### 3.1 Shared Services vs Per-Hypothesis Resources

```
Shared (singleton, in AppState):          Per-Hypothesis (owned by HypothesisManager):
┌──────────────────────────┐              ┌─────────────────────────────┐
│ SqliteStore              │              │ ReferenceModel              │
│ RocksStore               │              │ Executor (active run only)  │
│ FileStore                │              │ Adapter client connections  │
│ K8sScheduler             │              │ CancellationToken           │
│ AdapterRegistry          │              │ Progress tracking           │
│ MetricsState             │              └─────────────────────────────┘
└──────────────────────────┘
```

### 3.2 Component Interaction

```
HTTP API ──→ HypothesisManager ──→ Executor ──→ Reference (apply + verify)
                    │                  │
                    │                  ├──→ AdapterRegistry (get gRPC client)
                    │                  ├──→ RocksStore (read origin, read/write state)
                    │                  └──→ FileStore (append events, write checkpoints)
                    │
                    └──→ K8sScheduler (deploy/teardown adapter pods)
```

---

## 4. Reference Module

Replaces the old `verifier.rs` + `state_machine/`. Owns both the "what is correct" model
and the comparison logic.

### 4.1 ReferenceModel Trait

```rust
// reference/mod.rs

pub trait ReferenceModel: Send + Sync {
    /// Apply a batch of operations to the reference model.
    /// Updates internal state and records expectations.
    fn apply(&mut self, ops: &[Operation]);

    /// Take pending expectations for the most recent batch.
    /// Called after apply() to get what we expect from the adapter.
    fn take_expects(&mut self) -> HashMap<String, OpExpect>;

    /// Layer 1 — Response Verification.
    /// Compare adapter's BatchResponse against expectations.
    /// Returns list of failures (empty = pass).
    fn verify_response(
        &self,
        expects: &HashMap<String, OpExpect>,
        actual: &BatchResponse,
        tolerance: &Option<Tolerance>,
    ) -> Vec<ResponseFailure>;

    /// Layer 2 — Checkpoint Verification.
    /// Compare adapter's full state against reference model's state.
    /// Returns list of failures (empty = pass).
    fn verify_checkpoint(
        &self,
        actual_state: &HashMap<String, Option<RecordState>>,
        tolerance: &Option<Tolerance>,
    ) -> Vec<CheckpointFailure>;

    /// Get the set of keys touched since last clear.
    fn touched_keys(&self) -> &HashSet<String>;

    /// Snapshot reference state for the given keys.
    fn snapshot(&self, keys: &HashSet<String>) -> HashMap<String, Option<RecordState>>;

    /// Clear all state (called at start of each run).
    fn clear(&mut self);
}

/// Unified record state used by both KV and streaming references.
pub struct RecordState {
    pub value: Option<Bytes>,
    pub version_id: u64,
    pub metadata: HashMap<String, String>,
}
```

### 4.2 BasicKvReference

```rust
// reference/kv.rs
pub struct BasicKvReference {
    store: BTreeMap<String, KVRecord>,
    touched_keys: HashSet<String>,
    pending_expects: HashMap<String, OpExpect>,
}
```

Implements `ReferenceModel`. Same logic as spec §8 for apply().
verify_response() compares each op result status/value.
verify_checkpoint() compares full state snapshots with tolerance.

### 4.3 BasicStreamingReference

```rust
// reference/streaming.rs
pub struct BasicStreamingReference {
    /// topic -> partition -> Vec<Message>
    produced: HashMap<String, HashMap<u32, Vec<Message>>>,
    /// topic -> partition -> consumed offset
    consumed: HashMap<String, HashMap<u32, u64>>,
    touched_keys: HashSet<String>,
    pending_expects: HashMap<String, OpExpect>,
}

struct Message {
    offset: u64,
    key: Option<Bytes>,
    value: Bytes,
    timestamp: u64,
}
```

Implements `ReferenceModel`. Verification checks:
- **Layer 1 (response)**: produce acknowledged, consume returns expected messages
- **Layer 2 (checkpoint)**: no data loss — all produced messages are consumable,
  correct ordering per partition, no duplicates, no corruption

---

## 5. HypothesisManager

Long-lived, one per hypothesis. Created when hypothesis is created, destroyed when deleted.

```rust
// engine/hypothesis_manager.rs

pub struct HypothesisManager {
    hypothesis_id: String,
    profile: String,
    tolerance: Option<Tolerance>,

    // Per-hypothesis resources
    reference: Box<dyn ReferenceModel>,
    run_state: Option<ActiveRun>,

    // Shared services (references)
    sqlite: SqliteStore,
    rocks: RocksStore,
    files: FileStore,
    scheduler: K8sScheduler,
    registry: AdapterRegistry,
    metrics: MetricsState,
}

struct ActiveRun {
    run_id: String,
    cancel: CancellationToken,
    executor_handle: JoinHandle<Result<StopReason>>,
    progress: Arc<RwLock<ProgressInfo>>,
    adapter_statuses: Arc<RwLock<Vec<AdapterStatus>>>,
}

impl HypothesisManager {
    /// Create a new manager for a hypothesis.
    pub fn new(hypothesis: &Hypothesis, shared: &SharedServices) -> Self;

    /// Start a new run. Deploys adapters, creates executor, spawns main loop.
    pub async fn start_run(&mut self, config: StartRunRequest) -> Result<StartRunResponse>;

    /// Stop the current run. Signals cancellation, waits for cleanup.
    pub async fn stop_run(&mut self) -> Result<()>;

    /// Get current run status and progress.
    pub fn status(&self) -> Option<StatusResponse>;

    /// Check if a run is active.
    pub fn is_running(&self) -> bool;
}
```

**Manager registry** — global map of all managers:

```rust
// engine/mod.rs

pub struct ManagerRegistry {
    managers: Arc<RwLock<HashMap<String, Arc<Mutex<HypothesisManager>>>>>,
}

impl ManagerRegistry {
    pub async fn create(&self, hypothesis: &Hypothesis, shared: &SharedServices);
    pub async fn get(&self, id: &str) -> Option<Arc<Mutex<HypothesisManager>>>;
    pub async fn remove(&self, id: &str);
}
```

Lifecycle:
- `POST /api/hypothesis` → creates hypothesis in SQLite + RocksDB CF + file dir + `ManagerRegistry.create()`
- `POST /api/hypothesis/{id}/run` → `manager.start_run(config)`
- `DELETE /api/hypothesis/{id}/run` → `manager.stop_run()`
- `DELETE /api/hypothesis/{id}` → `manager.stop_run()` if running, then `ManagerRegistry.remove()`
- On pilot startup → load all hypotheses from SQLite → create managers for each

---

## 6. Executor

The main execution loop. Created per run by `HypothesisManager.start_run()`.

```rust
// engine/executor.rs

pub struct Executor {
    hypothesis_id: String,
    run_id: String,
    config: ExecutionConfig,
    reference: Box<dyn ReferenceModel>,
    cancel: CancellationToken,
    progress: Arc<RwLock<ProgressInfo>>,

    // Shared services
    rocks: RocksStore,
    files: FileStore,
    sqlite: SqliteStore,
    registry: AdapterRegistry,
    metrics: MetricsState,
}

impl Executor {
    /// Run the main execution loop (spec §7.1).
    /// Returns the reference model back to the manager when done.
    pub async fn run(mut self) -> (Box<dyn ReferenceModel>, Result<StopReason>) {
        // 1. Emit RunStarted event
        // 2. self.reference.clear()
        // 3. Clear RocksDB state
        // 4. Wait for adapter registration
        // 5. Main loop:
        //    a. Read batch from RocksDB origin:{seq}
        //    b. self.reference.apply(ops)
        //    c. expects = self.reference.take_expects()
        //    d. Send ExecuteBatch gRPC to adapter (with retry)
        //    e. Layer 1: self.reference.verify_response(expects, response, tolerance)
        //    f. Every N batches: Layer 2 checkpoint
        //       - adapter.ReadState(self.reference.touched_keys())
        //       - self.reference.verify_checkpoint(actual, tolerance)
        // 6. Final checkpoint
        // 7. Write results to SQLite
        // 8. Return (reference, result)
    }
}
```

Note: executor takes ownership of the reference model during a run and returns it
when done, so the manager can reuse it across runs.

---

## 7. Storage Layer (Shared Services)

SQLite, RocksDB, FileStore are shared singletons. See spec §4 for schemas.

Key addition: `store_origin()` shared helper used by both upload and generate endpoints.

```rust
/// Validate JSONL, write origin.jsonl, index into RocksDB.
async fn store_origin(
    hypothesis_id: &str,
    jsonl_bytes: &[u8],
    files: &FileStore,
    rocks: &RocksStore,
) -> Result<(usize, usize), ApiError>;
```

---

## 8. Scheduler (Shared Service)

Standalone shared service for K8s adapter lifecycle.

```rust
// scheduler/k8s.rs
pub struct K8sScheduler {
    client: kube::Client,
    namespace: String,
}

impl K8sScheduler {
    pub async fn deploy_adapter(&self, hypothesis_id: &str, config: &AdapterConfig) -> Result<()>;
    pub async fn teardown_adapter(&self, hypothesis_id: &str, adapter_name: &str) -> Result<()>;
    pub async fn wait_for_ready(&self, hypothesis_id: &str, adapter_name: &str, timeout: Duration) -> Result<()>;
}
```

Called by HypothesisManager during `start_run()` and `stop_run()`.

---

## 9. HTTP API

Handlers go through ManagerRegistry for run lifecycle:

```
POST   /api/hypothesis                    → create in SQLite + ManagerRegistry.create()
GET    /api/hypothesis                    → list from SQLite
GET    /api/hypothesis/{id}               → get from SQLite
DELETE /api/hypothesis/{id}               → manager.stop_run() + ManagerRegistry.remove() + cleanup
POST   /api/hypothesis/{id}/origin        → store_origin()
POST   /api/hypothesis/{id}/generate      → gen-core → store_origin()  (NEW)
POST   /api/hypothesis/{id}/run           → manager.start_run(config)
DELETE /api/hypothesis/{id}/run           → manager.stop_run()
GET    /api/hypothesis/{id}/status        → manager.status()
GET    /api/hypothesis/{id}/events        → FileStore.read_events()
GET    /api/hypothesis/{id}/bundle        → FileStore.create_bundle()
GET    /health
GET    /metrics
```

### Generate Endpoint

```
POST /api/hypothesis/{id}/generate
Content-Type: application/json
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `profile` | string | yes | — | `"basic-kv"` only in Phase 1 |
| `ops` | integer | yes | — | Total operations to generate |
| `keys` | integer | no | `100` | Key space size |
| `read_ratio` | float | no | `0.3` | Fraction of read ops |
| `cas_ratio` | float | no | `0.1` | Fraction of CAS write ops |
| `dr_ratio` | float | no | `0.05` | Fraction of delete_range write ops |
| `fence_every` | integer | no | `50` | Fence interval |
| `seed` | integer | no | random | RNG seed for reproducibility |

Response `201`: `{ hypothesis_id, total_ops, total_fences }`
Errors: `404` (not found), `409` (origin exists), `400` (invalid params)

---

## 10. Java SDK

### 10.1 regret-adapter-sdk-java (`sdk/java/`)

Maven project. Depends on gRPC + protobuf generated stubs from `regret.proto`.

**User-facing interface** (from spec §9.4):

```java
public interface Adapter {
    BatchResponse executeBatch(Batch batch) throws Exception;
    List<Record> readState(List<String> keys) throws Exception;
    default void cleanup() throws Exception {}
}
```

**SDK internals:**
- `RegretAdapterServer.serve(Adapter)` — starts gRPC server on port 9090
- Reads env vars: `REGRET_PILOT_ADDR`, `REGRET_HYPOTHESIS_ID`, `REGRET_ADAPTER_NAME`
- Calls `PilotService.RegisterAdapter` on startup
- Re-registers on reconnect
- Bridges gRPC ↔ `Adapter` interface
- Propagates `trace_id` to MDC for structured logging

**Types** (from spec §9.4):
- `Batch`, `Item` (sealed: `Op`, `Fence`), `Operation`
- Payload classes: `PutPayload`, `DeletePayload`, `DeleteRangePayload`, `CasPayload`, `GetPayload`, `RangeScanPayload`, `ListPayload`
- `OpResult` with factory methods: `.ok()`, `.notFound()`, `.versionMismatch()`, `.get()`, `.rangeScan()`, `.list()`, `.error()`
- `Record` with builder
- `BatchResponse`

### 10.2 oxia-adapter-java (`adapters/oxia-java/`)

Maven project. Depends on `regret-adapter-sdk-java` + `oxia-client-java`.
Implementation from spec §10.1 — `OxiaKVAdapter implements Adapter`.

---

## 11. AppState

```rust
#[derive(Clone)]
pub struct AppState {
    pub sqlite: SqliteStore,
    pub rocks: RocksStore,
    pub files: FileStore,
    pub scheduler: K8sScheduler,
    pub registry: AdapterRegistry,
    pub managers: ManagerRegistry,
    pub metrics: MetricsState,
}
```

---

## 12. Implementation Order

| Phase | Scope | Crates/Files |
|-------|-------|-------------|
| 1 | Skeleton | Cargo workspace, all Cargo.toml, proto crate, regret.proto |
| 2 | Gen | regret-gen-core (types, kv generator, stats) + regret-gen CLI |
| 3 | Storage | SqliteStore, RocksStore, FileStore, migrations, store_origin() |
| 4 | Reference | ReferenceModel trait, BasicKvReference, BasicStreamingReference |
| 5 | Engine | Events, Executor, HypothesisManager, ManagerRegistry |
| 6 | API | axum router, all handlers (CRUD, origin, generate, run, status, events, bundle) |
| 7 | Infra | K8sScheduler, AdapterRegistry, PilotService gRPC, metrics |
| 8 | Main | Entrypoint, config, startup (load hypotheses → create managers) |
| 9 | Java | sdk/java (Maven + gRPC stubs + Adapter interface + RegretAdapterServer) |
| 10 | Oxia | adapters/oxia-java (OxiaKVAdapter) |
