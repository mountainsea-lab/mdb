# P33 Scheduler Failure Handling and Suppression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add sanitized failure accounting and automatic suppression to the server-owned market-data storage maintenance scheduler after `max_consecutive_failures` is reached.

**Architecture:** Keep all scheduler failure and suppression semantics inside `fdc-server::market_data::maintenance_scheduler`. The scheduler state becomes the single source of truth for suppression, the scheduler loop exits when suppressed, and the existing read-only status route exposes suppression through current fields without adding mutating recovery routes.

**Tech Stack:** Rust, Tokio async tasks, Axum route contract tests, `fdc_storage::StorageMaintenanceOptions`, package-scoped Cargo tests and formatting.

---

## File Structure

- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`
  - Add state-level suppression helpers.
  - Add sanitized single-line error handling.
  - Add a server-owned test seam for scheduler attempt outcomes.
  - Update the scheduler loop to stop after suppression.
  - Add unit tests for RED/GREEN TDD coverage.
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add a route-level test proving a suppressed snapshot is visible via the existing read-only status endpoint.
  - Keep existing scheduler success/audit/manual-gate tests unchanged.
- Modify: `docs/DEVELOPMENT_STATUS.md`
  - Record P33 completion, commits, and verification evidence after implementation.

No `fdc-storage` source files should be modified.

---

## Task 1: RED tests for scheduler suppression state and sanitized errors

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`

- [ ] **Step 1: Add failing unit tests**

Append these tests inside the existing `#[cfg(test)] mod tests` in `crates/fdc-server/src/market_data/maintenance_scheduler.rs`:

```rust
    #[tokio::test]
    async fn storage_maintenance_scheduler_state_suppresses_after_failure_threshold() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "2",
            ),
        ])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let first_started_at = Utc::now();
        let first_finished_at = first_started_at + chrono::Duration::milliseconds(5);
        let first_next_run_at = first_finished_at + chrono::Duration::seconds(60);

        assert!(state.mark_started(first_started_at).await);
        state
            .mark_failed(first_finished_at, first_next_run_at, "first failure")
            .await;
        let first = state.snapshot().await;
        assert!(!first.running);
        assert_eq!(first.total_runs, 1);
        assert_eq!(first.failed_runs, 1);
        assert_eq!(first.consecutive_failures, 1);
        assert_eq!(first.last_status.as_deref(), Some("failed"));
        assert_eq!(first.last_error.as_deref(), Some("first failure"));
        assert_eq!(first.next_run_at, Some(first_next_run_at));
        assert!(!state.is_suppressed().await);

        let second_started_at = first_next_run_at;
        let second_finished_at = second_started_at + chrono::Duration::milliseconds(5);
        let second_next_run_at = second_finished_at + chrono::Duration::seconds(60);
        assert!(state.mark_started(second_started_at).await);
        state
            .mark_failed(
                second_finished_at,
                second_next_run_at,
                "second failure with\ncontrol\tcharacters",
            )
            .await;
        let second = state.snapshot().await;
        assert!(!second.running);
        assert_eq!(second.total_runs, 2);
        assert_eq!(second.failed_runs, 2);
        assert_eq!(second.consecutive_failures, 2);
        assert_eq!(
            second.last_status.as_deref(),
            Some("suppressed_after_failures")
        );
        assert_eq!(
            second.last_error.as_deref(),
            Some("second failure with control characters")
        );
        assert!(second.next_run_at.is_none());
        assert!(state.is_suppressed().await);
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_error_sanitization_bounds_status_text() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        ])
        .expect("config parses");
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let started_at = Utc::now();
        let finished_at = started_at + chrono::Duration::milliseconds(1);
        let next_run_at = finished_at + chrono::Duration::seconds(60);
        let long_error = format!("{}\n{}", "x".repeat(260), "secret line");

        assert!(state.mark_started(started_at).await);
        state.mark_failed(finished_at, next_run_at, long_error).await;

        let snapshot = state.snapshot().await;
        let error = snapshot.last_error.expect("last error should be recorded");
        assert!(error.len() <= 243, "error was not bounded: {error}");
        assert!(error.ends_with("..."), "error should include truncation marker");
        assert!(!error.contains('\n'));
        assert!(!error.contains('\t'));
    }
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_state_suppresses_after_failure_threshold
rtk cargo test -p fdc-server storage_maintenance_scheduler_error_sanitization_bounds_status_text
```

Expected: FAIL to compile because `StorageMaintenanceSchedulerState::is_suppressed` does not exist, and the existing sanitizer does not replace control characters.

---

## Task 2: Implement state-level suppression and sanitized error handling

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`

- [ ] **Step 1: Add suppression status constants and helpers**

Near the top of `maintenance_scheduler.rs`, after imports, add:

```rust
const SCHEDULER_STATUS_FAILED: &str = "failed";
const SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES: &str = "suppressed_after_failures";
```

Inside `impl StorageMaintenanceSchedulerState`, add these methods after `snapshot()`:

```rust
    pub async fn is_suppressed(&self) -> bool {
        let guard = self.inner.lock().await;
        scheduler_snapshot_is_suppressed(&guard.snapshot)
    }

    pub async fn mark_suppressed(&self) {
        let mut guard = self.inner.lock().await;
        guard.snapshot.running = false;
        guard.snapshot.last_status = Some(SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES.to_string());
        guard.snapshot.next_run_at = None;
    }
```

Add this private helper after the `impl StorageMaintenanceSchedulerState` block:

```rust
fn scheduler_snapshot_is_suppressed(snapshot: &StorageMaintenanceSchedulerSnapshot) -> bool {
    snapshot.enabled
        && snapshot.tiered
        && snapshot.consecutive_failures >= snapshot.max_consecutive_failures
}
```

- [ ] **Step 2: Update `mark_next_run_at`, `mark_started`, and `mark_failed`**

Replace the existing `mark_next_run_at`, `mark_started`, and `mark_failed` methods with:

```rust
    pub async fn mark_next_run_at(&self, next_run_at: DateTime<Utc>) {
        let mut guard = self.inner.lock().await;
        if scheduler_snapshot_is_suppressed(&guard.snapshot) {
            guard.snapshot.next_run_at = None;
            guard.snapshot.last_status = Some(SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES.to_string());
            return;
        }
        guard.snapshot.next_run_at = Some(next_run_at);
    }

    pub async fn mark_started(&self, started_at: DateTime<Utc>) -> bool {
        let mut guard = self.inner.lock().await;
        if scheduler_snapshot_is_suppressed(&guard.snapshot) {
            guard.snapshot.running = false;
            guard.snapshot.last_status = Some(SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES.to_string());
            guard.snapshot.next_run_at = None;
            return false;
        }
        if guard.snapshot.running {
            guard.snapshot.skipped_runs += 1;
            guard.snapshot.last_status = Some("skipped_overlap".to_string());
            return false;
        }
        guard.snapshot.running = true;
        guard.snapshot.total_runs += 1;
        guard.snapshot.last_started_at = Some(started_at);
        guard.snapshot.last_status = Some("running".to_string());
        guard.snapshot.last_error = None;
        true
    }

    pub async fn mark_failed(
        &self,
        finished_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
        error: impl Into<String>,
    ) {
        let mut guard = self.inner.lock().await;
        guard.snapshot.running = false;
        guard.snapshot.failed_runs += 1;
        guard.snapshot.consecutive_failures += 1;
        guard.snapshot.last_finished_at = Some(finished_at);
        guard.snapshot.last_error = Some(sanitize_scheduler_error(error));
        if scheduler_snapshot_is_suppressed(&guard.snapshot) {
            guard.snapshot.last_status = Some(SCHEDULER_STATUS_SUPPRESSED_AFTER_FAILURES.to_string());
            guard.snapshot.next_run_at = None;
        } else {
            guard.snapshot.last_status = Some(SCHEDULER_STATUS_FAILED.to_string());
            guard.snapshot.next_run_at = Some(next_run_at);
        }
    }
```

- [ ] **Step 3: Replace sanitizer implementation**

Replace `sanitize_scheduler_error` with:

```rust
fn sanitize_scheduler_error(error: impl Into<String>) -> String {
    let sanitized: String = error
        .into()
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    const MAX_LEN: usize = 240;
    if sanitized.len() <= MAX_LEN {
        return sanitized;
    }
    format!("{}...", &sanitized[..MAX_LEN])
}
```

- [ ] **Step 4: Run tests to verify GREEN**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_state_suppresses_after_failure_threshold
rtk cargo test -p fdc-server storage_maintenance_scheduler_error_sanitization_bounds_status_text
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add crates/fdc-server/src/market_data/maintenance_scheduler.rs
git commit -m "feat(server): suppress scheduler after repeated failures"
```

---

## Task 3: RED tests for attempt-level suppression seam

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`

- [ ] **Step 1: Add failing tests for attempt suppression and fake executor results**

Append these tests inside the existing scheduler test module:

```rust
    #[tokio::test]
    async fn storage_maintenance_scheduler_attempts_stop_after_suppression() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "1",
            ),
        ])
        .expect("config parses");
        let audit = Arc::new(MarketDataStorageMaintenanceAuditLog::new(10));
        let state = StorageMaintenanceSchedulerState::from_config(&config);
        let calls = Arc::new(AtomicUsize::new(0));

        let first_calls = Arc::clone(&calls);
        run_scheduler_attempt_with_executor(&config, Arc::clone(&audit), state.clone(), move |_options| {
            let first_calls = Arc::clone(&first_calls);
            async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                StorageMaintenanceSchedulerAttemptResult::Failed("simulated failure".to_string())
            }
        })
        .await;

        let second_calls = Arc::clone(&calls);
        run_scheduler_attempt_with_executor(&config, Arc::clone(&audit), state.clone(), move |_options| {
            let second_calls = Arc::clone(&second_calls);
            async move {
                second_calls.fetch_add(1, Ordering::SeqCst);
                StorageMaintenanceSchedulerAttemptResult::Completed
            }
        })
        .await;

        let snapshot = state.snapshot().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.total_runs, 1);
        assert_eq!(snapshot.failed_runs, 1);
        assert_eq!(snapshot.successful_runs, 0);
        assert_eq!(snapshot.consecutive_failures, 1);
        assert_eq!(
            snapshot.last_status.as_deref(),
            Some("suppressed_after_failures")
        );
        assert!(snapshot.next_run_at.is_none());
        assert!(audit.recent(10).await.entries.is_empty());
    }

    #[tokio::test]
    async fn storage_maintenance_scheduler_attempt_executor_success_resets_failures() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
            (
                "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
                "2",
            ),
        ])
        .expect("config parses");
        let audit = Arc::new(MarketDataStorageMaintenanceAuditLog::new(10));
        let state = StorageMaintenanceSchedulerState::from_config(&config);

        run_scheduler_attempt_with_executor(&config, Arc::clone(&audit), state.clone(), |_options| async {
            StorageMaintenanceSchedulerAttemptResult::Failed("temporary failure".to_string())
        })
        .await;
        run_scheduler_attempt_with_executor(&config, Arc::clone(&audit), state.clone(), |_options| async {
            StorageMaintenanceSchedulerAttemptResult::Completed
        })
        .await;

        let snapshot = state.snapshot().await;
        assert_eq!(snapshot.total_runs, 2);
        assert_eq!(snapshot.failed_runs, 1);
        assert_eq!(snapshot.successful_runs, 1);
        assert_eq!(snapshot.consecutive_failures, 0);
        assert_eq!(snapshot.last_status.as_deref(), Some("completed"));
        assert!(snapshot.last_error.is_none());
        assert!(snapshot.next_run_at.is_some());
        assert!(!state.is_suppressed().await);
    }
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_attempts_stop_after_suppression
rtk cargo test -p fdc-server storage_maintenance_scheduler_attempt_executor_success_resets_failures
```

Expected: FAIL to compile because `run_scheduler_attempt_with_executor` and `StorageMaintenanceSchedulerAttemptResult` do not exist.

---

## Task 4: Implement attempt executor seam and loop suppression check

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_scheduler.rs`

- [ ] **Step 1: Add attempt result enum**

After `StorageMaintenanceSchedulerState`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageMaintenanceSchedulerAttemptResult {
    Completed,
    Unsupported,
    Failed(String),
}
```

- [ ] **Step 2: Update scheduler loop to break when suppressed**

Replace `run_scheduler_loop` with:

```rust
async fn run_scheduler_loop(
    config: ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
) {
    let first_delay = if config.market_data_storage_maintenance_scheduler_jitter_seconds == 0 {
        Duration::from_millis(10)
    } else {
        Duration::from_secs(config.market_data_storage_maintenance_scheduler_jitter_seconds)
    };
    let interval = scheduler_interval(&config);
    let mut next_delay = first_delay;
    loop {
        if state.is_suppressed().await {
            state.mark_suppressed().await;
            break;
        }
        let next_run_at = Utc::now()
            + chrono::Duration::from_std(next_delay)
                .unwrap_or_else(|_| chrono::Duration::seconds(0));
        state.mark_next_run_at(next_run_at).await;
        tokio::time::sleep(next_delay).await;
        run_scheduler_attempt(
            &config,
            Arc::clone(&store),
            Arc::clone(&audit),
            state.clone(),
        )
        .await;
        if state.is_suppressed().await {
            state.mark_suppressed().await;
            break;
        }
        next_delay = interval;
    }
}
```

- [ ] **Step 3: Replace production attempt with seam-based implementation**

Replace `run_scheduler_attempt` with:

```rust
pub async fn run_scheduler_attempt(
    config: &ServerRuntimeConfig,
    store: Arc<QueryableMarketDataStore>,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
) {
    run_scheduler_attempt_with_executor(config, audit, state, move |options| {
        let store = Arc::clone(&store);
        async move {
            match store.run_maintenance_once_with_options(options).await {
                Ok(Some(_report)) => StorageMaintenanceSchedulerAttemptResult::Completed,
                Ok(None) => StorageMaintenanceSchedulerAttemptResult::Unsupported,
                Err(error) => StorageMaintenanceSchedulerAttemptResult::Failed(format!("{error}")),
            }
        }
    })
    .await;
}
```

Then add this helper below it:

```rust
pub async fn run_scheduler_attempt_with_executor<F, Fut>(
    config: &ServerRuntimeConfig,
    audit: Arc<MarketDataStorageMaintenanceAuditLog>,
    state: StorageMaintenanceSchedulerState,
    executor: F,
) where
    F: FnOnce(StorageMaintenanceOptions) -> Fut,
    Fut: std::future::Future<Output = StorageMaintenanceSchedulerAttemptResult>,
{
    let started_at = Utc::now();
    if !state.mark_started(started_at).await {
        return;
    }

    let next_run_at = Utc::now()
        + chrono::Duration::from_std(scheduler_interval(config))
            .unwrap_or_else(|_| chrono::Duration::seconds(0));
    let options = StorageMaintenanceOptions::default()
        .with_timeout(scheduler_timeout(config))
        .with_audit_sink(audit);

    match executor(options).await {
        StorageMaintenanceSchedulerAttemptResult::Completed => {
            state.mark_completed(Utc::now(), next_run_at).await;
        }
        StorageMaintenanceSchedulerAttemptResult::Unsupported => {
            state.mark_unsupported(Utc::now(), next_run_at).await;
        }
        StorageMaintenanceSchedulerAttemptResult::Failed(error) => {
            state.mark_failed(Utc::now(), next_run_at, error).await;
        }
    }
}
```

- [ ] **Step 4: Run attempt tests to verify GREEN**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler_attempts_stop_after_suppression
rtk cargo test -p fdc-server storage_maintenance_scheduler_attempt_executor_success_resets_failures
```

Expected: PASS.

- [ ] **Step 5: Run all scheduler unit tests**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler
```

Expected: PASS. Existing defaults, unsupported backend, success, failure, and overlap tests still pass.

- [ ] **Step 6: Commit**

Run:

```bash
git add crates/fdc-server/src/market_data/maintenance_scheduler.rs
git commit -m "feat(server): stop scheduler attempts after suppression"
```

---

## Task 5: RED/GREEN route-level suppressed status visibility

**Files:**
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add route-level failing test**

Insert this test after `storage_maintenance_scheduler_status_reports_configured_values_without_running`:

```rust
#[tokio::test]
async fn storage_maintenance_scheduler_status_reports_suppressed_failures() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "2",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let scheduler = state.market_data_storage_maintenance_scheduler();
    let first_started_at = chrono::Utc::now();
    let first_finished_at = first_started_at + chrono::Duration::milliseconds(1);
    let first_next_run_at = first_finished_at + chrono::Duration::seconds(60);
    assert!(scheduler.mark_started(first_started_at).await);
    scheduler
        .mark_failed(first_finished_at, first_next_run_at, "first scheduler failure")
        .await;
    let second_started_at = first_next_run_at;
    let second_finished_at = second_started_at + chrono::Duration::milliseconds(1);
    let second_next_run_at = second_finished_at + chrono::Duration::seconds(60);
    assert!(scheduler.mark_started(second_started_at).await);
    scheduler
        .mark_failed(
            second_finished_at,
            second_next_run_at,
            "second scheduler failure\nwith control characters",
        )
        .await;
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/scheduler/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("scheduler status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    let data = &json["data"];
    assert_eq!(data["enabled"], true);
    assert_eq!(data["running"], false);
    assert_eq!(data["backend"], "tiered");
    assert_eq!(data["tiered"], true);
    assert_eq!(data["max_consecutive_failures"], 2);
    assert_eq!(data["consecutive_failures"], 2);
    assert_eq!(data["total_runs"], 2);
    assert_eq!(data["successful_runs"], 0);
    assert_eq!(data["failed_runs"], 2);
    assert_eq!(data["last_status"], "suppressed_after_failures");
    assert_eq!(
        data["last_error"],
        "second scheduler failure with control characters"
    );
    assert!(data["last_started_at"].is_string());
    assert!(data["last_finished_at"].is_string());
    assert!(data["next_run_at"].is_null());
}
```

- [ ] **Step 2: Run route test to verify behavior**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler_status_reports_suppressed_failures
```

Expected before Task 2 implementation: RED because suppression status and sanitized error are not yet correct. Expected after Task 2 implementation: PASS.

- [ ] **Step 3: Commit route test**

Run:

```bash
git add crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "test(server): cover scheduler suppression status route"
```

---

## Task 6: Final verification and documentation

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run focused verification**

Run:

```bash
rtk cargo test -p fdc-server storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected:

- Scheduler unit and route tests pass.
- Manual run-once tests pass, proving gate separation remains intact.
- Audit tests pass, proving success path still records audit and reset semantics remain unchanged.
- `fdc-storage` dependency guard passes, proving the boundary remains generic.
- Package-scoped formatting check passes.

- [ ] **Step 2: Update development status**

Add a new section at the top of `docs/DEVELOPMENT_STATUS.md` after the header and before P32:

```markdown
## 2026-06-07 P33 Scheduler Failure Handling and Suppression

Completed:

- Added server-owned scheduler suppression after configured consecutive failure threshold.
- Suppressed scheduler attempts report `last_status="suppressed_after_failures"` and clear `next_run_at`.
- Added sanitized single-line bounded failure reporting through existing scheduler status fields.
- Added scheduler attempt test seam inside `fdc-server` without modifying `fdc-storage`.
- Proved suppressed attempts do not keep invoking maintenance executors.
- Preserved the read-only scheduler status route and manual run-once gate separation.
- Preserved the `fdc-storage` boundary: no server/runtime/admin semantics were added to storage.

Design and plan:

- `docs/superpowers/specs/2026-06-07-scheduler-failure-suppression-design.md`
- `docs/superpowers/plans/2026-06-07-scheduler-failure-suppression.md`

Commits:

- `336392e docs(server): design scheduler failure suppression`
- Add the actual P33 implementation commit hashes created by Tasks 2, 4, and 5.

Verification:

- RED scheduler suppression tests failed before implementation because suppression helpers and attempt seam did not exist.
- Record the actual passing output for `rtk cargo test -p fdc-server storage_maintenance_scheduler`.
- Record the actual passing output for `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_scheduler`.
- Record the actual passing output for `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once`.
- Record the actual passing output for `rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route`.
- Record the actual passing output for `rtk cargo test -p fdc-storage --test dependency_guard`.
- `cargo fmt -p fdc-server -p fdc-storage -- --check` - exit 0

Recommended next slice:

- **P34 scheduler recovery controls**: add an explicit gated admin route to clear suppression/retry state if operators need runtime recovery without restart.
```

Replace the record-the-actual-output lines with concrete pass counts and commit hashes from this implementation session.

- [ ] **Step 3: Commit documentation**

Run:

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record scheduler failure suppression status"
```

- [ ] **Step 4: Check final git status**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.

---

## Self-Review

- Spec coverage:
  - Suppression after `max_consecutive_failures`: Task 1 and Task 2.
  - Scheduler loop stops automatic attempts: Task 3 and Task 4.
  - Sanitized bounded status error: Task 1 and Task 2.
  - Route-level visibility through existing status route: Task 5.
  - Server does not panic on scheduler failure: Task 3 and Task 4 fake-failure attempts return normally.
  - `fdc-storage` boundary remains generic: File Structure and Task 6 dependency guard.
- Placeholder scan:
  - No `TBD`, unexplained placeholders, or incomplete code snippets remain. The development-status section explicitly instructs the implementer to record actual command output after running verification.
- Type consistency:
  - `StorageMaintenanceSchedulerAttemptResult`, `run_scheduler_attempt_with_executor`, `is_suppressed`, and `mark_suppressed` are introduced before later tasks depend on them.
  - Existing DTO fields are reused; no new status DTO fields are required.
