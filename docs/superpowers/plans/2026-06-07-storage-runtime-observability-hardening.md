# Storage Runtime Observability Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing maintenance audit GET response with safe, read-only process-lifetime observability metadata.

**Architecture:** Keep all observability state inside `fdc-server`'s server-owned `MarketDataStorageMaintenanceAuditLog`. The audit log snapshot carries metadata to the service, the service maps it into server-owned DTO fields, and the existing audit GET route returns it without storage calls or maintenance execution. `fdc-storage` remains unchanged.

**Tech Stack:** Rust, axum, serde, chrono, tokio mutex, existing `fdc-server` market-data service/router/model patterns.

---

## File map

- `crates/fdc-server/src/market_data/maintenance_audit.rs`
  - Replace raw `VecDeque` mutex payload with a state struct containing entries and counters.
  - Extend `MarketDataStorageMaintenanceAuditSnapshot` with capacity and metadata fields.
  - Update `record_maintenance`, `recent`, and `clear`.
  - Add unit tests for default metadata, record metadata, eviction metadata, reset metadata, and empty reset metadata.
- `crates/fdc-server/src/market_data/model.rs`
  - Extend `MarketDataStorageMaintenanceAuditResponse` with safe observability fields.
- `crates/fdc-server/src/market_data/service.rs`
  - Map snapshot metadata into the audit response.
- `crates/fdc-server/tests/production_server_router_contract.rs`
  - Extend audit route and reset route tests to assert metadata fields.
  - Add `limit=0` route test proving metadata is returned even when entries are empty.
- `docs/DEVELOPMENT_STATUS.md`
  - Record P27 completion and verification.

---

### Task 1: Audit log observability metadata

**Files:**
- Modify: `crates/fdc-server/src/market_data/maintenance_audit.rs`

- [ ] **Step 1: Write failing unit tests**

Add tests to the existing `#[cfg(test)] mod tests`:

```rust
#[tokio::test]
async fn audit_log_snapshot_includes_default_observability_metadata() {
    let log = MarketDataStorageMaintenanceAuditLog::new(3);

    let snapshot = log.recent(10).await;

    assert_eq!(snapshot.capacity, 3);
    assert_eq!(snapshot.total_entries, 0);
    assert_eq!(snapshot.total_recorded_entries, 0);
    assert_eq!(snapshot.reset_count, 0);
    assert_eq!(snapshot.total_cleared_entries, 0);
    assert!(snapshot.last_recorded_at.is_none());
    assert!(snapshot.last_reset_at.is_none());
    assert!(snapshot.entries.is_empty());
}

#[tokio::test]
async fn audit_log_recording_updates_process_lifetime_metadata() {
    let log = MarketDataStorageMaintenanceAuditLog::new(3);
    let first = audit_entry(1);
    let second = audit_entry(2);
    let expected_last_recorded_at = second.recorded_at;

    log.record_maintenance(first).await.unwrap();
    log.record_maintenance(second).await.unwrap();

    let snapshot = log.recent(10).await;

    assert_eq!(snapshot.capacity, 3);
    assert_eq!(snapshot.total_entries, 2);
    assert_eq!(snapshot.total_recorded_entries, 2);
    assert_eq!(snapshot.reset_count, 0);
    assert_eq!(snapshot.total_cleared_entries, 0);
    assert_eq!(snapshot.last_recorded_at, Some(expected_last_recorded_at));
    assert!(snapshot.last_reset_at.is_none());
}

#[tokio::test]
async fn audit_log_eviction_preserves_total_recorded_entries() {
    let log = MarketDataStorageMaintenanceAuditLog::new(2);
    log.record_maintenance(audit_entry(1)).await.unwrap();
    log.record_maintenance(audit_entry(2)).await.unwrap();
    log.record_maintenance(audit_entry(3)).await.unwrap();

    let snapshot = log.recent(10).await;

    assert_eq!(snapshot.total_entries, 2);
    assert_eq!(snapshot.total_recorded_entries, 3);
    assert_eq!(snapshot.entries[0].scanned_entries, 3);
    assert_eq!(snapshot.entries[1].scanned_entries, 2);
}

#[tokio::test]
async fn audit_log_clear_updates_reset_metadata() {
    let log = MarketDataStorageMaintenanceAuditLog::new(3);
    log.record_maintenance(audit_entry(1)).await.unwrap();
    log.record_maintenance(audit_entry(2)).await.unwrap();

    let cleared = log.clear().await;
    let snapshot = log.recent(10).await;

    assert_eq!(cleared, 2);
    assert_eq!(snapshot.total_entries, 0);
    assert_eq!(snapshot.total_recorded_entries, 2);
    assert_eq!(snapshot.reset_count, 1);
    assert_eq!(snapshot.total_cleared_entries, 2);
    assert!(snapshot.last_recorded_at.is_some());
    assert!(snapshot.last_reset_at.is_some());
}

#[tokio::test]
async fn audit_log_clear_empty_log_updates_reset_count_without_cleared_entries() {
    let log = MarketDataStorageMaintenanceAuditLog::new(3);

    let cleared = log.clear().await;
    let snapshot = log.recent(10).await;

    assert_eq!(cleared, 0);
    assert_eq!(snapshot.total_entries, 0);
    assert_eq!(snapshot.total_recorded_entries, 0);
    assert_eq!(snapshot.reset_count, 1);
    assert_eq!(snapshot.total_cleared_entries, 0);
    assert!(snapshot.last_recorded_at.is_none());
    assert!(snapshot.last_reset_at.is_some());
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server audit_log_snapshot_includes_default_observability_metadata audit_log_recording_updates_process_lifetime_metadata audit_log_eviction_preserves_total_recorded_entries audit_log_clear_updates_reset_metadata audit_log_clear_empty_log_updates_reset_count_without_cleared_entries
```

If cargo does not accept multiple filters in this environment, run:

```bash
rtk cargo test -p fdc-server audit_log_
```

Expected: FAIL because snapshot metadata fields do not exist.

- [ ] **Step 3: Implement audit log state and snapshot metadata**

In `maintenance_audit.rs`, add chrono import:

```rust
use chrono::{DateTime, Utc};
```

Add state struct:

```rust
#[derive(Debug, Default)]
struct MarketDataStorageMaintenanceAuditState {
    entries: VecDeque<StorageMaintenanceAuditEntry>,
    total_recorded_entries: u64,
    reset_count: u64,
    total_cleared_entries: u64,
    last_recorded_at: Option<DateTime<Utc>>,
    last_reset_at: Option<DateTime<Utc>>,
}
```

Change log struct:

```rust
pub struct MarketDataStorageMaintenanceAuditLog {
    capacity: usize,
    state: Mutex<MarketDataStorageMaintenanceAuditState>,
}
```

Change snapshot struct:

```rust
pub struct MarketDataStorageMaintenanceAuditSnapshot {
    pub capacity: usize,
    pub total_entries: usize,
    pub total_recorded_entries: u64,
    pub reset_count: u64,
    pub total_cleared_entries: u64,
    pub last_recorded_at: Option<DateTime<Utc>>,
    pub last_reset_at: Option<DateTime<Utc>>,
    pub entries: Vec<StorageMaintenanceAuditEntry>,
}
```

Update constructor:

```rust
Self {
    capacity: capacity.max(1),
    state: Mutex::new(MarketDataStorageMaintenanceAuditState::default()),
}
```

Update `recent`:

```rust
pub async fn recent(&self, limit: usize) -> MarketDataStorageMaintenanceAuditSnapshot {
    let state = self.state.lock().await;
    let limit = limit.min(self.capacity);
    let recent = state.entries.iter().rev().take(limit).cloned().collect();
    MarketDataStorageMaintenanceAuditSnapshot {
        capacity: self.capacity,
        total_entries: state.entries.len(),
        total_recorded_entries: state.total_recorded_entries,
        reset_count: state.reset_count,
        total_cleared_entries: state.total_cleared_entries,
        last_recorded_at: state.last_recorded_at,
        last_reset_at: state.last_reset_at,
        entries: recent,
    }
}
```

Update `clear`:

```rust
pub async fn clear(&self) -> usize {
    let mut state = self.state.lock().await;
    let cleared = state.entries.len();
    state.entries.clear();
    state.reset_count += 1;
    state.total_cleared_entries += cleared as u64;
    state.last_reset_at = Some(Utc::now());
    cleared
}
```

Update `record_maintenance`:

```rust
async fn record_maintenance(&self, entry: StorageMaintenanceAuditEntry) -> Result<()> {
    let mut state = self.state.lock().await;
    state.last_recorded_at = Some(entry.recorded_at);
    state.total_recorded_entries += 1;
    state.entries.push_back(entry);
    while state.entries.len() > self.capacity {
        state.entries.pop_front();
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server audit_log_
```

Expected: PASS for audit log unit tests.

- [ ] **Step 5: Commit**

```bash
git add crates/fdc-server/src/market_data/maintenance_audit.rs
git commit -m "feat(server): track storage audit observability metadata"
```

---

### Task 2: Expose audit metadata through existing GET route

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Write failing route metadata tests**

Extend `storage_maintenance_audit_route_returns_empty_log` with:

```rust
assert_eq!(json["data"]["capacity"], 32);
assert_eq!(json["data"]["total_entries"], 0);
assert_eq!(json["data"]["total_recorded_entries"], 0);
assert_eq!(json["data"]["reset_count"], 0);
assert_eq!(json["data"]["total_cleared_entries"], 0);
assert!(json["data"]["last_recorded_at"].is_null());
assert!(json["data"]["last_reset_at"].is_null());
```

Extend `storage_maintenance_audit_route_returns_successful_run_entry` with:

```rust
assert_eq!(json["data"]["capacity"], 32);
assert_eq!(json["data"]["total_entries"], 1);
assert_eq!(json["data"]["total_recorded_entries"], 1);
assert_eq!(json["data"]["reset_count"], 0);
assert_eq!(json["data"]["total_cleared_entries"], 0);
assert!(json["data"]["last_recorded_at"].as_str().unwrap().contains('T'));
assert!(json["data"]["last_reset_at"].is_null());
```

Add a route test after the audit reset tests:

```rust
#[tokio::test]
async fn storage_maintenance_audit_route_returns_metadata_after_reset() {
    let state = tiered_storage_state_with_audit_capacity_and_reset(3, true).await;
    let router = build_production_router(state.clone());

    state.ingest_test_trade("BTCUSDT", "audit-metadata-reset").await.unwrap();
    run_successful_storage_maintenance(router.clone()).await;

    let reset = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/audit/reset")
                .header("content-type", "application/json")
                .body(audit_reset_request("reset_maintenance_audit"))
                .expect("request should build"),
        )
        .await
        .expect("reset route should respond");
    assert_eq!(reset.status(), StatusCode::OK);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit?limit=0")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["capacity"], 3);
    assert_eq!(json["data"]["total_entries"], 0);
    assert_eq!(json["data"]["returned_entries"], 0);
    assert_eq!(json["data"]["total_recorded_entries"], 1);
    assert_eq!(json["data"]["reset_count"], 1);
    assert_eq!(json["data"]["total_cleared_entries"], 1);
    assert!(json["data"]["last_recorded_at"].as_str().unwrap().contains('T'));
    assert!(json["data"]["last_reset_at"].as_str().unwrap().contains('T'));
    assert!(json["data"]["entries"].as_array().unwrap().is_empty());
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route_returns_empty_log storage_maintenance_audit_route_returns_successful_run_entry storage_maintenance_audit_route_returns_metadata_after_reset
```

If cargo does not accept multiple filters, run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
```

Expected: FAIL because response DTO fields are missing.

- [ ] **Step 3: Extend response DTO**

In `model.rs`, extend `MarketDataStorageMaintenanceAuditResponse`:

```rust
pub capacity: usize,
pub total_recorded_entries: u64,
pub reset_count: u64,
pub total_cleared_entries: u64,
pub last_recorded_at: Option<String>,
pub last_reset_at: Option<String>,
```

- [ ] **Step 4: Map metadata in service**

In `storage_maintenance_audit`, populate the new fields:

```rust
MarketDataStorageMaintenanceAuditResponse {
    capacity: snapshot.capacity,
    total_entries: snapshot.total_entries,
    returned_entries: entries.len(),
    total_recorded_entries: snapshot.total_recorded_entries,
    reset_count: snapshot.reset_count,
    total_cleared_entries: snapshot.total_cleared_entries,
    last_recorded_at: snapshot.last_recorded_at.map(|timestamp| timestamp.to_rfc3339()),
    last_reset_at: snapshot.last_reset_at.map(|timestamp| timestamp.to_rfc3339()),
    entries,
}
```

- [ ] **Step 5: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
```

Expected: PASS for audit route tests, including metadata after reset.

- [ ] **Step 6: Commit**

```bash
git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/tests/production_server_router_contract.rs
git commit -m "feat(server): expose storage audit observability metadata"
```

---

### Task 3: Final verification and status update

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run focused regression suite**

Run:

```bash
rtk cargo test -p fdc-server audit_log_
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_reset_route
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
rtk git status --short
```

Expected: all tests and package-scoped fmt pass; status shows only expected status-doc changes before final commit.

- [ ] **Step 2: Update development status**

Add P27 above P26 in `docs/DEVELOPMENT_STATUS.md`:

```markdown
## 2026-06-07 P27 Storage Runtime Observability Hardening

Completed:

- Extended server-owned maintenance audit snapshots with safe observability metadata:
  - capacity
  - current stored entries
  - process-lifetime recorded entry count
  - reset count
  - total entries cleared by reset
  - last recorded timestamp
  - last reset timestamp
- Exposed metadata through existing read-only `GET /market-data/storage/maintenance/audit`.
- Preserved read-only behavior: audit GET does not call storage or trigger maintenance.
- Preserved the `fdc-storage` boundary.
```

- [ ] **Step 3: Commit status update**

```bash
git add docs/DEVELOPMENT_STATUS.md
git commit -m "docs: record storage runtime observability hardening status"
```

- [ ] **Step 4: Final clean verification**

Run:

```bash
rtk git status --short
```

Expected: clean working tree.
