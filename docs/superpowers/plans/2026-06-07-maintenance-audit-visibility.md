# Maintenance Audit Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record successful explicit storage maintenance runs in a bounded server-owned audit log and expose recent safe entries through a read-only route.

**Architecture:** `fdc-server` implements the generic `fdc-storage::StorageMaintenanceAuditSink` on a bounded in-memory audit log stored in `ProductionServerState`. The P23 maintenance service attaches that sink to `StorageMaintenanceOptions`, and a new GET route maps safe audit counters/timestamps into server-owned response DTOs.

**Tech Stack:** Rust, Tokio, Axum, Serde, async-trait, `fdc-storage`, `fdc-server`, package-scoped cargo tests.

---

## File map

- Modify `crates/fdc-server/Cargo.toml`
  - Add `async-trait = { workspace = true }`.
- Create `crates/fdc-server/src/market_data/maintenance_audit.rs`
  - Define bounded in-memory audit log and implement `StorageMaintenanceAuditSink`.
- Modify `crates/fdc-server/src/market_data/mod.rs`
  - Export the new `maintenance_audit` module.
- Modify `crates/fdc-server/src/runtime/app.rs`
  - Store an `Arc<MarketDataStorageMaintenanceAuditLog>` in `ProductionServerState` and expose accessor.
- Modify `crates/fdc-server/src/market_data/model.rs`
  - Add audit route response DTOs.
- Modify `crates/fdc-server/src/market_data/service.rs`
  - Attach audit sink to P23 maintenance options and add audit listing service.
- Modify `crates/fdc-server/src/market_data/router.rs`
  - Add `GET /market-data/storage/maintenance/audit`.
- Modify `crates/fdc-server/tests/production_server_router_contract.rs`
  - Add route contract tests.
- Modify `docs/DEVELOPMENT_STATUS.md`
  - Record P24 completion.

---

### Task 1: Add server maintenance audit log

**Files:**
- Modify: `crates/fdc-server/Cargo.toml`
- Create: `crates/fdc-server/src/market_data/maintenance_audit.rs`
- Modify: `crates/fdc-server/src/market_data/mod.rs`

- [ ] **Step 1: Write failing audit log tests**

Create `crates/fdc-server/src/market_data/maintenance_audit.rs` with tests first:

```rust
use std::collections::VecDeque;

use async_trait::async_trait;
use fdc_core::Result;
use fdc_storage::{StorageMaintenanceAuditEntry, StorageMaintenanceAuditSink};
use tokio::sync::Mutex;

pub const MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY: usize = 32;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn audit_entry(scanned_entries: usize) -> StorageMaintenanceAuditEntry {
        let started_at = Utc.timestamp_opt(scanned_entries as i64, 0).unwrap();
        StorageMaintenanceAuditEntry {
            recorded_at: started_at,
            started_at,
            finished_at: started_at,
            duration_ms: scanned_entries as i64,
            scanned_entries,
            ttl_deleted: 0,
            retention_demoted: 0,
            retention_deleted: 0,
            retained: 0,
            decode_errors: 0,
            compacted_tiers: 0,
            compaction_unsupported: 0,
            compaction_failed: 0,
            healthy_tiers: 4,
            degraded_tiers: 0,
        }
    }

    #[tokio::test]
    async fn audit_log_returns_newest_entries_first() {
        let log = MarketDataStorageMaintenanceAuditLog::new(32);
        log.record_maintenance(audit_entry(1)).await.unwrap();
        log.record_maintenance(audit_entry(2)).await.unwrap();

        let snapshot = log.recent(10).await;

        assert_eq!(snapshot.total_entries, 2);
        assert_eq!(snapshot.entries.len(), 2);
        assert_eq!(snapshot.entries[0].scanned_entries, 2);
        assert_eq!(snapshot.entries[1].scanned_entries, 1);
    }

    #[tokio::test]
    async fn audit_log_drops_oldest_entries_at_capacity() {
        let log = MarketDataStorageMaintenanceAuditLog::new(2);
        log.record_maintenance(audit_entry(1)).await.unwrap();
        log.record_maintenance(audit_entry(2)).await.unwrap();
        log.record_maintenance(audit_entry(3)).await.unwrap();

        let snapshot = log.recent(10).await;

        assert_eq!(snapshot.total_entries, 2);
        assert_eq!(snapshot.entries.len(), 2);
        assert_eq!(snapshot.entries[0].scanned_entries, 3);
        assert_eq!(snapshot.entries[1].scanned_entries, 2);
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server maintenance_audit
```

Expected: compile failure because `MarketDataStorageMaintenanceAuditLog` is not defined and `async-trait` is not a dependency.

- [ ] **Step 3: Add dependency**

In `crates/fdc-server/Cargo.toml`, add under `[dependencies]`:

```toml
async-trait = { workspace = true }
```

- [ ] **Step 4: Implement audit log**

Add implementation above the tests in `maintenance_audit.rs`:

```rust
#[derive(Debug)]
pub struct MarketDataStorageMaintenanceAuditLog {
    capacity: usize,
    entries: Mutex<VecDeque<StorageMaintenanceAuditEntry>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataStorageMaintenanceAuditSnapshot {
    pub total_entries: usize,
    pub entries: Vec<StorageMaintenanceAuditEntry>,
}

impl Default for MarketDataStorageMaintenanceAuditLog {
    fn default() -> Self {
        Self::new(MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY)
    }
}

impl MarketDataStorageMaintenanceAuditLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: Mutex::new(VecDeque::new()),
        }
    }

    pub async fn recent(&self, limit: usize) -> MarketDataStorageMaintenanceAuditSnapshot {
        let entries = self.entries.lock().await;
        let limit = limit.min(self.capacity);
        let recent = entries.iter().rev().take(limit).cloned().collect();
        MarketDataStorageMaintenanceAuditSnapshot {
            total_entries: entries.len(),
            entries: recent,
        }
    }
}

#[async_trait]
impl StorageMaintenanceAuditSink for MarketDataStorageMaintenanceAuditLog {
    async fn record_maintenance(&self, entry: StorageMaintenanceAuditEntry) -> Result<()> {
        let mut entries = self.entries.lock().await;
        entries.push_back(entry);
        while entries.len() > self.capacity {
            entries.pop_front();
        }
        Ok(())
    }
}
```

In `crates/fdc-server/src/market_data/mod.rs`, add:

```rust
pub mod maintenance_audit;
```

- [ ] **Step 5: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server maintenance_audit
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/fdc-server/Cargo.toml crates/fdc-server/src/market_data/maintenance_audit.rs crates/fdc-server/src/market_data/mod.rs
rtk git commit -m "feat(server): add storage maintenance audit log"
```

---

### Task 2: Wire audit log into production state and maintenance execution

**Files:**
- Modify: `crates/fdc-server/src/runtime/app.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`

- [ ] **Step 1: Add failing service/state test**

Add a unit test in `crates/fdc-server/src/market_data/service.rs` under an existing or new `#[cfg(test)] mod tests`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ServerRuntimeConfig, ProductionServerState};

    #[tokio::test]
    async fn successful_tiered_maintenance_records_audit_entry() {
        let config = ServerRuntimeConfig::from_env_pairs([
            ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
            ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ])
        .unwrap();
        let state = ProductionServerState::try_new(config).await.unwrap();

        let result = run_storage_maintenance_once(
            &state,
            MarketDataStorageMaintenanceRunRequest {
                confirm: "run_maintenance_once".to_string(),
                timeout_ms: None,
                reason: Some("unit-test".to_string()),
            },
        )
        .await;

        assert_eq!(result.http_status, StorageMaintenanceHttpStatus::Ok);
        let audit = state.market_data_storage_maintenance_audit().recent(10).await;
        assert_eq!(audit.entries.len(), 1);
        assert_eq!(audit.entries[0].healthy_tiers, 4);
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
rtk cargo test -p fdc-server successful_tiered_maintenance_records_audit_entry
```

Expected: compile failure because `ProductionServerState` lacks an audit log accessor or audit sink is not attached.

- [ ] **Step 3: Add audit log to state**

In `runtime/app.rs`, import:

```rust
use crate::market_data::maintenance_audit::MarketDataStorageMaintenanceAuditLog;
```

Add field:

```rust
market_data_storage_maintenance_audit: Arc<MarketDataStorageMaintenanceAuditLog>,
```

Initialize it in `new`, `try_new`, and `with_market_data_store` with:

```rust
Arc::new(MarketDataStorageMaintenanceAuditLog::default())
```

Add accessor:

```rust
pub fn market_data_storage_maintenance_audit(&self) -> Arc<MarketDataStorageMaintenanceAuditLog> {
    Arc::clone(&self.market_data_storage_maintenance_audit)
}
```

- [ ] **Step 4: Attach audit sink during maintenance**

In `run_storage_maintenance_once`, after creating `StorageMaintenanceOptions::default()`, add:

```rust
let audit_sink = state.market_data_storage_maintenance_audit();
options = options.with_audit_sink(audit_sink);
```

Keep timeout behavior unchanged by applying timeout after or before audit sink.

- [ ] **Step 5: Run test to verify pass**

Run:

```bash
rtk cargo test -p fdc-server successful_tiered_maintenance_records_audit_entry
```

Expected: 1 passed.

- [ ] **Step 6: Commit**

```bash
rtk git add crates/fdc-server/src/runtime/app.rs crates/fdc-server/src/market_data/service.rs
rtk git commit -m "feat(server): record storage maintenance audit entries"
```

---

### Task 3: Add audit response models and route contracts

**Files:**
- Modify: `crates/fdc-server/src/market_data/model.rs`
- Modify: `crates/fdc-server/src/market_data/service.rs`
- Modify: `crates/fdc-server/src/market_data/router.rs`
- Modify: `crates/fdc-server/tests/production_server_router_contract.rs`

- [ ] **Step 1: Add failing route tests**

Add tests to `production_server_router_contract.rs`:

```rust
#[tokio::test]
async fn storage_maintenance_audit_route_returns_empty_log() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_entries"], 0);
    assert!(json["data"]["entries"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn storage_maintenance_audit_route_returns_successful_run_entry() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config).await.unwrap();
    let router = build_production_router(state);

    let run_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/run-once")
                .header("content-type", "application/json")
                .body(maintenance_request("run_maintenance_once"))
                .expect("request should build"),
        )
        .await
        .expect("maintenance route should respond");
    assert_eq!(run_response.status(), StatusCode::OK);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit?limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_entries"], 1);
    let entries = json["data"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["scanned_entries"], 0);
    assert_eq!(entries[0]["healthy_tiers"], 4);
    assert!(entries[0].get("recorded_at").unwrap().as_str().unwrap().contains('T'));
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
```

Expected: route returns 404 or compile failure because route/model/service does not exist.

- [ ] **Step 3: Add response DTOs**

In `model.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceAuditResponse {
    pub returned_entries: usize,
    pub entries: Vec<MarketDataStorageMaintenanceAuditEntryResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataStorageMaintenanceAuditEntryResponse {
    pub recorded_at: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: i64,
    pub scanned_entries: usize,
    pub ttl_deleted: usize,
    pub retention_demoted: usize,
    pub retention_deleted: usize,
    pub retained: usize,
    pub decode_errors: usize,
    pub compacted_tiers: usize,
    pub compaction_unsupported: usize,
    pub compaction_failed: usize,
    pub healthy_tiers: usize,
    pub degraded_tiers: usize,
}
```

- [ ] **Step 4: Add service function**

In `service.rs`, add:

```rust
pub async fn storage_maintenance_audit(
    state: &ProductionServerState,
    limit: Option<usize>,
) -> MarketDataStorageMaintenanceAuditResponse {
    let limit = limit.unwrap_or(10).min(MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY);
    let snapshot = state.market_data_storage_maintenance_audit().recent(limit).await;
    let entries = snapshot.entries.into_iter().map(audit_entry_response).collect();
    MarketDataStorageMaintenanceAuditResponse {
        returned_entries: snapshot.total_entries,
        entries,
    }
}
```

Map `StorageMaintenanceAuditEntry` to response using `to_rfc3339()` for timestamps.

- [ ] **Step 5: Add route**

In `router.rs`, define query params:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceAuditQueryParams {
    pub limit: Option<usize>,
}
```

Add route:

```rust
.route("/market-data/storage/maintenance/audit", get(storage_maintenance_audit_handler))
```

Add handler returning `ServerApiResponse<MarketDataStorageMaintenanceAuditResponse>`.

- [ ] **Step 6: Run tests to verify pass**

Run:

```bash
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
```

Expected: 2 passed.

- [ ] **Step 7: Commit**

```bash
rtk git add crates/fdc-server/src/market_data/model.rs crates/fdc-server/src/market_data/service.rs crates/fdc-server/src/market_data/router.rs crates/fdc-server/tests/production_server_router_contract.rs
rtk git commit -m "feat(server): expose storage maintenance audit route"
```

---

### Task 4: Verification and status update

**Files:**
- Modify: `docs/DEVELOPMENT_STATUS.md`

- [ ] **Step 1: Run targeted verification**

Run:

```bash
rtk cargo test -p fdc-server maintenance_audit
rtk cargo test -p fdc-server successful_tiered_maintenance_records_audit_entry
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_audit_route
rtk cargo test -p fdc-server --test production_server_router_contract storage_maintenance_run_once
rtk cargo test -p fdc-server --test production_server_router_contract production_storage_health
rtk cargo test -p fdc-storage --test dependency_guard
cargo fmt -p fdc-server -p fdc-storage -- --check
```

Expected: all tests pass and package-scoped fmt exits 0.

- [ ] **Step 2: Update development status**

Add P24 entry to `docs/DEVELOPMENT_STATUS.md` with:

- design and plan paths
- audit log capacity and process-local behavior
- route path
- successful-run-only audit semantics
- verification commands and pass counts
- next recommended slice

- [ ] **Step 3: Commit status update**

```bash
rtk git add docs/DEVELOPMENT_STATUS.md
rtk git commit -m "docs: record maintenance audit visibility status"
```

- [ ] **Step 4: Final clean-tree check**

Run:

```bash
rtk git status --short
```

Expected: no output or `ok`.
