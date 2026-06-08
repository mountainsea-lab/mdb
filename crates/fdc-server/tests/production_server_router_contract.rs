use std::{collections::BTreeSet, sync::Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_server::{
    build_market_data_store_from_runtime_config, build_production_router,
    MarketDataStorageBackendConfig, MarketDataStoragePolicyProfileConfig,
    MarketDataStorageRuntimeConfig, ProductionServerState, ServerRuntimeConfig,
};
use fdc_storage::{
    QueryableMarketDataStore, QueryableStorage, StorageQuery, StorageTier, StorageTierScope,
    StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord, StorageWriteSink,
};
use tower::ServiceExt;

fn runtime_storage_record(key: &str) -> StorageWriteRecord {
    let mut metadata = StorageWriteMetadata::default();
    metadata
        .tags
        .insert("symbol".to_string(), "BTCUSDT".to_string());
    metadata
        .tags
        .insert("kind".to_string(), "trade".to_string());
    StorageWriteRecord::new(
        "market_data",
        "trades",
        key.as_bytes().to_vec(),
        b"value".to_vec(),
    )
    .with_metadata(metadata)
}

fn unique_test_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fdc-server-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn durable_tier_env(root: &std::path::Path) -> [(String, String); 5] {
    [
        (
            "FDC_MARKET_DATA_STORAGE_BACKEND".to_string(),
            "tiered".to_string(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_POLICY_PROFILE".to_string(),
            "generic_realtime".to_string(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_L2_REDB_PATH".to_string(),
            root.join("l2.redb").display().to_string(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH".to_string(),
            root.join("l3.duckdb").display().to_string(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH".to_string(),
            root.join("l4-rocksdb").display().to_string(),
        ),
    ]
}

fn maintenance_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}

fn audit_reset_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}

fn scheduler_reset_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}

fn scheduler_resume_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}

fn live_resume_request(confirm: &str) -> Body {
    Body::from(format!(
        r#"{{"confirm":"{confirm}","reason":"contract-test"}}"#
    ))
}

async fn tiered_storage_state_with_audit_capacity(capacity: usize) -> ProductionServerState {
    tiered_storage_state_with_audit_capacity_and_reset(capacity, false).await
}

async fn tiered_storage_state_with_audit_capacity_and_reset(
    capacity: usize,
    reset_enabled: bool,
) -> ProductionServerState {
    let capacity = capacity.to_string();
    let reset_enabled = if reset_enabled { "1" } else { "0" };
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY",
            capacity.as_str(),
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED",
            reset_enabled,
        ),
    ])
    .expect("tiered config should parse");

    ProductionServerState::try_new(config)
        .await
        .expect("tiered production state should build")
}

async fn run_successful_storage_maintenance(router: axum::Router) {
    let response = router
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

    assert_eq!(response.status(), StatusCode::OK);
}

async fn response_body_json(response: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    serde_json::from_slice(&body).expect("response body should be json")
}

fn p37_durable_config(root: &std::path::Path, extra_env: &[(&str, &str)]) -> ServerRuntimeConfig {
    let mut env = durable_tier_env(root).to_vec();
    env.extend(
        extra_env
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
    );

    ServerRuntimeConfig::from_env_pairs(
        env.iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .expect("p37 durable runtime config should parse")
}

async fn p37_query_trades(router: axum::Router, symbol: &str, limit: usize) -> serde_json::Value {
    let response = router
        .oneshot(
            Request::builder()
                .uri(format!("/market-data/trades?symbol={symbol}&limit={limit}"))
                .body(Body::empty())
                .expect("trade query request should build"),
        )
        .await
        .expect("trade query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    response_body_json(response).await
}

fn p37_trade_ids(json: &serde_json::Value) -> BTreeSet<String> {
    json["data"]["records"]
        .as_array()
        .expect("records should be an array")
        .iter()
        .map(|record| {
            record["payload"]["payload"]["Trade"]["trade_id"]
                .as_str()
                .expect("trade id should be a string")
                .to_string()
        })
        .collect()
}

fn p37_assert_trade_ids(json: &serde_json::Value, expected: &[&str]) {
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], expected.len());

    let actual = p37_trade_ids(json);
    let expected: BTreeSet<String> = expected
        .iter()
        .map(|trade_id| (*trade_id).to_string())
        .collect();
    assert_eq!(actual, expected);
}

async fn p37_l2_records(state: &ProductionServerState) -> Vec<StorageWriteRecord> {
    state
        .market_data_store()
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("p37 L2 query should succeed")
}

async fn p37_ingest_fixture_trades(
    state: &ProductionServerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    state.ingest_test_trade("BTCUSDT", "p37-btc-1").await?;
    state.ingest_test_trade("ETHUSDT", "p37-eth-1").await?;
    state.ingest_test_trade("BTCUSDT", "p37-btc-2").await?;
    Ok(())
}

async fn wait_for_scheduler_status_field_at_least(
    router: axum::Router,
    field: &str,
    minimum: u64,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let response = router
            .clone()
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
        if json["data"][field].as_u64().unwrap_or(0) >= minimum {
            return json;
        }
        if std::time::Instant::now() >= deadline {
            panic!("scheduler field {field} did not reach {minimum}: {json}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn runtime_builder_creates_memory_market_data_store() {
    let store = build_market_data_store_from_runtime_config(MarketDataStorageRuntimeConfig {
        backend: MarketDataStorageBackendConfig::Memory,
        policy_profile: MarketDataStoragePolicyProfileConfig::Compatibility,
        tiers: Default::default(),
    })
    .await
    .expect("memory store should build");

    store
        .write_batch(StorageWriteBatch::new(vec![runtime_storage_record(
            "memory-runtime",
        )]))
        .await
        .expect("memory write should succeed");

    assert_eq!(store.record_count(), 1);
}

#[tokio::test]
async fn runtime_builder_creates_tiered_market_data_store() {
    let store = build_market_data_store_from_runtime_config(MarketDataStorageRuntimeConfig {
        backend: MarketDataStorageBackendConfig::Tiered,
        policy_profile: MarketDataStoragePolicyProfileConfig::Compatibility,
        tiers: Default::default(),
    })
    .await
    .expect("tiered store should build");

    store
        .write_batch(StorageWriteBatch::new(vec![runtime_storage_record(
            "tiered-runtime",
        )]))
        .await
        .expect("tiered write should succeed");

    assert_eq!(store.record_count(), 1);
}

#[tokio::test]
async fn production_router_exposes_health_and_readiness() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let health = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("health should respond");
    assert_eq!(health.status(), StatusCode::OK);
    let health_body = axum::body::to_bytes(health.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let health_json: serde_json::Value = serde_json::from_slice(&health_body).expect("json");
    assert_eq!(health_json["status"], "healthy");

    let ready = router
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("ready should respond");
    assert_eq!(ready.status(), StatusCode::OK);
    let ready_body = axum::body::to_bytes(ready.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let ready_json: serde_json::Value = serde_json::from_slice(&ready_body).expect("json");
    assert_eq!(ready_json["status"], "ready");
    assert_eq!(ready_json["live_enabled"], false);
    assert_eq!(ready_json["market_data_store_available"], true);
}

#[tokio::test]
async fn production_storage_status_reports_memory_defaults() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["backend"], "memory");
    assert_eq!(json["data"]["policy_profile"], "compatibility");
    assert_eq!(json["data"]["tiered"], false);
    assert_eq!(json["data"]["durable_tiers_configured"], 0);
    assert_eq!(json["data"]["maintenance_enabled"], false);
    assert_eq!(json["data"]["maintenance_audit_reset_enabled"], false);
    assert_eq!(json["data"]["maintenance_audit_capacity"], 32);
    assert_eq!(json["data"]["tiers"].as_array().unwrap().len(), 4);
    assert_eq!(json["data"]["tiers"][0]["tier"], "L1");
    assert_eq!(json["data"]["tiers"][0]["engine"], "memory");
    assert_eq!(json["data"]["tiers"][0]["durable_path_configured"], false);
    assert!(json["data"]["tiers"]
        .as_array()
        .unwrap()
        .iter()
        .all(|tier| tier["engine"] == "memory"));
}

#[tokio::test]
async fn production_storage_status_reports_durable_tiered_config_without_full_paths() {
    let root = unique_test_path("storage-status");
    let env = durable_tier_env(&root);
    let config = ServerRuntimeConfig::from_env_pairs(
        env.iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .expect("durable runtime config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["data"]["backend"], "tiered");
    assert_eq!(json["data"]["policy_profile"], "generic_realtime");
    assert_eq!(json["data"]["tiered"], true);
    assert_eq!(json["data"]["durable_tiers_configured"], 3);
    assert_eq!(json["data"]["maintenance_enabled"], false);
    assert_eq!(json["data"]["maintenance_audit_reset_enabled"], false);
    assert_eq!(json["data"]["maintenance_audit_capacity"], 32);
    assert_eq!(json["data"]["tiers"][1]["tier"], "L2");
    assert_eq!(json["data"]["tiers"][1]["engine"], "redb");
    assert_eq!(json["data"]["tiers"][1]["durable_path_configured"], true);
    assert_eq!(json["data"]["tiers"][1]["path_hint"], "l2.redb");
    assert_eq!(json["data"]["tiers"][2]["engine"], "duckdb");
    assert_eq!(json["data"]["tiers"][2]["path_hint"], "l3.duckdb");
    assert_eq!(json["data"]["tiers"][3]["engine"], "rocksdb");
    assert_eq!(json["data"]["tiers"][3]["path_hint"], "l4-rocksdb");

    let body_text = String::from_utf8(body.to_vec()).expect("body should be utf8");
    assert!(
        !body_text.contains(root.to_string_lossy().as_ref()),
        "storage status must not leak full configured paths: {body_text}"
    );
}

#[tokio::test]
async fn production_storage_status_reports_maintenance_gate_metadata() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_RESET_ENABLED",
            "1",
        ),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_AUDIT_CAPACITY", "7"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["backend"], "memory");
    assert_eq!(json["data"]["tiered"], false);
    assert_eq!(json["data"]["durable_tiers_configured"], 0);
    assert_eq!(json["data"]["maintenance_enabled"], true);
    assert_eq!(json["data"]["maintenance_audit_reset_enabled"], true);
    assert_eq!(json["data"]["maintenance_audit_capacity"], 7);
}

#[tokio::test]
async fn production_storage_health_memory_backend_reports_healthy_non_tiered() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/health")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage health should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");

    assert_eq!(json["status"], "success");
    let data = &json["data"];
    assert_eq!(data["backend"], "memory");
    assert_eq!(data["tiered"], false);
    assert_eq!(data["status"], "healthy");
    assert_eq!(data["access_patterns"], 0);
    assert_eq!(data["migration_queue_len"], 0);
    assert!(data["tiers"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn production_storage_health_tiered_backend_reports_initialized_tiers() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
    ])
    .expect("tiered runtime config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/health")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage health should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");

    assert_eq!(json["status"], "success");
    let data = &json["data"];
    assert_eq!(data["backend"], "tiered");
    assert_eq!(data["tiered"], true);
    assert_eq!(data["status"], "healthy");
    assert_eq!(data["access_patterns"], 0);
    assert_eq!(data["migration_queue_len"], 0);

    let tiers = data["tiers"].as_array().unwrap();
    assert_eq!(tiers.len(), 4);
    for expected_tier in ["L1", "L2", "L3", "L4"] {
        let tier = tiers
            .iter()
            .find(|tier| tier["tier"] == expected_tier)
            .unwrap_or_else(|| panic!("missing tier {expected_tier}"));
        assert_eq!(tier["enabled"], true);
        assert_eq!(tier["initialized"], true);
        assert_eq!(tier["status"], "healthy");
        assert!(tier.get("key_count").is_some());
        assert!(tier.get("total_size").is_some());
        assert_eq!(tier["durable_path_configured"], false);
        assert!(tier["path_hint"].is_null());
        assert!(tier["path_exists"].is_null());
        assert!(tier["path_parent_exists"].is_null());
        assert!(tier["path_parent_writable"].is_null());
    }
}

#[tokio::test]
async fn production_storage_health_reports_durable_path_readiness_without_full_paths() {
    let root = unique_test_path("storage-health-paths");
    std::fs::create_dir_all(&root).expect("durable root should be created");
    let env = durable_tier_env(&root);
    let config = ServerRuntimeConfig::from_env_pairs(
        env.iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .expect("durable runtime config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/health")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("storage health should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    let tiers = json["data"]["tiers"].as_array().unwrap();

    let l1 = tiers.iter().find(|tier| tier["tier"] == "L1").unwrap();
    assert_eq!(l1["durable_path_configured"], false);
    assert!(l1["path_hint"].is_null());
    assert!(l1["path_exists"].is_null());
    assert!(l1["path_parent_exists"].is_null());
    assert!(l1["path_parent_writable"].is_null());

    for (tier_name, hint) in [("L2", "l2.redb"), ("L3", "l3.duckdb"), ("L4", "l4-rocksdb")] {
        let tier = tiers
            .iter()
            .find(|tier| tier["tier"] == tier_name)
            .unwrap_or_else(|| panic!("missing tier {tier_name}"));
        assert_eq!(tier["durable_path_configured"], true);
        assert_eq!(tier["path_hint"], hint);
        assert_eq!(tier["path_exists"], true);
        assert_eq!(tier["path_parent_exists"], true);
        assert_eq!(tier["path_parent_writable"], true);
    }

    let body_text = String::from_utf8(body.to_vec()).expect("body should be utf8");
    assert!(
        !body_text.contains(root.to_string_lossy().as_ref()),
        "storage health must not leak full configured paths: {body_text}"
    );
}

#[tokio::test]
async fn storage_maintenance_run_once_is_disabled_by_default() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    let router = build_production_router(state);

    let response = router
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

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "disabled");
}

#[tokio::test]
async fn storage_maintenance_scheduler_status_reports_disabled_defaults() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
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
    assert_eq!(json["status"], "success");
    let data = &json["data"];
    assert_eq!(data["enabled"], false);
    assert_eq!(data["running"], false);
    assert_eq!(data["backend"], "memory");
    assert_eq!(data["tiered"], false);
    assert_eq!(data["interval_seconds"], 3600);
    assert_eq!(data["timeout_ms"], 30000);
    assert_eq!(data["jitter_seconds"], 0);
    assert_eq!(data["max_consecutive_failures"], 3);
    assert_eq!(data["consecutive_failures"], 0);
    assert_eq!(data["total_runs"], 0);
    assert_eq!(data["successful_runs"], 0);
    assert_eq!(data["failed_runs"], 0);
    assert_eq!(data["skipped_runs"], 0);
    assert!(data["last_started_at"].is_null());
    assert!(data["last_finished_at"].is_null());
    assert!(data["last_status"].is_null());
    assert!(data["last_error"].is_null());
    assert!(data["next_run_at"].is_null());
}

#[tokio::test]
async fn storage_maintenance_scheduler_status_reports_configured_values_without_running() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
            "120",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
            "45000",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS",
            "30",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "5",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
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
    assert_eq!(data["interval_seconds"], 120);
    assert_eq!(data["timeout_ms"], 45000);
    assert_eq!(data["jitter_seconds"], 30);
    assert_eq!(data["max_consecutive_failures"], 5);
    assert_eq!(data["total_runs"], 0);
    assert!(data["next_run_at"].is_null());
}

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
        .mark_failed(
            first_finished_at,
            first_next_run_at,
            "first scheduler failure",
        )
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

#[tokio::test]
async fn storage_maintenance_scheduler_reset_is_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/reset")
                .header("content-type", "application/json")
                .body(scheduler_reset_request("reset_scheduler_suppression"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler reset should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "disabled");
}

#[tokio::test]
async fn storage_maintenance_scheduler_reset_requires_confirmation() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/reset")
                .header("content-type", "application/json")
                .body(scheduler_reset_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler reset should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "confirmation_required");
}

#[tokio::test]
async fn storage_maintenance_scheduler_reset_clears_suppressed_status_without_running_work() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED",
            "1",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "1",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let scheduler = state.market_data_storage_maintenance_scheduler();
    let started_at = chrono::Utc::now();
    let finished_at = started_at + chrono::Duration::milliseconds(1);
    let next_run_at = finished_at + chrono::Duration::seconds(60);
    assert!(scheduler.mark_started(started_at).await);
    scheduler
        .mark_failed(finished_at, next_run_at, "simulated reset route failure")
        .await;
    let router = build_production_router(state);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/reset")
                .header("content-type", "application/json")
                .body(scheduler_reset_request("reset_scheduler_suppression"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler reset should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "reset");
    assert_eq!(json["data"]["reason"], "contract-test");
    assert_eq!(json["data"]["previous_consecutive_failures"], 1);
    assert_eq!(json["data"]["consecutive_failures"], 0);

    let status_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/scheduler/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("scheduler status should respond");
    assert_eq!(status_response.status(), StatusCode::OK);
    let status_json = response_body_json(status_response).await;
    assert_eq!(status_json["data"]["last_status"], "reset");
    assert_eq!(status_json["data"]["consecutive_failures"], 0);
    assert!(status_json["data"]["last_error"].is_null());
    assert!(status_json["data"]["next_run_at"].is_null());
    assert_eq!(status_json["data"]["total_runs"], 1);
    assert_eq!(status_json["data"]["failed_runs"], 1);
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_is_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("resume_scheduler"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "disabled");
    assert_eq!(json["data"]["task_started"], false);
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_requires_confirmation() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "confirmation_required");
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_requires_scheduler_enabled() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("resume_scheduler"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["status"], "scheduler_disabled");
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_rejects_memory_backend() {
    let config = ServerRuntimeConfig::from_env_pairs([
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
            "1",
        ),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("resume_scheduler"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["status"], "unsupported_backend");
}

#[tokio::test]
async fn storage_maintenance_scheduler_resume_starts_new_loop_after_suppression() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
            "1",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "1",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECS",
            "60",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::with_market_data_store(
        config,
        Arc::new(QueryableMarketDataStore::new()),
    );
    let scheduler = state.market_data_storage_maintenance_scheduler();
    let task_handle = state.market_data_storage_maintenance_scheduler_task();
    let started_at = chrono::Utc::now();
    let finished_at = started_at + chrono::Duration::milliseconds(1);
    let next_run_at = finished_at + chrono::Duration::seconds(60);
    assert!(scheduler.mark_started(started_at).await);
    scheduler
        .mark_failed(finished_at, next_run_at, "simulated resume route failure")
        .await;
    assert!(scheduler.is_suppressed().await);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/resume")
                .header("content-type", "application/json")
                .body(scheduler_resume_request("resume_scheduler"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler resume should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "resumed");
    assert_eq!(json["data"]["previous_consecutive_failures"], 1);
    assert_eq!(json["data"]["consecutive_failures"], 0);
    assert_eq!(json["data"]["task_started"], true);

    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let snapshot = scheduler.snapshot().await;
    assert_eq!(snapshot.consecutive_failures, 0);
    assert!(snapshot.total_runs >= 1);
    assert!(snapshot.next_run_at.is_some() || snapshot.running);
    assert!(task_handle.is_active().await);
    task_handle.abort_active().await;
}

#[tokio::test]
async fn storage_maintenance_scheduler_reset_does_not_start_scheduler_task() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED",
            "1",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "1",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::with_market_data_store(
        config,
        Arc::new(QueryableMarketDataStore::new()),
    );
    let scheduler = state.market_data_storage_maintenance_scheduler();
    let task_handle = state.market_data_storage_maintenance_scheduler_task();
    let started_at = chrono::Utc::now();
    let finished_at = started_at + chrono::Duration::milliseconds(1);
    let next_run_at = finished_at + chrono::Duration::seconds(60);
    assert!(scheduler.mark_started(started_at).await);
    scheduler
        .mark_failed(finished_at, next_run_at, "simulated reset route failure")
        .await;
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/scheduler/reset")
                .header("content-type", "application/json")
                .body(scheduler_reset_request("reset_scheduler_suppression"))
                .expect("request should build"),
        )
        .await
        .expect("scheduler reset should respond");
    assert_eq!(response.status(), StatusCode::OK);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let snapshot = scheduler.snapshot().await;
    assert_eq!(snapshot.total_runs, 1);
    assert!(snapshot.next_run_at.is_none());
    assert!(!task_handle.is_active().await);
}

#[tokio::test]
async fn storage_maintenance_scheduler_disabled_default_does_not_spawn_or_audit() {
    let state = ProductionServerState::try_new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    )
    .await
    .expect("production state should build");
    let router = build_production_router(state);

    tokio::time::sleep(std::time::Duration::from_millis(75)).await;

    let status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/scheduler/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("scheduler status should respond");
    assert_eq!(status_response.status(), StatusCode::OK);
    let status_json = response_body_json(status_response).await;
    assert_eq!(status_json["data"]["enabled"], false);
    assert_eq!(status_json["data"]["running"], false);
    assert_eq!(status_json["data"]["total_runs"], 0);
    assert_eq!(status_json["data"]["successful_runs"], 0);
    assert_eq!(status_json["data"]["failed_runs"], 0);
    assert_eq!(status_json["data"]["skipped_runs"], 0);
    assert!(status_json["data"]["next_run_at"].is_null());

    let audit_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    assert_eq!(audit_response.status(), StatusCode::OK);
    let audit_json = response_body_json(audit_response).await;
    assert_eq!(audit_json["data"]["total_entries"], 0);
    assert_eq!(audit_json["data"]["total_recorded_entries"], 0);
}

#[tokio::test]
async fn storage_maintenance_manual_gate_does_not_enable_scheduler() {
    let config =
        ServerRuntimeConfig::from_env_pairs([("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1")])
            .expect("config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    tokio::time::sleep(std::time::Duration::from_millis(75)).await;

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
    assert_eq!(json["data"]["enabled"], false);
    assert_eq!(json["data"]["total_runs"], 0);
    assert_eq!(json["data"]["skipped_runs"], 0);
    assert!(json["data"]["next_run_at"].is_null());
}

#[tokio::test]
async fn storage_maintenance_scheduler_enabled_memory_backend_reports_unsupported_without_audit() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED",
        "1",
    )])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    tokio::time::sleep(std::time::Duration::from_millis(75)).await;

    let status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/scheduler/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("scheduler status should respond");
    assert_eq!(status_response.status(), StatusCode::OK);
    let status_json = response_body_json(status_response).await;
    assert_eq!(status_json["data"]["enabled"], true);
    assert_eq!(status_json["data"]["backend"], "memory");
    assert_eq!(status_json["data"]["tiered"], false);
    assert_eq!(status_json["data"]["running"], false);
    assert_eq!(status_json["data"]["total_runs"], 0);
    assert_eq!(status_json["data"]["successful_runs"], 0);
    assert_eq!(status_json["data"]["failed_runs"], 0);
    assert_eq!(status_json["data"]["skipped_runs"], 1);
    assert_eq!(status_json["data"]["last_status"], "unsupported_backend");
    assert!(status_json["data"]["next_run_at"].is_null());

    let audit_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    assert_eq!(audit_response.status(), StatusCode::OK);
    let audit_json = response_body_json(audit_response).await;
    assert_eq!(audit_json["data"]["total_entries"], 0);
}

#[tokio::test]
async fn storage_maintenance_scheduler_enabled_tiered_backend_runs_and_records_audit() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
            "60",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
            "5000",
        ),
    ])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config)
        .await
        .expect("production state should build");
    let router = build_production_router(state);

    let status_json =
        wait_for_scheduler_status_field_at_least(router.clone(), "successful_runs", 1).await;
    assert_eq!(status_json["data"]["enabled"], true);
    assert_eq!(status_json["data"]["backend"], "tiered");
    assert_eq!(status_json["data"]["tiered"], true);
    assert_eq!(status_json["data"]["total_runs"], 1);
    assert_eq!(status_json["data"]["successful_runs"], 1);
    assert_eq!(status_json["data"]["failed_runs"], 0);
    assert_eq!(status_json["data"]["consecutive_failures"], 0);
    assert_eq!(status_json["data"]["last_status"], "completed");
    assert!(status_json["data"]["last_started_at"].is_string());
    assert!(status_json["data"]["last_finished_at"].is_string());
    assert!(status_json["data"]["next_run_at"].is_string());

    let audit_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    assert_eq!(audit_response.status(), StatusCode::OK);
    let audit_json = response_body_json(audit_response).await;
    assert_eq!(audit_json["data"]["total_entries"], 1);
    assert_eq!(audit_json["data"]["total_recorded_entries"], 1);
    assert_eq!(audit_json["data"]["entries"][0]["healthy_tiers"], 4);
}

#[tokio::test]
async fn storage_maintenance_run_once_requires_confirmation() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config).await.unwrap();
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/run-once")
                .header("content-type", "application/json")
                .body(maintenance_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("maintenance route should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["status"], "confirmation_required");
}

#[tokio::test]
async fn storage_maintenance_run_once_rejects_memory_backend() {
    let config =
        ServerRuntimeConfig::from_env_pairs([("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1")])
            .expect("config should parse");
    let state = ProductionServerState::try_new(config).await.unwrap();
    let router = build_production_router(state);

    let response = router
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

    assert_eq!(response.status(), StatusCode::CONFLICT);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["data"]["status"], "unsupported_backend");
}

#[tokio::test]
async fn storage_maintenance_run_once_completes_for_enabled_tiered_backend() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1"),
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::try_new(config).await.unwrap();
    let router = build_production_router(state);

    let response = router
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

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "completed");
    assert!(json["data"]["duration_ms"].as_i64().unwrap() >= 0);
    assert_eq!(json["data"]["scanned_entries"], 0);
    assert_eq!(json["data"]["compaction_failed"], 0);
    assert_eq!(json["data"]["healthy_tiers"], 4);
}

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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["capacity"], 32);
    assert_eq!(json["data"]["total_entries"], 0);
    assert_eq!(json["data"]["returned_entries"], 0);
    assert_eq!(json["data"]["total_recorded_entries"], 0);
    assert_eq!(json["data"]["reset_count"], 0);
    assert_eq!(json["data"]["total_cleared_entries"], 0);
    assert!(json["data"]["last_recorded_at"].is_null());
    assert!(json["data"]["last_reset_at"].is_null());
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
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["capacity"], 32);
    assert_eq!(json["data"]["total_entries"], 1);
    assert_eq!(json["data"]["returned_entries"], 1);
    assert_eq!(json["data"]["total_recorded_entries"], 1);
    assert_eq!(json["data"]["reset_count"], 0);
    assert_eq!(json["data"]["total_cleared_entries"], 0);
    assert!(json["data"]["last_recorded_at"]
        .as_str()
        .unwrap()
        .contains('T'));
    assert!(json["data"]["last_reset_at"].is_null());
    let entries = json["data"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["scanned_entries"], 0);
    assert_eq!(entries[0]["healthy_tiers"], 4);
    assert!(entries[0]
        .get("recorded_at")
        .unwrap()
        .as_str()
        .unwrap()
        .contains('T'));
}

#[tokio::test]
async fn storage_maintenance_audit_route_clamps_to_configured_capacity_newest_first() {
    let state = tiered_storage_state_with_audit_capacity(2).await;
    let router = build_production_router(state.clone());

    state
        .ingest_test_trade("BTCUSDT", "audit-capacity-1")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;
    state
        .ingest_test_trade("BTCUSDT", "audit-capacity-2")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;
    state
        .ingest_test_trade("BTCUSDT", "audit-capacity-3")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;

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
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["total_entries"], 2);
    assert_eq!(json["data"]["returned_entries"], 2);
    assert_eq!(json["data"]["entries"][0]["scanned_entries"], 3);
    assert_eq!(json["data"]["entries"][1]["scanned_entries"], 2);
}

#[tokio::test]
async fn storage_maintenance_audit_route_limit_one_returns_newest_entry() {
    let state = tiered_storage_state_with_audit_capacity(3).await;
    let router = build_production_router(state.clone());

    state
        .ingest_test_trade("BTCUSDT", "audit-limit-1")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;
    state
        .ingest_test_trade("BTCUSDT", "audit-limit-2")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit?limit=1")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["total_entries"], 2);
    assert_eq!(json["data"]["returned_entries"], 1);
    assert_eq!(json["data"]["entries"][0]["scanned_entries"], 2);
}

#[tokio::test]
async fn storage_maintenance_audit_reset_route_is_disabled_by_default() {
    let state = tiered_storage_state_with_audit_capacity(3).await;
    let router = build_production_router(state.clone());

    state
        .ingest_test_trade("BTCUSDT", "audit-reset-disabled")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;

    let response = router
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

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "disabled");
    assert_eq!(json["data"]["cleared_entries"], 0);

    let audit = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    let audit_json = response_body_json(audit).await;
    assert_eq!(audit_json["data"]["total_entries"], 1);
}

#[tokio::test]
async fn storage_maintenance_audit_reset_route_requires_confirmation() {
    let state = tiered_storage_state_with_audit_capacity_and_reset(3, true).await;
    let router = build_production_router(state.clone());

    state
        .ingest_test_trade("BTCUSDT", "audit-reset-confirm")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/storage/maintenance/audit/reset")
                .header("content-type", "application/json")
                .body(audit_reset_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("reset route should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["accepted"], false);
    assert_eq!(json["data"]["status"], "confirmation_required");

    let audit = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    let audit_json = response_body_json(audit).await;
    assert_eq!(audit_json["data"]["total_entries"], 1);
}

#[tokio::test]
async fn storage_maintenance_audit_reset_route_clears_entries_when_enabled_and_confirmed() {
    let state = tiered_storage_state_with_audit_capacity_and_reset(3, true).await;
    let router = build_production_router(state.clone());

    state
        .ingest_test_trade("BTCUSDT", "audit-reset-1")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;
    state
        .ingest_test_trade("BTCUSDT", "audit-reset-2")
        .await
        .unwrap();
    run_successful_storage_maintenance(router.clone()).await;

    let response = router
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

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "reset");
    assert_eq!(json["data"]["cleared_entries"], 2);
    assert_eq!(json["data"]["remaining_entries"], 0);

    let audit = router
        .oneshot(
            Request::builder()
                .uri("/market-data/storage/maintenance/audit")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("audit route should respond");
    let audit_json = response_body_json(audit).await;
    assert_eq!(audit_json["data"]["total_entries"], 0);
    assert_eq!(audit_json["data"]["returned_entries"], 0);
}

#[tokio::test]
async fn storage_maintenance_audit_reset_route_succeeds_on_empty_log() {
    let state = tiered_storage_state_with_audit_capacity_and_reset(3, true).await;
    let router = build_production_router(state);

    let response = router
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

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["data"]["accepted"], true);
    assert_eq!(json["data"]["status"], "reset");
    assert_eq!(json["data"]["cleared_entries"], 0);
    assert_eq!(json["data"]["remaining_entries"], 0);
}

#[tokio::test]
async fn storage_maintenance_audit_route_returns_metadata_after_reset() {
    let state = tiered_storage_state_with_audit_capacity_and_reset(3, true).await;
    let router = build_production_router(state.clone());

    state
        .ingest_test_trade("BTCUSDT", "audit-metadata-reset")
        .await
        .unwrap();
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
    assert!(json["data"]["last_recorded_at"]
        .as_str()
        .unwrap()
        .contains('T'));
    assert!(json["data"]["last_reset_at"]
        .as_str()
        .unwrap()
        .contains('T'));
    assert!(json["data"]["entries"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn production_live_start_is_explicitly_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/start")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"timeout_secs":1,"max_envelopes":1}"#))
                .expect("request should build"),
        )
        .await
        .expect("start should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["state"], "idle");
    assert!(json["message"]
        .as_str()
        .unwrap()
        .contains("FDC_LIVE_ENABLED=1"));
}

#[tokio::test]
async fn production_live_status_exposes_retry_and_resume_fields() {
    let config =
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/live/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("status should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["state"], "idle");
    assert_eq!(json["data"]["consecutive_failures"], 0);
    assert_eq!(json["data"]["retry_count"], 0);
    assert!(json["data"]["last_error"].is_null());
    assert!(json["data"]["last_error_at_ns"].is_null());
    assert!(json["data"]["next_retry_at_ns"].is_null());
    assert!(json["data"]["suppressed_reason"].is_null());
    assert_eq!(json["data"]["resume_enabled"], false);
}

#[tokio::test]
async fn production_live_resume_is_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/resume")
                .header("content-type", "application/json")
                .body(live_resume_request("resume_live_collection"))
                .expect("request should build"),
        )
        .await
        .expect("resume should respond");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["resumed"], false);
}

#[tokio::test]
async fn production_live_resume_requires_confirmation() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_MARKET_DATA_LIVE_RESUME_ENABLED", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/resume")
                .header("content-type", "application/json")
                .body(live_resume_request("wrong"))
                .expect("request should build"),
        )
        .await
        .expect("resume should respond");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = response_body_json(response).await;
    assert_eq!(json["status"], "error");
    assert_eq!(json["data"]["resumed"], false);
}

#[tokio::test]
async fn production_live_resume_conflicts_when_running() {
    use fdc_server::market_data::service::start_fake_background_live_for_test;

    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_MARKET_DATA_LIVE_RESUME_ENABLED", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    start_fake_background_live_for_test(&state, 10, std::time::Duration::from_millis(25))
        .await
        .expect("fake live should start");
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/resume")
                .header("content-type", "application/json")
                .body(live_resume_request("resume_live_collection"))
                .expect("request should build"),
        )
        .await
        .expect("resume should respond");

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn production_trade_query_reads_shared_store_after_fixture_ingest_helper() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    state
        .ingest_test_trade("BTCUSDT", "prod-btc-1")
        .await
        .expect("fixture ingest should write");
    state
        .ingest_test_trade("ETHUSDT", "prod-eth-1")
        .await
        .expect("fixture ingest should write");
    state
        .ingest_test_trade("BTCUSDT", "prod-btc-2")
        .await
        .expect("fixture ingest should write");

    let router = build_production_router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 2);
}

#[tokio::test]
async fn production_trade_query_reads_tiered_backed_market_data_store() {
    let config =
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse");
    let store = Arc::new(
        QueryableMarketDataStore::memory_tiered()
            .await
            .expect("memory tiered store should initialize"),
    );
    let state = ProductionServerState::with_market_data_store(config, store);

    state
        .ingest_test_trade("BTCUSDT", "tiered-prod-btc-1")
        .await
        .expect("fixture ingest should write to tiered-backed store");

    let router = build_production_router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 1);
    assert_eq!(json["data"]["records"][0]["symbol"], "BTCUSDT");
}

#[tokio::test]
async fn production_state_try_new_uses_tiered_runtime_storage_config() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "compatibility"),
    ])
    .expect("tiered runtime config should parse");

    let state = ProductionServerState::try_new(config)
        .await
        .expect("state should assemble from runtime config");
    state
        .ingest_test_trade("BTCUSDT", "tiered-state")
        .await
        .expect("fixture ingest should write through configured store");

    let router = build_production_router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 1);
}

#[tokio::test]
async fn runtime_server_path_routes_live_fixture_with_tiered_generic_realtime() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "generic_realtime"),
    ])
    .expect("tiered generic realtime runtime config should parse");

    let state = ProductionServerState::try_new(config)
        .await
        .expect("state should assemble tiered generic realtime storage");
    state
        .ingest_test_trade("BTCUSDT", "generic-runtime-live-1")
        .await
        .expect("fixture ingest should write through configured store");

    let store = state.market_data_store();
    let hot_records = store
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("hot tier query should succeed");

    assert_eq!(hot_records.len(), 1);
    assert_eq!(
        hot_records[0].metadata.tags.get("mode").map(String::as_str),
        Some("live")
    );
    assert_eq!(
        hot_records[0]
            .metadata
            .tags
            .get("record.kind")
            .map(String::as_str),
        Some("trade")
    );

    let router = build_production_router(state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 1);
    assert_eq!(json["data"]["records"][0]["symbol"], "BTCUSDT");
    assert_eq!(
        json["data"]["records"][0]["payload"]["payload"]["Trade"]["trade_id"],
        "generic-runtime-live-1"
    );
}

#[tokio::test]
async fn runtime_server_reopens_configured_durable_tiers_and_serves_persisted_trade() {
    let root = unique_test_path("durable-reopen");
    let env = durable_tier_env(&root);
    let config = ServerRuntimeConfig::from_env_pairs(
        env.iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
    )
    .expect("durable runtime config should parse");

    let first_state = ProductionServerState::try_new(config.clone())
        .await
        .expect("first durable state should build");
    first_state
        .ingest_test_trade("BTCUSDT", "durable-reopen-live-1")
        .await
        .expect("fixture write should succeed");

    let before_reopen = first_state
        .market_data_store()
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("L2 query before reopen should succeed");
    assert_eq!(before_reopen.len(), 1);
    assert_eq!(
        before_reopen[0]
            .metadata
            .tags
            .get("mode")
            .map(String::as_str),
        Some("live")
    );

    drop(first_state);

    let reopened_state = ProductionServerState::try_new(config)
        .await
        .expect("reopened durable state should build");
    let persisted_l2 = reopened_state
        .market_data_store()
        .query_storage(
            &StorageQuery::new("market_data")
                .with_tier_scope(StorageTierScope::Only(StorageTier::L2)),
        )
        .await
        .expect("L2 query after reopen should succeed");
    assert_eq!(persisted_l2.len(), 1);
    assert_eq!(
        persisted_l2[0]
            .metadata
            .tags
            .get("record.kind")
            .map(String::as_str),
        Some("trade")
    );

    let router = build_production_router(reopened_state);
    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("query should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 1);
    assert_eq!(
        json["data"]["records"][0]["payload"]["payload"]["Trade"]["trade_id"],
        "durable-reopen-live-1"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn p37_tiered_acquisition_query_acceptance_survives_reopen() {
    let root = unique_test_path("p37-acquisition-query-reopen");
    let config = p37_durable_config(&root, &[]);

    let first_state = ProductionServerState::try_new(config.clone())
        .await
        .expect("first p37 durable state should build");
    p37_ingest_fixture_trades(&first_state)
        .await
        .expect("p37 fixture ingestion should write");

    let before_reopen = p37_l2_records(&first_state).await;
    assert_eq!(before_reopen.len(), 3);
    assert!(before_reopen.iter().all(|record| {
        record.metadata.tags.get("mode").map(String::as_str) == Some("live")
            && record
                .metadata
                .tags
                .get("record.kind")
                .map(String::as_str)
                == Some("trade")
    }));

    let before_router = build_production_router(first_state.clone());
    let before_json = p37_query_trades(before_router, "BTCUSDT", 10).await;
    p37_assert_trade_ids(&before_json, &["p37-btc-1", "p37-btc-2"]);
    assert!(before_json["data"]["records"]
        .as_array()
        .unwrap()
        .iter()
        .all(|record| record["symbol"] == "BTCUSDT"));

    drop(first_state);

    let reopened_state = ProductionServerState::try_new(config)
        .await
        .expect("reopened p37 durable state should build");
    let persisted_l2 = p37_l2_records(&reopened_state).await;
    assert_eq!(persisted_l2.len(), 3);
    assert!(persisted_l2.iter().all(|record| {
        record.metadata.tags.get("mode").map(String::as_str) == Some("live")
            && record
                .metadata
                .tags
                .get("record.kind")
                .map(String::as_str)
                == Some("trade")
    }));

    let reopened_router = build_production_router(reopened_state);
    let reopened_json = p37_query_trades(reopened_router, "BTCUSDT", 10).await;
    p37_assert_trade_ids(&reopened_json, &["p37-btc-1", "p37-btc-2"]);
    assert!(reopened_json["data"]["records"]
        .as_array()
        .unwrap()
        .iter()
        .all(|record| record["symbol"] == "BTCUSDT"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn production_live_supervisor_tracks_start_complete_and_rejects_concurrent_start() {
    use fdc_server::market_data::{
        model::{MarketDataLiveState, StartLiveMarketDataResponse},
        supervisor::MarketDataSupervisor,
    };

    let supervisor = MarketDataSupervisor::new();
    assert_eq!(supervisor.status().state, MarketDataLiveState::Idle);

    supervisor
        .try_start()
        .expect("idle supervisor should start");
    assert_eq!(supervisor.status().state, MarketDataLiveState::Starting);

    let error = supervisor
        .try_start()
        .expect_err("concurrent start should be rejected");
    assert!(error.to_string().contains("already starting"));

    supervisor.complete(StartLiveMarketDataResponse {
        state: MarketDataLiveState::Completed,
        task_id: None,
        envelopes_received: 2,
        storage_records_written: 2,
        market_data_store_records: 2,
    });

    let status = supervisor.status();
    assert_eq!(status.state, MarketDataLiveState::Completed);
    assert_eq!(
        status.last_result.as_ref().unwrap().storage_records_written,
        2
    );
    assert!(status.failure_message.is_none());
}

#[test]
fn production_live_supervisor_tracks_background_task_snapshot_and_stop() {
    use fdc_server::market_data::{model::MarketDataLiveState, supervisor::MarketDataSupervisor};

    let supervisor = MarketDataSupervisor::new();
    let task_id = supervisor
        .start_background(vec![
            "binance_spot:BTCUSDT:trade".to_string(),
            "binance_spot:BTCUSDT:order_book_l1".to_string(),
            "binance_spot:BTCUSDT:order_book".to_string(),
        ])
        .expect("background task should reserve");

    let running = supervisor.status();
    assert_eq!(running.state, MarketDataLiveState::Running);
    assert_eq!(running.task_id.as_deref(), Some(task_id.as_str()));
    assert!(running.started_at_ns.is_some());
    assert!(running.stopped_at_ns.is_none());
    assert_eq!(
        running.subscriptions,
        vec![
            "binance_spot:BTCUSDT:trade",
            "binance_spot:BTCUSDT:order_book_l1",
            "binance_spot:BTCUSDT:order_book",
        ]
    );

    supervisor.record_progress(2, 2, 2, Some(123));
    let progressed = supervisor.status();
    assert_eq!(progressed.envelopes_received, 2);
    assert_eq!(progressed.storage_records_written, 2);
    assert_eq!(progressed.market_data_store_records, 2);
    assert_eq!(progressed.last_record_at_ns, Some(123));

    assert!(supervisor.request_stop("requested"));
    assert_eq!(supervisor.status().state, MarketDataLiveState::Stopping);

    supervisor.stopped("requested");
    let stopped = supervisor.status();
    assert_eq!(stopped.state, MarketDataLiveState::Stopped);
    assert_eq!(stopped.stop_reason.as_deref(), Some("requested"));
    assert!(stopped.stopped_at_ns.is_some());
}

#[tokio::test]
async fn production_live_stop_is_idempotent_when_no_runner_is_active() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );
    let router = build_production_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/stop")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("stop should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["state"], "idle");
}

#[tokio::test]
async fn production_live_fake_background_start_stop_updates_status() {
    use fdc_server::market_data::service::start_fake_background_live_for_test;

    let config = ServerRuntimeConfig::from_env_pairs([("FDC_LIVE_ENABLED", "1")])
        .expect("config should parse");
    let state = ProductionServerState::new(config);

    let started =
        start_fake_background_live_for_test(&state, 3, std::time::Duration::from_millis(10))
            .await
            .expect("fake runner should start");
    assert_eq!(
        started.state,
        fdc_server::market_data::model::MarketDataLiveState::Running
    );

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let status = state.market_data_supervisor().status();
    assert_eq!(
        status.state,
        fdc_server::market_data::model::MarketDataLiveState::Running
    );
    assert!(status.envelopes_received >= 1);

    let router = build_production_router(state.clone());
    let stop = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/stop")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("stop should respond");
    let stop_body = axum::body::to_bytes(stop.into_body(), usize::MAX)
        .await
        .expect("body");
    let stop_json: serde_json::Value = serde_json::from_slice(&stop_body).expect("json");
    assert_eq!(stop_json["status"], "success");
    assert!(matches!(
        stop_json["data"]["state"].as_str().unwrap(),
        "stopping" | "stopped"
    ));
}

#[tokio::test]
async fn production_live_autostart_does_not_run_when_disabled_by_default() {
    let state = ProductionServerState::new(
        ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).expect("config should parse"),
    );

    state
        .start_live_autostart_if_enabled()
        .await
        .expect("disabled autostart should be ok");
    assert_eq!(
        state.market_data_supervisor().status().state,
        fdc_server::market_data::model::MarketDataLiveState::Idle
    );
}

#[tokio::test]
#[ignore = "enabled config may touch public internet; covered by production_live_smoke"]
async fn production_live_start_with_enabled_config_updates_status_on_failure() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "1"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "1"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/start")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"timeout_secs":1,"max_envelopes":1}"#))
                .expect("request should build"),
        )
        .await
        .expect("start should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("json");

    assert!(json["status"] == "success" || json["status"] == "error");
    assert!(json["data"]["state"].is_string());

    let status_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/live/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("status should respond");
    let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX)
        .await
        .expect("status body should read");
    let status_json: serde_json::Value = serde_json::from_slice(&status_body).expect("json");

    assert!(matches!(
        status_json["data"]["state"].as_str().unwrap(),
        "completed" | "failed"
    ));
}
