use std::sync::Arc;

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
    }
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
