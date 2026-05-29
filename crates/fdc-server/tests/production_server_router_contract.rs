use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_server::{build_production_router, ProductionServerState, ServerRuntimeConfig};
use tower::ServiceExt;

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
