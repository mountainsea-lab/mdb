use axum::{body::Body, http::Request};
use fdc_server::{build_production_router, ProductionServerState, ServerRuntimeConfig};
use tower::ServiceExt;

#[ignore = "requires public internet, FDC_LIVE_ENABLED=1, and FDC_LIVE_AUTOSTART=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_background_live_autostart_writes_trades_and_stop_finishes() {
    if std::env::var("FDC_LIVE_ENABLED").as_deref() != Ok("1")
        || std::env::var("FDC_LIVE_AUTOSTART").as_deref() != Ok("1")
    {
        eprintln!("skipping background live smoke because live flags are not set");
        return;
    }

    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_AUTOSTART", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "10"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "5"),
    ])
    .expect("config should parse");
    let state = ProductionServerState::new(config);
    state
        .start_live_autostart_if_enabled()
        .await
        .expect("autostart should spawn");
    let router = build_production_router(state.clone());

    tokio::time::sleep(std::time::Duration::from_secs(8)).await;

    let status = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/live/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("status should respond");
    let status_body = axum::body::to_bytes(status.into_body(), usize::MAX)
        .await
        .unwrap();
    let status_json: serde_json::Value = serde_json::from_slice(&status_body).unwrap();
    eprintln!("background status: {status_json:#}");
    assert_eq!(status_json["status"], "success");
    assert!(status_json["data"]["envelopes_received"].as_u64().unwrap() >= 1);

    let query = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("query should respond");
    let query_body = axum::body::to_bytes(query.into_body(), usize::MAX)
        .await
        .unwrap();
    let query_json: serde_json::Value = serde_json::from_slice(&query_body).unwrap();
    eprintln!("background query: {query_json:#}");
    assert_eq!(query_json["status"], "success");
    assert!(query_json["data"]["returned_records"].as_u64().unwrap() >= 1);

    let stop = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/stop")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("stop should respond");
    let stop_body = axum::body::to_bytes(stop.into_body(), usize::MAX)
        .await
        .unwrap();
    let stop_json: serde_json::Value = serde_json::from_slice(&stop_body).unwrap();
    eprintln!("background stop: {stop_json:#}");
    assert_eq!(stop_json["status"], "success");
}
