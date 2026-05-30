use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_server::{build_production_router, ProductionServerState, ServerRuntimeConfig};
use tower::ServiceExt;

#[ignore = "requires public internet and FDC_LIVE_ENABLED=1"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignored_production_live_start_writes_real_trades_and_query_reads_them() {
    if std::env::var("FDC_LIVE_ENABLED").as_deref() != Ok("1") {
        eprintln!("skipping production live smoke because FDC_LIVE_ENABLED=1 is not set");
        return;
    }

    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "20"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "20"),
    ])
    .expect("config should parse");
    let router = build_production_router(ProductionServerState::new(config));

    let start = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/market-data/live/start")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"timeout_secs":20,"max_envelopes":20}"#))
                .expect("request should build"),
        )
        .await
        .expect("start should respond");

    assert_eq!(start.status(), StatusCode::OK);
    let start_body = axum::body::to_bytes(start.into_body(), usize::MAX)
        .await
        .expect("body");
    let start_json: serde_json::Value = serde_json::from_slice(&start_body).expect("json");
    eprintln!("production live start response: {start_json:#}");
    assert_eq!(start_json["status"], "success");
    assert!(
        start_json["data"]["storage_records_written"]
            .as_u64()
            .unwrap()
            >= 1
    );

    let query = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?limit=5")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("query should respond");
    let query_body = axum::body::to_bytes(query.into_body(), usize::MAX)
        .await
        .expect("body");
    let query_json: serde_json::Value = serde_json::from_slice(&query_body).expect("json");
    eprintln!("production live query response: {query_json:#}");
    assert_eq!(query_json["status"], "success");
    assert!(query_json["data"]["returned_records"].as_u64().unwrap() >= 1);
}
