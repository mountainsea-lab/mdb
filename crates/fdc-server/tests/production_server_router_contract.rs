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
