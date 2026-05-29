use std::{fs, path::PathBuf, sync::Arc};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{
    build_demo_router, ApiAppState, ApiResponse, ApiRunnerStatusProjection,
    MarketDataTradesResponse,
};
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use serde_json::json;
use tokio::sync::Mutex;
use tower::ServiceExt;

fn initialized_state_with_control_runner() -> ApiAppState {
    let mut app = FdcServerApp::with_defaults();
    app.initialize().expect("test app should initialize");

    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = Arc::new(Mutex::new(BoundedMarketDataRunnerHandle::new(Arc::clone(
        &store,
    ))));

    ApiAppState::new(app)
        .with_market_data_store(store)
        .with_market_data_runner_control(runner)
}

#[tokio::test]
async fn demo_router_ready_route_returns_typed_readiness() {
    let router = build_demo_router(initialized_state_with_control_runner());

    let response = router
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("response should be json");

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["status"], "ready");
    assert_eq!(json["data"]["server_lifecycle_state"], "initialized");
}

#[tokio::test]
async fn demo_router_runs_fixture_then_status_and_market_data_queries_share_state() {
    let router = build_demo_router(initialized_state_with_control_runner());

    let start_response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runner/start-fixture")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "trades": [
                            {"symbol": "BTCUSDT", "trade_id": "btc-demo-1", "sequence": "seq-demo-1"}
                        ]
                    })
                    .to_string(),
                ))
                .expect("request should build"),
        )
        .await
        .expect("start route should respond");

    assert_eq!(start_response.status(), StatusCode::OK);
    let start_body = axum::body::to_bytes(start_response.into_body(), usize::MAX)
        .await
        .expect("start body should read");
    let start: ApiResponse<ApiRunnerStatusProjection> =
        serde_json::from_slice(&start_body).expect("start response should decode");
    assert_eq!(start.status, "success");
    assert_eq!(
        start.data.state,
        fdc_api::ApiRunnerLifecycleStatus::Completed
    );
    assert_eq!(
        start
            .data
            .last_result
            .as_ref()
            .unwrap()
            .storage_records_written,
        1
    );

    let status_response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/runner/status")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("status route should respond");

    assert_eq!(status_response.status(), StatusCode::OK);
    let status_body = axum::body::to_bytes(status_response.into_body(), usize::MAX)
        .await
        .expect("status body should read");
    let status: ApiResponse<ApiRunnerStatusProjection> =
        serde_json::from_slice(&status_body).expect("status response should decode");
    assert_eq!(
        status.data.state,
        fdc_api::ApiRunnerLifecycleStatus::Completed
    );
    assert_eq!(
        status
            .data
            .last_result
            .as_ref()
            .unwrap()
            .market_data_store_records,
        1
    );

    let market_data_response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("market-data route should respond");

    assert_eq!(market_data_response.status(), StatusCode::OK);
    let market_data_body = axum::body::to_bytes(market_data_response.into_body(), usize::MAX)
        .await
        .expect("market-data body should read");
    let market_data: ApiResponse<MarketDataTradesResponse> =
        serde_json::from_slice(&market_data_body).expect("market-data response should decode");

    assert_eq!(market_data.status, "success");
    assert_eq!(market_data.data.returned_records, 1);
    assert_eq!(
        market_data.data.records[0].symbol.as_deref(),
        Some("BTCUSDT")
    );
}

#[tokio::test]
async fn demo_router_cancel_route_uses_same_control_surface() {
    let router = build_demo_router(initialized_state_with_control_runner());

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runner/cancel")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("cancel route should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let cancel: ApiResponse<ApiRunnerStatusProjection> =
        serde_json::from_slice(&body).expect("cancel response should decode");

    assert_eq!(cancel.status, "success");
    assert_eq!(
        cancel.data.state,
        fdc_api::ApiRunnerLifecycleStatus::Cancelled
    );
}

#[test]
fn dependency_guard_lower_level_crates_do_not_reference_fdc_api() {
    let root = workspace_root();
    let forbidden = collect_forbidden_references(&root);

    assert!(
        forbidden.is_empty(),
        "lower-level crates must not reference fdc-api: {forbidden:?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("fdc-api should live under crates/fdc-api")
        .to_path_buf()
}

fn collect_forbidden_references(root: &PathBuf) -> Vec<String> {
    let lower_level_crates = [
        "crates/fdc-barter",
        "crates/fdc-ingestion",
        "crates/fdc-transform",
        "crates/fdc-orchestrator",
        "crates/fdc-storage",
        "crates/fdc-server",
    ];

    lower_level_crates
        .iter()
        .flat_map(|crate_path| {
            let crate_root = root.join(crate_path);
            let cargo_toml = crate_root.join("Cargo.toml");
            let src_dir = crate_root.join("src");
            let mut hits = Vec::new();

            if cargo_toml.exists() {
                let content = fs::read_to_string(&cargo_toml).expect("Cargo.toml should read");
                if content.contains("fdc-api") || content.contains("fdc_api") {
                    hits.push(cargo_toml.display().to_string());
                }
            }

            if src_dir.exists() {
                collect_source_hits(&src_dir, &mut hits);
            }

            hits
        })
        .collect()
}

fn collect_source_hits(dir: &PathBuf, hits: &mut Vec<String>) {
    for entry in fs::read_dir(dir).expect("source dir should read") {
        let entry = entry.expect("source entry should read");
        let path = entry.path();
        if path.is_dir() {
            collect_source_hits(&path, hits);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            let content = fs::read_to_string(&path).expect("source file should read");
            if content.contains("fdc_api") || content.contains("fdc-api") {
                hits.push(path.display().to_string());
            }
        }
    }
}
