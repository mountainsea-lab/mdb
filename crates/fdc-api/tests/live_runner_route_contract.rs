use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{
    build_demo_router, initialized_demo_app_state_with_control_runner, ApiResponse,
    LiveRunnerStartRequest, LiveRunnerStartResponse,
};
use tower::ServiceExt;

#[tokio::test]
async fn live_runner_start_requires_explicit_environment_gate() {
    std::env::remove_var("FDC_BARTER_LIVE_SMOKE");
    let router = build_demo_router(
        initialized_demo_app_state_with_control_runner().expect("demo state should initialize"),
    );

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/runner/start-live")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_string(&LiveRunnerStartRequest {
                        timeout_secs: Some(1),
                        max_envelopes: Some(1),
                    })
                    .expect("request should serialize"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: ApiResponse<LiveRunnerStartResponse> =
        serde_json::from_slice(&body).expect("response should decode");

    assert_eq!(payload.status, "error");
    assert_eq!(payload.data.started, false);
    assert_eq!(payload.data.storage_records_written, 0);
    assert!(payload
        .message
        .as_deref()
        .unwrap_or_default()
        .contains("FDC_BARTER_LIVE_SMOKE=1"));
}
