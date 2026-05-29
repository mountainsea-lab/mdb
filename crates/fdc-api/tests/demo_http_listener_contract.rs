use fdc_api::{build_demo_router, initialized_demo_app_state_with_control_runner};

#[test]
fn demo_http_listener_state_is_ready_and_has_runner_control() {
    let state = initialized_demo_app_state_with_control_runner()
        .expect("demo listener state should initialize");

    let readiness = state.readiness_projection();
    assert_eq!(readiness.status, fdc_api::ApiReadinessStatus::Ready);
    assert_eq!(readiness.server_lifecycle_state, "initialized");
    assert!(state.market_data_runner_control().is_some());

    let _router = build_demo_router(state);
}
