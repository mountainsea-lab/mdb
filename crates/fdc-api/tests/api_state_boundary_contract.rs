use std::path::{Path, PathBuf};

use fdc_api::{readiness_response_from_state, ApiAppState, ApiReadinessStatus};
use fdc_server::{FdcServerApp, ServerEnvironment, ServerLifecycleState};

#[test]
fn default_uninitialized_server_projects_not_ready() {
    let state = ApiAppState::new(FdcServerApp::with_defaults());
    let projection = state.readiness_projection();

    assert_eq!(projection.status, ApiReadinessStatus::NotReady);
    assert_eq!(projection.service_name, "fdc-server");
    assert_eq!(projection.environment, "development");
    assert_eq!(projection.server_lifecycle_state, "created");
    assert!(projection.components_ready);
    assert!(!projection.api_enabled);
    assert!(projection.market_data_orchestrator_enabled);
    assert_eq!(state.server_app().state(), ServerLifecycleState::Created);
}

#[test]
fn initialized_server_projects_ready() {
    let mut app = FdcServerApp::with_defaults();
    app.initialize()
        .expect("default server app should initialize");
    let state = ApiAppState::new(app);
    let projection = state.readiness_projection();

    assert_eq!(projection.status, ApiReadinessStatus::Ready);
    assert_eq!(projection.environment, "development");
    assert_eq!(projection.server_lifecycle_state, "initialized");
    assert!(projection.components_ready);
}

#[test]
fn readiness_response_wraps_projection_in_successful_api_response() {
    let mut app = FdcServerApp::with_defaults();
    app.initialize()
        .expect("default server app should initialize");
    let state = ApiAppState::new(app);

    let response = readiness_response_from_state(&state);

    assert_eq!(response.status, "success");
    assert!(response.message.is_none());
    assert_eq!(response.data.status, ApiReadinessStatus::Ready);
    assert_eq!(response.data.service_name, "fdc-server");
}

#[test]
fn server_environment_and_lifecycle_labels_are_stable_api_strings() {
    assert_eq!(
        fdc_api::server_environment_label(ServerEnvironment::Development),
        "development"
    );
    assert_eq!(
        fdc_api::server_environment_label(ServerEnvironment::Test),
        "test"
    );
    assert_eq!(
        fdc_api::server_environment_label(ServerEnvironment::Production),
        "production"
    );
    assert_eq!(
        fdc_api::server_lifecycle_state_label(ServerLifecycleState::Created),
        "created"
    );
    assert_eq!(
        fdc_api::server_lifecycle_state_label(ServerLifecycleState::Initialized),
        "initialized"
    );
    assert_eq!(
        fdc_api::server_lifecycle_state_label(ServerLifecycleState::Stopped),
        "stopped"
    );
}

#[test]
fn dependency_guard_lower_level_crates_do_not_reference_fdc_api() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-server/Cargo.toml"),
        workspace_root.join("crates/fdc-server/src"),
        workspace_root.join("crates/fdc-orchestrator/Cargo.toml"),
        workspace_root.join("crates/fdc-orchestrator/src"),
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
        workspace_root.join("crates/fdc-transform/Cargo.toml"),
        workspace_root.join("crates/fdc-transform/src"),
        workspace_root.join("crates/fdc-ingestion/Cargo.toml"),
        workspace_root.join("crates/fdc-ingestion/src"),
        workspace_root.join("crates/fdc-adapter/barter/Cargo.toml"),
        workspace_root.join("crates/fdc-adapter/barter/src"),
    ];

    let mut violations = Vec::new();
    for path in checked_paths {
        collect_forbidden_references(&path, &["fdc-api", "fdc_api"], &mut violations);
    }

    assert!(
        violations.is_empty(),
        "lower-level/application assembly crates must not depend on fdc-api: {violations:#?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fdc-api should live two levels under workspace root")
        .to_path_buf()
}

fn collect_forbidden_references(path: &Path, forbidden: &[&str], violations: &mut Vec<String>) {
    if path.is_dir() {
        for entry in std::fs::read_dir(path).expect("failed to read dependency guard directory") {
            let entry = entry.expect("failed to read dependency guard entry");
            collect_forbidden_references(&entry.path(), forbidden, violations);
        }
        return;
    }

    if path.extension().and_then(|extension| extension.to_str()) != Some("rs")
        && path.file_name().and_then(|file_name| file_name.to_str()) != Some("Cargo.toml")
    {
        return;
    }

    let content = std::fs::read_to_string(path).expect("failed to read dependency guard file");
    for needle in forbidden {
        if content.contains(needle) {
            violations.push(format!("{} contains {needle}", path.display()));
        }
    }
}
