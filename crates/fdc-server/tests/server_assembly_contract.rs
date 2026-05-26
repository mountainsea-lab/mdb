use std::path::{Path, PathBuf};
use std::sync::Arc;

use fdc_orchestrator::pipeline::OrchestratorPipelineResult;
use fdc_server::{
    FdcServerApp, FdcServerConfig, ServerComponents, ServerEnvironment, ServerLifecycleState,
};
use fdc_storage::{RecordingStorageSink, StorageWriteSink};

#[test]
fn default_app_uses_development_config_without_api_startup() {
    let app = FdcServerApp::with_defaults();

    assert_eq!(app.config().service_name, "fdc-server");
    assert_eq!(app.config().environment, ServerEnvironment::Development);
    assert!(!app.config().enable_api);
    assert!(app.config().enable_market_data_orchestrator);
    assert_eq!(app.state(), ServerLifecycleState::Created);
    assert!(
        !app.is_ready(),
        "created app should not be ready until initialized"
    );
}

#[test]
fn default_components_are_ready_and_database_free() {
    let components = ServerComponents::with_recording_storage_sink();

    assert!(components.is_ready());
    assert!(components.uses_recording_storage_sink());
}

#[test]
fn app_initialization_marks_ready_without_starting_network_services() {
    let mut app = FdcServerApp::with_defaults();

    app.initialize().expect("default app should initialize");

    assert_eq!(app.state(), ServerLifecycleState::Initialized);
    assert!(app.is_ready());
}

#[test]
fn app_stop_transitions_to_stopped() {
    let mut app = FdcServerApp::with_defaults();
    app.initialize().expect("default app should initialize");

    app.stop().expect("initialized app should stop");

    assert_eq!(app.state(), ServerLifecycleState::Stopped);
    assert!(!app.is_ready());
}

#[test]
fn custom_storage_sink_can_be_injected() {
    let sink = Arc::new(RecordingStorageSink::new());
    let dyn_sink: Arc<dyn StorageWriteSink> = sink.clone();
    let components = ServerComponents::new(dyn_sink);
    let config = FdcServerConfig::for_tests();

    let app = FdcServerApp::new(config, components);

    assert!(app.components().is_ready());
    assert!(!app.components().uses_recording_storage_sink());
    assert_eq!(sink.recorded_record_count(), 0);
}

#[test]
fn server_can_reference_orchestrator_public_types() {
    let result = OrchestratorPipelineResult::default();

    assert_eq!(result.envelopes_received, 0);
    assert_eq!(result.storage_records_written, 0);
}

#[test]
fn dependency_guard_lower_level_crates_do_not_reference_fdc_server() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-core/Cargo.toml"),
        workspace_root.join("crates/fdc-core/src"),
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
        workspace_root.join("crates/fdc-orchestrator/Cargo.toml"),
        workspace_root.join("crates/fdc-orchestrator/src"),
        workspace_root.join("crates/fdc-api/Cargo.toml"),
        workspace_root.join("crates/fdc-api/src"),
    ];

    let mut violations = Vec::new();
    for path in checked_paths {
        collect_forbidden_references(&path, &["fdc-server", "fdc_server"], &mut violations);
    }

    assert!(
        violations.is_empty(),
        "lower-level or protocol crates must not depend on fdc-server: {violations:#?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fdc-server should live two levels under workspace root")
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
