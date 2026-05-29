use std::{fs, path::PathBuf};

#[test]
fn first_mvp_acceptance_report_freezes_scope_and_verification() {
    let report = read_report();

    for required in [
        "First internal MVP: accepted",
        "docs/mvp/first-mvp-demo.md",
        "run_demo_flow_once",
        "build_demo_router",
        "CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_documentation_contract",
        "CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract",
        "CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_router_contract",
        "no-listener",
        "in-memory",
        "no persistence",
        "SQL integration is out of scope",
        "live network acquisition is not part of default verification",
        "gated local HTTP demo entrypoint",
    ] {
        assert!(
            report.contains(required),
            "MVP acceptance report must mention `{required}`"
        );
    }
}

#[test]
fn first_mvp_acceptance_report_has_no_placeholders() {
    let report = read_report();

    for forbidden in ["TODO", "TBD", "FIXME"] {
        assert!(
            !report.contains(forbidden),
            "MVP acceptance report must not contain placeholder `{forbidden}`"
        );
    }
}

fn read_report() -> String {
    fs::read_to_string(workspace_root().join("docs/mvp/first-mvp-acceptance-report.md"))
        .expect("docs/mvp/first-mvp-acceptance-report.md should exist and be readable")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("fdc-api should live under crates/fdc-api")
        .to_path_buf()
}
