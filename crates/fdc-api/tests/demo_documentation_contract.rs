use std::{fs, path::PathBuf};

#[test]
fn first_mvp_demo_guide_mentions_real_demo_flow_api_and_routes() {
    let guide = read_guide();

    for required in [
        "run_demo_flow_once",
        "default_demo_flow_request",
        "DemoFlowSummary",
        "CARGO_NET_OFFLINE=true rtk cargo test -p fdc-api --test demo_flow_contract",
        "GET /ready",
        "POST /runner/start-fixture",
        "GET /runner/status",
        "GET /market-data/trades",
        "no-listener",
        "in-memory",
        "no persistence",
        "SQL integration is out of scope",
        "live network acquisition is not part of the default MVP",
    ] {
        assert!(
            guide.contains(required),
            "MVP demo guide must mention `{required}`"
        );
    }
}

#[test]
fn first_mvp_demo_guide_has_no_placeholders() {
    let guide = read_guide();

    for forbidden in ["TODO", "TBD", "FIXME"] {
        assert!(
            !guide.contains(forbidden),
            "MVP demo guide must not contain placeholder `{forbidden}`"
        );
    }
}

fn read_guide() -> String {
    fs::read_to_string(workspace_root().join("docs/mvp/first-mvp-demo.md"))
        .expect("docs/mvp/first-mvp-demo.md should exist and be readable")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("fdc-api should live under crates/fdc-api")
        .to_path_buf()
}
