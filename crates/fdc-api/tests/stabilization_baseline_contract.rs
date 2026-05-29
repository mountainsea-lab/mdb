use std::{fs, path::PathBuf};

#[test]
fn post_mvp_stabilization_baseline_records_current_technical_debt() {
    let report = read_report();

    for required in [
        "post-MVP stabilization baseline",
        "CARGO_NET_OFFLINE=true rtk cargo check -p fdc-api",
        "28 warnings",
        "fdc-wasm",
        "fdc-types",
        "fdc-storage",
        "fdc-query",
        "fdc-ingestion",
        "Cargo.lock",
        ".gitignore",
        "dependency boundary",
        "B20b warning cleanup by crate",
        "B20c Cargo.lock policy decision",
    ] {
        assert!(
            report.contains(required),
            "stabilization baseline must mention `{required}`"
        );
    }
}

#[test]
fn post_mvp_stabilization_baseline_has_no_placeholders() {
    let report = read_report();

    for forbidden in ["TODO", "TBD", "FIXME"] {
        assert!(
            !report.contains(forbidden),
            "stabilization baseline must not contain placeholder `{forbidden}`"
        );
    }
}

fn read_report() -> String {
    fs::read_to_string(workspace_root().join("docs/mvp/post-mvp-stabilization-baseline.md"))
        .expect("docs/mvp/post-mvp-stabilization-baseline.md should exist and be readable")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("fdc-api should live under crates/fdc-api")
        .to_path_buf()
}
