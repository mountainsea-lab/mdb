#[test]
fn fdc_storage_does_not_depend_on_upper_layer_crates() {
    let storage_manifest = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    )
    .expect("fdc-storage Cargo.toml should exist");

    for forbidden in [
        "fdc-barter",
        "fdc-ingestion",
        "fdc-transform",
        "fdc-api",
        "fdc-server",
        "fdc-orchestrator",
    ] {
        assert!(
            !storage_manifest.contains(forbidden),
            "fdc-storage must not depend on upper-layer crate {forbidden}"
        );
    }
}
