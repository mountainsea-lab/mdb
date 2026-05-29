use std::{fs, path::PathBuf};

use fdc_api::{
    default_demo_flow_request, run_demo_flow_once, DemoFixtureTrade, DemoFlowRequest,
};

#[test]
fn default_demo_flow_request_is_deterministic_btcusdt_fixture() {
    let request = default_demo_flow_request();

    assert_eq!(request.query_symbol, "BTCUSDT");
    assert_eq!(request.query_limit, 10);
    assert_eq!(request.trades.len(), 1);
    assert_eq!(request.trades[0].symbol, "BTCUSDT");
    assert_eq!(request.trades[0].trade_id, "btc-demo-1");
    assert_eq!(request.trades[0].sequence.as_deref(), Some("seq-demo-1"));
}

#[tokio::test]
async fn demo_flow_default_request_returns_ready_completed_and_queryable_trade() {
    let summary = run_demo_flow_once(default_demo_flow_request())
        .await
        .expect("default demo flow should run");

    assert_eq!(summary.readiness.status, fdc_api::ApiReadinessStatus::Ready);
    assert_eq!(summary.readiness.server_lifecycle_state, "initialized");
    assert_eq!(
        summary.start_status.state,
        fdc_api::ApiRunnerLifecycleStatus::Completed
    );
    assert_eq!(
        summary.final_status.state,
        fdc_api::ApiRunnerLifecycleStatus::Completed
    );
    assert_eq!(
        summary
            .final_status
            .last_result
            .as_ref()
            .expect("final status should include last result")
            .storage_records_written,
        1
    );
    assert_eq!(summary.market_data.returned_records, 1);
    assert_eq!(summary.market_data.records[0].symbol.as_deref(), Some("BTCUSDT"));
}

#[tokio::test]
async fn demo_flow_can_query_one_symbol_from_multiple_fixture_trades() {
    let request = DemoFlowRequest {
        trades: vec![
            DemoFixtureTrade {
                symbol: "BTCUSDT".to_string(),
                trade_id: "btc-demo-1".to_string(),
                sequence: Some("seq-demo-1".to_string()),
            },
            DemoFixtureTrade {
                symbol: "ETHUSDT".to_string(),
                trade_id: "eth-demo-1".to_string(),
                sequence: Some("seq-demo-2".to_string()),
            },
        ],
        query_symbol: "ETHUSDT".to_string(),
        query_limit: 10,
    };

    let summary = run_demo_flow_once(request)
        .await
        .expect("multi-trade demo flow should run");

    assert_eq!(
        summary
            .final_status
            .last_result
            .as_ref()
            .expect("final status should include last result")
            .storage_records_written,
        2
    );
    assert_eq!(summary.market_data.returned_records, 1);
    assert_eq!(summary.market_data.records[0].symbol.as_deref(), Some("ETHUSDT"));
}

#[tokio::test]
async fn demo_flow_rejects_empty_fixture_trade_request() {
    let request = DemoFlowRequest {
        trades: Vec::new(),
        query_symbol: "BTCUSDT".to_string(),
        query_limit: 10,
    };

    let error = run_demo_flow_once(request)
        .await
        .expect_err("empty demo flow request should fail");

    assert!(error.to_string().contains("at least one fixture trade"));
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
