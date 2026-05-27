use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use fdc_api::{
    build_market_data_router, query_market_data_trades, ApiAppState, MarketDataTradeQueryParams,
};
use fdc_server::FdcServerApp;
use fdc_storage::{
    QueryableMarketDataStore, StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord,
    StorageWriteSink,
};
use tower::ServiceExt;

fn trade_record(symbol: &str, key: &[u8], trade_id: &str) -> StorageWriteRecord {
    let mut tags = BTreeMap::new();
    tags.insert("symbol".to_string(), symbol.to_string());
    tags.insert("kind".to_string(), "trade".to_string());
    tags.insert("adapter".to_string(), "barter".to_string());
    tags.insert("exchange".to_string(), "binance_spot".to_string());

    let payload = serde_json::json!({
        "event_id": format!("env-{trade_id}"),
        "symbol": symbol,
        "payload": { "Trade": { "trade_id": trade_id } }
    });

    StorageWriteRecord::new(
        "market_data",
        "trades",
        key.to_vec(),
        serde_json::to_vec(&payload).expect("payload should serialize"),
    )
    .with_metadata(StorageWriteMetadata {
        content_type: Some("application/json".to_string()),
        schema: Some("market_data.trade".to_string()),
        schema_version: Some("1".to_string()),
        source: Some("barter:binance_spot".to_string()),
        tags,
    })
}

async fn seeded_state() -> ApiAppState {
    let store = Arc::new(QueryableMarketDataStore::new());
    store
        .write_batch(StorageWriteBatch::new(vec![
            trade_record("BTCUSDT", b"btc-1", "btc-1"),
            trade_record("ETHUSDT", b"eth-1", "eth-1"),
            trade_record("BTCUSDT", b"btc-2", "btc-2"),
        ]))
        .await
        .expect("seed records should write");

    ApiAppState::new(FdcServerApp::with_defaults()).with_market_data_store(store)
}

#[tokio::test]
async fn pure_helper_filters_trades_by_symbol() {
    let state = seeded_state().await;

    let response = query_market_data_trades(
        &state,
        MarketDataTradeQueryParams {
            symbol: Some("BTCUSDT".to_string()),
            limit: Some(10),
        },
    );

    assert_eq!(response.status, "success");
    assert_eq!(response.data.returned_records, 2);
    assert!(response.data.records.iter().all(|record| {
        record.symbol.as_deref() == Some("BTCUSDT") && record.kind.as_deref() == Some("trade")
    }));
}

#[tokio::test]
async fn pure_helper_applies_limit() {
    let state = seeded_state().await;

    let response = query_market_data_trades(
        &state,
        MarketDataTradeQueryParams {
            symbol: Some("BTCUSDT".to_string()),
            limit: Some(1),
        },
    );

    assert_eq!(response.data.returned_records, 1);
    assert_eq!(response.data.records[0].key, "btc-1");
}

#[tokio::test]
async fn in_memory_router_returns_seeded_trade_json() {
    let state = seeded_state().await;
    let router = build_market_data_router(state);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/market-data/trades?symbol=BTCUSDT&limit=10")
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("response should be JSON");

    assert_eq!(json["status"], "success");
    assert_eq!(json["data"]["returned_records"], 2);
    assert_eq!(json["data"]["records"][0]["symbol"], "BTCUSDT");
    assert_eq!(json["data"]["records"][0]["payload"]["symbol"], "BTCUSDT");
}

#[test]
fn dependency_guard_lower_level_crates_do_not_reference_fdc_api() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-server/Cargo.toml"),
        workspace_root.join("crates/fdc-server/src"),
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
        workspace_root.join("crates/fdc-orchestrator/Cargo.toml"),
        workspace_root.join("crates/fdc-orchestrator/src"),
    ];

    let mut violations = Vec::new();
    for path in checked_paths {
        collect_forbidden_references(&path, &["fdc-api", "fdc_api"], &mut violations);
    }

    assert!(
        violations.is_empty(),
        "lower-level crates must not depend on fdc-api: {violations:#?}"
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
