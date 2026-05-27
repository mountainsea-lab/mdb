use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use fdc_storage::{
    MarketDataQuery, QueryableMarketDataStore, StorageWriteBatch, StorageWriteMetadata,
    StorageWriteRecord, StorageWriteSink,
};

fn market_data_record(symbol: &str, kind: &str, key: &[u8], value: &[u8]) -> StorageWriteRecord {
    let mut tags = BTreeMap::new();
    tags.insert("symbol".to_string(), symbol.to_string());
    tags.insert("kind".to_string(), kind.to_string());
    tags.insert("adapter".to_string(), "barter".to_string());
    tags.insert("exchange".to_string(), "binance_spot".to_string());

    StorageWriteRecord::new("market_data", "trades", key.to_vec(), value.to_vec()).with_metadata(
        StorageWriteMetadata {
            content_type: Some("application/json".to_string()),
            schema: Some(format!("market_data.{kind}")),
            schema_version: Some("1".to_string()),
            source: Some("barter:binance_spot".to_string()),
            tags,
        },
    )
}

#[tokio::test]
async fn queryable_store_returns_records_by_symbol() {
    let store = QueryableMarketDataStore::new();
    let record = market_data_record("BTCUSDT", "trade", b"btc-1", br#"{"symbol":"BTCUSDT"}"#);

    store
        .write_batch(StorageWriteBatch::new(vec![record.clone()]))
        .await
        .expect("valid market data record should write");

    let records = store.query(&MarketDataQuery::for_trades().with_symbol("BTCUSDT"));

    assert_eq!(records, vec![record]);
    assert_eq!(store.record_count(), 1);
}

#[tokio::test]
async fn queryable_store_filters_symbols_independently() {
    let store = QueryableMarketDataStore::new();
    let btc = market_data_record("BTCUSDT", "trade", b"btc-1", br#"{"symbol":"BTCUSDT"}"#);
    let eth = market_data_record("ETHUSDT", "trade", b"eth-1", br#"{"symbol":"ETHUSDT"}"#);

    store
        .write_batch(StorageWriteBatch::new(vec![btc, eth.clone()]))
        .await
        .expect("valid market data records should write");

    let records = store.query(&MarketDataQuery::for_trades().with_symbol("ETHUSDT"));

    assert_eq!(records, vec![eth]);
}

#[tokio::test]
async fn queryable_store_applies_trade_collection_and_limit() {
    let store = QueryableMarketDataStore::new();
    let first = market_data_record("BTCUSDT", "trade", b"btc-1", br#"{"trade_id":"1"}"#);
    let second = market_data_record("BTCUSDT", "trade", b"btc-2", br#"{"trade_id":"2"}"#);
    let book = StorageWriteRecord::new(
        "market_data",
        "order_book_l1",
        b"book-1".to_vec(),
        br#"{"symbol":"BTCUSDT"}"#.to_vec(),
    );

    store
        .write_batch(StorageWriteBatch::new(vec![first.clone(), second, book]))
        .await
        .expect("valid mixed market data records should write");

    let records = store.query(
        &MarketDataQuery::for_trades()
            .with_symbol("BTCUSDT")
            .with_limit(1),
    );

    assert_eq!(records, vec![first]);
}

#[tokio::test]
async fn queryable_store_rejects_invalid_batch_atomically() {
    let store = QueryableMarketDataStore::new();
    let seed = market_data_record("BTCUSDT", "trade", b"btc-1", br#"{"symbol":"BTCUSDT"}"#);
    store
        .write_batch(StorageWriteBatch::new(vec![seed.clone()]))
        .await
        .expect("seed record should write");

    let invalid = StorageWriteRecord::new("market_data", "trades", Vec::new(), b"value".to_vec());
    let error = store
        .write_batch(StorageWriteBatch::new(vec![invalid]))
        .await
        .expect_err("invalid record should be rejected");

    assert!(error
        .to_string()
        .contains("storage write record key must not be empty"));
    assert_eq!(store.all_records(), vec![seed]);
}

#[test]
fn dependency_guard_queryable_storage_stays_decoupled_from_upstream_crates() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
    ];

    let mut violations = Vec::new();
    for path in checked_paths {
        collect_forbidden_references(
            &path,
            &[
                "fdc-transform",
                "fdc_transform",
                "fdc-ingestion",
                "fdc_ingestion",
                "fdc-barter",
                "fdc_barter",
                "fdc-orchestrator",
                "fdc_orchestrator",
                "fdc-api",
                "fdc_api",
            ],
            &mut violations,
        );
    }

    assert!(
        violations.is_empty(),
        "queryable storage boundary must stay decoupled from upstream crates: {violations:#?}"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("fdc-storage should live two levels under workspace root")
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
