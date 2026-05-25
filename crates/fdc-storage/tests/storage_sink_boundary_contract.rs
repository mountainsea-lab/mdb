use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Duration;
use fdc_storage::{
    RecordingStorageSink, StorageAccessPatternHint, StorageDurabilityHint, StoragePlacementHint,
    StorageTier, StorageWriteBatch, StorageWriteMetadata, StorageWriteRecord, StorageWriteSink,
};

fn sample_metadata() -> StorageWriteMetadata {
    let mut tags = BTreeMap::new();
    tags.insert("asset_class".to_string(), "crypto".to_string());
    tags.insert("granularity".to_string(), "trade".to_string());

    StorageWriteMetadata {
        content_type: Some("application/json".to_string()),
        schema: Some("generic.market_data.trade".to_string()),
        schema_version: Some("1".to_string()),
        source: Some("integration-test".to_string()),
        tags,
    }
}

fn tiered_placement() -> StoragePlacementHint {
    StoragePlacementHint {
        target_tier: Some(StorageTier::L2),
        access_pattern: StorageAccessPatternHint::Hot,
        durability: StorageDurabilityHint::Persistent,
        shard_key: Some(b"binance:btc-usdt".to_vec()),
        ttl: Some(Duration::hours(6)),
    }
}

fn sample_record(key: &[u8], value: &[u8]) -> StorageWriteRecord {
    StorageWriteRecord::new("market_data", "trades", key.to_vec(), value.to_vec())
        .with_metadata(sample_metadata())
        .with_placement(tiered_placement())
}

#[tokio::test]
async fn recording_sink_accepts_valid_tier_aware_batch() {
    let sink = RecordingStorageSink::default();
    let batch = StorageWriteBatch::new(vec![sample_record(
        b"trade:binance:btc-usdt:1",
        br#"{"price":"100.00"}"#,
    )]);
    let batch_id = batch.batch_id;

    let outcome = sink
        .write_batch(batch)
        .await
        .expect("valid batch should write");

    assert_eq!(outcome.batch_id, batch_id);
    assert_eq!(outcome.accepted_records, 1);
    assert_eq!(outcome.rejected_records, 0);
    assert_eq!(sink.recorded_batch_count(), 1);
    assert_eq!(sink.recorded_record_count(), 1);
}

#[tokio::test]
async fn recording_sink_preserves_record_metadata_and_placement() {
    let sink = RecordingStorageSink::default();
    let record = sample_record(b"trade:binance:eth-usdt:1", br#"{"price":"200.00"}"#);
    let batch = StorageWriteBatch::new(vec![record.clone()]);

    sink.write_batch(batch)
        .await
        .expect("valid batch should write");

    let records = sink.recorded_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].namespace, "market_data");
    assert_eq!(records[0].collection, "trades");
    assert_eq!(records[0].key, b"trade:binance:eth-usdt:1".to_vec());
    assert_eq!(records[0].value, br#"{"price":"200.00"}"#.to_vec());
    assert_eq!(
        records[0].metadata.content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(
        records[0].metadata.schema.as_deref(),
        Some("generic.market_data.trade")
    );
    assert_eq!(records[0].metadata.schema_version.as_deref(), Some("1"));
    assert_eq!(
        records[0].metadata.source.as_deref(),
        Some("integration-test")
    );
    assert_eq!(
        records[0]
            .metadata
            .tags
            .get("asset_class")
            .map(String::as_str),
        Some("crypto")
    );
    assert_eq!(records[0].placement.target_tier, Some(StorageTier::L2));
    assert_eq!(
        records[0].placement.access_pattern,
        StorageAccessPatternHint::Hot
    );
    assert_eq!(
        records[0].placement.durability,
        StorageDurabilityHint::Persistent
    );
    assert_eq!(
        records[0].placement.shard_key.as_deref(),
        Some(&b"binance:btc-usdt"[..])
    );
    assert_eq!(records[0].placement.ttl, Some(Duration::hours(6)));
}

#[tokio::test]
async fn empty_batch_is_rejected_without_mutating_sink_state() {
    let sink = RecordingStorageSink::default();
    let valid_batch = StorageWriteBatch::new(vec![sample_record(b"valid", b"value")]);
    sink.write_batch(valid_batch)
        .await
        .expect("seed batch should write");

    let empty_batch = StorageWriteBatch::new(Vec::new());
    let error = sink
        .write_batch(empty_batch)
        .await
        .expect_err("empty batch should be rejected");

    assert!(error
        .to_string()
        .contains("storage write batch must not be empty"));
    assert_eq!(sink.recorded_batch_count(), 1);
    assert_eq!(sink.recorded_record_count(), 1);
}

#[tokio::test]
async fn invalid_record_is_rejected_atomically() {
    let sink = RecordingStorageSink::default();
    let valid_seed = StorageWriteBatch::new(vec![sample_record(b"seed", b"seed-value")]);
    sink.write_batch(valid_seed)
        .await
        .expect("seed batch should write");

    let invalid_record =
        StorageWriteRecord::new("market_data", "trades", Vec::new(), b"value".to_vec());
    let mixed_batch =
        StorageWriteBatch::new(vec![sample_record(b"valid-2", b"value-2"), invalid_record]);

    let error = sink
        .write_batch(mixed_batch)
        .await
        .expect_err("batch with empty record key should be rejected");

    assert!(error
        .to_string()
        .contains("storage write record key must not be empty"));
    assert_eq!(sink.recorded_batch_count(), 1);
    assert_eq!(sink.recorded_record_count(), 1);
}

#[tokio::test]
async fn sink_trait_is_usable_behind_arc_dyn_object() {
    let sink: Arc<dyn StorageWriteSink> = Arc::new(RecordingStorageSink::default());
    let batch = StorageWriteBatch::new(vec![sample_record(b"dyn-key", b"dyn-value")]);

    let outcome = sink
        .write_batch(batch)
        .await
        .expect("dyn sink should write");

    assert_eq!(outcome.accepted_records, 1);
    assert_eq!(outcome.rejected_records, 0);
}

#[test]
fn default_placement_is_unspecified_and_engine_free() {
    let placement = StoragePlacementHint::default();

    assert_eq!(placement.target_tier, None);
    assert_eq!(
        placement.access_pattern,
        StorageAccessPatternHint::Unspecified
    );
    assert_eq!(placement.durability, StorageDurabilityHint::Unspecified);
    assert_eq!(placement.shard_key, None);
    assert_eq!(placement.ttl, None);
}

#[test]
fn placement_hints_can_reference_all_existing_storage_tiers() {
    let placements = [
        StoragePlacementHint::for_tier(StorageTier::L1),
        StoragePlacementHint::for_tier(StorageTier::L2),
        StoragePlacementHint::for_tier(StorageTier::L3),
        StoragePlacementHint::for_tier(StorageTier::L4),
    ];

    assert_eq!(placements[0].target_tier, Some(StorageTier::L1));
    assert_eq!(placements[1].target_tier, Some(StorageTier::L2));
    assert_eq!(placements[2].target_tier, Some(StorageTier::L3));
    assert_eq!(placements[3].target_tier, Some(StorageTier::L4));
}

#[test]
fn dependency_guard_storage_boundary_stays_decoupled_from_upstream_crates() {
    let workspace_root = workspace_root();
    let checked_paths = [
        workspace_root.join("crates/fdc-storage/Cargo.toml"),
        workspace_root.join("crates/fdc-storage/src"),
    ];

    let forbidden = [
        "fdc-transform",
        "fdc_transform",
        "fdc-ingestion",
        "fdc_ingestion",
        "fdc-barter",
        "fdc_barter",
    ];
    let mut violations = Vec::new();

    for path in checked_paths {
        collect_forbidden_references(&path, &forbidden, &mut violations);
    }

    assert!(
        violations.is_empty(),
        "fdc-storage must not depend on upstream pipeline or adapter crates; violations: {violations:#?}"
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
        for entry in std::fs::read_dir(path).expect("failed to read directory for dependency guard")
        {
            let entry = entry.expect("failed to read directory entry");
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
