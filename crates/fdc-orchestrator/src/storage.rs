use std::collections::BTreeMap;

use fdc_core::{error::Error, Result};
use fdc_storage::{
    StorageAccessPatternHint, StorageDurabilityHint, StoragePlacementHint, StorageWriteMetadata,
    StorageWriteRecord,
};
use fdc_transform::{MarketDataDto, MarketDataKind};

pub fn market_data_dto_to_storage_record(dto: &MarketDataDto) -> Result<StorageWriteRecord> {
    let value = serde_json::to_vec(dto).map_err(Error::from)?;
    let collection = collection_for_kind(dto.kind);
    let schema = format!("market_data.{}", schema_kind_for_kind(dto.kind));
    let key = storage_key(dto);
    let shard_key = format!("{}:{}", dto.source_id, dto.symbol.as_str()).into_bytes();

    let tags = storage_tags_for_dto(dto);

    Ok(
        StorageWriteRecord::new("market_data", collection, key.into_bytes(), value)
            .with_metadata(StorageWriteMetadata {
                content_type: Some("application/json".to_string()),
                schema: Some(schema),
                schema_version: Some("1".to_string()),
                source: Some(dto.source_id.clone()),
                tags,
            })
            .with_placement(StoragePlacementHint {
                target_tier: None,
                access_pattern: access_pattern_for_kind(dto.kind),
                durability: durability_for_kind(dto.kind),
                shard_key: Some(shard_key),
                ttl: None,
            }),
    )
}

fn storage_key(dto: &MarketDataDto) -> String {
    format!(
        "{}:{}:{}:{}",
        dto.source_id,
        dto.symbol.as_str(),
        schema_kind_for_kind(dto.kind),
        dto.event_time.as_nanos()
    )
}

fn storage_tags_for_dto(dto: &MarketDataDto) -> BTreeMap<String, String> {
    let schema_kind = schema_kind_for_kind(dto.kind);
    let (data_kind, record_kind) = data_kind_tags_for_kind(dto.kind);

    let mut tags = BTreeMap::new();
    tags.insert("adapter".to_string(), dto.adapter.clone());
    tags.insert("exchange".to_string(), dto.exchange.clone());
    tags.insert("symbol".to_string(), dto.symbol.as_str().to_string());
    tags.insert("kind".to_string(), schema_kind.to_string());
    tags.insert("data.kind".to_string(), data_kind.to_string());
    tags.insert("record.kind".to_string(), record_kind.to_string());

    let mode = if dto.quality.is_backfill {
        "backfill"
    } else {
        "live"
    };
    tags.insert("mode".to_string(), mode.to_string());

    if dto.quality.is_replay {
        tags.insert("quality.is_replay".to_string(), "true".to_string());
    }
    if dto.quality.is_duplicate_candidate {
        tags.insert(
            "quality.is_duplicate_candidate".to_string(),
            "true".to_string(),
        );
    }
    if dto.quality.has_gap_before {
        tags.insert("quality.has_gap_before".to_string(), "true".to_string());
    }
    if dto.quality.is_out_of_order {
        tags.insert("quality.is_out_of_order".to_string(), "true".to_string());
    }

    tags
}

fn data_kind_tags_for_kind(kind: MarketDataKind) -> (&'static str, &'static str) {
    match kind {
        MarketDataKind::Trade => ("event", "trade"),
        MarketDataKind::OrderBookL1 => ("state", "order_book_l1"),
        MarketDataKind::OrderBook => ("state", "order_book"),
        MarketDataKind::Candle => ("aggregate", "candle"),
        MarketDataKind::Liquidation => ("event", "liquidation"),
        MarketDataKind::Raw => ("raw", "raw"),
    }
}

fn collection_for_kind(kind: MarketDataKind) -> &'static str {
    match kind {
        MarketDataKind::Trade => "trades",
        MarketDataKind::OrderBookL1 => "order_book_l1",
        MarketDataKind::OrderBook => "order_book",
        MarketDataKind::Candle => "candles",
        MarketDataKind::Liquidation => "liquidations",
        MarketDataKind::Raw => "raw",
    }
}

fn schema_kind_for_kind(kind: MarketDataKind) -> &'static str {
    match kind {
        MarketDataKind::Trade => "trade",
        MarketDataKind::OrderBookL1 => "order_book_l1",
        MarketDataKind::OrderBook => "order_book",
        MarketDataKind::Candle => "candle",
        MarketDataKind::Liquidation => "liquidation",
        MarketDataKind::Raw => "raw",
    }
}

fn access_pattern_for_kind(kind: MarketDataKind) -> StorageAccessPatternHint {
    match kind {
        MarketDataKind::Trade | MarketDataKind::OrderBookL1 | MarketDataKind::OrderBook => {
            StorageAccessPatternHint::Hot
        }
        MarketDataKind::Candle => StorageAccessPatternHint::Warm,
        MarketDataKind::Liquidation | MarketDataKind::Raw => StorageAccessPatternHint::Unspecified,
    }
}

fn durability_for_kind(kind: MarketDataKind) -> StorageDurabilityHint {
    match kind {
        MarketDataKind::Raw => StorageDurabilityHint::Unspecified,
        _ => StorageDurabilityHint::Persistent,
    }
}
