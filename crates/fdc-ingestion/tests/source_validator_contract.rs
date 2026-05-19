use fdc_core::types::TimestampNs;
use fdc_ingestion::{
    SourceEnvelope, SourceQualityFlags, SourceType, SourceValidationErrorType, SourceValidator,
};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct DummyMarketEvent {
    symbol: String,
}

fn valid_envelope() -> SourceEnvelope<DummyMarketEvent> {
    SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
        },
    )
}

#[tokio::test]
async fn validator_accepts_valid_source_envelope() {
    let validator = SourceValidator::default();
    let result = validator.validate(&valid_envelope()).await;

    assert!(result.is_valid);
    assert!(result.errors.is_empty());
    assert!(result.warnings.is_empty());
}

#[tokio::test]
async fn validator_rejects_empty_source_id() {
    let validator = SourceValidator::default();
    let envelope = SourceEnvelope::new(
        "   ",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
        },
    );

    let result = validator.validate(&envelope).await;

    assert!(!result.is_valid);
    assert_eq!(
        result.errors[0].error_type,
        SourceValidationErrorType::EmptySourceId
    );
}

#[tokio::test]
async fn validator_rejects_invalid_event_time() {
    let validator = SourceValidator::default();
    let envelope = SourceEnvelope::new(
        "dummy-source",
        SourceType::MarketData,
        TimestampNs::from_nanos(0),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
        },
    );

    let result = validator.validate(&envelope).await;

    assert!(!result.is_valid);
    assert_eq!(
        result.errors[0].error_type,
        SourceValidationErrorType::InvalidEventTime
    );
}

#[tokio::test]
async fn validator_warns_for_quality_flags() {
    let validator = SourceValidator::default();
    let mut quality = SourceQualityFlags::default();
    quality.is_duplicate_candidate = true;
    quality.has_gap_before = true;
    quality.is_out_of_order = true;

    let envelope = valid_envelope().with_quality(quality);
    let result = validator.validate(&envelope).await;

    assert!(result.is_valid);
    assert_eq!(result.warnings.len(), 3);
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning.warning_type == "duplicate_candidate"));
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning.warning_type == "gap_before"));
    assert!(result
        .warnings
        .iter()
        .any(|warning| warning.warning_type == "out_of_order"));
}

#[tokio::test]
async fn validator_tracks_stats() {
    let validator = SourceValidator::default();

    let valid = valid_envelope();
    let invalid = SourceEnvelope::new(
        "",
        SourceType::MarketData,
        TimestampNs::from_nanos(1_000),
        TimestampNs::from_nanos(1_100),
        DummyMarketEvent {
            symbol: "BTCUSDT".to_string(),
        },
    );

    validator.validate(&valid).await;
    validator.validate(&invalid).await;
    let stats = validator.get_stats().await;

    assert_eq!(stats.messages_validated, 2);
    assert_eq!(stats.validation_successes, 1);
    assert_eq!(stats.validation_failures, 1);
}
