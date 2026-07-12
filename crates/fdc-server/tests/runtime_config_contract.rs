use fdc_server::{
    MarketDataStorageBackendConfig, MarketDataStoragePolicyProfileConfig,
    MarketDataStorageRuntimeConfig, ServerRuntimeConfig, ServerRuntimeEnvironment,
};
use std::path::Path;

#[test]
fn runtime_config_defaults_are_safe_for_local_production_server() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("defaults should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:18080");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Development);
    assert!(!config.live_enabled);
    assert!(!config.live_autostart);
    assert_eq!(config.live_default_timeout_secs, 30);
    assert_eq!(config.live_default_max_envelopes, 100);
    assert!(config.live_retry_enabled);
    assert_eq!(config.live_retry_initial_delay_ms, 1000);
    assert_eq!(config.live_retry_max_delay_ms, 30000);
    assert_eq!(config.live_max_consecutive_failures, 3);
    assert!(!config.market_data_live_resume_enabled);
    assert!(!config.market_data_storage_maintenance_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_reset_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_resume_enabled);
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_interval_seconds,
        3600
    );
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_timeout_ms,
        30000
    );
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_jitter_seconds,
        0
    );
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_max_consecutive_failures,
        3
    );
    assert_eq!(
        config.market_data_storage,
        MarketDataStorageRuntimeConfig {
            backend: MarketDataStorageBackendConfig::Memory,
            policy_profile: MarketDataStoragePolicyProfileConfig::Compatibility,
            tiers: Default::default(),
        }
    );
}

#[test]
fn storage_maintenance_hook_is_disabled_by_default() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0]).unwrap();

    assert!(!config.market_data_storage_maintenance_enabled);
}

#[test]
fn storage_maintenance_hook_can_be_enabled_by_env() {
    let config =
        ServerRuntimeConfig::from_env_pairs([("FDC_MARKET_DATA_STORAGE_MAINTENANCE_ENABLED", "1")])
            .unwrap();

    assert!(config.market_data_storage_maintenance_enabled);
}

#[test]
fn storage_maintenance_scheduler_config_accepts_valid_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_ENABLED", "1"),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
            "120",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
            "45000",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_JITTER_SECONDS",
            "30",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
            "5",
        ),
    ])
    .expect("scheduler config should parse");

    assert!(config.market_data_storage_maintenance_scheduler_enabled);
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_interval_seconds,
        120
    );
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_timeout_ms,
        45000
    );
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_jitter_seconds,
        30
    );
    assert_eq!(
        config.market_data_storage_maintenance_scheduler_max_consecutive_failures,
        5
    );
}

#[test]
fn storage_maintenance_scheduler_reset_gate_can_be_enabled_by_env() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESET_ENABLED",
        "1",
    )])
    .expect("scheduler reset config should parse");

    assert!(config.market_data_storage_maintenance_scheduler_reset_enabled);
}

#[test]
fn storage_maintenance_scheduler_resume_gate_can_be_enabled_by_env() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_RESUME_ENABLED",
        "1",
    )])
    .expect("scheduler resume config should parse");

    assert!(config.market_data_storage_maintenance_scheduler_resume_enabled);
}

#[test]
fn storage_maintenance_scheduler_config_rejects_invalid_values() {
    let interval_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS",
        "59",
    )])
    .expect_err("short interval should be rejected");
    assert!(interval_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_INTERVAL_SECONDS"));

    let timeout_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS",
        "999",
    )])
    .expect_err("short timeout should be rejected");
    assert!(timeout_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_TIMEOUT_MS"));

    let failures_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES",
        "0",
    )])
    .expect_err("zero max failures should be rejected");
    assert!(failures_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_MAINTENANCE_SCHEDULER_MAX_CONSECUTIVE_FAILURES"));
}

#[test]
fn runtime_config_accepts_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_SERVER_ADDR", "127.0.0.1:19090"),
        ("FDC_SERVER_ENV", "production"),
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_AUTOSTART", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "12"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "34"),
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE", "compatibility"),
    ])
    .expect("env overrides should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:19090");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Production);
    assert!(config.live_enabled);
    assert!(config.live_autostart);
    assert_eq!(config.live_default_timeout_secs, 12);
    assert_eq!(config.live_default_max_envelopes, 34);
    assert_eq!(
        config.market_data_storage.backend,
        MarketDataStorageBackendConfig::Tiered
    );
    assert_eq!(
        config.market_data_storage.policy_profile,
        MarketDataStoragePolicyProfileConfig::Compatibility
    );
}

#[test]
fn runtime_config_rejects_invalid_values() {
    let error = ServerRuntimeConfig::from_env_pairs([("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "0")])
        .expect_err("zero timeout should be rejected");

    assert!(error.to_string().contains("FDC_LIVE_DEFAULT_TIMEOUT_SECS"));
}

#[test]
fn live_retry_config_accepts_valid_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_RETRY_ENABLED", "0"),
        ("FDC_LIVE_RETRY_INITIAL_DELAY_MS", "250"),
        ("FDC_LIVE_RETRY_MAX_DELAY_MS", "5000"),
        ("FDC_LIVE_MAX_CONSECUTIVE_FAILURES", "5"),
        ("FDC_MARKET_DATA_LIVE_RESUME_ENABLED", "1"),
    ])
    .expect("live retry config should parse");

    assert!(!config.live_retry_enabled);
    assert_eq!(config.live_retry_initial_delay_ms, 250);
    assert_eq!(config.live_retry_max_delay_ms, 5000);
    assert_eq!(config.live_max_consecutive_failures, 5);
    assert!(config.market_data_live_resume_enabled);
}

#[test]
fn live_retry_config_rejects_invalid_values() {
    let initial_error =
        ServerRuntimeConfig::from_env_pairs([("FDC_LIVE_RETRY_INITIAL_DELAY_MS", "99")])
            .expect_err("short initial retry delay should be rejected");
    assert!(initial_error
        .to_string()
        .contains("FDC_LIVE_RETRY_INITIAL_DELAY_MS"));

    let max_error = ServerRuntimeConfig::from_env_pairs([("FDC_LIVE_RETRY_MAX_DELAY_MS", "99")])
        .expect_err("short max retry delay should be rejected");
    assert!(max_error
        .to_string()
        .contains("FDC_LIVE_RETRY_MAX_DELAY_MS"));

    let ordering_error = ServerRuntimeConfig::from_env_pairs([
        ("FDC_LIVE_RETRY_INITIAL_DELAY_MS", "5000"),
        ("FDC_LIVE_RETRY_MAX_DELAY_MS", "1000"),
    ])
    .expect_err("max retry delay below initial delay should be rejected");
    assert!(ordering_error
        .to_string()
        .contains("FDC_LIVE_RETRY_MAX_DELAY_MS must be >= FDC_LIVE_RETRY_INITIAL_DELAY_MS"));

    let failures_error =
        ServerRuntimeConfig::from_env_pairs([("FDC_LIVE_MAX_CONSECUTIVE_FAILURES", "0")])
            .expect_err("zero max failures should be rejected");
    assert!(failures_error
        .to_string()
        .contains("FDC_LIVE_MAX_CONSECUTIVE_FAILURES"));
}

#[test]
fn runtime_config_rejects_invalid_market_data_storage_env_values() {
    let backend_error =
        ServerRuntimeConfig::from_env_pairs([("FDC_MARKET_DATA_STORAGE_BACKEND", "l3")])
            .expect_err("invalid backend should be rejected");
    assert!(backend_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_BACKEND must be memory or tiered"));

    let profile_error = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_POLICY_PROFILE",
        "typed_market_data",
    )])
    .expect_err("unsupported profile should be rejected");
    assert!(profile_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE must be compatibility"));
}

#[test]
fn runtime_config_accepts_generic_realtime_storage_policy_profile() {
    let config = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_STORAGE_POLICY_PROFILE",
        "generic_realtime",
    )])
    .expect("generic realtime profile should parse");

    assert_eq!(
        config.market_data_storage.policy_profile,
        MarketDataStoragePolicyProfileConfig::GenericRealtime
    );
}

#[test]
fn parses_market_data_storage_tier_paths() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_L2_REDB_PATH", "/tmp/fdc/l2.redb"),
        (
            "FDC_MARKET_DATA_STORAGE_L3_DUCKDB_PATH",
            "/tmp/fdc/l3.duckdb",
        ),
        (
            "FDC_MARKET_DATA_STORAGE_L4_ROCKSDB_PATH",
            "/tmp/fdc/l4-rocksdb",
        ),
    ])
    .unwrap();

    assert_eq!(
        config.market_data_storage.tiers.l2_redb_path.as_deref(),
        Some(Path::new("/tmp/fdc/l2.redb"))
    );
    assert_eq!(
        config.market_data_storage.tiers.l3_duckdb_path.as_deref(),
        Some(Path::new("/tmp/fdc/l3.duckdb"))
    );
    assert_eq!(
        config.market_data_storage.tiers.l4_rocksdb_path.as_deref(),
        Some(Path::new("/tmp/fdc/l4-rocksdb"))
    );
}

#[test]
fn rejects_empty_market_data_storage_tier_path() {
    let error = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_STORAGE_BACKEND", "tiered"),
        ("FDC_MARKET_DATA_STORAGE_L2_REDB_PATH", ""),
    ])
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("FDC_MARKET_DATA_STORAGE_L2_REDB_PATH must not be empty"),
        "unexpected error: {error}"
    );
}

fn production_local_example_env_pairs() -> Vec<(String, String)> {
    include_str!("../../../config/production.local.example.env")
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }

            let (key, value) = trimmed.split_once('=').unwrap_or_else(|| {
                panic!(
                    "config/production.local.example.env line {} must be KEY=VALUE, got {trimmed:?}",
                    index + 1
                )
            });
            let key = key.trim();
            let value = value.trim();
            assert!(
                !key.is_empty(),
                "env key must not be empty on line {}",
                index + 1
            );
            assert!(
                !value.contains('#'),
                "inline comments are not supported on env line {}; put comments on their own line",
                index + 1
            );
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

#[test]
fn production_local_example_env_parses_as_safe_tiered_production_config() {
    let config = ServerRuntimeConfig::from_env_pairs(production_local_example_env_pairs())
        .expect("production local example env should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:18080");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Production);
    assert_eq!(
        config.market_data_storage.backend,
        MarketDataStorageBackendConfig::Tiered
    );
    assert_eq!(
        config.market_data_storage.policy_profile,
        MarketDataStoragePolicyProfileConfig::GenericRealtime
    );
    assert_eq!(
        config.market_data_storage.tiers.l2_redb_path.as_deref(),
        Some(Path::new("./var/fdc-market-data/l2.redb"))
    );
    assert_eq!(
        config.market_data_storage.tiers.l3_duckdb_path.as_deref(),
        Some(Path::new("./var/fdc-market-data/l3.duckdb"))
    );
    assert_eq!(
        config.market_data_storage.tiers.l4_rocksdb_path.as_deref(),
        Some(Path::new("./var/fdc-market-data/l4-rocksdb"))
    );
}

#[test]
fn production_local_example_env_keeps_dangerous_controls_disabled() {
    let config = ServerRuntimeConfig::from_env_pairs(production_local_example_env_pairs())
        .expect("production local example env should parse");

    assert!(!config.live_enabled);
    assert!(!config.live_autostart);
    assert!(!config.market_data_live_resume_enabled);
    assert!(!config.market_data_storage_maintenance_audit_reset_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_reset_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_resume_enabled);

    assert!(config.market_data_storage_maintenance_enabled);
    assert_eq!(config.market_data_storage_maintenance_audit_capacity, 64);
    assert_eq!(config.live_default_timeout_secs, 30);
    assert_eq!(config.live_default_max_envelopes, 100);
    assert!(config.live_retry_enabled);
    assert_eq!(config.live_retry_initial_delay_ms, 1000);
    assert_eq!(config.live_retry_max_delay_ms, 30000);
    assert_eq!(config.live_max_consecutive_failures, 3);
}

#[test]
fn candle_acquisition_config_is_disabled_by_default() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("defaults should parse");

    assert!(!config.market_data_candle_acquisition.enabled);
    assert!(!config.market_data_candle_acquisition.autostart);
    assert_eq!(config.market_data_candle_acquisition.exchange, "binance_spot");
    assert!(config.market_data_candle_acquisition.symbols.is_empty());
    assert!(config.market_data_candle_acquisition.base_intervals.is_empty());
    assert!(config.market_data_candle_acquisition.verify_intervals.is_empty());
    assert_eq!(config.market_data_candle_acquisition.limit_per_page, 1000);
    assert_eq!(config.market_data_candle_acquisition.max_pages_per_run, 1);
}

#[test]
fn candle_acquisition_config_accepts_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CANDLES_ENABLED", "1"),
        ("FDC_MARKET_DATA_CANDLES_AUTOSTART", "1"),
        ("FDC_MARKET_DATA_CANDLES_EXCHANGE", "binance_spot"),
        ("FDC_MARKET_DATA_CANDLES_SYMBOLS", "BTCUSDT, ethusdt"),
        ("FDC_MARKET_DATA_CANDLES_BASE_INTERVALS", "1m,5m"),
        ("FDC_MARKET_DATA_CANDLES_VERIFY_INTERVALS", "1h,1d"),
        ("FDC_MARKET_DATA_CANDLES_START_NS", "1000000000"),
        ("FDC_MARKET_DATA_CANDLES_END_NS", "2000000000"),
        ("FDC_MARKET_DATA_CANDLES_LIMIT_PER_PAGE", "500"),
        ("FDC_MARKET_DATA_CANDLES_MAX_PAGES_PER_RUN", "3"),
    ])
    .expect("candle acquisition env should parse");

    assert!(config.market_data_candle_acquisition.enabled);
    assert!(config.market_data_candle_acquisition.autostart);
    assert_eq!(config.market_data_candle_acquisition.exchange, "binance_spot");
    assert_eq!(
        config.market_data_candle_acquisition.symbols,
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    );
    assert_eq!(
        config.market_data_candle_acquisition.base_intervals,
        vec!["1m".to_string(), "5m".to_string()]
    );
    assert_eq!(
        config.market_data_candle_acquisition.verify_intervals,
        vec!["1h".to_string(), "1d".to_string()]
    );
    assert_eq!(config.market_data_candle_acquisition.start_ns, Some(1_000_000_000));
    assert_eq!(config.market_data_candle_acquisition.end_ns, Some(2_000_000_000));
    assert_eq!(config.market_data_candle_acquisition.limit_per_page, 500);
    assert_eq!(config.market_data_candle_acquisition.max_pages_per_run, 3);
}

#[test]
fn candle_acquisition_config_rejects_enabled_without_symbols_or_intervals() {
    let missing_symbols = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CANDLES_ENABLED", "1"),
        ("FDC_MARKET_DATA_CANDLES_BASE_INTERVALS", "1m"),
    ])
    .expect_err("enabled candle acquisition requires symbols");
    assert!(missing_symbols
        .to_string()
        .contains("FDC_MARKET_DATA_CANDLES_SYMBOLS"));

    let missing_intervals = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CANDLES_ENABLED", "1"),
        ("FDC_MARKET_DATA_CANDLES_SYMBOLS", "BTCUSDT"),
    ])
    .expect_err("enabled candle acquisition requires base intervals");
    assert!(missing_intervals
        .to_string()
        .contains("FDC_MARKET_DATA_CANDLES_BASE_INTERVALS"));
}

#[test]
fn candle_acquisition_config_rejects_invalid_ranges() {
    let zero_limit = ServerRuntimeConfig::from_env_pairs([(
        "FDC_MARKET_DATA_CANDLES_LIMIT_PER_PAGE",
        "0",
    )])
    .expect_err("zero page limit should be rejected");
    assert!(zero_limit
        .to_string()
        .contains("FDC_MARKET_DATA_CANDLES_LIMIT_PER_PAGE"));

    let inverted_time = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CANDLES_START_NS", "2000"),
        ("FDC_MARKET_DATA_CANDLES_END_NS", "1000"),
    ])
    .expect_err("end before start should be rejected");
    assert!(inverted_time
        .to_string()
        .contains("FDC_MARKET_DATA_CANDLES_END_NS"));
}

#[test]
fn contract_acquisition_config_is_disabled_by_default() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("runtime config should parse");

    assert!(!config.market_data_contract_acquisition.enabled);
    assert!(!config.market_data_contract_acquisition.autostart);
    assert_eq!(
        config.market_data_contract_acquisition.exchange,
        "binance_futures_usd"
    );
    assert!(config.market_data_contract_acquisition.symbols.is_empty());
    assert_eq!(
        config.market_data_contract_acquisition.kinds,
        vec!["candle".to_string()]
    );
    assert!(config.market_data_contract_acquisition.intervals.is_empty());
    assert_eq!(config.market_data_contract_acquisition.limit_per_page, 1000);
    assert_eq!(config.market_data_contract_acquisition.max_pages_per_run, 1);
}

#[test]
fn contract_acquisition_config_accepts_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_AUTOSTART", "yes"),
        ("FDC_MARKET_DATA_CONTRACTS_EXCHANGE", "binance_futures_usd"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "btcusdt,ETHUSDT"),
        ("FDC_MARKET_DATA_CONTRACTS_KINDS", "candle"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m,5m"),
        ("FDC_MARKET_DATA_CONTRACTS_START_NS", "1000000000"),
        ("FDC_MARKET_DATA_CONTRACTS_END_NS", "2000000000"),
        ("FDC_MARKET_DATA_CONTRACTS_LIMIT_PER_PAGE", "500"),
        ("FDC_MARKET_DATA_CONTRACTS_MAX_PAGES_PER_RUN", "3"),
    ])
    .expect("runtime config should parse");

    assert!(config.market_data_contract_acquisition.enabled);
    assert!(config.market_data_contract_acquisition.autostart);
    assert_eq!(
        config.market_data_contract_acquisition.exchange,
        "binance_futures_usd"
    );
    assert_eq!(
        config.market_data_contract_acquisition.symbols,
        vec!["BTCUSDT", "ETHUSDT"]
    );
    assert_eq!(config.market_data_contract_acquisition.kinds, vec!["candle"]);
    assert_eq!(
        config.market_data_contract_acquisition.intervals,
        vec!["1m", "5m"]
    );
    assert_eq!(
        config.market_data_contract_acquisition.start_ns,
        Some(1_000_000_000)
    );
    assert_eq!(
        config.market_data_contract_acquisition.end_ns,
        Some(2_000_000_000)
    );
    assert_eq!(config.market_data_contract_acquisition.limit_per_page, 500);
    assert_eq!(config.market_data_contract_acquisition.max_pages_per_run, 3);
}

#[test]
fn contract_acquisition_config_rejects_invalid_phase1_values() {
    let missing_symbols = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
    ])
    .expect_err("enabled contract acquisition requires symbols");
    assert!(missing_symbols
        .to_string()
        .contains("FDC_MARKET_DATA_CONTRACTS_SYMBOLS"));

    let missing_intervals = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
    ])
    .expect_err("enabled contract candle acquisition requires intervals");
    assert!(missing_intervals
        .to_string()
        .contains("FDC_MARKET_DATA_CONTRACTS_INTERVALS"));

    let unsupported_exchange = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_EXCHANGE", "binance_spot"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
    ])
    .expect_err("phase1 supports only binance_futures_usd");
    assert!(unsupported_exchange
        .to_string()
        .contains("binance_futures_usd"));

    let unsupported_kind = ServerRuntimeConfig::from_env_pairs([
        ("FDC_MARKET_DATA_CONTRACTS_ENABLED", "1"),
        ("FDC_MARKET_DATA_CONTRACTS_SYMBOLS", "BTCUSDT"),
        ("FDC_MARKET_DATA_CONTRACTS_KINDS", "funding_rate"),
        ("FDC_MARKET_DATA_CONTRACTS_INTERVALS", "1m"),
    ])
    .expect_err("phase1 supports only candle kind");
    assert!(unsupported_kind
        .to_string()
        .contains("FDC_MARKET_DATA_CONTRACTS_KINDS"));
}
