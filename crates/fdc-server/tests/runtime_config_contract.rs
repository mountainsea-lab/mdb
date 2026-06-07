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
    assert!(!config.market_data_storage_maintenance_enabled);
    assert!(!config.market_data_storage_maintenance_scheduler_enabled);
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
