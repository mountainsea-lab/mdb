use fdc_server::{
    MarketDataStorageBackendConfig, MarketDataStoragePolicyProfileConfig,
    MarketDataStorageRuntimeConfig, ServerRuntimeConfig, ServerRuntimeEnvironment,
};

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
    assert_eq!(
        config.market_data_storage,
        MarketDataStorageRuntimeConfig {
            backend: MarketDataStorageBackendConfig::Memory,
            policy_profile: MarketDataStoragePolicyProfileConfig::Compatibility,
        }
    );
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
        "market_data_realtime",
    )])
    .expect_err("unsupported profile should be rejected");
    assert!(profile_error
        .to_string()
        .contains("FDC_MARKET_DATA_STORAGE_POLICY_PROFILE must be compatibility"));
}
