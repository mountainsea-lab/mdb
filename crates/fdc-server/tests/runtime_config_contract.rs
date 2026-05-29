use fdc_server::{ServerRuntimeConfig, ServerRuntimeEnvironment};

#[test]
fn runtime_config_defaults_are_safe_for_local_production_server() {
    let config = ServerRuntimeConfig::from_env_pairs([] as [(&str, &str); 0])
        .expect("defaults should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:18080");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Development);
    assert!(!config.live_enabled);
    assert_eq!(config.live_default_timeout_secs, 30);
    assert_eq!(config.live_default_max_envelopes, 100);
}

#[test]
fn runtime_config_accepts_env_overrides() {
    let config = ServerRuntimeConfig::from_env_pairs([
        ("FDC_SERVER_ADDR", "127.0.0.1:19090"),
        ("FDC_SERVER_ENV", "production"),
        ("FDC_LIVE_ENABLED", "1"),
        ("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "12"),
        ("FDC_LIVE_DEFAULT_MAX_ENVELOPES", "34"),
    ])
    .expect("env overrides should parse");

    assert_eq!(config.bind_addr.to_string(), "127.0.0.1:19090");
    assert_eq!(config.environment, ServerRuntimeEnvironment::Production);
    assert!(config.live_enabled);
    assert_eq!(config.live_default_timeout_secs, 12);
    assert_eq!(config.live_default_max_envelopes, 34);
}

#[test]
fn runtime_config_rejects_invalid_values() {
    let error = ServerRuntimeConfig::from_env_pairs([("FDC_LIVE_DEFAULT_TIMEOUT_SECS", "0")])
        .expect_err("zero timeout should be rejected");

    assert!(error.to_string().contains("FDC_LIVE_DEFAULT_TIMEOUT_SECS"));
}
