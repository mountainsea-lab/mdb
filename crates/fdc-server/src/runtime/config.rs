use std::{env, net::SocketAddr};

use fdc_core::{error::Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerRuntimeEnvironment {
    Development,
    Test,
    Production,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerRuntimeConfig {
    pub bind_addr: SocketAddr,
    pub environment: ServerRuntimeEnvironment,
    pub live_enabled: bool,
    pub live_default_timeout_secs: u64,
    pub live_default_max_envelopes: usize,
}

impl ServerRuntimeConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_env_pairs(env::vars())
    }

    pub fn from_env_pairs<I, K, V>(pairs: I) -> Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut bind_addr = "127.0.0.1:18080".to_string();
        let mut environment = ServerRuntimeEnvironment::Development;
        let mut live_enabled = false;
        let mut live_default_timeout_secs = 30_u64;
        let mut live_default_max_envelopes = 100_usize;

        for (key, value) in pairs {
            match key.as_ref() {
                "FDC_SERVER_ADDR" => bind_addr = value.as_ref().to_string(),
                "FDC_SERVER_ENV" => {
                    environment = match value.as_ref() {
                        "development" | "dev" => ServerRuntimeEnvironment::Development,
                        "test" => ServerRuntimeEnvironment::Test,
                        "production" | "prod" => ServerRuntimeEnvironment::Production,
                        other => {
                            return Err(Error::config(format!(
                                "FDC_SERVER_ENV must be development, test, or production, got {other}"
                            )));
                        }
                    };
                }
                "FDC_LIVE_ENABLED" => {
                    live_enabled = matches!(value.as_ref(), "1" | "true" | "yes" | "on");
                }
                "FDC_LIVE_DEFAULT_TIMEOUT_SECS" => {
                    live_default_timeout_secs =
                        parse_positive_u64("FDC_LIVE_DEFAULT_TIMEOUT_SECS", value.as_ref())?;
                }
                "FDC_LIVE_DEFAULT_MAX_ENVELOPES" => {
                    live_default_max_envelopes =
                        parse_positive_usize("FDC_LIVE_DEFAULT_MAX_ENVELOPES", value.as_ref())?;
                }
                _ => {}
            }
        }

        let bind_addr = bind_addr.parse().map_err(|error| {
            Error::config(format!("FDC_SERVER_ADDR must be a socket address: {error}"))
        })?;

        Ok(Self {
            bind_addr,
            environment,
            live_enabled,
            live_default_timeout_secs,
            live_default_max_envelopes,
        })
    }
}

fn parse_positive_u64(name: &str, value: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| Error::config(format!("{name} must be a positive integer: {error}")))?;
    if parsed == 0 {
        return Err(Error::config(format!("{name} must be greater than zero")));
    }
    Ok(parsed)
}

fn parse_positive_usize(name: &str, value: &str) -> Result<usize> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| Error::config(format!("{name} must be a positive integer: {error}")))?;
    if parsed == 0 {
        return Err(Error::config(format!("{name} must be greater than zero")));
    }
    Ok(parsed)
}
