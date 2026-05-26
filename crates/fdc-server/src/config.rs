#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FdcServerConfig {
    pub service_name: String,
    pub environment: ServerEnvironment,
    pub enable_api: bool,
    pub enable_market_data_orchestrator: bool,
}

impl FdcServerConfig {
    pub fn for_tests() -> Self {
        Self {
            service_name: "fdc-server-test".to_string(),
            environment: ServerEnvironment::Test,
            enable_api: false,
            enable_market_data_orchestrator: true,
        }
    }
}

impl Default for FdcServerConfig {
    fn default() -> Self {
        Self {
            service_name: "fdc-server".to_string(),
            environment: ServerEnvironment::Development,
            enable_api: false,
            enable_market_data_orchestrator: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerEnvironment {
    Development,
    Test,
    Production,
}
