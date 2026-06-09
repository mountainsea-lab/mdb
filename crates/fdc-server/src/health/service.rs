use crate::{
    health::model::{HealthResponse, ReadinessResponse, VersionData, VersionResponse},
    ProductionServerState,
};

pub fn health_response() -> HealthResponse {
    HealthResponse {
        status: "healthy".to_string(),
    }
}

pub fn readiness_response(state: &ProductionServerState) -> ReadinessResponse {
    ReadinessResponse {
        status: "ready".to_string(),
        live_enabled: state.config().live_enabled,
        market_data_store_available: true,
    }
}

pub fn version_response() -> VersionResponse {
    VersionResponse {
        status: "success".to_string(),
        data: VersionData {
            service: "fdc-server".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        message: None,
    }
}
