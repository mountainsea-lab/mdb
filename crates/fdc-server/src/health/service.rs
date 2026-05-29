use crate::{
    health::model::{HealthResponse, ReadinessResponse},
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
