use std::sync::Arc;

use fdc_server::{FdcServerApp, ServerEnvironment, ServerLifecycleState};
use serde::{Deserialize, Serialize};

use crate::models::ApiResponse;

#[derive(Clone)]
pub struct ApiAppState {
    server_app: Arc<FdcServerApp>,
}

impl ApiAppState {
    pub fn new(server_app: FdcServerApp) -> Self {
        Self {
            server_app: Arc::new(server_app),
        }
    }

    pub fn from_shared(server_app: Arc<FdcServerApp>) -> Self {
        Self { server_app }
    }

    pub fn server_app(&self) -> &FdcServerApp {
        &self.server_app
    }

    pub fn shared_server_app(&self) -> Arc<FdcServerApp> {
        Arc::clone(&self.server_app)
    }

    pub fn readiness_projection(&self) -> ApiReadinessProjection {
        let app = self.server_app();
        ApiReadinessProjection {
            status: if app.is_ready() {
                ApiReadinessStatus::Ready
            } else {
                ApiReadinessStatus::NotReady
            },
            service_name: app.config().service_name.clone(),
            environment: server_environment_label(app.config().environment).to_string(),
            server_lifecycle_state: server_lifecycle_state_label(app.state()).to_string(),
            components_ready: app.components().is_ready(),
            api_enabled: app.config().enable_api,
            market_data_orchestrator_enabled: app.config().enable_market_data_orchestrator,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiReadinessStatus {
    Ready,
    NotReady,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiReadinessProjection {
    pub status: ApiReadinessStatus,
    pub service_name: String,
    pub environment: String,
    pub server_lifecycle_state: String,
    pub components_ready: bool,
    pub api_enabled: bool,
    pub market_data_orchestrator_enabled: bool,
}

pub fn readiness_response_from_state(state: &ApiAppState) -> ApiResponse<ApiReadinessProjection> {
    ApiResponse::success(state.readiness_projection())
}

pub fn server_environment_label(environment: ServerEnvironment) -> &'static str {
    match environment {
        ServerEnvironment::Development => "development",
        ServerEnvironment::Test => "test",
        ServerEnvironment::Production => "production",
    }
}

pub fn server_lifecycle_state_label(state: ServerLifecycleState) -> &'static str {
    match state {
        ServerLifecycleState::Created => "created",
        ServerLifecycleState::Initialized => "initialized",
        ServerLifecycleState::Stopped => "stopped",
    }
}
