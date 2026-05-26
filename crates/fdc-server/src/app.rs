use fdc_core::{error::Error, Result};

use crate::{FdcServerConfig, ServerComponents};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerLifecycleState {
    Created,
    Initialized,
    Stopped,
}

pub struct FdcServerApp {
    config: FdcServerConfig,
    components: ServerComponents,
    state: ServerLifecycleState,
}

impl FdcServerApp {
    pub fn new(config: FdcServerConfig, components: ServerComponents) -> Self {
        Self {
            config,
            components,
            state: ServerLifecycleState::Created,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(FdcServerConfig::default(), ServerComponents::default())
    }

    pub fn initialize(&mut self) -> Result<()> {
        if self.config.service_name.trim().is_empty() {
            return Err(Error::validation("server service_name must not be empty"));
        }
        if !self.components.is_ready() {
            return Err(Error::validation("server components are not ready"));
        }

        self.state = ServerLifecycleState::Initialized;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        self.state = ServerLifecycleState::Stopped;
        Ok(())
    }

    pub fn config(&self) -> &FdcServerConfig {
        &self.config
    }

    pub fn components(&self) -> &ServerComponents {
        &self.components
    }

    pub fn state(&self) -> ServerLifecycleState {
        self.state
    }

    pub fn is_ready(&self) -> bool {
        self.state == ServerLifecycleState::Initialized && self.components.is_ready()
    }
}
