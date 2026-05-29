//! Application assembly boundary for Financial Data Center.
//!
//! `fdc-server` composes application-level components. It does not own adapter,
//! ingestion, transform, or storage mapping logic; that remains in
//! `fdc-orchestrator` and lower-level crates.

pub mod app;
pub mod components;
pub mod config;
pub mod mvp;
pub mod realtime;
pub mod runner;

pub use app::{FdcServerApp, ServerLifecycleState};
pub use components::{MarketDataOrchestratorResult, ServerComponents};
pub use config::{FdcServerConfig, ServerEnvironment};
pub use mvp::{
    run_barter_fixture_mvp_once, BoundedMarketDataMvpResult, BoundedMarketDataMvpRunner,
};
pub use realtime::{
    run_realtime_barter_envelope_stream, RealtimeMarketDataMvpConfig, RealtimeMarketDataMvpSummary,
};
pub use runner::{BoundedMarketDataRunnerHandle, BoundedRunnerFailure, BoundedRunnerState};
