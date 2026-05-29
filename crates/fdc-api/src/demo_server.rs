use std::sync::Arc;

use fdc_core::Result;
use fdc_server::{BoundedMarketDataRunnerHandle, FdcServerApp};
use fdc_storage::QueryableMarketDataStore;
use tokio::sync::Mutex;

use crate::ApiAppState;

pub fn initialized_demo_app_state_with_control_runner() -> Result<ApiAppState> {
    let mut app = FdcServerApp::with_defaults();
    app.initialize()?;

    let store = Arc::new(QueryableMarketDataStore::new());
    let runner = Arc::new(Mutex::new(BoundedMarketDataRunnerHandle::new(Arc::clone(
        &store,
    ))));

    Ok(ApiAppState::new(app)
        .with_market_data_store(store)
        .with_market_data_runner_control(runner))
}
