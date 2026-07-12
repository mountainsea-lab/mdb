use fdc_server::{
    build_production_router, shutdown_signal, ProductionServerState, ServerRuntimeConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenvy::dotenv();
    let config = ServerRuntimeConfig::from_env()?;
    let bind_addr = config.bind_addr;
    let state = ProductionServerState::try_new(config).await?;
    state.start_live_autostart_if_enabled().await?;
    state.start_candle_acquisition_autostart_if_enabled().await?;
    let router = build_production_router(state);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    eprintln!("fdc server listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}
