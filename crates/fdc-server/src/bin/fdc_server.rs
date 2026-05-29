use fdc_server::{
    build_production_router, shutdown_signal, ProductionServerState, ServerRuntimeConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerRuntimeConfig::from_env()?;
    let bind_addr = config.bind_addr;
    let state = ProductionServerState::new(config);
    let router = build_production_router(state);
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    eprintln!("fdc server listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}
