use std::net::SocketAddr;

use fdc_api::{build_demo_router, initialized_demo_app_state_with_control_runner};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = std::env::var("FDC_DEMO_API_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:18080".to_string())
        .parse()?;

    let state = initialized_demo_app_state_with_control_runner()?;
    let router = build_demo_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    eprintln!("fdc demo API listening on http://{}", listener.local_addr()?);
    axum::serve(listener, router).await?;
    Ok(())
}
