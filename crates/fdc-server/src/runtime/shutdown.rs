pub async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        eprintln!("fdc server: failed to listen for shutdown signal: {error}");
    }
    eprintln!("fdc server: shutdown signal received");
}
