use anyhow;

use clap::Parser;
use krakensync::core::Args;
use krakensync::network;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt::init();
    network::sync_peer(args).await
    //network::sync_demo().await
    // network::peer_sync_demo().await
    // network::main().await
}
