use clap::{Parser, Subcommand};
#[derive(Parser)]
#[command(
    version,
    about = "Host telemetry collector and protected history service"
)]
struct Cli {
    #[command(subcommand)]
    command: Mode,
}
#[derive(Subcommand)]
enum Mode {
    Serve,
    Agent,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match Cli::parse().command {
        Mode::Serve => resource_monitor::web::serve().await,
        Mode::Agent => resource_monitor::agent::run().await,
    }
}
