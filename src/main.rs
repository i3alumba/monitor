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
    ImportSqlite(resource_monitor::import::ImportArgs),
    ReceiveSnapshot(resource_monitor::import::ReceiveArgs),
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match Cli::parse().command {
        Mode::Serve => resource_monitor::web::serve().await,
        Mode::Agent => resource_monitor::agent::run().await,
        Mode::ImportSqlite(args) => {
            resource_monitor::import::run(
                args,
                &resource_monitor::config::env("MONITOR_REDIS_URL", "redis://127.0.0.1:6379/0"),
            )
            .await
        }
        Mode::ReceiveSnapshot(args) => resource_monitor::import::receive(args),
    }
}
