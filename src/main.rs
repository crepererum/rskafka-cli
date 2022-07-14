//! CLI for [`rskafka`].
use anyhow::Result;
use clap::Parser;
use client::{build_client, ClientCLIConfig};
use consume::{consume, ConsumeCLIConfig};
use logging::{setup_logging, LoggingCLIConfig};
use runtime::{setup_runtime, RuntimeCLIConfig};

mod client;
mod consume;
mod logging;
mod runtime;

/// CLI args.
#[derive(Debug, Parser)]
struct Args {
    /// Runtime config.
    #[clap(flatten)]
    runtime_cfg: RuntimeCLIConfig,

    /// Client config.
    #[clap(flatten)]
    client_cfg: ClientCLIConfig,

    /// Logging config.
    #[clap(flatten)]
    logging_cfg: LoggingCLIConfig,

    /// Command.
    #[clap(subcommand)]
    command: Command,
}

/// The actual CLI action.
#[derive(Debug, Parser)]
enum Command {
    /// Consume data.
    Consume(ConsumeCLIConfig),
}

/// Main entry point.
fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = setup_runtime(args.runtime_cfg)?;
    runtime.block_on(async move {
        setup_logging(args.logging_cfg)?;

        let client = build_client(args.client_cfg).await?;

        match args.command {
            Command::Consume(args) => {
                consume(client, args).await?;
            }
        }

        Ok(())
    })
}
