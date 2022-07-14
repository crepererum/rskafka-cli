//! CLI args to configure tokio.
use anyhow::Result;
use clap::Parser;
use tokio::runtime::{Builder, Runtime};

#[derive(Debug, Parser)]
pub struct RuntimeCLIConfig {
    /// Number of threads.
    #[clap(short = 'j', long)]
    threads: Option<usize>,
}

pub fn setup_runtime(config: RuntimeCLIConfig) -> Result<Runtime> {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    if let Some(threads) = config.threads {
        builder.worker_threads(threads);
    }
    let rt = builder.build()?;
    Ok(rt)
}
