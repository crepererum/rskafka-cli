//! CLI interface to build a [`Client`].
use anyhow::Result;
use clap::Parser;
use rskafka::client::{Client, ClientBuilder};

/// CLI config for a [`Client`].
#[derive(Debug, Parser)]
pub struct ClientCLIConfig {
    /// Bootstrap brokers.
    #[clap(long, required = true)]
    bootstrap_brokers: Vec<String>,

    /// SOCKS5 proxy
    #[clap(long)]
    socks5_proxy: Option<String>,
}

/// Build a [`Client`].
pub async fn build_client(config: ClientCLIConfig) -> Result<Client> {
    let mut client_builder = ClientBuilder::new(config.bootstrap_brokers);
    if let Some(socks5_proxy) = config.socks5_proxy {
        client_builder = client_builder.socks5_proxy(socks5_proxy);
    }
    let client = client_builder.build().await?;
    Ok(client)
}
