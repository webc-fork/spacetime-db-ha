mod handler;
mod upstream;

use std::sync::Arc;

use anyhow::Result;
use tokio::{net::TcpListener, sync::watch};
use tracing::{error, info};

use crate::{config::NodeConfig, raft::RaftPool};

/// The WebSocket proxy that sits between clients and SpacetimeDB.
///
/// Clients connect to this proxy as if it were SpacetimeDB. The proxy inspects
/// each message's BSATN tag byte to classify it as a read or write:
///
/// - **Writes** (tag 3: CallReducer, tag 4: CallProcedure) go through Raft
///   consensus before being forwarded to the local SpacetimeDB instance.
/// - **Reads** (tag 0: Subscribe, tag 1: Unsubscribe, tag 2: OneOffQuery)
///   are forwarded directly — no consensus needed since all replicas have
///   identical state.
///
/// The proxy routes writes to the correct shard based on the module name
/// extracted from the WebSocket path (e.g., `/database/subscribe/mymodule`).
pub struct Proxy {
    config: NodeConfig,
    pool: Arc<RaftPool>,
    /// Receives shutdown signal to stop accepting new connections.
    shutdown_rx: watch::Receiver<bool>,
}

impl Proxy {
    pub fn new(
        config: NodeConfig,
        pool: Arc<RaftPool>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            config,
            pool,
            shutdown_rx,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        info!(addr = %self.config.listen_addr, "WebSocket proxy listening");

        let mut shutdown = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    let pool = self.pool.clone();
                    let stdb_url = self.config.stdb_url.clone();
                    let conn_shutdown = self.shutdown_rx.clone();

                    tokio::spawn(async move {
                        match handler::handle_client(stream, pool, &stdb_url, conn_shutdown).await {
                            Ok(()) => info!(%addr, "Client disconnected"),
                            Err(e) => error!(%addr, error = %e, "Client connection error"),
                        }
                    });
                }
                _ = shutdown.changed() => {
                    info!("Shutdown signal received, stopping WebSocket proxy");
                    break;
                }
            }
        }

        Ok(())
    }
}
