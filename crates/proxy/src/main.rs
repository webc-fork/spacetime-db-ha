use std::{sync::Arc, time::Duration};

use anyhow::Result;
use axum::body::Body;
use clap::Parser;
use hyper::{Request, body::Incoming, service::service_fn};
use hyper_util::server::conn::auto::Builder;
use rustls::ServerConfig;
use tokio::{
    net::TcpListener,
    sync::watch::{Receiver, channel},
    time,
    time::sleep,
};
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod api;
mod config;
mod metrics;
mod raft;
mod router;
mod websocket;

#[derive(Parser)]
#[command(
    name = "rafttimedb",
    about = "Distributed clustering layer for SpacetimeDB"
)]
struct Cli {
    /// Node ID (unique within the cluster)
    #[arg(long, env = "RTDB_NODE_ID")]
    node_id: u64,

    /// Listen address for client WebSocket connections
    #[arg(long, default_value = "0.0.0.0:3001", env = "RTDB_LISTEN_ADDR")]
    listen_addr: String,

    /// Listen address for Raft inter-node communication
    #[arg(long, default_value = "0.0.0.0:4001", env = "RTDB_RAFT_ADDR")]
    raft_addr: String,

    /// Local SpacetimeDB WebSocket URL
    #[arg(long, default_value = "ws://127.0.0.1:3000", env = "RTDB_STDB_URL")]
    stdb_url: String,

    /// Comma-separated list of peer Raft addresses (format: "id=host:port")
    #[arg(long, env = "RTDB_PEERS", value_delimiter = ',')]
    peers: Vec<String>,

    /// Data directory for Raft log persistence
    #[arg(long, default_value = "./data", env = "RTDB_DATA_DIR")]
    data_dir: String,

    /// Path to TLS certificate file (PEM). Enables TLS when set with --tls-key.
    #[arg(long, env = "RTDB_TLS_CERT")]
    tls_cert: Option<String>,

    /// Path to TLS private key file (PEM). Required when --tls-cert is set.
    #[arg(long, env = "RTDB_TLS_KEY")]
    tls_key: Option<String>,

    /// Path to CA certificate for verifying peer nodes (PEM). Optional.
    #[arg(long, env = "RTDB_TLS_CA_CERT")]
    tls_ca_cert: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| "rafttimedb=info".into()),
        )
        .init();

    let cli = Cli::parse();

    // Validate TLS config
    if cli.tls_cert.is_some() != cli.tls_key.is_some() {
        anyhow::bail!("Both --tls-cert and --tls-key must be provided together");
    }

    info!(
        node_id = cli.node_id,
        listen_addr = %cli.listen_addr,
        raft_addr = %cli.raft_addr,
        stdb_url = %cli.stdb_url,
        tls = cli.tls_cert.is_some(),
        peers = ?cli.peers,
        "Starting RaftTimeDB"
    );

    let config = config::NodeConfig {
        node_id: cli.node_id,
        listen_addr: cli.listen_addr,
        raft_addr: cli.raft_addr.clone(),
        stdb_url: cli.stdb_url,
        peers: cli.peers,
        data_dir: cli.data_dir,
        tls_cert: cli.tls_cert,
        tls_key: cli.tls_key,
        tls_ca_cert: cli.tls_ca_cert,
    };

    // Shutdown coordination: send `true` to signal all tasks to stop
    let (shutdown_tx, shutdown_rx) = channel(false);

    // Initialize the RaftPool (creates shard 0, spawns forwarder + config listener)
    let pool = raft::RaftPool::new(&config, shutdown_rx.clone()).await?;

    // Spawn metrics updater (periodically updates Raft metrics for Prometheus)
    let pool_for_metrics = pool.clone();
    let mut metrics_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    pool_for_metrics.update_metrics().await;
                }
                _ = metrics_shutdown.changed() => {
                    return;
                }
            }
        }
    });

    // Build TLS config if cert and key are provided
    let tls_config = if config.tls_enabled() {
        Some(build_tls_config(
            config.tls_cert.as_ref().unwrap(),
            config.tls_key.as_ref().unwrap(),
        )?)
    } else {
        None
    };

    // Start the HTTP management API server on the raft address
    let app_state = Arc::new(api::AppState { pool: pool.clone() });
    let http_router = api::router(app_state);
    let http_listener = TcpListener::bind(&cli.raft_addr).await?;

    if let Some(ref tls) = tls_config {
        info!(addr = %cli.raft_addr, "HTTPS management API listening (TLS enabled)");
        let acceptor = TlsAcceptor::from(tls.clone());
        let router = http_router.clone();
        let http_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            serve_tls(http_listener, acceptor, router, http_shutdown).await;
        });
    } else {
        info!(addr = %cli.raft_addr, "HTTP management API listening");
        let mut http_shutdown = shutdown_rx.clone();
        tokio::spawn(async move {
            let graceful =
                axum::serve(http_listener, http_router).with_graceful_shutdown(async move {
                    let _ = http_shutdown.changed().await;
                });
            if let Err(e) = graceful.await {
                tracing::error!(error = %e, "HTTP server error");
            }
        });
    };

    // Start the WebSocket proxy
    let proxy = websocket::Proxy::new(config, pool, shutdown_rx.clone());

    // Run proxy until shutdown signal
    tokio::select! {
        result = proxy.run() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "WebSocket proxy error");
            }
        }
        _ = shutdown_signal() => {
            info!("Initiating graceful shutdown...");
            let _ = shutdown_tx.send(true);

            // Give connections time to drain
            sleep(Duration::from_secs(5)).await;
            info!("Shutdown complete");
        }
    }

    Ok(())
}

/// Wait for a shutdown signal (Ctrl+C on all platforms, SIGTERM on Unix).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
    }

    info!("Shutdown signal received");
}

/// Build a rustls ServerConfig from PEM cert and key files.
fn build_tls_config(cert_path: &str, key_path: &str) -> Result<Arc<rustls::ServerConfig>> {
    use std::{fs::File, io::BufReader};

    use rustls_pemfile::{certs, private_key};

    let cert_file = File::open(cert_path)?;
    let key_file = File::open(key_path)?;

    let certs: Vec<_> = certs(&mut BufReader::new(cert_file)).collect::<Result<Vec<_>, _>>()?;

    let key = private_key(&mut BufReader::new(key_file))?
        .ok_or_else(|| anyhow::anyhow!("No private key found in {}", key_path))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(Arc::new(config))
}

/// Serve an axum router over TLS using tokio-rustls.
async fn serve_tls(
    listener: TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    app: axum::Router,
    mut shutdown: Receiver<bool>,
) {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use tower::ServiceExt;

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _addr) = match result {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "TLS listener accept error");
                        continue;
                    }
                };

                let acceptor = acceptor.clone();
                let app = app.clone();

                tokio::spawn(async move {
                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(error = %e, "TLS handshake failed");
                            return;
                        }
                    };

                    let io = TokioIo::new(tls_stream);
                    let service = service_fn(move |req: hyper::Request<Incoming>| {
                        let app = app.clone();
                        async move {
                            let (parts, body) = req.into_parts();
                            let req = Request::from_parts(parts, Body::new(body));
                            app.oneshot(req).await
                        }
                    });

                    if let Err(e) = Builder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await
                    {
                        warn!(error = %e, "TLS connection serve error");
                    }
                });
            }
            _ = shutdown.changed() => {
                info!("TLS server shutting down");
                break;
            }
        }
    }
}
