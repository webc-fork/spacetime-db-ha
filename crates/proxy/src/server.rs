use std::{rc::Rc, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use compio::{
    net::TcpListener,
    runtime::spawn,
    signal::ctrl_c,
    time::{interval, sleep},
};
use futures::{FutureExt, select};
use tracing::{error, info};

use crate::{
    api,
    config::NodeConfig,
    raft::RaftPool,
    transport::{
        open_server_session,
        watch::{self, Receiver},
    },
    websocket::Proxy,
};

/// 指标刷新间隔。
const METRICS_INTERVAL: Duration = Duration::from_secs(1);

/// 收到关闭信号后等待存量连接收尾的时间。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// 启动完整节点：Raft 池、指标刷新、Zenoh 管理/共识面、compio HTTP JSON `/metrics`、Zenoh 客户端代理。
pub async fn serve(config: NodeConfig) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let pool = RaftPool::new(&config, shutdown_rx.clone()).await?;
    spawn_metrics_updater(pool.clone(), shutdown_rx.clone());

    // Raft 与集群管理面：监听 Zenoh 明文 QUIC (UDP)
    let raft_session = open_server_session(&config.raft_addr)
        .await
        .with_context(|| format!("bind Zenoh QUIC raft address {}", config.raft_addr))?;
    let api_pool = Rc::clone(&pool);
    let api_shutdown = shutdown_rx.clone();
    spawn(async move {
        if let Err(e) = api::serve_zenoh(raft_session, api_pool, api_shutdown).await {
            error!(error = %e, "Zenoh management/raft server error");
        }
    })
    .detach();

    // 指标抓取：监听 compio TCP HTTP JSON (与 UDP 明文 QUIC 端口互不冲突)
    if let Ok(tcp_listener) = TcpListener::bind(&config.raft_addr).await {
        info!(addr = %config.raft_addr, "HTTP JSON /metrics TCP responder listening (compio)");
        spawn(api::serve_metrics_tcp(
            tcp_listener,
            Rc::clone(&pool),
            shutdown_rx.clone(),
        ))
        .detach();
    }

    let proxy = Proxy::new(Arc::new(config), pool, shutdown_rx);

    select! {
        result = proxy.run().fuse() => result,
        _ = shutdown_signal().fuse() => {
            info!("Initiating graceful shutdown...");
            let _ = shutdown_tx.send(true);
            sleep(SHUTDOWN_GRACE).await;
            info!("Shutdown complete");
            Ok(())
        }
    }
}

/// 定期把 Raft 状态刷进原子指标。
fn spawn_metrics_updater(pool: Rc<RaftPool>, mut shutdown: Receiver<bool>) {
    spawn(async move {
        let mut ticker = interval(METRICS_INTERVAL);
        loop {
            select! {
                _ = ticker.tick().fuse() => pool.update_metrics(),
                _ = shutdown.changed().fuse() => return,
            }
        }
    })
    .detach();
}

/// 等待关闭信号：Unix 下响应 SIGTERM 与 Ctrl+C，其它平台只响应 Ctrl+C。
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use compio::signal::unix::signal;

        // SIGTERM = 15
        const SIGTERM: i32 = 15;
        select! {
            _ = ctrl_c().fuse() => {},
            _ = signal(SIGTERM).fuse() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = ctrl_c().await;
    }

    info!("Shutdown signal received");
}
