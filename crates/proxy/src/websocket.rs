//! Zenoh 客户端代理：客户端 <-> 本代理 <-> 本地 SpacetimeDB。
//!
//! 本模块按 BSATN 标签字节做读写分流：
//! - 写（标签 3 CallReducer / 4 CallProcedure）先过 Raft 共识再落地 SpacetimeDB；
//! - 读（标签 0 Subscribe / 1 Unsubscribe / 2 OneOffQuery）直接转发；
//! - 其它（ping/pong/text/close/空二进制）原样透传。

use compio::runtime::spawn;
pub mod handler;

use std::{rc::Rc, sync::Arc};

use anyhow::{Result, anyhow};
use futures::{FutureExt, select};
use tracing::{error, info};
use zenoh::Session;

use crate::{
    config::NodeConfig,
    metrics::CONNECTIONS_ACTIVE,
    raft::RaftPool,
    transport::{
        CrossfireChannel, KEY_STDB_CONNECT, close_session, decode_query_payload,
        open_server_session, reply_bitcode, watch::Receiver,
    },
    websocket::handler::{StdbConnectRequest, spawn_client_session},
};

/// Zenoh 明文 QUIC 代理服务。
pub struct Proxy {
    config: Arc<NodeConfig>,
    pool: Rc<RaftPool>,
    /// 收到关闭信号后停止接受新连接。
    shutdown: Receiver<bool>,
}

impl Proxy {
    pub fn new(config: Arc<NodeConfig>, pool: Rc<RaftPool>, shutdown: Receiver<bool>) -> Self {
        Self {
            config,
            pool,
            shutdown,
        }
    }

    /// 绑定 `listen_addr`（明文 QUIC `udp/<listen_addr>#rel=1`）后进入服务循环。
    pub async fn run(self) -> Result<()> {
        let session = open_server_session(&self.config.listen_addr).await?;
        info!(addr = %self.config.listen_addr, "Zenoh QUIC proxy listening");
        self.serve_session(session).await
    }

    /// 在已打开的 Zenoh 服务端会话上服务，便于测试注入。
    pub async fn serve_session(self, session: Session) -> Result<()> {
        let mut shutdown = self.shutdown.clone();
        let connect_queryable = session
            .declare_queryable(KEY_STDB_CONNECT)
            .with(CrossfireChannel::new())
            .await
            .map_err(|e| anyhow!("Failed to declare {KEY_STDB_CONNECT} queryable: {e}"))?;

        loop {
            select! {
                query = connect_queryable.recv().fuse() => {
                    let Ok(query) = query else { break };
                    let req: StdbConnectRequest = match decode_query_payload(&query) {
                        Ok(r) => r,
                        Err(e) => {
                            reply_bitcode::<()>(&query, Err(e.to_string())).await;
                            continue;
                        }
                    };

                    let session_clone = session.clone();
                    let pool = Rc::clone(&self.pool);
                    let config = Arc::clone(&self.config);
                    let conn_shutdown = self.shutdown.clone();

                    spawn(async move {
                        match spawn_client_session(session_clone, req, pool, config, conn_shutdown).await {
                            Ok(resp) => reply_bitcode(&query, Ok(resp)).await,
                            Err(e) => {
                                error!(error = %e, "Client session handshake failed");
                                reply_bitcode::<()>(&query, Err(e.to_string())).await;
                            }
                        }
                    })
                    .detach();
                }
                _ = shutdown.changed().fuse() => {
                    info!("Shutdown signal received, stopping Zenoh QUIC proxy");
                    drop(connect_queryable);
                    close_session(&session);
                    return Ok(());
                }
            }
        }
        close_session(&session);
        Ok(())
    }
}

/// 已建立的连接计数守卫：连接结束（含 panic 展开）时自动减一。
pub(crate) struct ConnectionGuard;

impl ConnectionGuard {
    pub(crate) fn new() -> Self {
        CONNECTIONS_ACTIVE.inc();
        Self
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        CONNECTIONS_ACTIVE.dec();
    }
}
