//! 分片写转发（基于 Zenoh + 明文 QUIC + `bitcode`）。
//!
//! 除源节点外，各节点都要把已提交的写投递给本地 SpacetimeDB。
//! 条目按 apply 顺序到达本模块，因此只要按到达顺序投递，各节点执行顺序就与 Raft 日志一致。
//!
//! 不同数据库各用一个共享上游连接（`db_path` 区分），互不阻塞、不会串库。

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use compio::{runtime, time::sleep};
use crossfire::{
    AsyncRx, MAsyncTx,
    mpsc::{Array, bounded_async},
};
use futures::{FutureExt, select};
use gxhash::{HashMap, HashMapExt};
use tracing::{info, warn};

use crate::{
    raft::state_machine::{StateMachineStore, WriteReceiver},
    transport::{ZenohClientPool, watch},
    websocket::handler::{Message, ZenohStdbStream, recv_s2c_message},
};

/// 上游连接失败后的重试间隔。
const RECONNECT_DELAY: Duration = Duration::from_millis(200);

/// 上游会话握手超时。
const CONNECT_RPC_TIMEOUT: Duration = Duration::from_secs(3);

/// 上游单条消息发送超时。
const SEND_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// 单库上游投递有界队列容量（修复 P2-12：有界背压）。
pub const UPSTREAM_QUEUE_CAPACITY: usize = 1024;

/// 单库上游投递有界队列发送端（基于 `crossfire`）。
pub type UpstreamSender = MAsyncTx<Array<(u64, Message)>>;
/// 单库上游投递有界队列接收端（基于 `crossfire`）。
pub type UpstreamReceiver = AsyncRx<Array<(u64, Message)>>;

/// 全局递增流标识，用于生成唯一 `s2c` 订阅键。
static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

/// 分配唯一的客户端流 ID。
pub(crate) fn next_stream_id() -> u64 {
    NEXT_STREAM_ID.fetch_add(1, Ordering::Relaxed)
}

/// 启动分片的写转发任务（带状态机 `last_delivered` 水位回报）。
pub fn spawn(
    node_id: u64,
    stdb_url: String,
    write_rx: WriteReceiver,
    state_machine: Option<StateMachineStore>,
    shutdown_rx: watch::Receiver<bool>,
) {
    spawn_pooled(
        node_id,
        stdb_url,
        ZenohClientPool::new(),
        write_rx,
        state_machine,
        shutdown_rx,
    );
}

/// 启动复用节点级 [`ZenohClientPool`] 的分片写转发任务（全库与客户端代理共享同一底层 QUIC 会话）。
pub fn spawn_pooled(
    node_id: u64,
    stdb_url: String,
    zenoh_pool: ZenohClientPool,
    write_rx: WriteReceiver,
    state_machine: Option<StateMachineStore>,
    shutdown_rx: watch::Receiver<bool>,
) {
    runtime::spawn(run_with_pool(
        node_id,
        stdb_url,
        zenoh_pool,
        write_rx,
        state_machine,
        shutdown_rx,
    ))
    .detach();
}

/// 转发循环（供测试直接调用，不绑定状态机）。
pub async fn run(
    node_id: u64,
    stdb_url: String,
    write_rx: WriteReceiver,
    shutdown_rx: watch::Receiver<bool>,
) {
    run_with_state_machine(node_id, stdb_url, write_rx, None, shutdown_rx).await;
}

/// 转发循环：每条写按到达顺序推入对应数据库的有界队列，并在投递后推进 `last_delivered`。
pub async fn run_with_state_machine(
    node_id: u64,
    stdb_url: String,
    write_rx: WriteReceiver,
    state_machine: Option<StateMachineStore>,
    shutdown_rx: watch::Receiver<bool>,
) {
    run_with_pool(
        node_id,
        stdb_url,
        ZenohClientPool::new(),
        write_rx,
        state_machine,
        shutdown_rx,
    )
    .await;
}

/// 复用指定 [`ZenohClientPool`] 的转发循环：不同数据库按 `db_path` 隔离逻辑流（独立 `conn_id`），共享同一底层 QUIC 会话。
pub async fn run_with_pool(
    node_id: u64,
    stdb_url: String,
    zenoh_pool: ZenohClientPool,
    write_rx: WriteReceiver,
    state_machine: Option<StateMachineStore>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // db_path -> 该库的有界投递队列。每库一个任务：库内保序，库间并行。
    let mut upstreams: HashMap<String, UpstreamSender> = HashMap::new();

    loop {
        select! {
            write = write_rx.recv().fuse() => {
                let Ok(write) = write else { return };

                // 源节点由客户端连接自己直投，事务结果才能回到发起方
                if write.origin_node_id == node_id {
                    continue;
                }

                // 优先按引用查表，避免已建立上游队列时在热路径上每次克隆 db_path
                let sender = if let Some(tx) = upstreams.get(&write.db_path) {
                    tx
                } else {
                    let tx = connect_pooled(
                        stdb_url.clone(),
                        write.db_path.clone(),
                        zenoh_pool.clone(),
                        state_machine.clone(),
                        shutdown_rx.clone(),
                    );
                    upstreams.entry(write.db_path).or_insert(tx)
                };

                let item = (write.log_index, Message::Binary(write.raw_message));
                if sender.send(item).await.is_err() {
                    return;
                }
            }
            _ = shutdown_rx.changed().fuse() => {
                info!("Forwarder shutting down");
                return;
            }
        }
    }
}

/// 建立某个数据库的上游投递通道，断线后自动重连。
pub fn connect(
    stdb_url: String,
    db_path: String,
    state_machine: Option<StateMachineStore>,
    shutdown_rx: watch::Receiver<bool>,
) -> UpstreamSender {
    connect_pooled(
        stdb_url,
        db_path,
        ZenohClientPool::new(),
        state_machine,
        shutdown_rx,
    )
}

/// 建立复用 [`ZenohClientPool`] 的单库上游投递通道，断线后自动重连。
pub fn connect_pooled(
    stdb_url: String,
    db_path: String,
    zenoh_pool: ZenohClientPool,
    state_machine: Option<StateMachineStore>,
    shutdown_rx: watch::Receiver<bool>,
) -> UpstreamSender {
    let (tx, rx) = bounded_async(UPSTREAM_QUEUE_CAPACITY);
    runtime::spawn(upstream_task(
        stdb_url,
        db_path,
        zenoh_pool,
        rx,
        state_machine,
        shutdown_rx,
    ))
    .detach();
    tx
}

/// 单库投递任务：复用 [`ZenohStdbStream::connect_pooled`] 建立上游多路流并在断线后自动重连，队列里的写在重连后继续发送，不会丢失。
async fn upstream_task(
    stdb_url: String,
    db_path: String,
    zenoh_pool: ZenohClientPool,
    rx: UpstreamReceiver,
    state_machine: Option<StateMachineStore>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut pending: Option<(u64, Message)> = None;

    loop {
        let stream = match ZenohStdbStream::connect_pooled(
            &zenoh_pool,
            &stdb_url,
            &db_path,
            CONNECT_RPC_TIMEOUT,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    error = %e,
                    stdb_url = %stdb_url,
                    db_path = %db_path,
                    "Forwarder: SpacetimeDB Zenoh connect failed, retrying"
                );
                select! {
                    _ = sleep(RECONNECT_DELAY).fuse() => {}
                    _ = shutdown_rx.changed().fuse() => return,
                }
                continue;
            }
        };

        let (sender, s2c_sub) = stream.split();
        info!(db_path = %db_path, "Forwarder connected to SpacetimeDB over Zenoh QUIC");

        loop {
            let next_item = if let Some(item) = pending.take() {
                Some(item)
            } else {
                select! {
                    item = rx.recv().fuse() => item.ok(),
                    incoming = recv_s2c_message(&s2c_sub).fuse() => {
                        match incoming {
                            Ok(Some(Message::Close(_)) | None) | Err(_) => break,
                            Ok(Some(_)) => continue,
                        }
                    }
                    _ = shutdown_rx.changed().fuse() => {
                        sender.close();
                        return;
                    }
                }
            };

            let Some((log_index, msg)) = next_item else {
                sender.close();
                return;
            };

            match sender.send_with_timeout(&msg, SEND_RPC_TIMEOUT).await {
                Ok(()) => {
                    if let Some(sm) = &state_machine
                        && log_index > 0
                    {
                        sm.mark_delivered(log_index);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Forwarder: SpacetimeDB send failed, reconnecting");
                    zenoh_pool.evict(&stdb_url);
                    pending = Some((log_index, msg));
                    break;
                }
            }
        }

        sender.close();
    }
}
