//! 重启：状态机水位已落盘，重新启动不会把已投递历史重放给 SpacetimeDB，
//! 但会精确重放 `(last_delivered, last_applied]` 区间内崩溃前未完成投递的写（修复 P0-2）。

#![recursion_limit = "256"]

use std::{
    collections::BTreeMap,
    net::UdpSocket,
    rc::Rc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use compio::{
    runtime::{Runtime, spawn},
    time::{sleep, timeout},
};
use crossfire::mpsc::unbounded_async;
use openraft::{BasicNode, async_runtime::watch::WatchReceiver, storage::RaftStateMachine};
use spacetime_db_ha::{
    config::NodeConfig,
    raft::{RaftPool, state_machine::StateMachineStore},
    transport::{
        CrossfireChannel, KEY_STDB_CONNECT, decode_query_payload, open_server_session,
        reply_bitcode, watch,
    },
    websocket::handler::{Message, StdbConnectRequest, StdbConnectResponse},
};
use tempfile::TempDir;

const LEADER_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const DB_PATH: &str = "/database/subscribe/testdb";

fn free_udp_addr() -> String {
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.local_addr().unwrap().to_string()
}

fn config(data_dir: &str, raft_addr: &str, stdb_url: &str) -> NodeConfig {
    NodeConfig {
        node_id: 1,
        listen_addr: "127.0.0.1:0".to_owned(),
        raft_addr: raft_addr.to_owned(),
        raft_advertise_addr: None,
        stdb_url: stdb_url.to_owned(),
        peers: Vec::new(),
        data_dir: data_dir.to_owned(),
    }
}

/// 启动单节点池，同时返回关闭信号发送端。
async fn start(
    data_dir: &str,
    raft_addr: &str,
    stdb_url: &str,
) -> (Rc<RaftPool>, watch::Sender<bool>) {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let pool = RaftPool::new(&config(data_dir, raft_addr, stdb_url), shutdown_rx)
        .await
        .unwrap();
    (pool, shutdown_tx)
}

async fn wait_for_leader(pool: &RaftPool) {
    let raft = pool.get_raft(0).expect("shard 0 must exist");
    let deadline = Instant::now() + LEADER_TIMEOUT;
    while Instant::now() < deadline {
        if raft.metrics().borrow_watched().current_leader == Some(1) {
            return;
        }
        sleep(POLL_INTERVAL).await;
    }
    panic!("leader not elected within {LEADER_TIMEOUT:?}");
}

async fn write(pool: &RaftPool, payload: &'static [u8]) -> u64 {
    let log_id = pool
        .propose_write(0, Bytes::from_static(payload), DB_PATH)
        .await
        .unwrap();
    pool.wait_applied(0, log_id.index).await.unwrap();
    log_id.index
}

#[test]
fn restart_resumes_from_persisted_applied_state() {
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_string_lossy().into_owned();
    let raft_addr = free_udp_addr();

    // 第一次启动：初始化单节点集群并提交一条写，且标记该写已投递
    let first_index = {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (pool, shutdown) = start(&data_dir, &raft_addr, "127.0.0.1:1").await;
            pool.get_raft(0)
                .unwrap()
                .initialize(BTreeMap::from([(
                    1,
                    BasicNode {
                        addr: raft_addr.clone(),
                    },
                )]))
                .await
                .unwrap();
            wait_for_leader(&pool).await;
            let idx = write(&pool, b"\x03first").await;
            pool.mark_delivered(0, idx);
            let _ = shutdown.send(true);
            if let Some(r) = pool.get_raft(0) {
                let _ = r.shutdown().await;
            }
            idx
        })
    };

    // 第二次启动：先直接读回落盘水位，确认不是从 0 开始
    let second_index = {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            {
                let mut probe = StateMachineStore::open(dir.path(), 0).unwrap();
                let (applied, _) = probe.applied_state().await.unwrap();
                assert_eq!(
                    applied.map(|log_id| log_id.index),
                    Some(first_index),
                    "重启后 applied_state 必须是崩溃前的水位，否则会重放历史写"
                );
                assert_eq!(probe.last_delivered_index(), first_index);
            }

            let (pool, shutdown) = start(&data_dir, &raft_addr, "127.0.0.1:1").await;
            wait_for_leader(&pool).await;
            let idx = write(&pool, b"\x03second").await;
            let _ = shutdown.send(true);
            if let Some(r) = pool.get_raft(0) {
                let _ = r.shutdown().await;
            }
            idx
        })
    };

    assert!(
        second_index > first_index,
        "重启后应接着原日志续写（first={first_index}, second={second_index}）"
    );
}

#[test]
fn crash_before_delivery_replays_undelivered_entries_on_restart() {
    // 修复 P0-2 验证：源节点在 apply 落盘后、投递上游前崩溃，重启后自动重放 (last_delivered, last_applied]
    let dir = TempDir::new().unwrap();
    let data_dir = dir.path().to_string_lossy().into_owned();
    let raft_addr = free_udp_addr();

    // 阶段 1：写入两条记录，第一条标记已投递，第二条模拟崩溃未投递
    {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (pool, shutdown) = start(&data_dir, &raft_addr, "127.0.0.1:1").await;
            pool.get_raft(0)
                .unwrap()
                .initialize(BTreeMap::from([(
                    1,
                    BasicNode {
                        addr: raft_addr.clone(),
                    },
                )]))
                .await
                .unwrap();
            wait_for_leader(&pool).await;

            let idx1 = write(&pool, b"\x03delivered").await;
            pool.mark_delivered(0, idx1);

            let _idx2 = write(&pool, b"\x03undelivered").await;
            // 故意不调用 mark_delivered(0, _idx2)，模拟崩溃前未投递

            let _ = shutdown.send(true);
            if let Some(r) = pool.get_raft(0) {
                let _ = r.shutdown().await;
            }
        });
    }

    // 阶段 2：拉起假 SpacetimeDB 并重启节点，验证仅重放第二条未投递的写
    {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let stdb_addr = free_udp_addr();
            let session = open_server_session(&stdb_addr).await.unwrap();
            let (tx, rx) = unbounded_async();

            spawn(async move {
                let connect_qabl = session
                    .declare_queryable(KEY_STDB_CONNECT)
                    .with(CrossfireChannel::new())
                    .await
                    .unwrap();
                while let Ok(query) = connect_qabl.recv().await {
                    let Ok(_req) = decode_query_payload::<StdbConnectRequest>(&query) else {
                        continue;
                    };
                    let c2s_qabl = session
                        .declare_queryable("stdb/c2s/1")
                        .with(CrossfireChannel::new())
                        .await
                        .unwrap();
                    let tx = tx.clone();
                    spawn(async move {
                        while let Ok(q) = c2s_qabl.recv().await {
                            if let Ok(Message::Binary(b)) = decode_query_payload::<Message>(&q) {
                                let _ = tx.send(b);
                            }
                            reply_bitcode(&q, Ok(())).await;
                        }
                    })
                    .detach();
                    reply_bitcode(&query, Ok(StdbConnectResponse { conn_id: 1 })).await;
                }
            })
            .detach();

            let (pool, shutdown) = start(&data_dir, &raft_addr, &stdb_addr).await;
            let replayed = timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("should replay undelivered write")
                .unwrap();
            assert_eq!(&replayed[..], b"\x03undelivered");

            // 第一条已投递的写绝不会被重复投递
            assert!(
                timeout(Duration::from_millis(300), rx.recv())
                    .await
                    .is_err()
            );

            let _ = shutdown.send(true);
            if let Some(r) = pool.get_raft(0) {
                let _ = r.shutdown().await;
            }
        });
    }
}
