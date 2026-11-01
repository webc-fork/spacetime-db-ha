//! 写转发：不同数据库各用一条上游 Zenoh 连接，库内保序、跨库不串写。

#![recursion_limit = "256"]

use std::{
    net::UdpSocket,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use compio::{runtime::spawn, time::timeout};
use crossfire::{
    AsyncRx,
    mpsc::{List, bounded_async, unbounded_async},
};
use futures::{FutureExt, select};
use spacetime_db_ha::{
    raft::{CommittedWrite, forwarder},
    transport::{
        CrossfireChannel, KEY_STDB_CONNECT, STDB_C2S_PREFIX, close_session, decode_query_payload,
        open_server_session, prefix_u64_key, reply_bitcode, watch,
    },
    websocket::handler::{Message, StdbConnectRequest, StdbConnectResponse},
};

/// 单条消息等待上限。
const RECV_TIMEOUT: Duration = Duration::from_secs(5);
/// 本节点 ID。
const LOCAL_NODE: u64 = 1;
/// 其它节点 ID。
const REMOTE_NODE: u64 = 2;

fn free_udp_addr() -> String {
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.local_addr().unwrap().to_string()
}

/// 假 SpacetimeDB（Zenoh + 明文 QUIC）：按接入顺序给连接编号，记录 (连接号, 消息)。
async fn spawn_mock_stdb() -> (String, AsyncRx<List<(usize, Vec<u8>)>>, watch::Sender<bool>) {
    let addr = free_udp_addr();
    let session = open_server_session(&addr).await.unwrap();
    let (tx, rx) = unbounded_async();
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let next_conn = Arc::new(AtomicUsize::new(0));

    spawn(async move {
        let connect_qabl = session
            .declare_queryable(KEY_STDB_CONNECT)
            .with(CrossfireChannel::new())
            .await
            .unwrap();
        loop {
            select! {
                q = connect_qabl.recv().fuse() => {
                    let Ok(query) = q else { break };
                    let Ok(_req) = decode_query_payload::<StdbConnectRequest>(&query) else {
                        reply_bitcode::<()>(&query, Err("bad request".to_owned())).await;
                        continue;
                    };
                    let conn_idx = next_conn.fetch_add(1, Ordering::Relaxed);
                    let conn_id = (conn_idx + 1) as u64;
                    let c2s_key = prefix_u64_key(STDB_C2S_PREFIX, conn_id);
                    let c2s_qabl = session
                        .declare_queryable(&c2s_key)
                        .with(CrossfireChannel::new())
                        .await
                        .unwrap();
                    let tx = tx.clone();

                    spawn(async move {
                        while let Ok(msg_query) = c2s_qabl.recv().await {
                            if let Ok(Message::Binary(data)) = decode_query_payload::<Message>(&msg_query) {
                                let _ = tx.send((conn_idx, data.to_vec()));
                            }
                            reply_bitcode(&msg_query, Ok(())).await;
                        }
                    })
                    .detach();

                    reply_bitcode(&query, Ok(StdbConnectResponse { conn_id })).await;
                }
                _ = stop_rx.changed().fuse() => break,
            }
        }
        close_session(&session);
    })
    .detach();

    (addr, rx, stop_tx)
}

fn write(log_index: u64, origin_node_id: u64, db_path: &str, payload: &[u8]) -> CommittedWrite {
    CommittedWrite {
        log_index,
        origin_node_id,
        db_path: db_path.to_owned(),
        raw_message: Bytes::copy_from_slice(payload),
    }
}

#[compio::test]
async fn each_database_gets_its_own_upstream_connection() {
    let (stdb_url, received, _stop_stdb) = spawn_mock_stdb().await;
    let (write_tx, write_rx) = bounded_async(64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = spawn(forwarder::run(LOCAL_NODE, stdb_url, write_rx, shutdown_rx));

    write_tx
        .send(write(1, REMOTE_NODE, "/db-one", b"\x01"))
        .await
        .unwrap();
    let (conn_one, first) = timeout(RECV_TIMEOUT, received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, b"\x01");

    write_tx
        .send(write(2, REMOTE_NODE, "/db-two", b"\x02"))
        .await
        .unwrap();
    let (conn_two, second) = timeout(RECV_TIMEOUT, received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second, b"\x02");
    assert_ne!(
        conn_one, conn_two,
        "不同数据库必须使用不同的上游连接，否则会写错库"
    );

    // 同一数据库的后续写复用同一条连接，并保持顺序
    write_tx
        .send(write(3, REMOTE_NODE, "/db-one", b"\x03"))
        .await
        .unwrap();
    write_tx
        .send(write(4, REMOTE_NODE, "/db-one", b"\x04"))
        .await
        .unwrap();
    for expected in [b"\x03", b"\x04"] {
        let (conn, msg) = timeout(RECV_TIMEOUT, received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(conn, conn_one);
        assert_eq!(msg, expected);
    }

    let _ = shutdown_tx.send(true);
    let _ = task.await;
}

#[compio::test]
async fn locally_originated_writes_are_not_forwarded() {
    let (stdb_url, received, _stop_stdb) = spawn_mock_stdb().await;
    let (write_tx, write_rx) = bounded_async(64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = spawn(forwarder::run(LOCAL_NODE, stdb_url, write_rx, shutdown_rx));

    // 本节点提交的写由客户端连接自己直投，转发任务必须跳过，否则会执行两次
    write_tx
        .send(write(1, LOCAL_NODE, "/db-one", b"\x01"))
        .await
        .unwrap();
    write_tx
        .send(write(2, REMOTE_NODE, "/db-one", b"\x02"))
        .await
        .unwrap();

    let (_, forwarded) = timeout(RECV_TIMEOUT, received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(forwarded, b"\x02");
    assert!(
        timeout(Duration::from_millis(300), received.recv())
            .await
            .is_err(),
        "本节点提交的写不应被转发任务重复投递"
    );

    let _ = shutdown_tx.send(true);
    let _ = task.await;
}
