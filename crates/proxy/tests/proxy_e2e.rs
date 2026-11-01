//! 端到端集成测试（Compio + Rimalloc + Zenoh + 明文 QUIC + `bitcode`）：
//! 1. 单节点客户端 -> 代理 -> Raft -> 本地 SpacetimeDB 读写分流、防重与 HTTP JSON `/metrics` 抓取；
//! 2. 3 节点集群共识复制、Follower 写自动转发 Leader、分片动态创建与级联删除（修复 P1-6 / P3-18）。

#![recursion_limit = "256"]

use std::{
    collections::BTreeMap,
    net::UdpSocket,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::spawn,
    time::{sleep, timeout},
};
use crossfire::{
    AsyncRx,
    mpsc::{List, unbounded_async},
};
use futures::{FutureExt, select};
use openraft::{BasicNode, async_runtime::watch::WatchReceiver};
use spacetime_db_ha::{
    api::{self, AddRouteRequest, CreateShardRequest, RemoveShardRequest, ShardListResponse},
    config::NodeConfig,
    raft::RaftPool,
    transport::{
        CrossfireChannel, ZenohClientPool, close_session, decode_query_payload,
        open_server_session, reply_bitcode, watch,
    },
    websocket::{
        Proxy,
        handler::{Message, StdbConnectRequest, StdbConnectResponse, ZenohStdbStream},
    },
};
use tempfile::TempDir;

/// 测试整体超时。
const TEST_TIMEOUT: Duration = Duration::from_secs(30);
/// 选主超时。
const LEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// 单条消息投递超时。
const MESSAGE_TIMEOUT: Duration = Duration::from_secs(5);
/// 轮询 leader 的间隔。
const LEADER_POLL: Duration = Duration::from_millis(50);
/// 等待「没有更多消息」的静默期。
const QUIET_PERIOD: Duration = Duration::from_millis(300);

fn free_udp_addr() -> String {
    let sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    sock.local_addr().unwrap().to_string()
}

/// 假 SpacetimeDB（Zenoh + 明文 QUIC）：把收到的二进制消息交给测试端。
async fn spawn_mock_stdb() -> (String, AsyncRx<List<Bytes>>, watch::Sender<bool>) {
    let addr = free_udp_addr();
    let session = open_server_session(&addr).await.unwrap();
    let (tx, rx) = unbounded_async();
    let (stop_tx, mut stop_rx) = watch::channel(false);

    spawn(async move {
        let connect_qabl = session
            .declare_queryable("stdb/connect")
            .with(CrossfireChannel::new())
            .await
            .unwrap();
        let mut next_conn: u64 = 1;
        loop {
            select! {
                q = connect_qabl.recv().fuse() => {
                    let Ok(query) = q else { break };
                    let Ok(req) = decode_query_payload::<StdbConnectRequest>(&query) else {
                        reply_bitcode::<()>(&query, Err("bad connect".to_owned())).await;
                        continue;
                    };
                    let conn_id = next_conn;
                    next_conn += 1;

                    let c2s_key = format!("stdb/c2s/{conn_id}");
                    let s2c_key = format!("stdb/s2c/{}", req.stream_id);
                    let c2s_qabl = session
                        .declare_queryable(&c2s_key)
                        .with(CrossfireChannel::new())
                        .await
                        .unwrap();
                    let tx = tx.clone();
                    let s_clone = session.clone();

                    spawn(async move {
                        while let Ok(msg_query) = c2s_qabl.recv().await {
                            if let Ok(msg) = decode_query_payload::<Message>(&msg_query) {
                                match msg {
                                    Message::Binary(data) => {
                                        let _ = tx.send(data);
                                    }
                                    Message::Close(_) => {
                                        reply_bitcode(&msg_query, Ok(())).await;
                                        break;
                                    }
                                    other => {
                                        let bytes = bitcode::encode(&other);
                                        let _ = s_clone.put(&s2c_key, bytes).await;
                                    }
                                }
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

/// 等指定分片选出 leader。
async fn wait_for_leader(pool: &RaftPool, shard_id: u64) -> u64 {
    let deadline = Instant::now() + LEADER_TIMEOUT;

    while Instant::now() < deadline {
        if let Some(raft) = pool.get_raft(shard_id)
            && let Some(leader) = raft.metrics().borrow_watched().current_leader
        {
            return leader;
        }
        sleep(LEADER_POLL).await;
    }
    panic!("leader not elected on shard {shard_id} within {LEADER_TIMEOUT:?}");
}

#[compio::test]
async fn writes_go_through_raft_and_reads_are_forwarded_once() {
    timeout(TEST_TIMEOUT, async {
        let data_dir = TempDir::new().unwrap();
        let (stdb_url, received, _stop_stdb) = spawn_mock_stdb().await;
        let proxy_addr = free_udp_addr();
        let raft_addr = free_udp_addr();

        let config = NodeConfig {
            node_id: 1,
            listen_addr: proxy_addr.clone(),
            raft_addr: raft_addr.clone(),
            raft_advertise_addr: None,
            stdb_url,
            peers: Vec::new(),
            data_dir: data_dir.path().to_string_lossy().into_owned(),
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pool = RaftPool::new(&config, shutdown_rx.clone()).await.unwrap();
        pool.get_raft(0)
            .unwrap()
            .initialize(BTreeMap::from([(
                1,
                BasicNode {
                    addr: config.advertise_raft_addr(),
                },
            )]))
            .await
            .unwrap();
        assert_eq!(wait_for_leader(&pool, 0).await, 1);

        let tcp_listener = TcpListener::bind(&raft_addr).await.unwrap();
        spawn(api::serve_metrics_tcp(
            tcp_listener,
            pool.clone(),
            shutdown_rx.clone(),
        ))
        .detach();

        let proxy_session = open_server_session(&proxy_addr).await.unwrap();
        spawn(Proxy::new(Arc::new(config), pool.clone(), shutdown_rx).serve_session(proxy_session))
            .detach();

        let client = ZenohStdbStream::connect(&proxy_addr, "/database/subscribe/testdb")
            .await
            .unwrap();

        // 写：先过 Raft，再由连接处理逻辑投递给上游
        let write = Bytes::from_static(&[3, 1, 2, 3]);
        client.send(Message::Binary(write.clone())).await.unwrap();
        assert_eq!(
            timeout(MESSAGE_TIMEOUT, received.recv())
                .await
                .unwrap()
                .unwrap(),
            write
        );

        // 读：直接转发
        let read = Bytes::from_static(&[0, 9]);
        client.send(Message::Binary(read.clone())).await.unwrap();
        assert_eq!(
            timeout(MESSAGE_TIMEOUT, received.recv())
                .await
                .unwrap()
                .unwrap(),
            read
        );

        // 本节点发起的写不应被转发任务重复投递
        assert!(
            timeout(QUIET_PERIOD, received.recv()).await.is_err(),
            "upstream received a duplicate write"
        );

        // 验证基于 compio + const &str 的 HTTP JSON /metrics 端点
        let mut metrics_conn = TcpStream::connect(&raft_addr).await.unwrap();
        metrics_conn
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .0
            .unwrap();
        let BufResult(Ok(n), resp_buf) = metrics_conn.read(Vec::with_capacity(1024)).await else {
            panic!("failed to read HTTP JSON metrics response");
        };
        let resp_str = String::from_utf8_lossy(&resp_buf[..n]);
        assert!(resp_str.starts_with("HTTP/1.1 200 OK\r\nContent-Type: application/json"));
        let (_, json_body) = resp_str.split_once("\r\n\r\n").unwrap();
        assert!(json_body.starts_with(r#"{"spacetime_db_ha_raft_term":"#));
        assert!(json_body.contains(r#""spacetime_db_ha_raft_is_leader":1"#));
        assert!(json_body.contains(r#""spacetime_db_ha_write_latency_seconds":{"buckets":{"#));
        assert!(json_body.ends_with('}'));

        client.close().await;
        let _ = shutdown_tx.send(true);
    })
    .await
    .expect("proxy end-to-end test timed out");
}

#[compio::test]
async fn three_node_cluster_replicates_writes_and_manages_shards_over_zenoh_quic() {
    // 修复 P3-18 / P1-6 / P1-8：验证 3 节点 Zenoh 明文 QUIC 集群选主、Follower 转发写、多节点复制以及分片创建与级联删除
    timeout(TEST_TIMEOUT, async {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let dir3 = TempDir::new().unwrap();

        let (stdb1, rx1, _stop1) = spawn_mock_stdb().await;
        let (stdb2, rx2, _stop2) = spawn_mock_stdb().await;
        let (stdb3, rx3, _stop3) = spawn_mock_stdb().await;

        let raft1 = free_udp_addr();
        let raft2 = free_udp_addr();
        let raft3 = free_udp_addr();
        let listen2 = free_udp_addr();

        let cfg1 = NodeConfig {
            node_id: 1,
            listen_addr: free_udp_addr(),
            raft_addr: raft1.clone(),
            raft_advertise_addr: Some(raft1.clone()),
            stdb_url: stdb1,
            peers: vec![format!("2={raft2}"), format!("3={raft3}")],
            data_dir: dir1.path().to_string_lossy().into_owned(),
        };
        let cfg2 = NodeConfig {
            node_id: 2,
            listen_addr: listen2.clone(),
            raft_addr: raft2.clone(),
            raft_advertise_addr: Some(raft2.clone()),
            stdb_url: stdb2,
            peers: vec![format!("1={raft1}"), format!("3={raft3}")],
            data_dir: dir2.path().to_string_lossy().into_owned(),
        };
        let cfg3 = NodeConfig {
            node_id: 3,
            listen_addr: free_udp_addr(),
            raft_addr: raft3.clone(),
            raft_advertise_addr: Some(raft3.clone()),
            stdb_url: stdb3,
            peers: vec![format!("1={raft1}"), format!("2={raft2}")],
            data_dir: dir3.path().to_string_lossy().into_owned(),
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let s1 = open_server_session(&raft1).await.unwrap();
        let s2 = open_server_session(&raft2).await.unwrap();
        let s3 = open_server_session(&raft3).await.unwrap();

        let pool1 = RaftPool::new(&cfg1, shutdown_rx.clone()).await.unwrap();
        let pool2 = RaftPool::new(&cfg2, shutdown_rx.clone()).await.unwrap();
        let pool3 = RaftPool::new(&cfg3, shutdown_rx.clone()).await.unwrap();

        spawn(api::serve_zenoh(s1, pool1.clone(), shutdown_rx.clone())).detach();
        spawn(api::serve_zenoh(s2, pool2.clone(), shutdown_rx.clone())).detach();
        spawn(api::serve_zenoh(s3, pool3.clone(), shutdown_rx.clone())).detach();

        let members: BTreeMap<u64, String> =
            BTreeMap::from([(1, raft1.clone()), (2, raft2.clone()), (3, raft3.clone())]);

        let zclient = ZenohClientPool::new();
        let _: String = zclient.rpc(&raft1, "cluster/init", &members).await.unwrap();

        let leader = wait_for_leader(&pool1, 0).await;
        assert_eq!(wait_for_leader(&pool2, 0).await, leader);
        assert_eq!(wait_for_leader(&pool3, 0).await, leader);

        // 在节点 2 上启动客户端代理，验证无论节点 2 是 Leader 还是 Follower，写都会复制到全部 3 个节点
        let proxy2_session = open_server_session(&listen2).await.unwrap();
        spawn(
            Proxy::new(Arc::new(cfg2), pool2.clone(), shutdown_rx.clone())
                .serve_session(proxy2_session),
        )
        .detach();

        let client = ZenohStdbStream::connect(&listen2, "/database/subscribe/testdb")
            .await
            .unwrap();
        let payload = Bytes::from_static(&[3, 0xCA, 0xFE]);
        client.send(Message::Binary(payload.clone())).await.unwrap();

        assert_eq!(
            timeout(MESSAGE_TIMEOUT, rx1.recv()).await.unwrap().unwrap(),
            payload
        );
        assert_eq!(
            timeout(MESSAGE_TIMEOUT, rx2.recv()).await.unwrap().unwrap(),
            payload
        );
        assert_eq!(
            timeout(MESSAGE_TIMEOUT, rx3.recv()).await.unwrap().unwrap(),
            payload
        );

        // 验证分片创建、路由绑定与分片级联删除（P1-6 & P1-7）
        let _: String = zclient
            .rpc(
                &raft1,
                "cluster/shards/create",
                &CreateShardRequest { shard_id: 2 },
            )
            .await
            .unwrap();
        wait_for_leader(&pool1, 2).await;

        let _: String = zclient
            .rpc(
                &raft1,
                "cluster/shards/route/add",
                &AddRouteRequest {
                    module_name: "combat".to_owned(),
                    shard_id: 2,
                },
            )
            .await
            .unwrap();

        let list_before: ShardListResponse =
            zclient.rpc(&raft1, "cluster/shards", &()).await.unwrap();
        assert!(list_before.shards.contains(&2));
        assert_eq!(list_before.routes.get("combat"), Some(&2));

        let _: String = zclient
            .rpc(
                &raft1,
                "cluster/shards/remove",
                &RemoveShardRequest { shard_id: 2 },
            )
            .await
            .unwrap();

        let list_after: ShardListResponse =
            zclient.rpc(&raft1, "cluster/shards", &()).await.unwrap();
        assert!(!list_after.shards.contains(&2));
        assert!(!list_after.routes.contains_key("combat"));

        client.close().await;
        let _ = shutdown_tx.send(true);
    })
    .await
    .expect("3-node e2e test timed out");
}
