use std::sync::{Arc, Mutex as StdMutex};

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{Mutex, watch::Receiver},
};
use tokio_tungstenite::tungstenite::{
    Message, http,
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use tracing::{debug, info, warn};

use crate::{metrics, raft::RaftPool, router};

/// SpacetimeDB v2 WebSocket protocol BSATN tag bytes.
/// These are the first byte of the binary message, indicating the ClientMessage variant.
pub mod stdb_tags {
    pub const SUBSCRIBE: u8 = 0;
    pub const UNSUBSCRIBE: u8 = 1;
    pub const ONE_OFF_QUERY: u8 = 2;
    pub const CALL_REDUCER: u8 = 3;
    pub const CALL_PROCEDURE: u8 = 4;
}

/// Message classification result for routing decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    /// Write operation — must go through Raft consensus before forwarding.
    Write,
    /// Read operation — forward directly to local SpacetimeDB.
    Read,
    /// Not a binary message or empty — forward as-is.
    PassThrough,
}

/// Classify a WebSocket message by reading the BSATN tag byte.
///
/// This is the core routing decision: one byte determines the entire path.
/// - Tag 3 (CallReducer) and 4 (CallProcedure) are writes -> Raft consensus
/// - Tags 0-2 (Subscribe, Unsubscribe, OneOffQuery) are reads -> direct forward
/// - Everything else (ping/pong/close/text/empty) -> pass through
pub fn classify_message(msg: &Message) -> MessageKind {
    match msg {
        Message::Binary(data) if !data.is_empty() => {
            let tag = data[0];
            if is_write_tag(tag) {
                MessageKind::Write
            } else {
                MessageKind::Read
            }
        }
        _ => MessageKind::PassThrough,
    }
}

/// Returns true if the BSATN tag byte indicates a write operation.
pub fn is_write_tag(tag: u8) -> bool {
    matches!(tag, stdb_tags::CALL_REDUCER | stdb_tags::CALL_PROCEDURE)
}

/// Returns true if the BSATN tag byte indicates a known read operation.
pub fn is_read_tag(tag: u8) -> bool {
    matches!(
        tag,
        stdb_tags::SUBSCRIBE | stdb_tags::UNSUBSCRIBE | stdb_tags::ONE_OFF_QUERY
    )
}

/// Returns true if the BSATN tag byte is a known SpacetimeDB v2 client message.
pub fn is_known_tag(tag: u8) -> bool {
    tag <= stdb_tags::CALL_PROCEDURE
}

/// Build a close frame with leader hint for client reconnection.
async fn leader_close_frame(pool: &RaftPool, shard_id: u64) -> Option<CloseFrame> {
    let (leader_id, leader_addr) = pool.current_leader(shard_id).await?;
    Some(CloseFrame {
        code: CloseCode::Away,
        reason: format!("leader={}:{}", leader_id, leader_addr).into(),
    })
}

/// Handle a single client WebSocket connection.
///
/// 1. Accept the client's WebSocket upgrade, extracting the request path
/// 2. Extract module name from path to determine shard routing
/// 3. Open a corresponding connection to the local SpacetimeDB using the same path
/// 4. For each message from the client:
///    - Classify via `classify_message` (reads one byte)
///    - Write -> propose through Raft (routed to correct shard), then forward
///    - Read/PassThrough -> forward directly
/// 5. Relay all SpacetimeDB responses back to the client
pub async fn handle_client(
    stream: TcpStream,
    pool: Arc<RaftPool>,
    stdb_url: &str,
    shutdown: Receiver<bool>,
) -> Result<()> {
    metrics::CONNECTIONS_ACTIVE.inc();

    let result = handle_client_inner(stream, pool.clone(), stdb_url, shutdown).await;

    metrics::CONNECTIONS_ACTIVE.dec();
    result
}

async fn handle_client_inner(
    stream: TcpStream,
    pool: Arc<RaftPool>,
    stdb_url: &str,
    mut shutdown: Receiver<bool>,
) -> Result<()> {
    // Extract the request URI path during the WebSocket handshake.
    // SpacetimeDB requires a path like /database/subscribe/MODULE_NAME.
    let path_holder: Arc<StdMutex<String>> = Arc::new(StdMutex::new(String::new()));
    let path_for_cb = path_holder.clone();

    let client_ws = tokio_tungstenite::accept_hdr_async(
        stream,
        move |req: &http::Request<()>,
              resp: http::Response<()>|
              -> Result<http::Response<()>, http::Response<Option<String>>> {
            let uri_path = req
                .uri()
                .path_and_query()
                .map(|pq| pq.as_str().to_string())
                .unwrap_or_else(|| "/".to_string());
            *path_for_cb.lock().unwrap() = uri_path;
            Ok(resp)
        },
    )
    .await?;

    let request_path = path_holder.lock().unwrap().clone();
    let upstream_url = format!("{}{}", stdb_url, request_path);
    info!(path = %request_path, upstream = %upstream_url, "Client connected, opening upstream");

    // Extract module name from path and determine shard
    let module_name = router::extract_module_name(&request_path);
    let shard_id = if let Some(name) = module_name {
        pool.route_module(name).await
    } else {
        0 // default shard
    };

    // Broadcast the database path to the shard's forwarder
    pool.send_db_path(shard_id, request_path).await;

    let (client_tx, mut client_rx) = client_ws.split();
    let client_tx = Arc::new(Mutex::new(client_tx));

    let (upstream_ws, _) = tokio_tungstenite::connect_async(&upstream_url).await?;
    let (upstream_tx, mut upstream_rx) = upstream_ws.split();
    let upstream_tx = Arc::new(Mutex::new(upstream_tx));

    // Client -> Proxy -> (Raft | direct) -> SpacetimeDB
    let pool_clone = pool.clone();
    let upstream_tx_clone = upstream_tx.clone();
    let inbound = tokio::spawn(async move {
        while let Some(msg) = client_rx.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Client read error");
                    break;
                }
            };

            let kind = classify_message(&msg);

            match kind {
                MessageKind::Write => {
                    if let Message::Binary(data) = &msg {
                        debug!(
                            tag = data[0],
                            len = data.len(),
                            shard_id,
                            "Write -> Raft consensus"
                        );
                    }
                    if let Message::Binary(data) = &msg
                        && let Err(e) = pool_clone.propose_write(shard_id, data.to_vec()).await
                    {
                        warn!(error = %e, "Raft propose failed");
                        continue;
                    }
                    if let Err(e) = upstream_tx_clone.lock().await.send(msg).await {
                        warn!(error = %e, "Upstream send failed");
                        break;
                    }
                }
                MessageKind::Read => {
                    metrics::READS_TOTAL.inc();
                    if let Err(e) = upstream_tx_clone.lock().await.send(msg).await {
                        warn!(error = %e, "Upstream send failed");
                        break;
                    }
                }
                MessageKind::PassThrough => {
                    if let Err(e) = upstream_tx_clone.lock().await.send(msg).await {
                        warn!(error = %e, "Upstream send failed");
                        break;
                    }
                }
            }
        }
    });

    // SpacetimeDB -> Proxy -> Client
    let client_tx_clone = client_tx.clone();
    let outbound = tokio::spawn(async move {
        while let Some(msg) = upstream_rx.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "Upstream read error");
                    break;
                }
            };
            if let Err(e) = client_tx_clone.lock().await.send(msg).await {
                warn!(error = %e, "Client send failed");
                break;
            }
        }
    });

    // Wait for either direction to end or a shutdown signal
    tokio::select! {
        _ = inbound => {},
        _ = outbound => {},
        _ = shutdown.changed() => {
            info!("Shutdown signal received, closing client connection");
            // Send close frame with leader hint for reconnection
            if let Some(close_frame) = leader_close_frame(&pool, shard_id).await {
                let _ = client_tx.lock().await
                    .send(Message::Close(Some(close_frame)))
                    .await;
            }
        },
    }

    Ok(())
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

    // -- Tag classification tests --

    #[test]
    fn test_call_reducer_is_write() {
        assert!(is_write_tag(stdb_tags::CALL_REDUCER));
    }

    #[test]
    fn test_call_procedure_is_write() {
        assert!(is_write_tag(stdb_tags::CALL_PROCEDURE));
    }

    #[test]
    fn test_subscribe_is_not_write() {
        assert!(!is_write_tag(stdb_tags::SUBSCRIBE));
    }

    #[test]
    fn test_unsubscribe_is_not_write() {
        assert!(!is_write_tag(stdb_tags::UNSUBSCRIBE));
    }

    #[test]
    fn test_one_off_query_is_not_write() {
        assert!(!is_write_tag(stdb_tags::ONE_OFF_QUERY));
    }

    #[test]
    fn test_read_tags() {
        assert!(is_read_tag(stdb_tags::SUBSCRIBE));
        assert!(is_read_tag(stdb_tags::UNSUBSCRIBE));
        assert!(is_read_tag(stdb_tags::ONE_OFF_QUERY));
        assert!(!is_read_tag(stdb_tags::CALL_REDUCER));
        assert!(!is_read_tag(stdb_tags::CALL_PROCEDURE));
    }

    #[test]
    fn test_known_tags() {
        for tag in 0..=4u8 {
            assert!(is_known_tag(tag), "tag {tag} should be known");
        }
        for tag in 5..=255u8 {
            assert!(!is_known_tag(tag), "tag {tag} should be unknown");
        }
    }

    #[test]
    fn test_unknown_tags_are_not_writes() {
        for tag in 5..=255u8 {
            assert!(!is_write_tag(tag), "tag {tag} should not be a write");
        }
    }

    // -- Message classification tests --

    #[test]
    fn test_classify_call_reducer_message() {
        // Tag 3 = CallReducer, followed by dummy payload
        let msg = Message::Binary(vec![3, 0, 1, 2, 3].into());
        assert_eq!(classify_message(&msg), MessageKind::Write);
    }

    #[test]
    fn test_classify_call_procedure_message() {
        let msg = Message::Binary(vec![4, 10, 20].into());
        assert_eq!(classify_message(&msg), MessageKind::Write);
    }

    #[test]
    fn test_classify_subscribe_message() {
        let msg = Message::Binary(vec![0, 5, 6, 7].into());
        assert_eq!(classify_message(&msg), MessageKind::Read);
    }

    #[test]
    fn test_classify_unsubscribe_message() {
        let msg = Message::Binary(vec![1, 99].into());
        assert_eq!(classify_message(&msg), MessageKind::Read);
    }

    #[test]
    fn test_classify_one_off_query_message() {
        let msg = Message::Binary(vec![2, 0, 0, 0].into());
        assert_eq!(classify_message(&msg), MessageKind::Read);
    }

    #[test]
    fn test_classify_empty_binary_is_passthrough() {
        let msg = Message::Binary(vec![].into());
        assert_eq!(classify_message(&msg), MessageKind::PassThrough);
    }

    #[test]
    fn test_classify_text_is_passthrough() {
        let msg = Message::Text("hello".into());
        assert_eq!(classify_message(&msg), MessageKind::PassThrough);
    }

    #[test]
    fn test_classify_ping_is_passthrough() {
        let msg = Message::Ping(vec![].into());
        assert_eq!(classify_message(&msg), MessageKind::PassThrough);
    }

    #[test]
    fn test_classify_pong_is_passthrough() {
        let msg = Message::Pong(vec![].into());
        assert_eq!(classify_message(&msg), MessageKind::PassThrough);
    }

    #[test]
    fn test_classify_close_is_passthrough() {
        let msg = Message::Close(None);
        assert_eq!(classify_message(&msg), MessageKind::PassThrough);
    }

    #[test]
    fn test_classify_unknown_tag_is_read() {
        // Unknown tags (>4) are treated as reads (forwarded directly, not replicated).
        // This is safe because SpacetimeDB will handle or reject them.
        let msg = Message::Binary(vec![255, 0, 0].into());
        assert_eq!(classify_message(&msg), MessageKind::Read);
    }

    #[test]
    fn test_classify_single_byte_write() {
        // Minimal valid write message: just the tag byte
        let msg = Message::Binary(vec![3].into());
        assert_eq!(classify_message(&msg), MessageKind::Write);
    }

    // -- Property: writes and reads are mutually exclusive --

    #[test]
    fn test_write_and_read_tags_are_disjoint() {
        for tag in 0..=255u8 {
            assert!(
                !(is_write_tag(tag) && is_read_tag(tag)),
                "tag {tag} classified as both read and write"
            );
        }
    }

    // -- Fuzz-like: every possible tag byte --

    #[test]
    fn test_all_256_tags_classify_without_panic() {
        for tag in 0..=255u8 {
            let msg = Message::Binary(vec![tag, 0, 0, 0].into());
            let kind = classify_message(&msg);
            // Should never panic, and should return a valid enum
            match kind {
                MessageKind::Write | MessageKind::Read | MessageKind::PassThrough => {}
            }
        }
    }
}
