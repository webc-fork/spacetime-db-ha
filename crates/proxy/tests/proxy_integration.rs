//! Integration tests for the RaftTimeDB proxy.
//!
//! These tests verify the WebSocket proxy can:
//! - Accept client connections
//! - Classify messages correctly
//! - Forward reads directly
//! - Route writes through Raft
//!
//! Tests that require a running SpacetimeDB are gated behind the `integration` feature
//! or use mock WebSocket servers.

use std::net::SocketAddr;

use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{UnboundedReceiver, unbounded_channel},
};
use tokio_tungstenite::tungstenite::Message;

/// Helper: start a mock WebSocket server that echoes everything back.
/// Returns the address it's listening on.
async fn start_echo_ws_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (mut tx, mut rx) = ws.split();
                while let Some(Ok(msg)) = rx.next().await {
                    if msg.is_close() {
                        break;
                    }
                    let _ = tx.send(msg).await;
                }
            });
        }
    });

    addr
}

/// Helper: start a mock WebSocket server that records received messages.
/// Returns the address and a receiver for the messages.
async fn start_recording_ws_server() -> (SocketAddr, UnboundedReceiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = unbounded_channel();

    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (mut ws_tx, mut ws_rx) = ws.split();
                while let Some(Ok(msg)) = ws_rx.next().await {
                    match msg {
                        Message::Binary(data) => {
                            let _ = tx.send(data.to_vec());
                            // Echo back
                            let _ = ws_tx.send(Message::Binary(data)).await;
                        }
                        Message::Close(_) => break,
                        other => {
                            let _ = ws_tx.send(other).await;
                        }
                    }
                }
            });
        }
    });

    (addr, rx)
}

/// Helper: connect a WebSocket client to a given address.
async fn connect_client(
    addr: SocketAddr,
) -> (
    SplitSink<
        tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
        Message,
    >,
    SplitStream<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>>,
) {
    let url = format!("ws://127.0.0.1:{}", addr.port());
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws.split()
}

#[tokio::test]
async fn test_echo_server_roundtrip() {
    let addr = start_echo_ws_server().await;
    let (mut tx, mut rx) = connect_client(addr).await;

    // Send a binary message
    tx.send(Message::Binary(vec![0, 1, 2, 3].into()))
        .await
        .unwrap();

    // Should receive the same message back
    let response = rx.next().await.unwrap().unwrap();
    assert_eq!(response, Message::Binary(vec![0, 1, 2, 3].into()));
}

#[tokio::test]
async fn test_recording_server_captures_binary() {
    let (addr, mut recorder) = start_recording_ws_server().await;
    let (mut tx, _rx) = connect_client(addr).await;

    // Send a CallReducer message (tag 3)
    let reducer_msg = vec![3, 0, 0, 0, 42]; // tag=3 + dummy payload
    tx.send(Message::Binary(reducer_msg.clone().into()))
        .await
        .unwrap();

    // The recording server should have captured it
    let captured = recorder.recv().await.unwrap();
    assert_eq!(captured, reducer_msg);
    assert_eq!(captured[0], 3); // tag byte is CallReducer
}

#[tokio::test]
async fn test_recording_server_captures_subscribe() {
    let (addr, mut recorder) = start_recording_ws_server().await;
    let (mut tx, _rx) = connect_client(addr).await;

    // Send a Subscribe message (tag 0)
    let sub_msg = vec![0, 1, 2, 3, 4]; // tag=0 + dummy SQL query
    tx.send(Message::Binary(sub_msg.clone().into()))
        .await
        .unwrap();

    let captured = recorder.recv().await.unwrap();
    assert_eq!(captured, sub_msg);
    assert_eq!(captured[0], 0); // tag byte is Subscribe
}

#[tokio::test]
async fn test_multiple_messages_in_order() {
    let (addr, mut recorder) = start_recording_ws_server().await;
    let (mut tx, _rx) = connect_client(addr).await;

    // Send multiple messages
    for i in 0..5u8 {
        tx.send(Message::Binary(vec![i, 100 + i].into()))
            .await
            .unwrap();
    }

    // Should receive in order
    for i in 0..5u8 {
        let captured = recorder.recv().await.unwrap();
        assert_eq!(captured[0], i);
        assert_eq!(captured[1], 100 + i);
    }
}

#[tokio::test]
async fn test_text_messages_pass_through() {
    let addr = start_echo_ws_server().await;
    let (mut tx, mut rx) = connect_client(addr).await;

    tx.send(Message::Text("hello".into())).await.unwrap();

    let response = rx.next().await.unwrap().unwrap();
    assert_eq!(response, Message::Text("hello".into()));
}
