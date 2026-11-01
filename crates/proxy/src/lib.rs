//! spacetime-db-ha —— 基于 Raft 共识与 Zenoh 明文 QUIC 的 SpacetimeDB 分布式代理。
//!
//! 客户端把本代理当作 SpacetimeDB 连接；代理按 BSATN 标签字节区分读写：
//! 写请求走 Raft 复制后再落到本地 SpacetimeDB，读请求直接转发。

#![recursion_limit = "256"]

use rimalloc::Rimalloc;

#[global_allocator]
static ALLOC: Rimalloc = Rimalloc;

pub mod api;
pub mod config;
pub mod metrics;
pub mod raft;
pub mod router;
pub mod server;
pub mod transport;
pub mod websocket;
