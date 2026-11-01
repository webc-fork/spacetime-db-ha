use std::sync::LazyLock;

use prometheus::{Histogram, IntCounter, IntGauge, TextEncoder};

pub static RAFT_TERM: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!("rafttimedb_raft_term", "Current Raft term").unwrap()
});

pub static RAFT_IS_LEADER: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "rafttimedb_raft_is_leader",
        "Whether this node is the Raft leader (1=yes, 0=no)"
    )
    .unwrap()
});

pub static CONNECTIONS_ACTIVE: LazyLock<IntGauge> = LazyLock::new(|| {
    prometheus::register_int_gauge!(
        "rafttimedb_connections_active",
        "Number of active WebSocket client connections"
    )
    .unwrap()
});

pub static WRITES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "rafttimedb_writes_total",
        "Total write operations proposed through Raft"
    )
    .unwrap()
});

pub static READS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "rafttimedb_reads_total",
        "Total read operations forwarded directly to SpacetimeDB"
    )
    .unwrap()
});

pub static WRITE_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
    prometheus::register_histogram!(
        "rafttimedb_write_latency_seconds",
        "Write operation latency (Raft consensus + forward)",
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .unwrap()
});

pub static ENTRIES_APPLIED: LazyLock<IntCounter> = LazyLock::new(|| {
    prometheus::register_int_counter!(
        "rafttimedb_entries_applied_total",
        "Total Raft log entries applied to state machine"
    )
    .unwrap()
});

pub fn encode_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = String::new();
    encoder.encode_utf8(&metric_families, &mut buffer).unwrap();
    buffer
}
