//! 基于 Rust 标准库原子类型与 `askama::Template` 编译期模板实现的零锁最小开销 JSON 指标收集与导出器。
//!
//! 所有指标均为 `const` 构造的静态实例，不依赖 `lazy_static`、`LazyLock` 或 `serde_json`；
//! 导出 JSON 时通过 `askama::Template` 编译期静态模板 + `zmij` 栈上浮点格式化直接渲染为单次预分配 `String`。

use std::{
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use askama::Template;
use zmij::Buffer as ZmijBuffer;

/// 直方图分桶数量。
const BUCKET_COUNT: usize = 9;

/// 每秒纳秒数（浮点转换因子）。
const NANOS_PER_SEC: f64 = 1_000_000_000.0;

/// 写操作延迟直方图桶上界（毫秒）。
const LATENCY_BUCKETS_MS: [u64; BUCKET_COUNT] = [1, 5, 10, 25, 50, 100, 250, 500, 1_000];

/// 编译期将毫秒数组转换为纳秒数组，保证桶定义单一事实来源且热路径纯整数比较。
const fn ms_array_to_nanos<const N: usize>(ms: [u64; N]) -> [u64; N] {
    let mut out = [0u64; N];
    let mut i = 0;
    while i < N {
        out[i] = Duration::from_millis(ms[i]).as_nanos() as u64;
        i += 1;
    }
    out
}

/// 写操作延迟直方图桶上界（纳秒，编译期由 `ms_array_to_nanos` 计算）。
const LATENCY_BUCKETS_NANOS: [u64; BUCKET_COUNT] = ms_array_to_nanos(LATENCY_BUCKETS_MS);

/// 编译期预编译的指标 JSON 导出模板（由 `askama` 在编译期展开为定长缓冲预分配与静态切片直写）。
#[derive(Template)]
#[template(
    source = r#"{"spacetime_db_ha_raft_term":{{ raft_term }},"spacetime_db_ha_raft_is_leader":{{ raft_is_leader }},"spacetime_db_ha_shards_active":{{ shards_active }},"spacetime_db_ha_delivery_lag_entries":{{ delivery_lag }},"spacetime_db_ha_db_stale_shards":{{ db_stale_shards }},"spacetime_db_ha_delivery_backpressure_total":{{ delivery_backpressure }},"spacetime_db_ha_connections_active":{{ connections_active }},"spacetime_db_ha_writes_total":{{ writes_total }},"spacetime_db_ha_reads_total":{{ reads_total }},"spacetime_db_ha_write_latency_seconds":{"buckets":{"0.001":{{ buckets[0] }},"0.005":{{ buckets[1] }},"0.01":{{ buckets[2] }},"0.025":{{ buckets[3] }},"0.05":{{ buckets[4] }},"0.1":{{ buckets[5] }},"0.25":{{ buckets[6] }},"0.5":{{ buckets[7] }},"1.0":{{ buckets[8] }},"+Inf":{{ count }}},"sum":{{ sum_secs }},"count":{{ count }}},"spacetime_db_ha_entries_applied_total":{{ entries_applied }}}"#,
    ext = "txt"
)]
struct MetricsJsonTemplate<'a> {
    raft_term: i64,
    raft_is_leader: i64,
    shards_active: i64,
    delivery_lag: i64,
    db_stale_shards: i64,
    delivery_backpressure: u64,
    connections_active: i64,
    writes_total: u64,
    reads_total: u64,
    buckets: [u64; BUCKET_COUNT],
    count: u64,
    sum_secs: &'a str,
    entries_applied: u64,
}

/// 无锁整型 Gauge 指标。
pub struct IntGauge {
    val: AtomicI64,
}

impl Default for IntGauge {
    fn default() -> Self {
        Self::new()
    }
}

impl IntGauge {
    pub const fn new() -> Self {
        Self {
            val: AtomicI64::new(0),
        }
    }

    pub fn set(&self, v: i64) {
        self.val.store(v, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.val.load(Ordering::Relaxed)
    }

    pub fn inc(&self) {
        self.val.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec(&self) {
        self.val.fetch_sub(1, Ordering::Relaxed);
    }
}

/// 无锁整型单调递增 Counter 指标。
pub struct IntCounter {
    val: AtomicU64,
}

impl Default for IntCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl IntCounter {
    pub const fn new() -> Self {
        Self {
            val: AtomicU64::new(0),
        }
    }

    pub fn inc(&self) {
        self.val.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_by(&self, v: u64) {
        self.val.fetch_add(v, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.val.load(Ordering::Relaxed)
    }
}

/// 无锁直方图指标（固定 9 个非累积桶，写入仅单次原子自增，导出时前缀求和）。
pub struct Histogram {
    buckets: [AtomicU64; BUCKET_COUNT],
    count: AtomicU64,
    sum_nanos: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; BUCKET_COUNT],
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
        }
    }

    /// 启动一次耗时测量，Drop 时自动将耗时记入对应桶。
    pub fn start_timer(&'static self) -> HistogramTimer {
        HistogramTimer {
            hist: self,
            start: Instant::now(),
        }
    }

    /// 记录一次 `Duration` 耗时（热路径纯整数运算，单桶原子更新）。
    pub fn observe_duration(&self, elapsed: Duration) {
        self.observe_nanos(elapsed.as_nanos() as u64);
    }

    fn observe_nanos(&self, nanos: u64) {
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        let idx = LATENCY_BUCKETS_NANOS.partition_point(|&bound| bound < nanos);
        if let Some(bucket) = self.buckets.get(idx) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// 直方图自动计时守卫。
pub struct HistogramTimer {
    hist: &'static Histogram,
    start: Instant,
}

impl Drop for HistogramTimer {
    fn drop(&mut self) {
        self.hist.observe_duration(self.start.elapsed());
    }
}

pub static RAFT_TERM: IntGauge = IntGauge::new();
pub static RAFT_IS_LEADER: IntGauge = IntGauge::new();
pub static SHARDS_ACTIVE: IntGauge = IntGauge::new();
pub static DELIVERY_LAG: IntGauge = IntGauge::new();
pub static DB_STALE_SHARDS: IntGauge = IntGauge::new();
pub static DELIVERY_BACKPRESSURE: IntCounter = IntCounter::new();
pub static CONNECTIONS_ACTIVE: IntGauge = IntGauge::new();
pub static WRITES_TOTAL: IntCounter = IntCounter::new();
pub static READS_TOTAL: IntCounter = IntCounter::new();
pub static WRITE_LATENCY: Histogram = Histogram::new();
pub static ENTRIES_APPLIED: IntCounter = IntCounter::new();

/// 导出所有指标为最小开销 JSON 字符串（基于 `askama::Template` 编译期模板 + `zmij`）。
pub fn encode_metrics() -> String {
    let mut buckets = [0u64; BUCKET_COUNT];
    let mut cumulative: u64 = 0;
    for (dst, src) in buckets.iter_mut().zip(&WRITE_LATENCY.buckets) {
        cumulative += src.load(Ordering::Relaxed);
        *dst = cumulative;
    }
    let count = WRITE_LATENCY.count.load(Ordering::Relaxed).max(cumulative);
    let sum_secs_val = WRITE_LATENCY.sum_nanos.load(Ordering::Relaxed) as f64 / NANOS_PER_SEC;

    let mut fbuf = ZmijBuffer::new();
    let tpl = MetricsJsonTemplate {
        raft_term: RAFT_TERM.get(),
        raft_is_leader: RAFT_IS_LEADER.get(),
        shards_active: SHARDS_ACTIVE.get(),
        delivery_lag: DELIVERY_LAG.get(),
        db_stale_shards: DB_STALE_SHARDS.get(),
        delivery_backpressure: DELIVERY_BACKPRESSURE.get(),
        connections_active: CONNECTIONS_ACTIVE.get(),
        writes_total: WRITES_TOTAL.get(),
        reads_total: READS_TOTAL.get(),
        buckets,
        count,
        sum_secs: fbuf.format(sum_secs_val),
        entries_applied: ENTRIES_APPLIED.get(),
    };
    tpl.render().unwrap_or_default()
}
