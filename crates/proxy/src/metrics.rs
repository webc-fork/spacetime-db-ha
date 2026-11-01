//! 基于 Rust 标准库原子类型与编译期 `const &str` 静态片段实现的零锁最小开销 JSON 指标收集与导出器。
//!
//! 所有指标均为 `const` 构造的静态实例，不依赖 `lazy_static`、`LazyLock` 或 `serde_json`；
//! 导出 JSON 时通过编译期静态字符串片段拼接 + `itoa` / `zmij` 栈上缓冲直接写入单次预分配 `String`。

use std::{
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
    time::{Duration, Instant},
};

use itoa::Buffer as ItoaBuffer;
use zmij::Buffer as ZmijBuffer;

/// 直方图分桶数量。
const BUCKET_COUNT: usize = 9;

/// 每秒纳秒数（浮点转换因子）。
const NANOS_PER_SEC: f64 = 1_000_000_000.0;

/// JSON 指标导出预分配容量（字节）。
const METRICS_BUF_CAPACITY: usize = 512;

/// 写操作延迟直方图桶上界（纳秒，编译期常量，热路径纯整数比较）。
const LATENCY_BUCKETS_NANOS: [u64; BUCKET_COUNT] = [
    1_000_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
];

/// 编译期静态 JSON 键名前缀片段（零运行时转义或反射开销）。
const JSON_P0: &str = r#"{"spacetime_db_ha_raft_term":"#;
const JSON_P1: &str = r#","spacetime_db_ha_raft_is_leader":"#;
const JSON_P2: &str = r#","spacetime_db_ha_shards_active":"#;
const JSON_P3: &str = r#","spacetime_db_ha_delivery_lag_entries":"#;
const JSON_P4: &str = r#","spacetime_db_ha_connections_active":"#;
const JSON_P5: &str = r#","spacetime_db_ha_writes_total":"#;
const JSON_P6: &str = r#","spacetime_db_ha_reads_total":"#;
const JSON_P7: &str = r#","spacetime_db_ha_write_latency_seconds":{"buckets":{"#;

/// 编译期静态直方图分桶 JSON 键名片段。
const JSON_BUCKET_PREFIXES: [&str; BUCKET_COUNT] = [
    r#""0.001":"#,
    r#","0.005":"#,
    r#","0.01":"#,
    r#","0.025":"#,
    r#","0.05":"#,
    r#","0.1":"#,
    r#","0.25":"#,
    r#","0.5":"#,
    r#","1.0":"#,
];

const JSON_BUCKET_INF: &str = r#","+Inf":"#;
const JSON_SUM: &str = r#"},"sum":"#;
const JSON_COUNT: &str = r#","count":"#;
const JSON_ENTRIES_APPLIED: &str = r#"},"spacetime_db_ha_entries_applied_total":"#;
const JSON_SUFFIX: &str = "}";

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

    /// 记录一次观测值（秒）。
    pub fn observe(&self, secs: f64) {
        let nanos = (secs.max(0.0) * NANOS_PER_SEC) as u64;
        self.observe_nanos(nanos);
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
pub static CONNECTIONS_ACTIVE: IntGauge = IntGauge::new();
pub static WRITES_TOTAL: IntCounter = IntCounter::new();
pub static READS_TOTAL: IntCounter = IntCounter::new();
pub static WRITE_LATENCY: Histogram = Histogram::new();
pub static ENTRIES_APPLIED: IntCounter = IntCounter::new();

/// 导出所有指标为最小开销 JSON 字符串（编译期 `const &str` 静态片段 + `itoa` + `zmij`）。
pub fn encode_metrics() -> String {
    let mut out = String::with_capacity(METRICS_BUF_CAPACITY);
    let mut ibuf = ItoaBuffer::new();
    let mut fbuf = ZmijBuffer::new();

    out.push_str(JSON_P0);
    out.push_str(ibuf.format(RAFT_TERM.get()));
    out.push_str(JSON_P1);
    out.push_str(ibuf.format(RAFT_IS_LEADER.get()));
    out.push_str(JSON_P2);
    out.push_str(ibuf.format(SHARDS_ACTIVE.get()));
    out.push_str(JSON_P3);
    out.push_str(ibuf.format(DELIVERY_LAG.get()));
    out.push_str(JSON_P4);
    out.push_str(ibuf.format(CONNECTIONS_ACTIVE.get()));
    out.push_str(JSON_P5);
    out.push_str(ibuf.format(WRITES_TOTAL.get()));
    out.push_str(JSON_P6);
    out.push_str(ibuf.format(READS_TOTAL.get()));
    out.push_str(JSON_P7);

    let mut cumulative: u64 = 0;
    for (bucket, prefix) in WRITE_LATENCY.buckets.iter().zip(&JSON_BUCKET_PREFIXES) {
        cumulative += bucket.load(Ordering::Relaxed);
        out.push_str(prefix);
        out.push_str(ibuf.format(cumulative));
    }
    let count = WRITE_LATENCY.count.load(Ordering::Relaxed).max(cumulative);
    out.push_str(JSON_BUCKET_INF);
    out.push_str(ibuf.format(count));

    let sum_secs = WRITE_LATENCY.sum_nanos.load(Ordering::Relaxed) as f64 / NANOS_PER_SEC;
    out.push_str(JSON_SUM);
    out.push_str(fbuf.format(sum_secs));
    out.push_str(JSON_COUNT);
    out.push_str(ibuf.format(count));
    out.push_str(JSON_ENTRIES_APPLIED);
    out.push_str(ibuf.format(ENTRIES_APPLIED.get()));
    out.push_str(JSON_SUFFIX);

    out
}
