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

// ============================================================================
// 编译期公共 JSON 指标键名与结构片段（基于 const_str 组合）
// ============================================================================

/// 指标公共命名空间前缀。
const METRIC_NS: &str = "spacetime_db_ha_";
/// JSON 首键定界符。
const JSON_FIRST_KEY_START: &str = "{\"";
/// JSON 后续键定界符。
const JSON_NEXT_KEY_START: &str = ",\"";
/// JSON 键名结束定界符。
const JSON_KEY_END: &str = "\":";

/// 编译期静态 JSON 键名前缀片段（零运行时转义或反射开销）。
const JSON_P0: &str =
    const_str::concat!(JSON_FIRST_KEY_START, METRIC_NS, "raft_term", JSON_KEY_END);
const JSON_P1: &str = const_str::concat!(
    JSON_NEXT_KEY_START,
    METRIC_NS,
    "raft_is_leader",
    JSON_KEY_END
);
const JSON_P2: &str = const_str::concat!(
    JSON_NEXT_KEY_START,
    METRIC_NS,
    "shards_active",
    JSON_KEY_END
);
const JSON_P3: &str = const_str::concat!(
    JSON_NEXT_KEY_START,
    METRIC_NS,
    "delivery_lag_entries",
    JSON_KEY_END
);
const JSON_P4: &str = const_str::concat!(
    JSON_NEXT_KEY_START,
    METRIC_NS,
    "connections_active",
    JSON_KEY_END
);
const JSON_P5: &str =
    const_str::concat!(JSON_NEXT_KEY_START, METRIC_NS, "writes_total", JSON_KEY_END);
const JSON_P6: &str =
    const_str::concat!(JSON_NEXT_KEY_START, METRIC_NS, "reads_total", JSON_KEY_END);
const JSON_P7: &str = const_str::concat!(
    JSON_NEXT_KEY_START,
    METRIC_NS,
    "write_latency_seconds",
    JSON_KEY_END,
    JSON_FIRST_KEY_START,
    "buckets",
    JSON_KEY_END
);

/// 编译期静态直方图分桶 JSON 键名片段。
const JSON_BUCKET_PREFIXES: [&str; BUCKET_COUNT] = [
    const_str::concat!(JSON_FIRST_KEY_START, "0.001", JSON_KEY_END),
    const_str::concat!(JSON_NEXT_KEY_START, "0.005", JSON_KEY_END),
    const_str::concat!(JSON_NEXT_KEY_START, "0.01", JSON_KEY_END),
    const_str::concat!(JSON_NEXT_KEY_START, "0.025", JSON_KEY_END),
    const_str::concat!(JSON_NEXT_KEY_START, "0.05", JSON_KEY_END),
    const_str::concat!(JSON_NEXT_KEY_START, "0.1", JSON_KEY_END),
    const_str::concat!(JSON_NEXT_KEY_START, "0.25", JSON_KEY_END),
    const_str::concat!(JSON_NEXT_KEY_START, "0.5", JSON_KEY_END),
    const_str::concat!(JSON_NEXT_KEY_START, "1.0", JSON_KEY_END),
];

const JSON_BUCKET_INF: &str = const_str::concat!(JSON_NEXT_KEY_START, "+Inf", JSON_KEY_END);
const JSON_SUM: &str = const_str::concat!("}", JSON_NEXT_KEY_START, "sum", JSON_KEY_END);
const JSON_COUNT: &str = const_str::concat!(JSON_NEXT_KEY_START, "count", JSON_KEY_END);
const JSON_ENTRIES_APPLIED: &str = const_str::concat!(
    "}",
    JSON_NEXT_KEY_START,
    METRIC_NS,
    "entries_applied_total",
    JSON_KEY_END
);
const JSON_SUFFIX: &str = "}";

/// 编译期计算全部静态 JSON 键名片段的总字节长度。
const JSON_STATIC_LEN: usize = {
    let mut len = JSON_P0.len()
        + JSON_P1.len()
        + JSON_P2.len()
        + JSON_P3.len()
        + JSON_P4.len()
        + JSON_P5.len()
        + JSON_P6.len()
        + JSON_P7.len()
        + JSON_BUCKET_INF.len()
        + JSON_SUM.len()
        + JSON_COUNT.len()
        + JSON_ENTRIES_APPLIED.len()
        + JSON_SUFFIX.len();
    let mut i = 0;
    while i < BUCKET_COUNT {
        len += JSON_BUCKET_PREFIXES[i].len();
        i += 1;
    }
    len
};

/// JSON 指标导出预分配容量（编译期由静态键长 + 20 个数值的最大字符长度向上取整至 2 的幂计算）。
const METRICS_BUF_CAPACITY: usize = (JSON_STATIC_LEN + 20 * 20).next_power_of_two();

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
