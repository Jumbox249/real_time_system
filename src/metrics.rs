/// SharedMetrics – thread-safe performance observability store.
///
/// Tracks all KPIs required by the assignment:
///   • Processing latency (p50 / p90 / p99 percentiles)
///   • Scheduling drift (actual vs expected task-start time)
///   • Overflow / drop counts per pipeline architecture
///   • Deadline misses (> 2 ms hot-path deadline)
///   • Sync-strategy comparison counters
///   • Watchdog reset events
///   • Fail-safe mode transitions
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::types::OverflowEvent;

// ─── Percentile statistics ────────────────────────────────────────────────────

const WINDOW: usize = 10_000;

/// Rolling-window latency sample store with percentile queries.
#[derive(Debug, Default, Clone)]
pub struct LatencySamples {
    samples: VecDeque<f64>,
}

impl LatencySamples {
    pub fn push(&mut self, v: f64) {
        if self.samples.len() >= WINDOW {
            self.samples.pop_front();
        }
        self.samples.push_back(v);
    }

    pub fn count(&self) -> usize { self.samples.len() }

    pub fn mean(&self) -> f64 {
        if self.samples.is_empty() { return 0.0; }
        self.samples.iter().sum::<f64>() / self.samples.len() as f64
    }

    pub fn std_dev(&self) -> f64 {
        if self.samples.len() < 2 { return 0.0; }
        let m = self.mean();
        let v = self.samples.iter().map(|x| (x - m).powi(2)).sum::<f64>()
              / self.samples.len() as f64;
        v.sqrt()
    }

    pub fn max(&self) -> f64 {
        self.samples.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    }

    /// Return the p-th percentile (0.0–100.0).
    pub fn percentile(&self, p: f64) -> f64 {
        if self.samples.is_empty() { return 0.0; }
        let mut sorted: Vec<f64> = self.samples.iter().cloned().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    pub fn p50(&self) -> f64 { self.percentile(50.0) }
    pub fn p90(&self) -> f64 { self.percentile(90.0) }
    pub fn p99(&self) -> f64 { self.percentile(99.0) }

    pub fn snapshot(&self) -> Vec<f64> { self.samples.iter().cloned().collect() }
}

// ─── Leaderboard entry ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LeaderboardEntry {
    pub domain: String,
    pub edits:  u64,
}

// ─── SharedMetrics ────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct SharedMetrics {
    // ── Ingestion / backpressure ─────────────────────────────────────────────
    /// Total SSE events received by the async pipeline.
    pub async_events_received:   u64,
    /// Total SSE events received by the threaded pipeline.
    pub threaded_events_received: u64,
    /// Overflow events (oldest-drop) in the async pipeline.
    pub async_overflow_count:    u64,
    /// Overflow events in the threaded pipeline.
    pub threaded_overflow_count: u64,

    // ── Processing latency (T2 → T3), µs ────────────────────────────────────
    /// Hot-path latency for high-priority (human) packets.
    pub human_latency_us:   LatencySamples,
    /// Hot-path latency for low-priority (bot) packets.
    pub bot_latency_us:     LatencySamples,
    /// Deadline misses (> 2 ms = 2000 µs) total.
    pub deadline_misses:    u64,

    // ── Scheduling drift, µs ────────────────────────────────────────────────
    /// Drift for human edits: actual start − expected start.
    pub human_drift_us: LatencySamples,
    /// Drift for bot edits.
    pub bot_drift_us:   LatencySamples,

    // ── Leaderboard (domain → edit count) ───────────────────────────────────
    pub domain_counts: HashMap<String, u64>,

    // ── Sync-strategy benchmark counters ────────────────────────────────────
    /// Total leaderboard write operations (Mutex strategy).
    pub mutex_write_ops:   u64,
    /// Total leaderboard write operations (RwLock strategy).
    pub rwlock_write_ops:  u64,
    /// Total leaderboard write operations (Atomic strategy).
    pub atomic_write_ops:  u64,

    // ── Watchdog & fail-safe ─────────────────────────────────────────────────
    pub watchdog_resets:       u64,
    pub fail_safe_activations: u64,
    pub current_mode:          String,
    pub uptime:                Duration,

    // ── Throughput ───────────────────────────────────────────────────────────
    /// samples/sec for each architecture (updated by comparison runner).
    pub async_throughput:    f64,
    pub threaded_throughput: f64,

    // ── Event logs (for report evidence) ────────────────────────────────────
    /// Bounded log of overflow events (drop-oldest backpressure triggers).
    pub overflow_log:      Vec<OverflowEvent>,
    /// Bounded log of deadline misses for report evidence.
    pub deadline_miss_log: Vec<DeadlineMissEvent>,
}

/// One deadline miss record – stored in `SharedMetrics.deadline_miss_log`.
#[derive(Debug, Clone)]
pub struct DeadlineMissEvent {
    pub occurred_at: std::time::Instant,
    pub latency_us:  f64,
    pub domain:      String,
    pub priority:    crate::types::Priority,
}

impl SharedMetrics {
    /// Return the top-N domains by edit count.
    pub fn top_domains(&self, n: usize) -> Vec<LeaderboardEntry> {
        let mut entries: Vec<_> = self.domain_counts.iter()
            .map(|(k, &v)| LeaderboardEntry { domain: k.clone(), edits: v })
            .collect();
        entries.sort_by(|a, b| b.edits.cmp(&a.edits));
        entries.truncate(n);
        entries
    }

    /// Append an overflow event, capping the log at 10 000 entries.
    pub fn push_overflow(&mut self, ev: OverflowEvent) {
        if self.overflow_log.len() >= 10_000 {
            self.overflow_log.remove(0);
        }
        self.overflow_log.push(ev);
    }

    /// Append a deadline-miss event, capping the log at 10 000 entries.
    pub fn push_deadline_miss(&mut self, ev: DeadlineMissEvent) {
        if self.deadline_miss_log.len() >= 10_000 {
            self.deadline_miss_log.remove(0);
        }
        self.deadline_miss_log.push(ev);
    }
}

/// Convenience handle shared across all components.
pub type MetricsHandle = Arc<Mutex<SharedMetrics>>;

pub fn new_metrics() -> MetricsHandle {
    Arc::new(Mutex::new(SharedMetrics {
        current_mode:      "NORMAL".to_owned(),
        overflow_log:      Vec::new(),
        deadline_miss_log: Vec::new(),
        ..Default::default()
    }))
}
