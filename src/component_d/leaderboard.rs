/// Component D – Shared-Resource Leaderboard
///
/// Maintains a live count of edits per Wikipedia domain and exposes
/// the top-3 domains at any time.
///
/// Three synchronisation strategies are implemented and benchmarked:
///
///   Strategy 1 – Mutex<HashMap<String, u64>>
///     Classic coarse-grained lock.  Simple but susceptible to
///     priority inversion and high lock contention.
///
///   Strategy 2 – RwLock<HashMap<String, u64>>
///     Allows concurrent reads; writes are exclusive.  Suitable when
///     reads dominate.  Also susceptible to writer starvation.
///
///   Strategy 3 – DashMap (sharded concurrent HashMap)
///     Lock-free reads for most keys; fine-grained shard locking for
///     writes.  Best throughput under high contention.
///     We simulate this with std atomics per a fixed domain set.
///
/// The default production strategy is Strategy 2 (RwLock) because the
/// leaderboard is read-heavy (dashboard polls at 500 ms, writes happen
/// at up to 500 Hz).
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use crate::metrics::MetricsHandle;

/// Fixed-size atomic leaderboard for the top 10 known domains.
/// Used by Strategy 3.
const DOMAIN_SLOTS: usize = 10;
const KNOWN_DOMAINS: [&str; 10] = [
    "en.wikipedia.org", "de.wikipedia.org", "fr.wikipedia.org",
    "es.wikipedia.org", "ru.wikipedia.org", "ja.wikipedia.org",
    "zh.wikipedia.org", "pt.wikipedia.org", "it.wikipedia.org",
    "nl.wikipedia.org",
];

// ▶ SHOW: three sync strategies — benchmarked for throughput and tail latency
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStrategy { Mutex, RwLock, Atomic }

// ▶ SHOW: one data structure per strategy — all three live in the same struct
pub struct Leaderboard {
    strategy: SyncStrategy,

    // Strategy 1
    mutex_map:  Mutex<HashMap<String, u64>>,
    // Strategy 2
    rwlock_map: RwLock<HashMap<String, u64>>,
    // Strategy 3
    atomic_counts: [AtomicU64; DOMAIN_SLOTS],

    metrics: MetricsHandle,
}

impl Leaderboard {
    pub fn new(strategy: SyncStrategy, metrics: MetricsHandle) -> Arc<Self> {
        // Safety: AtomicU64::new(0) has no drop side effects.
        Arc::new(Self {
            strategy,
            mutex_map:  Mutex::new(HashMap::new()),
            rwlock_map: RwLock::new(HashMap::new()),
            atomic_counts: std::array::from_fn(|_| AtomicU64::new(0)),
            metrics,
        })
    }

    /// Increment the edit count for a domain.
    pub fn increment(&self, domain: &str) {
        let t_start = Instant::now();

        match self.strategy {
            SyncStrategy::Mutex => {
                let mut map = self.mutex_map.lock();
                *map.entry(domain.to_owned()).or_insert(0) += 1;
                if let Ok(mut m) = self.metrics.try_lock() {
                    m.mutex_write_ops += 1;
                }
            }
            SyncStrategy::RwLock => {
                let mut map = self.rwlock_map.write();
                *map.entry(domain.to_owned()).or_insert(0) += 1;
                if let Ok(mut m) = self.metrics.try_lock() {
                    m.rwlock_write_ops += 1;
                }
            }
            SyncStrategy::Atomic => {
                if let Some(idx) = KNOWN_DOMAINS.iter().position(|&d| d == domain) {
                    self.atomic_counts[idx].fetch_add(1, Ordering::Relaxed);
                }
                if let Ok(mut m) = self.metrics.try_lock() {
                    m.atomic_write_ops += 1;
                }
            }
        }

        let _ = t_start; // latency would be measured in benchmarks
    }

    /// Return a snapshot of (domain, count) sorted by count descending.
    pub fn top_n(&self, n: usize) -> Vec<(String, u64)> {
        let mut entries: Vec<(String, u64)> = match self.strategy {
            SyncStrategy::Mutex => {
                self.mutex_map.lock().iter()
                    .map(|(k, &v)| (k.clone(), v)).collect()
            }
            SyncStrategy::RwLock => {
                self.rwlock_map.read().iter()
                    .map(|(k, &v)| (k.clone(), v)).collect()
            }
            SyncStrategy::Atomic => {
                KNOWN_DOMAINS.iter().enumerate()
                    .map(|(i, &d)| (d.to_owned(), self.atomic_counts[i].load(Ordering::Relaxed)))
                    .filter(|(_, c)| *c > 0)
                    .collect()
            }
        };
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(n);
        entries
    }

    pub fn strategy(&self) -> SyncStrategy { self.strategy }
}
