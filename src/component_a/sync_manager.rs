/// SyncManager – three-mode concurrent diagnostic recorder.
///
/// Chosen strategy: **Lock-Free** (default)
///   Sensors push SyncRecords into a bounded ArrayQueue without ever
///   blocking.  A background consumer thread drains the queue and writes
///   diagnostic data without stalling the critical real-time path.
///
/// Available modes:
///   1. Mutex    – simple, but susceptible to priority inversion
///   2. Atomic   – wait-free counters; limited to simple numeric metrics
///   3. LockFree – (selected) ArrayQueue + background consumer thread
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crossbeam::queue::ArrayQueue;

use crate::types::{SensorType, SyncMode, SyncRecord};

const QUEUE_CAPACITY: usize = 512;

// ─── Internal mutex-mode state ────────────────────────────────────────────────

#[derive(Default)]
struct MutexState {
    records:   Vec<SyncRecord>,
    write_count: u64,
}

// ─── SyncManager ─────────────────────────────────────────────────────────────

pub struct SyncManager {
    mode: SyncMode,

    // Mutex mode ──────────────────────────────────────────────────────────────
    mutex_state: Mutex<MutexState>,

    // Atomic mode ─────────────────────────────────────────────────────────────
    atomic_counter: AtomicU64,
    atomic_drops:   AtomicU64,

    // Lock-free mode ──────────────────────────────────────────────────────────
    lf_queue:  Arc<ArrayQueue<SyncRecord>>,
    lf_drops:  AtomicU64,
}

impl SyncManager {
    pub fn new(mode: SyncMode) -> Arc<Self> {
        let mgr = Arc::new(Self {
            mode,
            mutex_state:    Mutex::new(MutexState::default()),
            atomic_counter: AtomicU64::new(0),
            atomic_drops:   AtomicU64::new(0),
            lf_queue:  Arc::new(ArrayQueue::new(QUEUE_CAPACITY)),
            lf_drops:  AtomicU64::new(0),
        });

        // Spawn background consumer only for LockFree mode.
        if mode == SyncMode::LockFree {
            let queue = Arc::clone(&mgr.lf_queue);
            let drops = Arc::new(AtomicU64::new(0));
            let drops_clone = Arc::clone(&drops);
            std::thread::Builder::new()
                .name("sync-consumer".into())
                .spawn(move || {
                    loop {
                        while let Some(_rec) = queue.pop() {
                            // In a real system this would write to disk / telemetry.
                            // Here we simply consume to prevent queue overflow.
                        }
                        // drops_clone available for metrics if needed.
                        let _ = drops_clone.load(std::sync::atomic::Ordering::Relaxed);
                        std::thread::sleep(std::time::Duration::from_micros(500));
                    }
                })
                .expect("failed to spawn sync consumer");
            // lf_drops tracks drops from the producer side.
            drops.fetch_add(0, std::sync::atomic::Ordering::Relaxed);
        }

        mgr
    }

    /// Record one sensor sample using the configured synchronization strategy.
    /// This method is called from the critical sensor/processor path.
    pub fn record_sample(&self, sensor_type: SensorType, value: f64, sequence: u64) {
        match self.mode {
            SyncMode::Mutex => {
                if let Ok(mut state) = self.mutex_state.lock() {
                    state.records.push(SyncRecord {
                        sensor_type,
                        value,
                        sequence,
                        timestamp: Instant::now(),
                    });
                    state.write_count += 1;
                    // Bound memory growth.
                    if state.records.len() > 4096 {
                        state.records.drain(..2048);
                    }
                }
            }

            SyncMode::Atomic => {
                // Atomic mode: only increment a counter (no complex data storage).
                self.atomic_counter.fetch_add(1, Ordering::Relaxed);
            }

            SyncMode::LockFree => {
                let rec = SyncRecord {
                    sensor_type,
                    value,
                    sequence,
                    timestamp: Instant::now(),
                };
                // Non-blocking push: drop the record if the queue is full.
                if self.lf_queue.push(rec).is_err() {
                    self.lf_drops.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    // ── Diagnostics ──────────────────────────────────────────────────────────

    pub fn mode(&self) -> SyncMode {
        self.mode
    }

    pub fn lock_free_drop_count(&self) -> u64 {
        self.lf_drops.load(Ordering::Relaxed)
    }

    pub fn atomic_sample_count(&self) -> u64 {
        self.atomic_counter.load(Ordering::Relaxed)
    }

    pub fn mutex_write_count(&self) -> u64 {
        self.mutex_state.lock().map(|s| s.write_count).unwrap_or(0)
    }
}
