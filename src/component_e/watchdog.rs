/// Component E – Network Watchdog Timer
///
/// Assignment requirement:
///   "Implement a watchdog timer that triggers a 'Network Reset'
///    if no data is received for 10 seconds."
///
/// Design:
///   • A dedicated watchdog thread polls `last_event_ms` every second.
///   • If `now_ms − last_event_ms > 10_000`, it increments the reset
///     counter, logs the event, and signals the ingestion layer to
///     reconnect (by setting `reconnect_flag`).
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::MetricsHandle;

pub const WATCHDOG_TIMEOUT_MS: i64 = 10_000; // 10 seconds

pub struct Watchdog {
    last_event_ms:  Arc<AtomicI64>,
    reset_count:    Arc<AtomicU64>,
    reconnect_flag: Arc<AtomicBool>,
    metrics:        MetricsHandle,
}

impl Watchdog {
    pub fn new(last_event_ms: Arc<AtomicI64>, metrics: MetricsHandle) -> Self {
        Self {
            last_event_ms,
            reset_count:    Arc::new(AtomicU64::new(0)),
            reconnect_flag: Arc::new(AtomicBool::new(false)),
            metrics,
        }
    }

    /// Handle to the reconnect flag – the SSE client checks this.
    pub fn reconnect_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.reconnect_flag)
    }

    /// Total number of network resets triggered so far.
    pub fn reset_count(&self) -> u64 {
        self.reset_count.load(Ordering::Relaxed)
    }

    /// Spawn the watchdog loop on a dedicated OS thread.
    pub fn spawn(self, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("watchdog".into())
            .spawn(move || self.run(stop))
            .expect("watchdog thread")
    }

    fn run(self, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));

            let last = self.last_event_ms.load(Ordering::Relaxed);
            if last == 0 { continue; } // not started yet

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);

            let silence_ms = now_ms - last;
            if silence_ms > WATCHDOG_TIMEOUT_MS {
                let n = self.reset_count.fetch_add(1, Ordering::Relaxed) + 1;
                if let Ok(mut m) = self.metrics.try_lock() {
                    m.watchdog_resets = n;
                }
                eprintln!(
                    "[watchdog] Network Reset #{n} triggered — \
                     {silence_ms} ms silence (threshold: {WATCHDOG_TIMEOUT_MS} ms)"
                );
                // Signal SSE client to reconnect.
                self.reconnect_flag.store(true, Ordering::Relaxed);
                // Reset timer so we don't fire again immediately.
                self.last_event_ms.store(now_ms, Ordering::Relaxed);
            }
        }
    }
}
