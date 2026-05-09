/// Component E – Fail-Safe Mode Controller
///
/// Assignment requirement:
///   "If processing jitter exceeds a defined threshold, the system must
///    automatically enter a 'Degraded Mode' to maintain stability."
///
/// State machine:
///
///   ┌─────────┐  jitter > 2000 µs   ┌──────────┐
///   │ NORMAL  │────────────────────►│ DEGRADED │
///   └─────────┘                     └────┬─────┘
///        ▲          jitter < 500 µs      │
///        │  ◄──────────────────────── ───┘
///   ┌──────────┐   after 20 clean cycles
///   │ RECOVERY │
///   └──────────┘
///
/// In DEGRADED mode:
///   • Bot (low-priority) packets are dropped at the hot-path entry.
///   • This reduces CPU load, allowing latency to recover.
///
/// In RECOVERY mode:
///   • Bot packets are re-admitted at 50 % rate (every other packet).
///   • After `RECOVERY_WINDOW` consecutive packets within deadline, mode
///     returns to NORMAL.
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;

use crate::metrics::MetricsHandle;
use crate::types::{
    SystemMode, JITTER_THRESHOLD_US, RECOVERY_THRESHOLD_US, RECOVERY_WINDOW,
};

// Mode encoded as u8 for atomic storage.
const MODE_NORMAL:   u8 = 0;
const MODE_DEGRADED: u8 = 1;
const MODE_RECOVERY: u8 = 2;

pub struct FailSafe {
    mode:                AtomicU8,
    degraded_clean:      AtomicU32, // consecutive clean cycles needed to leave DEGRADED
    clean_cycles:        AtomicU32, // consecutive clean cycles needed to leave RECOVERY
    recovery_tick:       AtomicU32, // toggles every packet in recovery mode
    metrics:             MetricsHandle,
}

impl FailSafe {
    pub fn new(metrics: MetricsHandle) -> Arc<Self> {
        Arc::new(Self {
            mode:           AtomicU8::new(MODE_NORMAL),
            degraded_clean: AtomicU32::new(0),
            clean_cycles:   AtomicU32::new(0),
            recovery_tick:  AtomicU32::new(0),
            metrics,
        })
    }

    pub fn current_mode(&self) -> SystemMode {
        match self.mode.load(Ordering::Relaxed) {
            MODE_DEGRADED => SystemMode::Degraded,
            MODE_RECOVERY => SystemMode::Recovery,
            _             => SystemMode::Normal,
        }
    }

    pub fn is_degraded(&self) -> bool {
        self.mode.load(Ordering::Relaxed) == MODE_DEGRADED
    }

    pub fn is_recovery(&self) -> bool {
        self.mode.load(Ordering::Relaxed) == MODE_RECOVERY
    }

    /// Called by the hot-path with the processing latency of each packet.
    /// Updates the state machine based on the observed jitter.
    pub fn record_latency(&self, latency_us: f64) {
        let current = self.mode.load(Ordering::Relaxed);

        match current {
            MODE_NORMAL => {
                if latency_us > JITTER_THRESHOLD_US {
                    self.transition_to(MODE_DEGRADED);
                }
            }
            MODE_DEGRADED => {
                if latency_us < RECOVERY_THRESHOLD_US {
                    let n = self.degraded_clean.fetch_add(1, Ordering::Relaxed) + 1;
                    if n >= 25 {
                        self.degraded_clean.store(0, Ordering::Relaxed);
                        self.clean_cycles.store(0, Ordering::Relaxed);
                        self.transition_to(MODE_RECOVERY);
                    }
                } else {
                    // Any spike resets the degraded clean counter.
                    self.degraded_clean.store(0, Ordering::Relaxed);
                }
            }
            MODE_RECOVERY => {
                if latency_us < RECOVERY_THRESHOLD_US {
                    let n = self.clean_cycles.fetch_add(1, Ordering::Relaxed) + 1;
                    if n >= RECOVERY_WINDOW {
                        self.transition_to(MODE_NORMAL);
                    }
                } else if latency_us > JITTER_THRESHOLD_US {
                    // Jitter spiked again – fall back to Degraded.
                    self.degraded_clean.store(0, Ordering::Relaxed);
                    self.clean_cycles.store(0, Ordering::Relaxed);
                    self.transition_to(MODE_DEGRADED);
                } else {
                    self.clean_cycles.store(0, Ordering::Relaxed);
                }
            }
            _ => {}
        }
    }

    /// Returns `true` if a bot packet should be processed in the current mode.
    /// In Degraded mode, always drops bots.  In Recovery, drops every other.
    pub fn allow_bot_packet(&self) -> bool {
        match self.mode.load(Ordering::Relaxed) {
            MODE_DEGRADED => false,
            MODE_RECOVERY => {
                // Admit every other bot packet (50 % rate).
                let tick = self.recovery_tick.fetch_add(1, Ordering::Relaxed);
                tick % 2 == 0
            }
            _ => true,
        }
    }

    fn transition_to(&self, new_mode: u8) {
        let old = self.mode.swap(new_mode, Ordering::Relaxed);
        if old != new_mode {
            let label = match new_mode {
                MODE_DEGRADED => "DEGRADED",
                MODE_RECOVERY => "RECOVERY",
                _             => "NORMAL",
            };
            eprintln!("[fail-safe] mode → {label}");
            if let Ok(mut m) = self.metrics.try_lock() {
                m.current_mode = label.to_owned();
                if new_mode == MODE_DEGRADED {
                    m.fail_safe_activations += 1;
                }
            }
        }
    }
}
