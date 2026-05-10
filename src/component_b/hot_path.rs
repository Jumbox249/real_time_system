/// Hot Path Processor (Component B)
///
/// Enforces a strict 2 ms completion deadline on every packet from the
/// moment it leaves the ingestion channel (T2) until processing is
/// finalised (T3).
///
/// Processing steps on the hot path:
///   1. Dequeue ChangePacket from the priority channel (T2 stamp).
///   2. Update the domain leaderboard (shared resource, Component D).
///   3. Record scheduling drift (Component C).
///   4. Check deadline: if T3 − T2 > 2 ms, log a deadline miss.
///   5. In Degraded mode (Component E): skip bot packets entirely.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::component_d::leaderboard::Leaderboard;
use crate::component_e::fail_safe::FailSafe;
use crate::metrics::{DeadlineMissEvent, MetricsHandle};
use crate::types::{ChangePacket, Priority, StressConfig};

/// Hard per-packet processing deadline.
pub const HOT_DEADLINE: Duration = Duration::from_millis(2);

pub struct HotPathProcessor {
    metrics:        MetricsHandle,
    leaderboard:    Arc<Leaderboard>,
    fail_safe:      Arc<FailSafe>,
    stress:         StressConfig,
    stress_counter: AtomicU64,
}

impl HotPathProcessor {
    pub fn new(
        metrics:     MetricsHandle,
        leaderboard: Arc<Leaderboard>,
        fail_safe:   Arc<FailSafe>,
        stress:      StressConfig,
    ) -> Self {
        Self {
            metrics, leaderboard, fail_safe, stress,
            stress_counter: AtomicU64::new(0),
        }
    }

    /// Process one ChangePacket on the hot path.
    /// Returns `true` if the packet was processed within the deadline.
    pub fn process(&self, mut pkt: ChangePacket) -> bool {
        // ▶ SHOW: T2 stamp — packet leaves the queue, hot path starts here
        let t2 = Instant::now();
        pkt.t2 = Some(t2);

        // Scheduling drift = T2 - T1 (Component C requirement).
        // Recorded before the degraded-mode early return so we still
        // measure queueing for dropped bot packets.
        if let Some(t1) = pkt.t1 {
            let drift_us = t2.duration_since(t1).as_micros() as f64;
            if let Ok(mut m) = self.metrics.try_lock() {
                match pkt.priority {
                    Priority::High => m.human_drift_us.push(drift_us),
                    Priority::Low  => m.bot_drift_us.push(drift_us),
                }
            }
        }

        // ── Degraded mode: skip bot packets ──────────────────────────────────
        if self.fail_safe.is_degraded() && pkt.priority == Priority::Low {
            return true; // skip but not a miss
        }

        // ── Stress demo: inject 3 ms busy-spin every Nth packet ──────────────
        // Only active during Phase 2 (15–25 s from program_start) so Phase 4
        // can cleanly recover to NORMAL once the injection window closes.
        if self.stress.enabled && self.stress.inject_latency_every_nth > 0 {
            let origin  = self.stress.program_start.unwrap_or_else(Instant::now);
            let elapsed = origin.elapsed().as_secs();
            if elapsed >= 15 && elapsed < 25 {
                let n = self.stress_counter.fetch_add(1, Ordering::Relaxed);
                if n % self.stress.inject_latency_every_nth == 0 {
                    let target = Instant::now() + Duration::from_millis(3);
                    while Instant::now() < target {
                        std::hint::spin_loop();
                    }
                }
            }
        }

        // ── Leaderboard update (Component D) ─────────────────────────────────
        self.leaderboard.increment(&pkt.server_name);

        // ▶ SHOW: T3 stamp — processing done; T3−T2 must be < 2 ms
        let t3 = Instant::now();
        pkt.t3 = Some(t3);

        let latency_ns = t2.elapsed().as_nanos() as f64;
        // ▶ SHOW: 2 ms deadline check — logs a miss if exceeded
        let deadline_ok = latency_ns <= HOT_DEADLINE.as_nanos() as f64;

        if let Ok(mut m) = self.metrics.try_lock() {
            match pkt.priority {
                Priority::High => m.human_latency_us.push(latency_ns),
                Priority::Low  => m.bot_latency_us.push(latency_ns),
            }
            if !deadline_ok {
                let latency_us = latency_ns / 1000.0;
                m.deadline_misses += 1;
                let miss_ev = DeadlineMissEvent {
                    occurred_at: t3,
                    latency_us,
                    domain:   pkt.server_name.clone(),
                    priority: pkt.priority,
                };
                eprintln!("[deadline-miss] {:.0} µs domain={} priority={:?}",
                          latency_us, pkt.server_name, pkt.priority);
                m.push_deadline_miss(miss_ev);
            }
        }

        // ── Update fail-safe jitter monitor ──────────────────────────────────
        self.fail_safe.record_latency(latency_ns / 1000.0);

        deadline_ok
    }

    /// Spawn the hot-path processing loop on a new OS thread.
    /// Reads from `rx` (crossbeam Receiver) and processes each packet.
    pub fn spawn_threaded(
        self,
        rx:   crossbeam::channel::Receiver<ChangePacket>,
        stop: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("hot-path".into())
            .spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let pkt = match rx.recv_timeout(Duration::from_millis(10)) {
                        Ok(p)  => p,
                        Err(_) => continue,
                    };
                    self.process(pkt);
                }
            })
            .expect("hot-path thread")
    }

    /// Async version of the hot-path loop (for the Tokio pipeline).
    pub async fn run_async(
        self,
        mut rx: tokio::sync::mpsc::Receiver<ChangePacket>,
        stop:   Arc<AtomicBool>,
    ) {
        while !stop.load(Ordering::Relaxed) {
            let pkt = match tokio::time::timeout(
                Duration::from_millis(10),
                rx.recv(),
            ).await {
                Ok(Some(p)) => p,
                _           => continue,
            };
            self.process(pkt);
            tokio::task::yield_now().await;
        }
    }
}
