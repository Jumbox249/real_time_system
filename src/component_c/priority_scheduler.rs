/// Component C – Priority Scheduler & Scheduling Drift Measurement
///
/// Assignment requirement:
///   "Implement a mechanism where 'Human' edits override lower-priority
///    'Bot' edits. You must measure and report Scheduling Drift
///    (actual vs. expected task start times)."
///
/// Implementation:
///   • Two bounded crossbeam channels – one per priority level.
///   • The dispatcher always drains the High-priority (human) channel
///     completely before taking items from the Low-priority (bot) channel.
///   • Scheduling Drift = actual_dequeue_time − expected_dequeue_time.
///     Expected time is when the packet entered the queue (T1).
///
/// Drift measurements are recorded per-priority in SharedMetrics,
/// allowing direct comparison of human vs. bot scheduling drift.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{bounded, Receiver, Sender, TrySendError};

use crate::metrics::MetricsHandle;
use crate::types::{ChangePacket, Priority};

/// Internal queue depth per priority level.
pub const QUEUE_DEPTH: usize = 256;

pub struct PriorityScheduler {
    high_tx:  Sender<ChangePacket>,
    low_tx:   Sender<ChangePacket>,
    high_rx:  Receiver<ChangePacket>,
    low_rx:   Receiver<ChangePacket>,
    metrics:  MetricsHandle,
}

impl PriorityScheduler {
    pub fn new(metrics: MetricsHandle) -> Self {
        let (high_tx, high_rx) = bounded(QUEUE_DEPTH);
        let (low_tx,  low_rx)  = bounded(QUEUE_DEPTH);
        Self { high_tx, low_tx, high_rx, low_rx, metrics }
    }

    /// Enqueue a ChangePacket into the appropriate priority channel.
    /// Stamps T1 (queue entry time) for drift measurement.
    /// Returns `false` if the relevant queue is full (packet dropped).
    pub fn enqueue(&self, mut pkt: ChangePacket) -> bool {
        pkt.t1 = Some(Instant::now()); // T1: queue entry

        let tx = match pkt.priority {
            Priority::High => &self.high_tx,
            Priority::Low  => &self.low_tx,
        };

        match tx.try_send(pkt) {
            Ok(_)                        => true,
            Err(TrySendError::Full(_))   => false,
            Err(TrySendError::Disconnected(_)) => false,
        }
    }

    // ▶ SHOW: human-first drain — high_rx is always checked before low_rx
    /// Dequeue the next packet, preferring high-priority items.
    /// Stamps T2 (actual dequeue time) and records scheduling drift.
    pub fn dequeue_next(&self) -> Option<ChangePacket> {
        // Always serve high-priority first.
        let mut pkt = if let Ok(p) = self.high_rx.try_recv() {
            p
        } else if let Ok(p) = self.low_rx.try_recv() {
            p
        } else {
            return None;
        };

        let t2 = Instant::now();
        pkt.t2 = Some(t2);

        // Drift = T2 − T1 (µs).
        if let Some(t1) = pkt.t1 {
            let drift_us = t2.duration_since(t1).as_micros() as f64;
            if let Ok(mut m) = self.metrics.try_lock() {
                match pkt.priority {
                    Priority::High => m.human_drift_us.push(drift_us),
                    Priority::Low  => m.bot_drift_us.push(drift_us),
                }
            }
        }

        Some(pkt)
    }

    /// Run the scheduling dispatch loop, feeding packets to `out_tx`.
    /// Runs until `stop` is set. Uses `&self` so it can be called from an Arc.
    pub fn run_dispatch_loop(
        &self,
        out_tx: Sender<ChangePacket>,
        stop:   Arc<AtomicBool>,
    ) {
        while !stop.load(Ordering::Relaxed) {
            match self.dequeue_next() {
                Some(pkt) => { let _ = out_tx.try_send(pkt); }
                None      => std::thread::sleep(Duration::from_micros(100)),
            }
        }
    }

    /// Sender handle for high-priority packets (exposes to Component A).
    pub fn high_tx(&self) -> Sender<ChangePacket> { self.high_tx.clone() }
    /// Sender handle for low-priority packets.
    pub fn low_tx(&self)  -> Sender<ChangePacket> { self.low_tx.clone() }

    /// Drop-oldest backpressure: evict the head of the queue for `priority`.
    /// Called when `enqueue` returns false (queue full) before retrying.
    pub fn evict_oldest(&self, priority: Priority) {
        let rx = match priority {
            Priority::High => &self.high_rx,
            Priority::Low  => &self.low_rx,
        };
        // Discard the oldest item; ignore if already empty.
        let _ = rx.try_recv();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::new_metrics;
    use crate::types::{ChangePacket, Priority};

    fn pkt(priority: Priority) -> ChangePacket {
        ChangePacket {
            user:           "u".to_owned(),
            server_name:    "en.wikipedia.org".to_owned(),
            title:          "T".to_owned(),
            change_type:    "edit".to_owned(),
            priority,
            wiki_timestamp: 0,
            t0: Instant::now(),
            t1: None, t2: None, t3: None,
        }
    }

    #[test]
    fn high_priority_drains_before_low() {
        let sched = PriorityScheduler::new(new_metrics());
        assert!(sched.enqueue(pkt(Priority::Low)));
        assert!(sched.enqueue(pkt(Priority::High)));
        assert!(sched.enqueue(pkt(Priority::Low)));

        assert_eq!(sched.dequeue_next().unwrap().priority, Priority::High);
        assert_eq!(sched.dequeue_next().unwrap().priority, Priority::Low);
        assert_eq!(sched.dequeue_next().unwrap().priority, Priority::Low);
        assert!(sched.dequeue_next().is_none());
    }

    #[test]
    fn drift_is_recorded_on_dequeue() {
        let metrics = new_metrics();
        let sched   = PriorityScheduler::new(Arc::clone(&metrics));
        sched.enqueue(pkt(Priority::High));
        std::thread::sleep(Duration::from_millis(2));
        let _ = sched.dequeue_next();

        let m = metrics.lock().unwrap();
        assert!(m.human_drift_us.p50() > 0.0,
                "drift should be recorded after a real delay");
    }
}
