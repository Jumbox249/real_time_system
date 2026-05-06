/// Receiver – Component B dispatch unit
///
/// A low-latency front-end that dequeues packets from the IPC channel,
/// stamps T3, and forwards them to the Controller.
/// No heavy computation takes place here – all processing is deferred
/// to the Controller to keep receiver latency minimal.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver as CbReceiver, Sender};

use crate::metrics::MetricsHandle;
use crate::types::SensorData;

pub struct Receiver {
    metrics: MetricsHandle,
}

impl Receiver {
    pub fn new(metrics: MetricsHandle) -> Self {
        Self { metrics }
    }

    pub fn spawn(
        self,
        ipc_rx:  CbReceiver<SensorData>,
        ctrl_tx: Sender<SensorData>,
        stop:    Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("receiver".into())
            .spawn(move || self.run(ipc_rx, ctrl_tx, stop))
            .expect("failed to spawn receiver thread")
    }

    fn run(
        self,
        ipc_rx:  CbReceiver<SensorData>,
        ctrl_tx: Sender<SensorData>,
        stop:    Arc<AtomicBool>,
    ) {
        while !stop.load(Ordering::Relaxed) {
            let mut pkt = match ipc_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(p)  => p,
                Err(_) => continue,
            };

            // ── Stamp T3 ────────────────────────────────────────────────────
            let t3 = Instant::now();
            pkt.t3 = Some(t3);

            // ── Record end-to-end latency (T3 – T0) ─────────────────────────
            let e2e_ms = t3.duration_since(pkt.t0).as_secs_f64() * 1_000.0;
            if let Ok(mut m) = self.metrics.try_lock() {
                m.e2e_latency_ms.push(e2e_ms);
            }

            // ── Dispatch to Controller ───────────────────────────────────────
            let _ = ctrl_tx.try_send(pkt);
        }
    }
}
