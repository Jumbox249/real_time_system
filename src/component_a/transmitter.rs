/// Transmitter Bridge – IPC Layer (Component A → Component B)
///
/// A bounded channel (1024-packet capacity) decouples the high-frequency
/// processor loop from the actuation subsystem.
///
/// Safety strategy – Fail-Fast (backpressure):
///   If the IPC channel is congested because Component B is stalling,
///   the transmitter intentionally **drops** the packet rather than
///   blocking the Processor.  Blocking would propagate deadline misses
///   to the Processor, causing a "convoy effect" across the pipeline.
///   Dropping stale packets preserves Processor responsiveness and ensures
///   newer sensor data always takes priority.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender, TryRecvError};

use crate::metrics::MetricsHandle;
use crate::types::SensorData;

/// Hard timeout for a single IPC send attempt (100 µs).
const SEND_TIMEOUT: Duration = Duration::from_micros(100);

pub struct Transmitter {
    metrics: MetricsHandle,
}

impl Transmitter {
    pub fn new(metrics: MetricsHandle) -> Self {
        Self { metrics }
    }

    /// Spawns the transmitter loop on a new OS thread.
    pub fn spawn(
        self,
        proc_rx: Receiver<SensorData>,
        ipc_tx:  Sender<SensorData>,
        stop:    Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("transmitter".into())
            .spawn(move || self.run(proc_rx, ipc_tx, stop))
            .expect("failed to spawn transmitter thread")
    }

    fn run(self, proc_rx: Receiver<SensorData>, ipc_tx: Sender<SensorData>, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::Relaxed) {
            // Receive processed packet from the Processor.
            let mut pkt = match proc_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(p)  => p,
                Err(_) => continue,
            };

            let send_start = Instant::now();

            // ── Fail-Fast send ──────────────────────────────────────────────
            // try_send returns Err immediately if the channel is full.
            // No blocking, no waiting – drop stale data to keep latency low.
            let result = ipc_tx.try_send({
                pkt.t2 = Some(Instant::now());
                pkt.clone()
            });

            let ipc_latency_us = send_start.elapsed().as_micros() as f64;

            match result {
                Ok(_) => {
                    if let Ok(mut m) = self.metrics.try_lock() {
                        m.ipc_latency_us.push(ipc_latency_us);
                    }
                }
                Err(_) => {
                    // Channel full – drop packet (fail-fast).
                    if let Ok(mut m) = self.metrics.try_lock() {
                        m.ipc_packets_dropped += 1;
                    }
                }
            }
        }
    }
}
