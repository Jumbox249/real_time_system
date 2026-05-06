/// Processing Unit – Component A
///
/// Implements:
///   • Windowed Simple Moving Average (SMA) filter – noise suppression
///   • Statistical variance-based anomaly detection
///   • Black-box arithmetic operations to simulate realistic CPU load
///   • Dynamic threshold adaptation via the closed-loop feedback channel
///
/// Deadline: every packet must be processed within 200 µs of T0.
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender, TryRecvError};

use crate::metrics::MetricsHandle;
use crate::types::{FeedbackMessage, SensorData, SensorType};
use crate::component_a::sync_manager::SyncManager;

const PROC_DEADLINE: Duration = Duration::from_micros(200);
const SMA_WINDOW:    usize    = 8;
/// Initial anomaly detection threshold (multiple of rolling std dev).
const ANOMALY_SIGMA: f64 = 3.0;

/// Per-sensor SMA state.
struct SmaFilter {
    window: VecDeque<f64>,
    sum:    f64,
}

impl SmaFilter {
    fn new() -> Self {
        Self { window: VecDeque::with_capacity(SMA_WINDOW), sum: 0.0 }
    }

    /// Push a new sample and return the current moving average.
    fn update(&mut self, v: f64) -> f64 {
        if self.window.len() >= SMA_WINDOW {
            self.sum -= self.window.pop_front().unwrap_or(0.0);
        }
        self.window.push_back(v);
        self.sum += v;
        self.sum / self.window.len() as f64
    }

    fn variance(&self) -> f64 {
        if self.window.len() < 2 {
            return 0.0;
        }
        let mean = self.sum / self.window.len() as f64;
        self.window.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / self.window.len() as f64
    }
}

pub struct Processor {
    metrics:      MetricsHandle,
    sync_manager: Arc<SyncManager>,
    /// Dynamic anomaly threshold – adjusted when actuators report deadline misses.
    anomaly_threshold: f64,
}

impl Processor {
    pub fn new(metrics: MetricsHandle, sync_manager: Arc<SyncManager>) -> Self {
        Self {
            metrics,
            sync_manager,
            anomaly_threshold: ANOMALY_SIGMA,
        }
    }

    /// Spawns the processor loop on a new OS thread.
    pub fn spawn(
        mut self,
        sensor_rx:   Receiver<SensorData>,
        proc_tx:     Sender<SensorData>,
        feedback_rx: Receiver<FeedbackMessage>,
        stop:        Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("processor".into())
            .spawn(move || self.run(sensor_rx, proc_tx, feedback_rx, stop))
            .expect("failed to spawn processor thread")
    }

    fn run(
        &mut self,
        sensor_rx:   Receiver<SensorData>,
        proc_tx:     Sender<SensorData>,
        feedback_rx: Receiver<FeedbackMessage>,
        stop:        Arc<AtomicBool>,
    ) {
        // One SMA filter per sensor type.
        let mut force_sma = SmaFilter::new();
        let mut pos_sma   = SmaFilter::new();
        let mut temp_sma  = SmaFilter::new();

        while !stop.load(Ordering::Relaxed) {
            // ── Poll feedback channel (non-blocking) ────────────────────────
            loop {
                match feedback_rx.try_recv() {
                    Ok(fb) => {
                        if fb.deadline_miss {
                            // Relax threshold to reduce computational load.
                            self.anomaly_threshold =
                                (self.anomaly_threshold + 0.5).min(6.0);
                        } else {
                            // Tighten threshold gradually when system is healthy.
                            self.anomaly_threshold =
                                (self.anomaly_threshold - 0.05).max(ANOMALY_SIGMA);
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            // ── Wait for next sensor packet ─────────────────────────────────
            let mut pkt = match sensor_rx.recv_timeout(Duration::from_millis(10)) {
                Ok(p)  => p,
                Err(_) => continue,
            };

            let proc_start = Instant::now();

            // ── SMA filter ──────────────────────────────────────────────────
            let filtered = match pkt.sensor_type {
                SensorType::Force       => force_sma.update(pkt.raw_value),
                SensorType::Position    => pos_sma.update(pkt.raw_value),
                SensorType::Temperature => temp_sma.update(pkt.raw_value),
            };
            pkt.filtered_value = filtered;

            // ── Anomaly detection (variance-based) ──────────────────────────
            let variance  = match pkt.sensor_type {
                SensorType::Force       => force_sma.variance(),
                SensorType::Position    => pos_sma.variance(),
                SensorType::Temperature => temp_sma.variance(),
            };
            let std_dev   = variance.sqrt();
            let deviation = (pkt.raw_value - filtered).abs();
            pkt.is_anomaly = deviation > self.anomaly_threshold * std_dev.max(0.01);

            // ── Black-box CPU load simulation (mimics real workload) ─────────
            // Volatile arithmetic prevents the compiler from optimising away.
            let _ = black_box_work(pkt.raw_value, pkt.sequence);

            // ── Record in SyncManager ───────────────────────────────────────
            self.sync_manager.record_sample(
                pkt.sensor_type,
                pkt.filtered_value,
                pkt.sequence,
            );

            // ── Timestamp T1 ────────────────────────────────────────────────
            pkt.t1 = Some(Instant::now());
            let latency_us = proc_start.elapsed().as_micros() as f64;

            // ── Deadline check ──────────────────────────────────────────────
            let deadline_miss = proc_start.elapsed() > PROC_DEADLINE;
            if let Ok(mut m) = self.metrics.try_lock() {
                m.processing_latency_us.push(latency_us);
                if deadline_miss {
                    m.processor_deadline_misses += 1;
                }
            }

            // ── Forward to Transmitter ──────────────────────────────────────
            // Try-send: if the transmitter channel is full the packet is dropped
            // to protect the processor's own deadline.
            let _ = proc_tx.try_send(pkt);
        }
    }
}

/// Simulates realistic CPU black-box arithmetic (prevents constant folding).
#[inline(never)]
fn black_box_work(seed: f64, seq: u64) -> f64 {
    let mut acc = seed;
    for i in 0..64u64 {
        acc = (acc * 1.000_001 + (i ^ seq) as f64 * 0.000_001).sin() * 100.0;
    }
    acc
}
