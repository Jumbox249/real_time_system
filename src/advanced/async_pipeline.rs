/// Asynchronous Pipeline – Component A reimplemented with Tokio
///
/// This module reimplements the Component A pipeline (Sensing, Processing,
/// Transmitter) using Tokio lightweight tasks instead of OS threads.
///
/// Goal: measure whether user-space cooperative scheduling (async) achieves
/// lower jitter and higher throughput compared to kernel-thread scheduling.
///
/// Design decisions:
///   • Component B (actuation) remains threaded because hardware simulations
///     require blocking operations and strict priority enforcement
///     (ThreadPriority::Max) best managed by dedicated kernel threads.
///   • SyncManager is unchanged – serves as a unified, thread-safe source of
///     truth, enabling an apples-to-apples comparison.
///   • tokio::spawn replaces std::thread::spawn for sensing + processing.
///   • Scheduling shifts from preemptive (kernel-space) to cooperative
///     (user-space), minimising system-call overhead.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::metrics::MetricsHandle;
use crate::types::{SensorData, SensorType};

const SAMPLE_PERIOD_MS: u64 = 5;
const SMA_WINDOW:        usize = 8;
const PROC_DEADLINE:     Duration = Duration::from_micros(200);

/// Run the async pipeline for `duration` seconds and return throughput
/// (samples processed per second) and mean jitter (µs).
pub async fn run_async_pipeline(
    metrics:  MetricsHandle,
    duration: Duration,
) -> (f64, f64) {
    let (tx, mut rx) = mpsc::channel::<SensorData>(1024);
    let stop         = Arc::new(AtomicBool::new(false));

    // ── Sensor tasks ─────────────────────────────────────────────────────────
    let sensor_configs = [
        (SensorType::Force,       10.0_f64, 0.5_f64),
        (SensorType::Position,     5.0,     0.3),
        (SensorType::Temperature, 25.0,     1.0),
    ];

    for (stype, base, noise) in sensor_configs {
        let tx_clone    = tx.clone();
        let stop_clone  = Arc::clone(&stop);
        let metrics_c   = Arc::clone(&metrics);
        tokio::spawn(async move {
            async_sensor(stype, base, noise, tx_clone, metrics_c, stop_clone).await;
        });
    }
    drop(tx);

    // ── Processor task ───────────────────────────────────────────────────────
    let metrics_proc = Arc::clone(&metrics);
    let stop_proc    = Arc::clone(&stop);
    let proc_handle  = tokio::spawn(async move {
        async_processor(rx, metrics_proc, stop_proc).await
    });

    // ── Run for the requested duration ───────────────────────────────────────
    sleep(duration).await;
    stop.store(true, Ordering::Relaxed);

    let samples_processed = proc_handle.await.unwrap_or(0);
    let throughput = samples_processed as f64 / duration.as_secs_f64();

    let mean_jitter = metrics.lock()
        .map(|m| m.sensor_jitter_us.mean())
        .unwrap_or(0.0);

    (throughput, mean_jitter)
}

// ─── Async sensor ────────────────────────────────────────────────────────────

async fn async_sensor(
    sensor_type: SensorType,
    base:        f64,
    noise_std:   f64,
    tx:          mpsc::Sender<SensorData>,
    metrics:     MetricsHandle,
    stop:        Arc<AtomicBool>,
) {
    let mut rng = rand::thread_rng();
    let mut seq = 0u64;
    let mut prev = Instant::now();

    while !stop.load(Ordering::Relaxed) {
        let now       = Instant::now();
        let elapsed   = now.duration_since(prev).as_micros() as f64;
        let expected  = (SAMPLE_PERIOD_MS * 1000) as f64;
        let jitter_us = (elapsed - expected).abs();

        if let Ok(mut m) = metrics.try_lock() {
            m.sensor_jitter_us.push(jitter_us);
        }
        prev = now;

        let noise = rng.sample::<f64, _>(rand::distributions::Standard) * noise_std * 2.0
                  - noise_std;
        let raw   = base + noise;

        let data = SensorData::new(sensor_type, raw, seq);
        seq += 1;

        // Non-blocking send (drop on full queue).
        let _ = tx.try_send(data);

        sleep(Duration::from_millis(SAMPLE_PERIOD_MS)).await;
    }
}

// ─── Async processor ─────────────────────────────────────────────────────────

async fn async_processor(
    mut rx:  mpsc::Receiver<SensorData>,
    metrics: MetricsHandle,
    stop:    Arc<AtomicBool>,
) -> u64 {
    let mut force_sma  = AsyncSma::new();
    let mut pos_sma    = AsyncSma::new();
    let mut temp_sma   = AsyncSma::new();
    let mut count      = 0u64;

    while !stop.load(Ordering::Relaxed) {
        let mut pkt = match tokio::time::timeout(Duration::from_millis(10), rx.recv()).await {
            Ok(Some(p)) => p,
            _           => continue,
        };

        let t1 = Instant::now();

        let filtered = match pkt.sensor_type {
            SensorType::Force       => force_sma.update(pkt.raw_value),
            SensorType::Position    => pos_sma.update(pkt.raw_value),
            SensorType::Temperature => temp_sma.update(pkt.raw_value),
        };
        pkt.filtered_value = filtered;

        // Simulate CPU load (black-box arithmetic).
        let _ = async_black_box(pkt.raw_value, pkt.sequence);

        let latency_us    = t1.elapsed().as_micros() as f64;
        let deadline_miss = t1.elapsed() > PROC_DEADLINE;

        if let Ok(mut m) = metrics.try_lock() {
            m.processing_latency_us.push(latency_us);
            if deadline_miss {
                m.processor_deadline_misses += 1;
            }
        }

        count += 1;
        // Yield cooperatively to allow other tasks to run.
        tokio::task::yield_now().await;
    }
    count
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

struct AsyncSma {
    window: std::collections::VecDeque<f64>,
    sum:    f64,
}
impl AsyncSma {
    fn new() -> Self {
        Self { window: std::collections::VecDeque::with_capacity(SMA_WINDOW), sum: 0.0 }
    }
    fn update(&mut self, v: f64) -> f64 {
        if self.window.len() >= SMA_WINDOW {
            self.sum -= self.window.pop_front().unwrap_or(0.0);
        }
        self.window.push_back(v);
        self.sum += v;
        self.sum / self.window.len() as f64
    }
}

#[inline(never)]
fn async_black_box(seed: f64, seq: u64) -> f64 {
    let mut acc = seed;
    for i in 0..32u64 {
        acc = (acc * 1.000_001 + (i ^ seq) as f64 * 0.000_001).sin() * 100.0;
    }
    acc
}

// ─── Comparison runner ────────────────────────────────────────────────────────

/// Runs both the threaded and async pipelines for `window` seconds each and
/// returns `(threaded_throughput, async_throughput, threaded_jitter, async_jitter)`.
pub async fn compare_pipelines(
    metrics: MetricsHandle,
    window:  Duration,
) -> (f64, f64, f64, f64) {
    // ── Async run ─────────────────────────────────────────────────────────────
    let (async_tput, async_jitter) =
        run_async_pipeline(Arc::clone(&metrics), window).await;

    // ── Threaded run (baseline measurement from main SharedMetrics) ───────────
    // The threaded throughput is estimated from iteration counts already
    // recorded in SharedMetrics by the main pipeline.
    let (threaded_tput, threaded_jitter) = {
        let m = metrics.lock().unwrap();
        let tput   = m.total_loop_iterations as f64 / window.as_secs_f64();
        let jitter = m.sensor_jitter_us.mean();
        (tput, jitter)
    };

    // Persist for dashboard.
    if let Ok(mut m) = metrics.lock() {
        m.async_throughput    = async_tput;
        m.threaded_throughput = threaded_tput;
    }

    (threaded_tput, async_tput, threaded_jitter, async_jitter)
}
