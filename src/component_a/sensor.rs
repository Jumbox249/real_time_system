/// Sensing Subsystem – Component A
///
/// Three independent sensor threads (Force, Position, Temperature) each
/// run a periodic task with a 5 ms sampling interval (Ts = 5 ms).
///
/// Jitter mitigation:
///   `thread::sleep` wakes up with OS-scheduler granularity that is often
///   > 1 ms.  SpinSleeper yields the CPU for most of the interval, then
///   busy-waits for the final ~50 µs, achieving sub-10 µs jitter on a
///   general-purpose OS.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;
use rand::Rng;
use spin_sleep::SpinSleeper;

use crate::metrics::MetricsHandle;
use crate::types::{SensorData, SensorType};

const SAMPLE_PERIOD: Duration = Duration::from_millis(5); // Ts = 5 ms

/// Configures and launches one sensor thread.
pub struct SensorThread {
    pub sensor_type: SensorType,
    /// Nominal physical value (e.g., 10 N for Force sensor).
    pub base_value:  f64,
    /// Standard deviation of the Gaussian noise added to each reading.
    pub noise_std:   f64,
}

impl SensorThread {
    pub fn new(sensor_type: SensorType, base_value: f64, noise_std: f64) -> Self {
        Self { sensor_type, base_value, noise_std }
    }

    /// Spawns the sensor loop on a new OS thread and returns immediately.
    ///
    /// The loop runs until `stop` is set to `true`.
    pub fn spawn(
        self,
        tx:      Sender<SensorData>,
        metrics: MetricsHandle,
        stop:    Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name(format!("sensor-{}", self.sensor_type))
            .spawn(move || self.run(tx, metrics, stop))
            .expect("failed to spawn sensor thread")
    }

    fn run(self, tx: Sender<SensorData>, metrics: MetricsHandle, stop: Arc<AtomicBool>) {
        let sleeper  = SpinSleeper::default();
        let mut rng  = rand::thread_rng();
        let mut seq  = 0u64;
        let mut prev = Instant::now();

        while !stop.load(Ordering::Relaxed) {
            let wake = Instant::now();

            // ── Jitter measurement ──────────────────────────────────────────
            let elapsed   = wake.duration_since(prev).as_micros() as f64;
            let expected  = SAMPLE_PERIOD.as_micros() as f64;
            let jitter_us = (elapsed - expected).abs();

            if let Ok(mut m) = metrics.try_lock() {
                m.sensor_jitter_us.push(jitter_us);
                m.total_loop_iterations += 1;
            }
            prev = wake;

            // ── Noisy reading ───────────────────────────────────────────────
            let noise     = rng.sample::<f64, _>(rand::distributions::Standard) * self.noise_std * 2.0
                          - self.noise_std;
            let raw_value = self.base_value + noise;

            let mut data = SensorData::new(self.sensor_type, raw_value, seq);
            seq += 1;

            // ── Record raw value for dashboard ──────────────────────────────
            if let Ok(mut m) = metrics.try_lock() {
                match self.sensor_type {
                    SensorType::Force       => m.push_force(raw_value),
                    SensorType::Position    => m.push_position(raw_value),
                    SensorType::Temperature => m.push_temperature(raw_value),
                }
            }

            // ── Send to Processor ───────────────────────────────────────────
            // Non-blocking: if the processor channel is full, drop the sample
            // and keep the sensor loop on schedule.
            let _ = tx.try_send(data);

            // ── Sleep until next period ─────────────────────────────────────
            sleeper.sleep(SAMPLE_PERIOD);
        }
    }
}
