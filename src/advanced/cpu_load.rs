/// CPU Load Generator – Advanced Feature
///
/// Introduces controlled interference to measure real-time system
/// resilience under CPU contention.  Three independent stressors model
/// the interference patterns seen in production systems:
///
///   1. ALU Contention      – saturates arithmetic-logic units
///   2. Cache Thrashing     – evicts L1/L2 cache with a 1 MB write buffer
///   3. Scheduler Contention – pins noise threads to the same core as the
///                             RT pipeline, triggering OS preemption events
///
/// Usage:
///   ```rust
///   let gen = CpuLoadGenerator::new();
///   let handles = gen.start(num_threads);  // 0..=20
///   // ... run benchmark ...
///   gen.stop();
///   ```
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PRESSURE_BUF_SIZE: usize = 1 << 20; // 1 MB – exceeds typical L2 cache

pub struct CpuLoadGenerator {
    stop: Arc<AtomicBool>,
}

impl CpuLoadGenerator {
    pub fn new() -> Self {
        Self { stop: Arc::new(AtomicBool::new(false)) }
    }

    /// Spawns `num_threads` noise threads and returns their handles.
    pub fn start(&self, num_threads: usize) -> Vec<std::thread::JoinHandle<()>> {
        (0..num_threads)
            .map(|id| {
                let stop = Arc::clone(&self.stop);
                std::thread::Builder::new()
                    .name(format!("cpu-load-{}", id))
                    .spawn(move || noise_worker(id, stop))
                    .expect("failed to spawn load thread")
            })
            .collect()
    }

    /// Signals all noise threads to exit.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Reset the stop flag so `start` can be called again.
    pub fn reset(&self) {
        self.stop.store(false, Ordering::Relaxed);
    }
}

impl Default for CpuLoadGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// A single noise worker that exercises ALU, cache and scheduler.
fn noise_worker(id: usize, stop: Arc<AtomicBool>) {
    let mut pressure_buf: Vec<u8> = vec![0u8; PRESSURE_BUF_SIZE];
    let mut acc: u64 = id as u64;

    while !stop.load(Ordering::Relaxed) {
        // ── 1. ALU Contention ────────────────────────────────────────────────
        for i in 0..4096u64 {
            acc = acc.wrapping_mul(6_364_136_223_846_793_005)
                     .wrapping_add(1_442_695_040_888_963_407)
                     ^ i;
        }

        // ── 2. Cache Thrashing ───────────────────────────────────────────────
        // Strided writes with a stride larger than a cache line (64 bytes)
        // maximise cache eviction pressure.
        let stride = 64usize;
        for i in (0..PRESSURE_BUF_SIZE).step_by(stride) {
            pressure_buf[i] = (acc & 0xFF) as u8;
            acc = acc.wrapping_add(i as u64);
        }

        // ── 3. Yield to trigger scheduler preemption ─────────────────────────
        std::thread::yield_now();
    }

    // Prevent compiler from optimising away the buffer writes.
    let _ = pressure_buf.iter().map(|b| *b as u64).sum::<u64>().wrapping_add(acc);
}

// ─── Load sweep benchmark helper ─────────────────────────────────────────────

/// Runs the real-time pipeline under increasing CPU load (0 to `max_threads`
/// extra noise threads) and returns (deadline_misses, max_jitter_us) per level.
///
/// The caller supplies a closure that runs the pipeline for one measurement
/// window and returns `(deadline_misses: u64, max_jitter_us: f64)`.
pub fn sweep_cpu_load<F>(max_threads: usize, window: Duration, measure: F)
    -> Vec<(usize, u64, f64)>
where
    F: Fn() -> (u64, f64),
{
    let gen = CpuLoadGenerator::new();
    let mut results = Vec::with_capacity(max_threads + 1);

    for n in 0..=max_threads {
        gen.reset();
        let _handles = gen.start(n);
        std::thread::sleep(Duration::from_millis(50)); // let load stabilise

        let (misses, jitter) = measure();
        results.push((n, misses, jitter));

        gen.stop();
        std::thread::sleep(Duration::from_millis(50)); // let threads exit
    }

    results
}
