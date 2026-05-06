/// Component A Benchmarks
///
/// Measures:
///   • Sensor jitter  – SpinSleeper vs thread::sleep
///   • Processing latency – SMA + anomaly detection per packet
///   • IPC transmission latency – try_send on a bounded channel
///
/// Run: cargo bench --bench component_a_bench
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crossbeam::channel::bounded;
use spin_sleep::SpinSleeper;

// ─── Sensor jitter benchmarks ─────────────────────────────────────────────────

/// Measure the absolute jitter of SpinSleeper vs thread::sleep for 5 ms loops.
fn bench_jitter_spin(c: &mut Criterion) {
    let mut group = c.benchmark_group("sensor_jitter");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(50);

    let sleeper = SpinSleeper::default();
    let period  = Duration::from_millis(5);

    group.bench_function("spin_sleep_5ms", |b| {
        let mut prev = Instant::now();
        b.iter(|| {
            sleeper.sleep(black_box(period));
            let now     = Instant::now();
            let elapsed = now.duration_since(prev).as_micros() as f64;
            let jitter  = (elapsed - 5_000.0).abs();
            prev = now;
            black_box(jitter)
        });
    });

    group.bench_function("thread_sleep_5ms", |b| {
        let mut prev = Instant::now();
        b.iter(|| {
            std::thread::sleep(black_box(period));
            let now     = Instant::now();
            let elapsed = now.duration_since(prev).as_micros() as f64;
            let jitter  = (elapsed - 5_000.0).abs();
            prev = now;
            black_box(jitter)
        });
    });

    group.finish();
}

// ─── Processing latency benchmark ────────────────────────────────────────────

const SMA_WINDOW: usize = 8;

/// Inline SMA filter identical to the one in Processor.
fn sma_update(window: &mut VecDeque<f64>, sum: &mut f64, v: f64) -> f64 {
    if window.len() >= SMA_WINDOW {
        *sum -= window.pop_front().unwrap_or(0.0);
    }
    window.push_back(v);
    *sum += v;
    *sum / window.len() as f64
}

fn bench_processing_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("processing_latency");

    let mut window = VecDeque::with_capacity(SMA_WINDOW);
    let mut sum    = 0.0_f64;

    group.bench_function("sma_filter", |b| {
        let mut i = 0.0_f64;
        b.iter(|| {
            i += 0.001;
            let filtered = sma_update(&mut window, &mut sum, black_box(10.0 + i));
            // Variance for anomaly detection.
            let mean = sum / window.len() as f64;
            let var: f64 = window.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                         / window.len() as f64;
            let std_dev   = var.sqrt();
            let deviation = (10.0 + i - filtered).abs();
            let anomaly   = deviation > 3.0 * std_dev.max(0.01);
            black_box((filtered, anomaly))
        });
    });

    group.bench_function("black_box_work", |b| {
        let mut i = 0u64;
        b.iter(|| {
            i += 1;
            let mut acc = black_box(10.0_f64);
            for j in 0..64u64 {
                acc = (acc * 1.000_001 + (j ^ i) as f64 * 0.000_001).sin() * 100.0;
            }
            black_box(acc)
        });
    });

    group.finish();
}

// ─── IPC transmission latency benchmark ──────────────────────────────────────

fn bench_ipc_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("ipc_latency");

    for capacity in [64usize, 256, 1024] {
        group.bench_with_input(
            BenchmarkId::new("try_send_try_recv", capacity),
            &capacity,
            |b, &cap| {
                let (tx, rx) = bounded::<f64>(cap);
                b.iter(|| {
                    let _ = tx.try_send(black_box(42.0_f64));
                    let _ = rx.try_recv();
                });
            },
        );
    }

    // Measure latency of a single non-blocking try_send on a non-full channel.
    group.bench_function("single_try_send_100us_deadline", |b| {
        let (tx, _rx) = bounded::<f64>(1024);
        b.iter(|| {
            let start  = Instant::now();
            let _      = tx.try_send(black_box(99.9_f64));
            let lat    = start.elapsed().as_micros();
            black_box(lat < 100) // true → within deadline
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_jitter_spin,
    bench_processing_latency,
    bench_ipc_latency,
);
criterion_main!(benches);
