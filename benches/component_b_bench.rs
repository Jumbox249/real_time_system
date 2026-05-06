/// Component B Benchmarks
///
/// Measures:
///   • End-to-end latency – receiver dispatch
///   • Actuator deadline compliance
///   • PID computation latency
///   • Feedback loop emission latency
///
/// Run: cargo bench --bench component_b_bench
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::{Duration, Instant};

use crossbeam::channel::bounded;
use pid::Pid;

// ─── End-to-end / receiver latency ───────────────────────────────────────────

fn bench_receiver_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("receiver_latency");
    group.measurement_time(Duration::from_secs(10));

    let (tx, rx) = bounded::<u64>(1024);

    group.bench_function("dispatch_timestamping", |b| {
        b.iter(|| {
            let _ = tx.try_send(black_box(1u64));
            match rx.try_recv() {
                Ok(_v) => {
                    let t3 = Instant::now();
                    black_box(t3)
                }
                Err(_) => black_box(Instant::now()),
            }
        });
    });

    group.finish();
}

// ─── Actuator deadline check ──────────────────────────────────────────────────

const ACTUATOR_DEADLINE: Duration = Duration::from_millis(2);

fn bench_actuator_deadline(c: &mut Criterion) {
    let mut group = c.benchmark_group("actuator_deadline");

    group.bench_function("deadline_check_only", |b| {
        b.iter(|| {
            let start = Instant::now();
            // Simulate the minimum actuator work (clamp + factor multiply).
            let output = black_box(42.0_f64).clamp(-100.0, 100.0) * 0.85;
            let miss   = start.elapsed() > ACTUATOR_DEADLINE;
            black_box((output, miss))
        });
    });

    // Dispatch latency per actuator type.
    for (label, factor) in [("gripper", 0.85f64), ("motor", 0.92), ("stabiliser", 0.78)] {
        group.bench_function(format!("dispatch_{label}"), |b| {
            b.iter(|| {
                let signal = black_box(55.0_f64).clamp(-100.0, 100.0) * factor;
                black_box(signal)
            });
        });
    }

    group.finish();
}

// ─── PID computation benchmark ────────────────────────────────────────────────

fn bench_pid_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("pid_latency");

    let make_pid = |sp: f64| {
        let mut p: Pid<f64> = Pid::new(sp, 100.0);
        p.p(1.2, 100.0).i(0.05, 50.0).d(0.01, 10.0);
        p
    };

    let mut force_pid = make_pid(10.0);
    let mut pos_pid   = make_pid(5.0);
    let mut temp_pid  = make_pid(25.0);

    group.bench_function("pid_force", |b| {
        let mut measurement = 9.8_f64;
        b.iter(|| {
            measurement += 0.001;
            let out = force_pid.next_control_output(black_box(measurement)).output;
            black_box(out)
        });
    });

    group.bench_function("pid_position", |b| {
        let mut measurement = 4.9_f64;
        b.iter(|| {
            measurement += 0.001;
            let out = pos_pid.next_control_output(black_box(measurement)).output;
            black_box(out)
        });
    });

    group.bench_function("pid_temperature", |b| {
        let mut measurement = 24.5_f64;
        b.iter(|| {
            measurement += 0.001;
            let out = temp_pid.next_control_output(black_box(measurement)).output;
            black_box(out)
        });
    });

    group.finish();
}

// ─── Feedback loop latency ────────────────────────────────────────────────────

fn bench_feedback_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("feedback_latency");

    let (tx, rx) = bounded::<bool>(64);

    group.bench_function("feedback_try_send", |b| {
        b.iter(|| {
            let start = Instant::now();
            let _     = tx.try_send(black_box(false));
            let _     = rx.try_recv();
            let lat   = start.elapsed().as_nanos();
            black_box(lat < 500_000) // within 0.5 ms deadline
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_receiver_latency,
    bench_actuator_deadline,
    bench_pid_latency,
    bench_feedback_latency,
);
criterion_main!(benches);
