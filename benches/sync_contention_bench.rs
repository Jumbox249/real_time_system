/// Shared Resource Synchronisation Benchmarks
///
/// Compares write contention across three synchronisation strategies:
///   • Mutex   – standard blocking lock
///   • Atomics – CAS-based wait-free counter
///   • Lock-free (ArrayQueue) – non-blocking bounded queue
///
/// Run: cargo bench --bench sync_contention_bench
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crossbeam::queue::ArrayQueue;

// ─── Single-thread write contention ──────────────────────────────────────────

fn bench_single_thread_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_contention_write");
    group.measurement_time(Duration::from_secs(5));

    // Mutex
    let mutex_state: Mutex<Vec<f64>> = Mutex::new(Vec::with_capacity(4096));
    group.bench_function("mutex_write", |b| {
        b.iter(|| {
            let mut guard = mutex_state.lock().unwrap();
            guard.push(black_box(42.0_f64));
            if guard.len() > 4096 { guard.drain(..2048); }
        });
    });

    // Atomic
    let atomic_counter = AtomicU64::new(0);
    group.bench_function("atomic_write", |b| {
        b.iter(|| {
            atomic_counter.fetch_add(black_box(1), Ordering::Relaxed)
        });
    });

    // Lock-free ArrayQueue
    let lf_queue: ArrayQueue<f64> = ArrayQueue::new(512);
    group.bench_function("lock_free_push", |b| {
        b.iter(|| {
            let _ = lf_queue.push(black_box(42.0_f64));
            let _ = lf_queue.pop(); // drain to avoid overflow
        });
    });

    group.finish();
}

// ─── Multi-thread write contention (N writers) ───────────────────────────────

fn bench_multi_thread_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_contention_multi_write");
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(10);

    for num_writers in [2usize, 4, 8] {
        // Mutex multi-writer
        {
            let state = Arc::new(Mutex::new(0u64));
            group.bench_with_input(
                BenchmarkId::new("mutex_writers", num_writers),
                &num_writers,
                |b, &n| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..n)
                            .map(|_| {
                                let s = Arc::clone(&state);
                                std::thread::spawn(move || {
                                    let mut g = s.lock().unwrap();
                                    *g = g.wrapping_add(black_box(1));
                                })
                            })
                            .collect();
                        for h in handles { h.join().unwrap(); }
                    });
                },
            );
        }

        // Atomic multi-writer
        {
            let counter = Arc::new(AtomicU64::new(0));
            group.bench_with_input(
                BenchmarkId::new("atomic_writers", num_writers),
                &num_writers,
                |b, &n| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..n)
                            .map(|_| {
                                let c = Arc::clone(&counter);
                                std::thread::spawn(move || {
                                    c.fetch_add(black_box(1), Ordering::Relaxed)
                                })
                            })
                            .collect();
                        for h in handles { h.join().unwrap(); }
                    });
                },
            );
        }

        // Lock-free multi-writer
        {
            let queue = Arc::new(ArrayQueue::<u64>::new(1024));
            group.bench_with_input(
                BenchmarkId::new("lock_free_writers", num_writers),
                &num_writers,
                |b, &n| {
                    b.iter(|| {
                        let handles: Vec<_> = (0..n)
                            .map(|_| {
                                let q = Arc::clone(&queue);
                                std::thread::spawn(move || {
                                    let _ = q.push(black_box(1u64));
                                    let _ = q.pop();
                                })
                            })
                            .collect();
                        for h in handles { h.join().unwrap(); }
                    });
                },
            );
        }
    }

    group.finish();
}

// ─── Priority inversion simulation ───────────────────────────────────────────

/// Simulate priority inversion: high-priority task waits on a Mutex held
/// by a low-priority task.  Atomic/lock-free alternatives avoid this entirely.
fn bench_priority_inversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("priority_inversion_sim");

    let mutex = Arc::new(Mutex::new(0u64));

    group.bench_function("mutex_contended_high_prio", |b| {
        let m = Arc::clone(&mutex);
        b.iter(|| {
            // Simulate low-priority thread holding the lock.
            let m_low  = Arc::clone(&m);
            let holder = std::thread::spawn(move || {
                let mut g = m_low.lock().unwrap();
                // Simulate work inside the lock (causes blocking of high-prio task).
                std::hint::black_box(&mut *g);
                std::thread::yield_now();
            });

            // High-priority thread tries to acquire the same lock.
            let wait_start = Instant::now();
            {
                let g = m.lock().unwrap();
                black_box(*g);
            }
            let wait_us = wait_start.elapsed().as_micros();
            holder.join().unwrap();
            black_box(wait_us)
        });
    });

    group.bench_function("atomic_no_blocking", |b| {
        let counter = Arc::new(AtomicU64::new(0));
        b.iter(|| {
            // Atomic increment never blocks – zero priority inversion risk.
            let v = counter.fetch_add(black_box(1), Ordering::Relaxed);
            black_box(v)
        });
    });

    group.finish();
}

use std::time::Instant;
criterion_group!(
    benches,
    bench_single_thread_write,
    bench_multi_thread_write,
    bench_priority_inversion,
);
criterion_main!(benches);
