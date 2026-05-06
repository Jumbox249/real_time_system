/// sync_bench – Criterion benchmarks for Component D synchronisation strategies
///
/// Compares three concurrent leaderboard update strategies:
///   1. Mutex<HashMap<String, u64>>  – coarse exclusive lock (parking_lot)
///   2. RwLock<HashMap<String, u64>> – reader-writer lock (parking_lot)
///   3. AtomicU64 per domain slot    – lock-free increment
///
/// Each group tests scalability by varying writer thread count: 1, 2, 4, 8.
///
/// Run:  cargo bench --bench sync_bench

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use wiki_rt_monitor::component_d::{Leaderboard, SyncStrategy};
use wiki_rt_monitor::metrics::new_metrics;

// ─── Helpers ──────────────────────────────────────────────────────────────────

const DOMAIN: &str = "en.wikipedia.org";
const OPS_PER_THREAD: u64 = 1_000;

/// Spawn `writers` threads, each calling `f` for `OPS_PER_THREAD` iterations.
fn par_write<F>(writers: usize, f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    let f = Arc::new(f);
    let handles: Vec<_> = (0..writers)
        .map(|_| {
            let ff = Arc::clone(&f);
            std::thread::spawn(move || {
                for _ in 0..OPS_PER_THREAD {
                    ff();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().ok();
    }
}

// ─── Benchmark: Mutex ─────────────────────────────────────────────────────────

fn bench_mutex(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_strategy/Mutex");

    for writers in [1usize, 2, 4, 8] {
        let total_ops = (writers as u64) * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops));

        group.bench_with_input(
            BenchmarkId::new("writers", writers),
            &writers,
            |b, &w| {
                b.iter(|| {
                    let map = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
                    par_write(w, move || {
                        *map.lock()
                            .entry(black_box(DOMAIN).to_owned())
                            .or_insert(0) += 1;
                    });
                });
            },
        );
    }

    group.finish();
}

// ─── Benchmark: RwLock ────────────────────────────────────────────────────────

fn bench_rwlock(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_strategy/RwLock");

    for writers in [1usize, 2, 4, 8] {
        let total_ops = (writers as u64) * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops));

        group.bench_with_input(
            BenchmarkId::new("writers", writers),
            &writers,
            |b, &w| {
                b.iter(|| {
                    let map = Arc::new(RwLock::new(HashMap::<String, u64>::new()));
                    par_write(w, move || {
                        *map.write()
                            .entry(black_box(DOMAIN).to_owned())
                            .or_insert(0) += 1;
                    });
                });
            },
        );
    }

    group.finish();
}

// ─── Benchmark: AtomicU64 ─────────────────────────────────────────────────────

fn bench_atomic(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_strategy/Atomic");

    for writers in [1usize, 2, 4, 8] {
        let total_ops = (writers as u64) * OPS_PER_THREAD;
        group.throughput(Throughput::Elements(total_ops));

        group.bench_with_input(
            BenchmarkId::new("writers", writers),
            &writers,
            |b, &w| {
                b.iter(|| {
                    let counter = Arc::new(AtomicU64::new(0));
                    par_write(w, move || {
                        counter.fetch_add(black_box(1), Ordering::Relaxed);
                    });
                });
            },
        );
    }

    group.finish();
}

// ─── Benchmark: Leaderboard (Component D API) ─────────────────────────────────
//
// Tests the full Leaderboard abstraction at each SyncStrategy, including
// the top_n() read that produces a sorted snapshot.

fn bench_leaderboard_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("leaderboard/increment");

    let domains = [
        "en.wikipedia.org",
        "de.wikipedia.org",
        "fr.wikipedia.org",
        "ja.wikipedia.org",
        "ko.wikipedia.org",
    ];

    for strategy in [SyncStrategy::Mutex, SyncStrategy::RwLock, SyncStrategy::Atomic] {
        let label = format!("{strategy:?}");
        group.bench_function(&label, |b| {
            let metrics = new_metrics();
            let lb = Leaderboard::new(strategy, Arc::clone(&metrics));
            let mut i = 0usize;
            b.iter(|| {
                lb.increment(black_box(domains[i % domains.len()]));
                i += 1;
            });
        });
    }

    group.finish();
}

fn bench_leaderboard_top_n(c: &mut Criterion) {
    let mut group = c.benchmark_group("leaderboard/top_n");

    let domains = [
        "en.wikipedia.org",
        "de.wikipedia.org",
        "fr.wikipedia.org",
        "ja.wikipedia.org",
        "es.wikipedia.org",
        "pt.wikipedia.org",
        "ru.wikipedia.org",
        "zh.wikipedia.org",
        "it.wikipedia.org",
        "nl.wikipedia.org",
    ];

    for strategy in [SyncStrategy::Mutex, SyncStrategy::RwLock, SyncStrategy::Atomic] {
        let label = format!("{strategy:?}");
        group.bench_function(&label, |b| {
            let metrics = new_metrics();
            let lb = Leaderboard::new(strategy, Arc::clone(&metrics));
            // Pre-populate with 10 000 edits spread across domains.
            for i in 0..10_000u64 {
                lb.increment(domains[(i as usize) % domains.len()]);
            }
            b.iter(|| {
                let top = lb.top_n(black_box(3));
                black_box(top)
            });
        });
    }

    group.finish();
}

// ─── Benchmark: write-heavy mixed read/write ──────────────────────────────────
//
// 8 writer threads + 2 reader threads (top_n) simultaneously.

fn bench_mixed_readwrite(c: &mut Criterion) {
    let mut group = c.benchmark_group("leaderboard/mixed_readwrite");

    for strategy in [SyncStrategy::Mutex, SyncStrategy::RwLock, SyncStrategy::Atomic] {
        let label = format!("{strategy:?}");
        group.throughput(Throughput::Elements(8 * OPS_PER_THREAD));

        group.bench_function(&label, |b| {
            b.iter(|| {
                let metrics = new_metrics();
                let lb = Arc::new(Leaderboard::new(strategy, Arc::clone(&metrics)));

                // 8 writer threads.
                let writer_handles: Vec<_> = (0..8)
                    .map(|i| {
                        let lb2 = Arc::clone(&lb);
                        std::thread::spawn(move || {
                            for j in 0..OPS_PER_THREAD {
                                lb2.increment(if (i + j) % 2 == 0 {
                                    "en.wikipedia.org"
                                } else {
                                    "de.wikipedia.org"
                                });
                            }
                        })
                    })
                    .collect();

                // 2 reader threads.
                let reader_handles: Vec<_> = (0..2)
                    .map(|_| {
                        let lb2 = Arc::clone(&lb);
                        std::thread::spawn(move || {
                            for _ in 0..(OPS_PER_THREAD / 10) {
                                black_box(lb2.top_n(3));
                            }
                        })
                    })
                    .collect();

                for h in writer_handles.into_iter().chain(reader_handles) {
                    h.join().ok();
                }
            });
        });
    }

    group.finish();
}

// ─── Criterion groups ─────────────────────────────────────────────────────────

criterion_group!(primitives, bench_mutex, bench_rwlock, bench_atomic);
criterion_group!(
    leaderboard_ops,
    bench_leaderboard_record,
    bench_leaderboard_top_n,
    bench_mixed_readwrite,
);

criterion_main!(primitives, leaderboard_ops);
