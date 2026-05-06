/// Component D – Synchronisation Strategy Benchmark
///
/// Quantitatively compares Mutex, RwLock, and Atomic types for updating
/// global statistics under high thread contention.
///
/// This module is called directly from `benches/sync_bench.rs` (Criterion)
/// AND from `main.rs` for the inline micro-benchmark in the summary output.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub struct SyncBenchResult {
    pub strategy:    &'static str,
    pub writers:     usize,
    pub ops_per_sec: f64,
    pub mean_ns:     f64,
    pub p99_ns:      f64,
}

/// Run a contention benchmark with `num_writers` competing threads,
/// each performing `ops_each` increment operations.
pub fn benchmark_mutex(num_writers: usize, ops_each: u64) -> SyncBenchResult {
    let map = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let key = "en.wikipedia.org";

    let start = Instant::now();
    let mut latencies = Vec::with_capacity((num_writers * ops_each as usize).min(100_000));

    let handles: Vec<_> = (0..num_writers).map(|_| {
        let m  = Arc::clone(&map);
        let k  = key.to_owned();
        std::thread::spawn(move || {
            let mut lats = Vec::with_capacity(ops_each as usize);
            for _ in 0..ops_each {
                let t0 = Instant::now();
                *m.lock().entry(k.clone()).or_insert(0) += 1;
                lats.push(t0.elapsed().as_nanos() as f64);
            }
            lats
        })
    }).collect();

    for h in handles {
        if let Ok(lats) = h.join() { latencies.extend(lats); }
    }

    let elapsed  = start.elapsed().as_secs_f64();
    let total    = (num_writers as u64 * ops_each) as f64;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99 = latencies.get((latencies.len() as f64 * 0.99) as usize)
                       .cloned().unwrap_or(0.0);
    let mean = latencies.iter().sum::<f64>() / latencies.len().max(1) as f64;

    SyncBenchResult {
        strategy: "Mutex",
        writers:  num_writers,
        ops_per_sec: total / elapsed,
        mean_ns: mean,
        p99_ns:  p99,
    }
}

pub fn benchmark_rwlock(num_writers: usize, ops_each: u64) -> SyncBenchResult {
    let map = Arc::new(RwLock::new(HashMap::<String, u64>::new()));
    let key = "en.wikipedia.org";

    let start = Instant::now();
    let mut latencies = Vec::with_capacity((num_writers * ops_each as usize).min(100_000));

    let handles: Vec<_> = (0..num_writers).map(|_| {
        let m  = Arc::clone(&map);
        let k  = key.to_owned();
        std::thread::spawn(move || {
            let mut lats = Vec::with_capacity(ops_each as usize);
            for _ in 0..ops_each {
                let t0 = Instant::now();
                *m.write().entry(k.clone()).or_insert(0) += 1;
                lats.push(t0.elapsed().as_nanos() as f64);
            }
            lats
        })
    }).collect();

    for h in handles {
        if let Ok(lats) = h.join() { latencies.extend(lats); }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total   = (num_writers as u64 * ops_each) as f64;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99  = latencies.get((latencies.len() as f64 * 0.99) as usize)
                        .cloned().unwrap_or(0.0);
    let mean = latencies.iter().sum::<f64>() / latencies.len().max(1) as f64;

    SyncBenchResult {
        strategy: "RwLock",
        writers: num_writers,
        ops_per_sec: total / elapsed,
        mean_ns: mean,
        p99_ns:  p99,
    }
}

pub fn benchmark_atomic(num_writers: usize, ops_each: u64) -> SyncBenchResult {
    let counter = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut latencies = Vec::with_capacity((num_writers * ops_each as usize).min(100_000));

    let handles: Vec<_> = (0..num_writers).map(|_| {
        let c = Arc::clone(&counter);
        std::thread::spawn(move || {
            let mut lats = Vec::with_capacity(ops_each as usize);
            for _ in 0..ops_each {
                let t0 = Instant::now();
                c.fetch_add(1, Ordering::Relaxed);
                lats.push(t0.elapsed().as_nanos() as f64);
            }
            lats
        })
    }).collect();

    for h in handles {
        if let Ok(lats) = h.join() { latencies.extend(lats); }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let total   = (num_writers as u64 * ops_each) as f64;
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p99  = latencies.get((latencies.len() as f64 * 0.99) as usize)
                        .cloned().unwrap_or(0.0);
    let mean = latencies.iter().sum::<f64>() / latencies.len().max(1) as f64;

    SyncBenchResult {
        strategy: "Atomic",
        writers: num_writers,
        ops_per_sec: total / elapsed,
        mean_ns: mean,
        p99_ns:  p99,
    }
}

/// Run all three strategies and print a comparison table.
pub fn run_all_and_print(num_writers: usize, ops_each: u64) {
    let results = [
        benchmark_mutex(num_writers, ops_each),
        benchmark_rwlock(num_writers, ops_each),
        benchmark_atomic(num_writers, ops_each),
    ];

    println!();
    println!("  ── Sync-strategy benchmark ({num_writers} writers, {ops_each} ops each) ──");
    println!("  {:8}  {:>14}  {:>10}  {:>10}",
             "Strategy", "ops/sec", "mean (ns)", "p99 (ns)");
    println!("  {}", "─".repeat(50));
    for r in &results {
        println!("  {:8}  {:>14.0}  {:>10.1}  {:>10.1}",
                 r.strategy, r.ops_per_sec, r.mean_ns, r.p99_ns);
    }
}
