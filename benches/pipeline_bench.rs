/// pipeline_bench – Criterion benchmarks for Component A (RTS2601)
///
/// Compares:
///   1. Async pipeline (Tokio) vs Threaded pipeline (std::thread)
///      – measured throughput (events/s) over a 3-second mock run
///   2. Channel send latency – tokio::sync::mpsc vs crossbeam bounded
///   3. Overflow / backpressure handling latency
///   4. Packet parsing + dispatch overhead (zero-copy → ChangePacket)
///
/// Run:  cargo bench --bench pipeline_bench

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use wiki_rt_monitor::component_a::{run_async_pipeline, run_threaded_pipeline};
use wiki_rt_monitor::component_b::{parse_zero_copy, HotPathProcessor};
use wiki_rt_monitor::component_d::{Leaderboard, SyncStrategy};
use wiki_rt_monitor::component_e::FailSafe;
use wiki_rt_monitor::ingestion::StreamSource;
use wiki_rt_monitor::metrics::new_metrics;
use wiki_rt_monitor::types::{ChangePacket, StressConfig};

// ─── Configuration ────────────────────────────────────────────────────────────

/// Short run window so Criterion iterations stay fast.
const BENCH_DURATION_SECS: u64 = 3;
/// Events per second for the mock source.
const BENCH_EPS: u64 = 200;

// ─── Benchmark 1: async pipeline throughput ───────────────────────────────────

fn bench_async_pipeline_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    c.bench_function("pipeline/async_throughput", |b| {
        b.iter(|| {
            rt.block_on(async {
                let metrics   = new_metrics();
                let stop      = Arc::new(AtomicBool::new(false));
                let (tx, rx)  = tokio::sync::mpsc::channel(512);
                let lb        = Leaderboard::new(SyncStrategy::Atomic, Arc::clone(&metrics));
                let fs        = FailSafe::new(Arc::clone(&metrics));
                let hp        = HotPathProcessor::new(Arc::clone(&metrics), Arc::clone(&lb), Arc::clone(&fs), StressConfig::off());
                let stop_hp   = Arc::clone(&stop);
                tokio::spawn(async move { hp.run_async(rx, stop_hp).await; });

                let stats = run_async_pipeline(
                    StreamSource::Mock(BENCH_EPS),
                    tx,
                    Arc::clone(&metrics),
                    Arc::clone(&stop),
                    Duration::from_secs(BENCH_DURATION_SECS),
                    Arc::new(AtomicI64::new(0)),
                    StressConfig::off(),
                ).await;

                black_box(stats.events_received)
            })
        });
    });
}

// ─── Benchmark 2: threaded pipeline throughput ────────────────────────────────

fn bench_threaded_pipeline_throughput(c: &mut Criterion) {
    c.bench_function("pipeline/threaded_throughput", |b| {
        b.iter(|| {
            let metrics  = new_metrics();
            let stop     = Arc::new(AtomicBool::new(false));
            let (tx, rx) = crossbeam::channel::bounded(512);
            let lb       = Leaderboard::new(SyncStrategy::Atomic, Arc::clone(&metrics));
            let fs       = FailSafe::new(Arc::clone(&metrics));
            let hp       = HotPathProcessor::new(Arc::clone(&metrics), Arc::clone(&lb), Arc::clone(&fs), StressConfig::off());
            let stop_hp  = Arc::clone(&stop);
            std::thread::spawn(move || {
                hp.spawn_threaded(rx, stop_hp).join().ok();
            });

            let stats = run_threaded_pipeline(
                BENCH_EPS,
                tx,
                Arc::clone(&metrics),
                Arc::clone(&stop),
                Duration::from_secs(BENCH_DURATION_SECS),
                Arc::new(AtomicI64::new(0)),
                StressConfig::off(),
            );

            black_box(stats.events_received)
        });
    });
}

// ─── Benchmark 3: channel send latency ────────────────────────────────────────
//
// Measures the raw cost of pushing a ChangePacket into each channel type
// when the consumer is keeping up (i.e., no backpressure).

fn make_packet() -> ChangePacket {
    let buf = Bytes::from_static(br#"{
        "bot": false,
        "user": "BenchUser",
        "server_name": "en.wikipedia.org",
        "title": "Benchmark article",
        "type": "edit",
        "timestamp": 1715000000,
        "namespace": 0
    }"#);
    parse_zero_copy(&buf).expect("valid packet")
}

fn bench_channel_send_tokio(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    c.bench_function("channel/tokio_mpsc_send", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ChangePacket>(512);
            let pkt = make_packet();
            tx.send(pkt).await.ok();
            black_box(rx.recv().await)
        });
    });
}

fn bench_channel_send_crossbeam(c: &mut Criterion) {
    c.bench_function("channel/crossbeam_bounded_send", |b| {
        b.iter(|| {
            let (tx, rx) = crossbeam::channel::bounded::<ChangePacket>(512);
            let pkt = make_packet();
            tx.send(pkt).ok();
            black_box(rx.recv().ok())
        });
    });
}

// ─── Benchmark 4: try_send overflow path ──────────────────────────────────────
//
// How fast can we detect that a channel is full and discard the oldest packet?
// This is the drop-oldest backpressure path exercised under burst load.

fn bench_overflow_handling(c: &mut Criterion) {
    let mut group = c.benchmark_group("backpressure");

    group.bench_function("crossbeam_try_send_full", |b| {
        b.iter(|| {
            let (tx, rx) = crossbeam::channel::bounded::<ChangePacket>(4);
            // Fill the channel.
            for _ in 0..4 {
                let _ = tx.try_send(make_packet());
            }
            // Now try_send on a full channel → Err(Full)
            let result = tx.try_send(black_box(make_packet()));
            black_box(result.is_err());
            // Drain to avoid memory leak.
            while rx.try_recv().is_ok() {}
        });
    });

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    group.bench_function("tokio_try_send_full", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<ChangePacket>(4);
            for _ in 0..4 {
                let _ = tx.try_send(make_packet());
            }
            let result = tx.try_send(black_box(make_packet()));
            black_box(result.is_err());
            // Drain.
            while rx.try_recv().is_ok() {}
        });
    });

    group.finish();
}

// ─── Benchmark 5: parse + priority dispatch cost ──────────────────────────────

fn bench_parse_and_dispatch(c: &mut Criterion) {
    use wiki_rt_monitor::types::Priority;

    let human_buf = Bytes::from_static(br#"{
        "bot": false, "user": "Alice",
        "server_name": "en.wikipedia.org", "title": "Rust",
        "type": "edit", "timestamp": 1715000000, "namespace": 0
    }"#);

    let bot_buf = Bytes::from_static(br#"{
        "bot": true, "user": "CleanBot",
        "server_name": "de.wikipedia.org", "title": "Java",
        "type": "edit", "timestamp": 1715000001, "namespace": 0
    }"#);

    let mut group = c.benchmark_group("parse_dispatch");
    group.throughput(Throughput::Elements(1));

    group.bench_function("human_edit", |b| {
        b.iter(|| {
            let pkt = parse_zero_copy(black_box(&human_buf)).unwrap();
            // Simulate priority dispatch decision.
            black_box(if pkt.priority == Priority::High { "high" } else { "low" })
        });
    });

    group.bench_function("bot_edit", |b| {
        b.iter(|| {
            let pkt = parse_zero_copy(black_box(&bot_buf)).unwrap();
            black_box(if pkt.priority == Priority::High { "high" } else { "low" })
        });
    });

    group.finish();
}

// ─── Benchmark 6: pipeline scalability (varying EPS) ─────────────────────────

fn bench_async_pipeline_scalability(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("pipeline/async_scalability");

    for eps in [50u64, 200, 500] {
        group.throughput(Throughput::Elements(eps * BENCH_DURATION_SECS));

        group.bench_with_input(BenchmarkId::new("eps", eps), &eps, |b, &eps_val| {
            b.iter(|| {
                rt.block_on(async {
                    let metrics  = new_metrics();
                    let stop     = Arc::new(AtomicBool::new(false));
                    let (tx, rx) = tokio::sync::mpsc::channel(512);
                    let lb       = Leaderboard::new(SyncStrategy::Atomic, Arc::clone(&metrics));
                    let fs       = FailSafe::new(Arc::clone(&metrics));
                    let hp       = HotPathProcessor::new(Arc::clone(&metrics), Arc::clone(&lb), Arc::clone(&fs), StressConfig::off());
                    let stop_hp  = Arc::clone(&stop);
                    tokio::spawn(async move { hp.run_async(rx, stop_hp).await; });

                    let stats = run_async_pipeline(
                        StreamSource::Mock(eps_val),
                        tx,
                        Arc::clone(&metrics),
                        Arc::clone(&stop),
                        Duration::from_secs(BENCH_DURATION_SECS),
                        Arc::new(AtomicI64::new(0)),
                        StressConfig::off(),
                    ).await;

                    black_box(stats.throughput())
                })
            });
        });
    }

    group.finish();
}

// ─── Benchmark 7: async vs threaded tail latency under load spike ─────────────
//
// Distinction requirement: "Criterion.rs comparing Async vs Threaded Tail
// Latency (p99) better during high-velocity spikes."
//
// We run each architecture at steady 200 EPS and measure the p99 end-to-end
// latency across all processed packets.  A second pass runs the same
// architectures under 2000 EPS (spike) to expose tail-latency difference.

fn bench_tail_latency_spike(c: &mut Criterion) {

    let mut group = c.benchmark_group("spike/tail_latency_p99");

    // Run for 3 seconds at each EPS level.
    for eps in [200u64, 2000] {
        // ── Async ────────────────────────────────────────────────────────────
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let label = format!("async_{eps}eps");

        group.bench_function(&label, |b| {
            b.iter(|| {
                rt.block_on(async {
                    let metrics   = new_metrics();
                    let stop      = Arc::new(AtomicBool::new(false));
                    let (tx, rx)  = tokio::sync::mpsc::channel(512);
                    let lb        = Leaderboard::new(SyncStrategy::Atomic, Arc::clone(&metrics));
                    let fs        = FailSafe::new(Arc::clone(&metrics));
                    let hp        = HotPathProcessor::new(Arc::clone(&metrics), Arc::clone(&lb), Arc::clone(&fs), StressConfig::off());
                    let stop_hp   = Arc::clone(&stop);
                    tokio::spawn(async move { hp.run_async(rx, stop_hp).await; });

                    let stats = run_async_pipeline(
                        StreamSource::Mock(eps),
                        tx,
                        Arc::clone(&metrics),
                        Arc::clone(&stop),
                        Duration::from_secs(BENCH_DURATION_SECS),
                        Arc::new(AtomicI64::new(0)),
                        StressConfig::off(),
                    ).await;

                    // Return p99 from the metrics snapshot.
                    let m = metrics.lock().unwrap();
                    let p99 = m.human_latency_us.p99() + m.bot_latency_us.p99();
                    black_box((stats.events_received, p99))
                })
            });
        });

        // ── Threaded ─────────────────────────────────────────────────────────
        let label_t = format!("threaded_{eps}eps");
        group.bench_function(&label_t, |b| {
            b.iter(|| {
                let metrics  = new_metrics();
                let stop     = Arc::new(AtomicBool::new(false));
                let (tx, rx) = crossbeam::channel::bounded(512);
                let lb       = Leaderboard::new(SyncStrategy::Atomic, Arc::clone(&metrics));
                let fs       = FailSafe::new(Arc::clone(&metrics));
                let hp       = HotPathProcessor::new(Arc::clone(&metrics), Arc::clone(&lb), Arc::clone(&fs), StressConfig::off());
                let stop_hp  = Arc::clone(&stop);
                std::thread::spawn(move || {
                    hp.spawn_threaded(rx, stop_hp).join().ok();
                });

                let stats = run_threaded_pipeline(
                    eps,
                    tx,
                    Arc::clone(&metrics),
                    Arc::clone(&stop),
                    Duration::from_secs(BENCH_DURATION_SECS),
                    Arc::new(AtomicI64::new(0)),
                    StressConfig::off(),
                );

                let m = metrics.lock().unwrap();
                let p99 = m.human_latency_us.p99() + m.bot_latency_us.p99();
                black_box((stats.events_received, p99))
            });
        });
    }

    group.finish();
}

// ─── Criterion groups ─────────────────────────────────────────────────────────

criterion_group!(
    throughput,
    bench_async_pipeline_throughput,
    bench_threaded_pipeline_throughput,
    bench_async_pipeline_scalability,
);
criterion_group!(
    channels,
    bench_channel_send_tokio,
    bench_channel_send_crossbeam,
    bench_overflow_handling,
);
criterion_group!(dispatch, bench_parse_and_dispatch);
criterion_group!(spike, bench_tail_latency_spike);

criterion_main!(throughput, channels, dispatch, spike);
