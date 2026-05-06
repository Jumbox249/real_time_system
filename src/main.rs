/// Wikipedia Real-Time Monitoring Engine
///
/// RTS2601 Assignment – main entry point.
///
/// Runs both pipeline architectures in parallel for the configured
/// duration, then prints a full performance summary.
///
///   cargo run --release              (live Wikipedia stream, 60 s)
///   cargo run --release -- --mock    (mock stream, 30 s, no network)
///   cargo run --release --bin compare_pipelines  (detailed comparison)
///   cargo bench                      (Criterion benchmarks)

use wiki_rt_monitor::component_a::{run_async_pipeline, run_threaded_pipeline};
use wiki_rt_monitor::component_b::HotPathProcessor;
use wiki_rt_monitor::component_d::{run_all_and_print, Leaderboard, SyncStrategy};
use wiki_rt_monitor::component_e::{FailSafe, Watchdog};
use wiki_rt_monitor::ingestion::StreamSource;
use wiki_rt_monitor::metrics::new_metrics;
use wiki_rt_monitor::types::StressConfig;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const RUN_SECS: u64 = 60;
const MOCK_EPS: u64 = 2000; // events/sec in mock mode

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let use_mock  = args.iter().any(|a| a == "--mock" || a == "-m");
    let stress_on = args.iter().any(|a| a == "--stress");
    let stress    = if stress_on { StressConfig::default_demo() } else { StressConfig::off() };

    let source   = if use_mock {
        println!("[main] Using mock stream ({MOCK_EPS} events/s)");
        StreamSource::Mock(MOCK_EPS)
    } else {
        println!("[main] Connecting to live Wikipedia SSE stream");
        StreamSource::Live
    };
    if stress_on {
        println!("[main] Stress demo mode enabled — expect Degraded transitions and a Watchdog reset");
    }

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║   Wikipedia Real-Time Monitoring Engine  –  RTS2601       ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    let metrics      = new_metrics();
    let stop         = Arc::new(AtomicBool::new(false));
    let last_event   = Arc::new(AtomicI64::new(0));
    let start        = Instant::now();

    // ── Shared components ─────────────────────────────────────────────────────
    let leaderboard = Leaderboard::new(SyncStrategy::RwLock, Arc::clone(&metrics));
    let fail_safe   = FailSafe::new(Arc::clone(&metrics));

    // ── Watchdog (Component E) ────────────────────────────────────────────────
    let watchdog = Watchdog::new(Arc::clone(&last_event), Arc::clone(&metrics));
    let _wdog_handle = watchdog.spawn(Arc::clone(&stop));

    // ── Async pipeline (Architecture 1) ──────────────────────────────────────
    let (async_pkt_tx, async_pkt_rx) = tokio::sync::mpsc::channel(512);
    let metrics_a   = Arc::clone(&metrics);
    let stop_a      = Arc::clone(&stop);
    let duration    = Duration::from_secs(RUN_SECS);

    let async_future = run_async_pipeline(
        source,
        async_pkt_tx,
        Arc::clone(&metrics_a),
        Arc::clone(&stop_a),
        duration,
        Arc::clone(&last_event),
        stress,
    );

    // Hot path for async pipeline.
    let hp_async = HotPathProcessor::new(
        Arc::clone(&metrics),
        Arc::clone(&leaderboard),
        Arc::clone(&fail_safe),
        stress,
    );
    let stop_hp = Arc::clone(&stop);
    tokio::spawn(async move {
        hp_async.run_async(async_pkt_rx, stop_hp).await;
    });

    // ── Threaded pipeline (Architecture 2) – spawn separately ────────────────
    let (thr_pkt_tx, thr_pkt_rx) = crossbeam::channel::bounded(512);
    let metrics_t   = Arc::clone(&metrics);
    let stop_t      = Arc::clone(&stop);
    let lb_t        = Arc::clone(&leaderboard);
    let fs_t        = Arc::clone(&fail_safe);
    let last_event_t = Arc::clone(&last_event);

    std::thread::Builder::new()
        .name("threaded-pipeline".into())
        .spawn(move || {
            run_threaded_pipeline(
                MOCK_EPS, // threaded always uses mock for offline testing
                thr_pkt_tx,
                metrics_t,
                stop_t,
                duration,
                last_event_t,
                stress,
            );
        })
        .expect("threaded pipeline thread");

    // Hot path for threaded pipeline.
    let hp_threaded = HotPathProcessor::new(
        Arc::clone(&metrics),
        lb_t,
        fs_t,
        stress,
    );
    let stop_hp2 = Arc::clone(&stop);
    std::thread::Builder::new()
        .name("threaded-hot-path".into())
        .spawn(move || {
            hp_threaded.spawn_threaded(thr_pkt_rx, stop_hp2).join().ok();
        })
        .expect("hot-path thread");

    // ── Run async pipeline (blocks for RUN_SECS) ─────────────────────────────
    println!("[main] Running for {RUN_SECS}s…  (--mock for offline mode)");
    let _ = async_future.await;
    stop.store(true, Ordering::Relaxed);

    std::thread::sleep(Duration::from_millis(200)); // let threads exit

    // ── Performance summary ───────────────────────────────────────────────────
    let elapsed = start.elapsed().as_secs_f64();
    let m = metrics.lock().unwrap();

    println!();
    println!("════════════════════════ PERFORMANCE SUMMARY ════════════════════════");
    println!("  Uptime:                         {elapsed:.1} s");
    println!();
    println!("  ── Component A: Ingestion ─────────────────────────────────────────");
    println!("  Async events received:          {}", m.async_events_received);
    println!("  Async throughput:               {:.0} events/s", m.async_throughput);
    println!("  Async overflow drops:           {}", m.async_overflow_count);
    println!("  Threaded events received:       {}", m.threaded_events_received);
    println!("  Threaded throughput:            {:.0} events/s", m.threaded_throughput);
    println!("  Threaded overflow drops:        {}", m.threaded_overflow_count);
    println!();
    println!("  ── Component B: Hot Path (2 ms deadline) ──────────────────────────");
    println!("  Deadline misses:                {}", m.deadline_misses);
    println!("  Human latency  p50/p90/p99:     {:.1} / {:.1} / {:.1} µs",
             m.human_latency_us.p50(), m.human_latency_us.p90(), m.human_latency_us.p99());
    println!("  Bot latency    p50/p90/p99:     {:.1} / {:.1} / {:.1} µs",
             m.bot_latency_us.p50(), m.bot_latency_us.p90(), m.bot_latency_us.p99());
    println!();
    println!("  ── Component C: Scheduling Drift ──────────────────────────────────");
    println!("  Human drift    p50/p90/p99:     {:.1} / {:.1} / {:.1} µs",
             m.human_drift_us.p50(), m.human_drift_us.p90(), m.human_drift_us.p99());
    println!("  Bot drift      p50/p90/p99:     {:.1} / {:.1} / {:.1} µs",
             m.bot_drift_us.p50(), m.bot_drift_us.p90(), m.bot_drift_us.p99());
    println!();
    println!("  ── Component D: Leaderboard (Top-3 Domains) ───────────────────────");
    drop(m); // release metrics lock before top_n read (uses leaderboard directly)
    let top3 = leaderboard.top_n(3);
    for (i, (domain, count)) in top3.iter().enumerate() {
        println!("  {}. {:30} {:>8} edits", i + 1, domain, count);
    }
    let m = metrics.lock().unwrap();
    println!("  Mutex write ops:                {}", m.mutex_write_ops);
    println!("  RwLock write ops:               {}", m.rwlock_write_ops);
    println!("  Atomic write ops:               {}", m.atomic_write_ops);
    println!();
    println!("  ── Component E: Fault Tolerance ───────────────────────────────────");
    println!("  Watchdog resets:                {}", m.watchdog_resets);
    println!("  Fail-safe activations:          {}", m.fail_safe_activations);
    println!("  Current mode:                   {}", m.current_mode);
    drop(m);
    println!("═══════════════════════════════════════════════════════════════════════");

    println!();
    println!("  ── Sync-Strategy Benchmark (inline) ───────────────────────────────");
    run_all_and_print(4, 10_000);

    println!();
    println!("  Run `cargo bench` for full Criterion benchmark reports.");
    println!("  Run `cargo run --release --bin compare_pipelines -- --mock` for");
    println!("  detailed async vs threaded comparison with p50/p90/p99 percentiles.");
}
