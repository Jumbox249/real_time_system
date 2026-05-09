/// Wikipedia Real-Time Monitoring Engine
///
/// RTS2601 Assignment – main entry point.
///
/// Runs both pipeline architectures in parallel for the configured
/// duration, then prints a full performance summary and writes event logs.
///
///   cargo run --release              (live Wikipedia stream, 60 s)
///   cargo run --release -- --mock    (mock stream, 60 s, no network)
///   cargo run --release -- --demo    (scripted 4-phase fault-tolerance demo)
///   cargo run --release -- --stress  (stress mode with injected latency)
///   cargo run --release --bin compare_pipelines  (detailed comparison)
///   cargo bench                      (Criterion benchmarks)

use wiki_rt_monitor::component_a::{run_async_pipeline_with_reconnect, run_threaded_pipeline};
use wiki_rt_monitor::component_b::HotPathProcessor;
use wiki_rt_monitor::component_d::{run_all_and_print, Leaderboard, SyncStrategy};
use wiki_rt_monitor::component_e::{FailSafe, Watchdog};
use wiki_rt_monitor::ingestion::StreamSource;
use wiki_rt_monitor::metrics::new_metrics;
use wiki_rt_monitor::types::StressConfig;

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::io::Write;

const RUN_SECS: u64 = 60;
const MOCK_EPS: u64 = 2000; // events/sec in mock mode

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let use_mock  = args.iter().any(|a| a == "--mock" || a == "-m");
    let stress_on = args.iter().any(|a| a == "--stress");
    let demo_mode = args.iter().any(|a| a == "--demo");

    // --demo implies --mock and a scripted stress config.
    let stress = if stress_on || demo_mode {
        StressConfig::default_demo()
    } else {
        StressConfig::off()
    };

    let source = if use_mock || demo_mode {
        println!("[main] Using mock stream ({MOCK_EPS} events/s)");
        StreamSource::Mock(MOCK_EPS)
    } else {
        println!("[main] Connecting to live Wikipedia SSE stream");
        StreamSource::Live
    };

    if demo_mode {
        println!("[main] DEMO mode — 4-phase scripted fault-tolerance walkthrough");
        println!("[main]   Phase 1 ( 0-15s): baseline 2000 eps, expect NORMAL");
        println!("[main]   Phase 2 (15-25s): 3ms latency injected every 50 pkts, expect DEGRADED");
        println!("[main]   Phase 3 (25-35s): mock stream silenced, expect Watchdog reset");
        println!("[main]   Phase 4 (35-60s): latency recovers, expect RECOVERY -> NORMAL");
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
    let reconnect_flag = watchdog.reconnect_flag();
    let _wdog_handle = watchdog.spawn(Arc::clone(&stop));

    // ── Async pipeline (Architecture 1) ──────────────────────────────────────
    let (async_pkt_tx, async_pkt_rx) = tokio::sync::mpsc::channel(512);
    let metrics_a   = Arc::clone(&metrics);
    let stop_a      = Arc::clone(&stop);
    let duration    = Duration::from_secs(RUN_SECS);

    let async_future = run_async_pipeline_with_reconnect(
        source,
        async_pkt_tx,
        Arc::clone(&metrics_a),
        Arc::clone(&stop_a),
        duration,
        Arc::clone(&last_event),
        stress,
        reconnect_flag,
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
    let metrics_t    = Arc::clone(&metrics);
    let stop_t       = Arc::clone(&stop);
    let lb_t         = Arc::clone(&leaderboard);
    let fs_t         = Arc::clone(&fail_safe);
    let last_event_t = Arc::clone(&last_event);

    std::thread::Builder::new()
        .name("threaded-pipeline".into())
        .spawn(move || {
            run_threaded_pipeline(
                MOCK_EPS,
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

    // ── Demo mode: print phase markers ───────────────────────────────────────
    if demo_mode {
        let metrics_demo = Arc::clone(&metrics);
        tokio::spawn(async move {
            let phases = [
                (15u64, "PHASE 2: latency injection active — expect DEGRADED"),
                (25u64, "PHASE 3: stream silent — Watchdog will fire in 10s"),
                (35u64, "PHASE 4: stream resumes — expect RECOVERY -> NORMAL"),
            ];
            for (secs, label) in phases {
                tokio::time::sleep(Duration::from_secs(secs)).await;
                let mode = metrics_demo.lock().map(|m| m.current_mode.clone()).unwrap_or_default();
                println!("\n[demo] +{secs}s — {label}");
                println!("[demo]   current mode: {mode}");
            }
        });
    }

    // ── Run async pipeline (blocks for RUN_SECS) ─────────────────────────────
    println!("[main] Running for {RUN_SECS}s…");
    let _ = async_future.await;
    stop.store(true, Ordering::Relaxed);

    std::thread::sleep(Duration::from_millis(200)); // let threads exit

    // ── Write event logs ──────────────────────────────────────────────────────
    {
        let m = metrics.lock().unwrap();
        write_overflow_log(&m.overflow_log);
        write_deadline_miss_log(&m.deadline_miss_log);
    }

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
    println!("  Async overflow drops:           {} (logged to logs/overflow_events.csv)", m.async_overflow_count);
    println!("  Threaded events received:       {}", m.threaded_events_received);
    println!("  Threaded throughput:            {:.0} events/s", m.threaded_throughput);
    println!("  Threaded overflow drops:        {}", m.threaded_overflow_count);
    println!();
    println!("  ── Component B: Hot Path (2 ms deadline) ──────────────────────────");
    println!("  Deadline misses:                {} (logged to logs/deadline_misses.csv)", m.deadline_misses);
    println!("  Human latency  p50/p90/p99:     {:.1} / {:.1} / {:.1} us",
             m.human_latency_us.p50(), m.human_latency_us.p90(), m.human_latency_us.p99());
    println!("  Bot latency    p50/p90/p99:     {:.1} / {:.1} / {:.1} us",
             m.bot_latency_us.p50(), m.bot_latency_us.p90(), m.bot_latency_us.p99());
    println!();
    println!("  ── Component C: Scheduling Drift ──────────────────────────────────");
    println!("  Human drift    p50/p90/p99:     {:.1} / {:.1} / {:.1} us",
             m.human_drift_us.p50(), m.human_drift_us.p90(), m.human_drift_us.p99());
    println!("  Bot drift      p50/p90/p99:     {:.1} / {:.1} / {:.1} us",
             m.bot_drift_us.p50(), m.bot_drift_us.p90(), m.bot_drift_us.p99());
    let human_p99 = m.human_drift_us.p99();
    let bot_p99   = m.bot_drift_us.p99();
    if human_p99 < bot_p99 {
        println!("  [OK] Human drift p99 < bot drift p99 — priority scheduling confirmed.");
    }
    println!();
    println!("  ── Component D: Leaderboard (Top-3 Domains) ───────────────────────");
    drop(m); // release metrics lock before top_n (avoids deadlock)
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
    println!("=======================================================================");

    println!();
    println!("  ── Sync-Strategy Benchmark (inline) ───────────────────────────────");
    run_all_and_print(4, 10_000);

    println!();
    println!("  Run `cargo bench` for full Criterion benchmark reports.");
    println!("  Run `cargo run --release --bin compare_pipelines -- --mock` for");
    println!("  detailed async vs threaded comparison with p50/p90/p99 percentiles.");
    println!("  Run `cargo run --release --bin alloc_proof` to verify zero heap");
    println!("  allocations on the hot path.");
}

/// Write overflow events to logs/overflow_events.csv.
fn write_overflow_log(events: &[wiki_rt_monitor::types::OverflowEvent]) {
    if events.is_empty() { return; }
    std::fs::create_dir_all("logs").ok();
    match std::fs::File::create("logs/overflow_events.csv") {
        Ok(mut f) => {
            writeln!(f, "total_drops,domain,priority").ok();
            for ev in events {
                writeln!(f, "{},{},{:?}", ev.total_drops, ev.domain, ev.priority).ok();
            }
            println!("[logs] overflow_events.csv written ({} events)", events.len());
        }
        Err(e) => eprintln!("[logs] could not write overflow_events.csv: {e}"),
    }
}

/// Write deadline miss events to logs/deadline_misses.csv.
fn write_deadline_miss_log(events: &[wiki_rt_monitor::metrics::DeadlineMissEvent]) {
    if events.is_empty() { return; }
    std::fs::create_dir_all("logs").ok();
    match std::fs::File::create("logs/deadline_misses.csv") {
        Ok(mut f) => {
            writeln!(f, "latency_us,domain,priority").ok();
            for ev in events {
                writeln!(f, "{:.1},{},{:?}", ev.latency_us, ev.domain, ev.priority).ok();
            }
            println!("[logs] deadline_misses.csv written ({} events)", events.len());
        }
        Err(e) => eprintln!("[logs] could not write deadline_misses.csv: {e}"),
    }
}
