/// Pipeline Comparison Binary
///
/// Runs the async and threaded pipelines back-to-back for `WINDOW_SECS`
/// each, then prints a detailed table with p50/p90/p99 tail-latency
/// comparison (required for Distinction-level analysis).
///
/// Run: cargo run --release --bin compare_pipelines -- --mock

use wiki_rt_monitor::component_a::{run_async_pipeline, run_threaded_pipeline};
use wiki_rt_monitor::component_b::HotPathProcessor;
use wiki_rt_monitor::component_d::{Leaderboard, SyncStrategy};
use wiki_rt_monitor::component_e::FailSafe;
use wiki_rt_monitor::ingestion::StreamSource;
use wiki_rt_monitor::metrics::new_metrics;
use wiki_rt_monitor::types::StressConfig;

use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Arc;
use std::time::Duration;

const WINDOW_SECS: u64 = 15;
const MOCK_EPS:    u64 = 500;

#[tokio::main]
async fn main() {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║   Async vs Threaded Pipeline Comparison  –  RTS2601       ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");
    println!("  Each pipeline runs for {WINDOW_SECS}s at {MOCK_EPS} events/s (mock).\n");

    // ── Run async pipeline ────────────────────────────────────────────────────
    println!("[1/2] Async pipeline (Tokio)…");
    let async_metrics = new_metrics();
    let stop_a        = Arc::new(AtomicBool::new(false));
    let (a_tx, a_rx)  = tokio::sync::mpsc::channel(512);
    let lb_a  = Leaderboard::new(SyncStrategy::Atomic, Arc::clone(&async_metrics));
    let fs_a  = FailSafe::new(Arc::clone(&async_metrics));
    let hp_a  = HotPathProcessor::new(Arc::clone(&async_metrics), Arc::clone(&lb_a), Arc::clone(&fs_a), StressConfig::off());
    let stop_hp = Arc::clone(&stop_a);
    tokio::spawn(async move { hp_a.run_async(a_rx, stop_hp).await; });

    let last_event_a = Arc::new(AtomicI64::new(0));
    let async_stats = run_async_pipeline(
        StreamSource::Mock(MOCK_EPS),
        a_tx,
        Arc::clone(&async_metrics),
        Arc::clone(&stop_a),
        Duration::from_secs(WINDOW_SECS),
        last_event_a,
        StressConfig::off(),
    ).await;

    // ── Run threaded pipeline ─────────────────────────────────────────────────
    println!("[2/2] Threaded pipeline (std::thread)…");
    let thr_metrics  = new_metrics();
    let stop_t       = Arc::new(AtomicBool::new(false));
    let (t_tx, t_rx) = crossbeam::channel::bounded(512);
    let lb_t  = Leaderboard::new(SyncStrategy::Mutex, Arc::clone(&thr_metrics));
    let fs_t  = FailSafe::new(Arc::clone(&thr_metrics));
    let hp_t  = HotPathProcessor::new(Arc::clone(&thr_metrics), Arc::clone(&lb_t), Arc::clone(&fs_t), StressConfig::off());
    let stop_hp2 = Arc::clone(&stop_t);
    std::thread::spawn(move || { hp_t.spawn_threaded(t_rx, stop_hp2).join().ok(); });

    let last_event_t = Arc::new(AtomicI64::new(0));
    let thr_stats = run_threaded_pipeline(
        MOCK_EPS,
        t_tx,
        Arc::clone(&thr_metrics),
        Arc::clone(&stop_t),
        Duration::from_secs(WINDOW_SECS),
        last_event_t,
        StressConfig::off(),
    );

    // ── Print comparison ──────────────────────────────────────────────────────
    let am = async_metrics.lock().unwrap();
    let tm = thr_metrics.lock().unwrap();

    println!();
    println!("════════════════════ PIPELINE COMPARISON REPORT ════════════════════");
    println!("  {:32}  {:>12}  {:>12}", "Metric", "Async", "Threaded");
    println!("  {}", "─".repeat(60));

    macro_rules! row {
        ($label:expr, $a:expr, $t:expr, $fmt:literal) => {
            println!(concat!("  {:32}  {:>12", $fmt, "}  {:>12", $fmt, "}"),
                     $label, $a, $t);
        };
    }

    row!("Events received",         am.async_events_received,   tm.threaded_events_received, "");
    row!("Throughput (events/s)",   am.async_throughput as u64,  tm.threaded_throughput as u64, "");
    row!("Overflow drops",          am.async_overflow_count,     tm.threaded_overflow_count, "");
    row!("Deadline misses",         am.deadline_misses,          tm.deadline_misses, "");
    row!("Pipeline duration (s)",
         async_stats.duration_secs as u64,
         thr_stats.duration_secs as u64, "");

    println!("  {}", "─".repeat(60));
    // ▶ SHOW: p50/p90/p99 — tail latency comparison across both architectures
    println!("  {:32}  {:>12}  {:>12}", "Human latency p50 (µs)",
             format!("{:.1}", am.human_latency_us.p50() / 1000.0),
             format!("{:.1}", tm.human_latency_us.p50() / 1000.0));
    println!("  {:32}  {:>12}  {:>12}", "Human latency p90 (µs)",
             format!("{:.1}", am.human_latency_us.p90() / 1000.0),
             format!("{:.1}", tm.human_latency_us.p90() / 1000.0));
    println!("  {:32}  {:>12}  {:>12}", "Human latency p99 (µs)",
             format!("{:.1}", am.human_latency_us.p99() / 1000.0),
             format!("{:.1}", tm.human_latency_us.p99() / 1000.0));
    println!("  {}", "─".repeat(60));
    println!("  {:32}  {:>12}  {:>12}", "Bot latency p50 (µs)",
             format!("{:.1}", am.bot_latency_us.p50() / 1000.0),
             format!("{:.1}", tm.bot_latency_us.p50() / 1000.0));
    println!("  {:32}  {:>12}  {:>12}", "Bot latency p90 (µs)",
             format!("{:.1}", am.bot_latency_us.p90() / 1000.0),
             format!("{:.1}", tm.bot_latency_us.p90() / 1000.0));
    println!("  {:32}  {:>12}  {:>12}", "Bot latency p99 (µs)",
             format!("{:.1}", am.bot_latency_us.p99() / 1000.0),
             format!("{:.1}", tm.bot_latency_us.p99() / 1000.0));
    println!("  {}", "─".repeat(60));
    println!("  {:32}  {:>12}  {:>12}", "Human drift p50 (µs)",
             format!("{:.1}", am.human_drift_us.p50()),
             format!("{:.1}", tm.human_drift_us.p50()));
    println!("  {:32}  {:>12}  {:>12}", "Human drift p90 (µs)",
             format!("{:.1}", am.human_drift_us.p90()),
             format!("{:.1}", tm.human_drift_us.p90()));
    println!("  {:32}  {:>12}  {:>12}", "Human drift p99 (µs)",
             format!("{:.1}", am.human_drift_us.p99()),
             format!("{:.1}", tm.human_drift_us.p99()));
    println!("  {:32}  {:>12}  {:>12}", "Bot drift p50 (µs)",
             format!("{:.1}", am.bot_drift_us.p50()),
             format!("{:.1}", tm.bot_drift_us.p50()));
    println!("  {:32}  {:>12}  {:>12}", "Bot drift p90 (µs)",
             format!("{:.1}", am.bot_drift_us.p90()),
             format!("{:.1}", tm.bot_drift_us.p90()));
    println!("  {:32}  {:>12}  {:>12}", "Bot drift p99 (µs)",
             format!("{:.1}", am.bot_drift_us.p99()),
             format!("{:.1}", tm.bot_drift_us.p99()));
    println!("  {}", "─".repeat(60));
    println!("  {:32}  {:>12}  {:>12}", "Fail-safe activations",
             am.fail_safe_activations, tm.fail_safe_activations);
    println!("════════════════════════════════════════════════════════════════════");

    // ▶ SHOW: Key findings — programmatic winner for throughput and tail latency
    println!();
    println!("  Key findings:");
    let a_tput = am.async_throughput;
    let t_tput = tm.threaded_throughput;
    if a_tput > t_tput {
        println!("  • Async achieves {:.1}% higher throughput (user-space scheduling).",
            (a_tput - t_tput) / t_tput * 100.0);
    } else {
        println!("  • Threaded achieves {:.1}% higher throughput (preemptive OS scheduling).",
            (t_tput - a_tput) / a_tput * 100.0);
    }

    let a_p99 = am.human_latency_us.p99() / 1000.0;
    let t_p99 = tm.human_latency_us.p99() / 1000.0;
    if a_p99 < t_p99 {
        println!("  • Async shows lower human-edit tail latency (p99: {a_p99:.1} µs vs {t_p99:.1} µs).");
    } else {
        println!("  • Threaded shows lower human-edit tail latency (p99: {t_p99:.1} µs vs {a_p99:.1} µs).");
        println!("    Preemptive scheduling with OS priorities enables tighter tail-latency bounds.");
    }
}
