/// Component A – Architecture 2: Multi-Threaded Pipeline (std::thread)
///
/// Design:
///   • One OS thread runs the mock/SSE ingestion and writes raw JSON bytes
///     into a bounded `crossbeam::channel`.
///   • A second OS thread drains the channel, parses zero-copy, and enqueues
///     ChangePackets into the PriorityScheduler (Component C).
///   • A third OS thread dispatches from the PriorityScheduler (human-first)
///     to the downstream hot-path channel.
///   • Backpressure: drop-OLDEST – when the priority queue is full, the head
///     of the relevant queue is evicted before the new packet is enqueued. An
///     OverflowEvent is logged with a high-precision timestamp.
///
/// Concurrency model: kernel-space preemptive scheduling.
/// Thread priorities can be set per-thread (useful for actuator analogy).
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam::channel::{bounded, Receiver, Sender};

use crate::component_c::priority_scheduler::PriorityScheduler;
use crate::ingestion::mock_stream;
use crate::metrics::MetricsHandle;
use crate::types::{ChangePacket, OverflowEvent, StressConfig, WikiChange};

pub const CHANNEL_CAPACITY: usize = 512;

#[derive(Debug, Default, Clone)]
pub struct ThreadedPipelineStats {
    pub events_received: u64,
    pub overflows:       u64,
    pub duration_secs:   f64,
}

impl ThreadedPipelineStats {
    pub fn throughput(&self) -> f64 {
        if self.duration_secs > 0.0 { self.events_received as f64 / self.duration_secs } else { 0.0 }
    }
}

// ▶ SHOW: threaded pipeline — OS preemptive scheduling, bounded crossbeam channel
/// Run the threaded pipeline, blocking the calling thread for `duration`.
/// `last_event` is updated to a SystemTime epoch-ms on every successfully
/// parsed event so the Watchdog (Component E) can detect ingestion silence.
///
/// Routing: raw bytes → parse → PriorityScheduler → packet_tx (hot path)
pub fn run_threaded_pipeline(
    events_per_second: u64,
    packet_tx:         crossbeam::channel::Sender<ChangePacket>,
    metrics:           MetricsHandle,
    stop:              Arc<AtomicBool>,
    duration:          Duration,
    last_event:        Arc<AtomicI64>,
    stress:            StressConfig,
) -> ThreadedPipelineStats {
    let start              = Instant::now();
    let (raw_tx, raw_rx)   = bounded::<Bytes>(CHANNEL_CAPACITY);
    let overflow_counter   = Arc::new(AtomicU64::new(0));
    let events_counter     = Arc::new(AtomicU64::new(0));

    // Shared priority scheduler (Component C) – human edits drain before bot.
    let scheduler = Arc::new(PriorityScheduler::new(Arc::clone(&metrics)));

    // ── Ingestion thread ──────────────────────────────────────────────────────
    let stop_ingest    = Arc::clone(&stop);
    let raw_tx_ingest  = raw_tx.clone();
    let silence_window = stress.silence_window;
    let program_start  = stress.program_start;
    std::thread::Builder::new()
        .name("threaded-ingestion".into())
        .spawn(move || {
            mock_stream::run_mock_stream_blocking(
                adapt_to_sync_sender(raw_tx_ingest),
                events_per_second,
                stop_ingest,
                silence_window,
                program_start,
            );
        })
        .expect("failed to spawn ingestion thread");
    drop(raw_tx);

    // ── Parsing thread – enqueues into PriorityScheduler ─────────────────────
    let stop_parse      = Arc::clone(&stop);
    let metrics2        = Arc::clone(&metrics);
    let sched2          = Arc::clone(&scheduler);
    let ovf             = Arc::clone(&overflow_counter);
    let evts            = Arc::clone(&events_counter);
    let last_event_p    = Arc::clone(&last_event);

    let parse_handle = std::thread::Builder::new()
        .name("threaded-parser".into())
        .spawn(move || {
            threaded_parse_loop(raw_rx, sched2, metrics2, stop_parse, ovf, evts, last_event_p);
        })
        .expect("failed to spawn parser thread");

    // ── Dispatch thread – drains PriorityScheduler (human-first) → hot path ──
    let stop_dispatch   = Arc::clone(&stop);
    let sched3          = Arc::clone(&scheduler);
    let packet_tx3      = packet_tx.clone();
    let dispatch_handle = std::thread::Builder::new()
        .name("threaded-dispatch".into())
        .spawn(move || {
            sched3.run_dispatch_loop(packet_tx3, stop_dispatch);
        })
        .expect("failed to spawn dispatch thread");

    // Block until duration elapsed.
    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    let _ = parse_handle.join();
    let _ = dispatch_handle.join();

    let secs     = start.elapsed().as_secs_f64();
    let events   = events_counter.load(Ordering::Relaxed);
    let overflows = overflow_counter.load(Ordering::Relaxed);

    if let Ok(mut m) = metrics.try_lock() {
        m.threaded_events_received = events;
        m.threaded_overflow_count  = overflows;
        m.threaded_throughput      = events as f64 / secs.max(0.001);
    }

    ThreadedPipelineStats { events_received: events, overflows, duration_secs: secs }
}

fn threaded_parse_loop(
    raw_rx:    Receiver<Bytes>,
    scheduler: Arc<PriorityScheduler>,
    metrics:   MetricsHandle,
    stop:      Arc<AtomicBool>,
    overflows: Arc<AtomicU64>,
    events:    Arc<AtomicU64>,
    last_event: Arc<AtomicI64>,
) {
    while !stop.load(Ordering::Relaxed) {
        let buf = match raw_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(b)  => b,
            Err(_) => continue,
        };

        // Zero-copy parse: WikiChange<'_> borrows from `buf`.
        let change: WikiChange<'_> = match serde_json::from_slice(&buf) {
            Ok(c)  => c,
            Err(_) => continue,
        };

        events.fetch_add(1, Ordering::Relaxed);

        // Watchdog heartbeat (Component E).
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        last_event.store(now_ms, Ordering::Relaxed);

        // Build packet (T0 stamped in from_change, T1 stamped in scheduler.enqueue).
        let pkt = ChangePacket::from_change(&change);

        // ── Drop-oldest backpressure via PriorityScheduler ───────────────────
        if !scheduler.enqueue(pkt.clone()) {
            let n = overflows.fetch_add(1, Ordering::Relaxed) + 1;
            let ev = OverflowEvent {
                dropped_at:  Instant::now(),
                domain:      pkt.server_name.clone(),
                priority:    pkt.priority,
                total_drops: n,
            };
            eprintln!("[overflow] threaded drop-oldest at {:?} domain={} priority={:?} total={}",
                      ev.dropped_at, ev.domain, ev.priority, n);
            if let Ok(mut m) = metrics.try_lock() {
                m.threaded_overflow_count = n;
                m.push_overflow(ev);
            }
            // Evict oldest from the appropriate queue and retry.
            scheduler.evict_oldest(pkt.priority);
            scheduler.enqueue(pkt);
        }
    }
}

// ─── Adapter: crossbeam Sender → std SyncSender ──────────────────────────────
// The mock stream uses std::sync::mpsc::SyncSender for its blocking version.
// We bridge it by spawning a relay thread.

fn adapt_to_sync_sender(cb_tx: Sender<Bytes>) -> std::sync::mpsc::SyncSender<Bytes> {
    let (std_tx, std_rx) = std::sync::mpsc::sync_channel::<Bytes>(CHANNEL_CAPACITY);
    std::thread::Builder::new()
        .name("cb-bridge".into())
        .spawn(move || {
            while let Ok(b) = std_rx.recv() {
                let _ = cb_tx.try_send(b);
            }
        })
        .expect("bridge thread");
    std_tx
}
