/// Component A – Architecture 1: Async/Await Pipeline (Tokio)
///
/// Design:
///   • A Tokio task runs the SSE client / mock stream and writes raw JSON
///     bytes into a bounded `tokio::sync::mpsc` channel.
///   • A second Tokio task drains that channel, parses packets zero-copy,
///     and enqueues ChangePackets into the PriorityScheduler (Component C).
///   • A third Tokio task drains the PriorityScheduler (human-first) and
///     forwards to the hot-path consumer channel.
///   • Backpressure: when the ingestion channel is full, the OLDEST packet
///     is evicted (drop-oldest) and the new packet is enqueued. An
///     OverflowEvent is logged with a high-precision timestamp.
///
/// Concurrency model: user-space cooperative scheduling (Tokio task executor).
/// All tasks run on the Tokio thread pool; no OS thread per sensor.
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::component_c::priority_scheduler::PriorityScheduler;
use crate::ingestion::{mock_stream, sse_client, StreamSource};
use crate::metrics::MetricsHandle;
use crate::types::{ChangePacket, OverflowEvent, StressConfig, WikiChange};

/// Bounded channel capacity for the ingestion buffer.
pub const CHANNEL_CAPACITY: usize = 512;

/// Statistics returned after a run.
#[derive(Debug, Default, Clone)]
pub struct AsyncPipelineStats {
    pub events_received: u64,
    pub overflows:       u64,
    pub duration_secs:   f64,
}

impl AsyncPipelineStats {
    pub fn throughput(&self) -> f64 {
        if self.duration_secs > 0.0 { self.events_received as f64 / self.duration_secs } else { 0.0 }
    }
}

/// Run the async pipeline for `duration`, returning throughput statistics.
/// `packet_tx` is the downstream channel to the hot-path processing stage.
/// `last_event` is updated to a SystemTime epoch-ms on every successfully
/// parsed event so the Watchdog (Component E) can detect ingestion silence.
///
/// Routing: raw bytes → parse → PriorityScheduler → packet_tx (hot path)
/// The PriorityScheduler ensures human edits drain before bot edits.
pub async fn run_async_pipeline(
    source:         StreamSource,
    packet_tx:      Sender<ChangePacket>,
    metrics:        MetricsHandle,
    stop:           Arc<AtomicBool>,
    duration:       Duration,
    last_event:     Arc<AtomicI64>,
    stress:         StressConfig,
) -> AsyncPipelineStats {
    run_async_pipeline_inner(source, packet_tx, metrics, stop, duration, last_event, stress, None).await
}

/// Extended variant that accepts a watchdog reconnect flag for the SSE client.
pub async fn run_async_pipeline_with_reconnect(
    source:         StreamSource,
    packet_tx:      Sender<ChangePacket>,
    metrics:        MetricsHandle,
    stop:           Arc<AtomicBool>,
    duration:       Duration,
    last_event:     Arc<AtomicI64>,
    stress:         StressConfig,
    reconnect_flag: Arc<AtomicBool>,
) -> AsyncPipelineStats {
    run_async_pipeline_inner(source, packet_tx, metrics, stop, duration, last_event, stress, Some(reconnect_flag)).await
}

async fn run_async_pipeline_inner(
    source:         StreamSource,
    packet_tx:      Sender<ChangePacket>,
    metrics:        MetricsHandle,
    stop:           Arc<AtomicBool>,
    duration:       Duration,
    last_event:     Arc<AtomicI64>,
    stress:         StressConfig,
    reconnect_flag: Option<Arc<AtomicBool>>,
) -> AsyncPipelineStats {
    let start          = Instant::now();
    let (raw_tx, raw_rx) = mpsc::channel::<Bytes>(CHANNEL_CAPACITY);

    // Shared priority scheduler (Component C) – human edits drain before bot.
    let scheduler = Arc::new(PriorityScheduler::new(Arc::clone(&metrics)));

    // ── SSE / mock ingestion task ─────────────────────────────────────────────
    let stop_ingest   = Arc::clone(&stop);
    let last_ms_clone = Arc::clone(&last_event);
    match source {
        StreamSource::Live => {
            let raw_tx2 = raw_tx.clone();
            let rf      = reconnect_flag.clone();
            tokio::spawn(async move {
                sse_client::run_sse_client_with_reconnect(raw_tx2, last_ms_clone, stop_ingest, rf).await;
            });
        }
        StreamSource::Mock(eps) => {
            let raw_tx2 = raw_tx.clone();
            let silence = stress.silence_window;
            let prog_start = stress.program_start;
            tokio::spawn(async move {
                mock_stream::run_mock_stream(raw_tx2, eps, stop_ingest, silence, prog_start).await;
            });
        }
    }
    drop(raw_tx); // all clones are now in the spawned tasks

    // ── Parsing + enqueue into PriorityScheduler ──────────────────────────────
    let stop_parse    = Arc::clone(&stop);
    let metrics2      = Arc::clone(&metrics);
    let sched2        = Arc::clone(&scheduler);
    let last_event2   = Arc::clone(&last_event);

    let stats_handle = tokio::spawn(async move {
        drain_and_parse(raw_rx, sched2, metrics2, stop_parse, last_event2).await
    });

    // ── Priority-aware dispatch task → hot path ───────────────────────────────
    // Drains the PriorityScheduler (human first) and forwards to hot path.
    let stop_dispatch = Arc::clone(&stop);
    let sched3        = Arc::clone(&scheduler);
    let packet_tx3    = packet_tx.clone();
    tokio::spawn(async move {
        dispatch_loop_async(sched3, packet_tx3, stop_dispatch).await;
    });

    // ── Run for the configured duration ──────────────────────────────────────
    tokio::time::sleep(duration).await;
    stop.store(true, Ordering::Relaxed);

    let (events_received, overflows) = stats_handle.await.unwrap_or((0, 0));

    // Write throughput to shared metrics.
    let secs = start.elapsed().as_secs_f64();
    if let Ok(mut m) = metrics.try_lock() {
        m.async_events_received  = events_received;
        m.async_overflow_count   = overflows;
        m.async_throughput       = events_received as f64 / secs.max(0.001);
    }

    AsyncPipelineStats {
        events_received,
        overflows,
        duration_secs: secs,
    }
}

/// Drain the raw-bytes channel, parse zero-copy, and enqueue into scheduler.
/// Returns `(events_received, overflow_count)`.
async fn drain_and_parse(
    mut raw_rx:  Receiver<Bytes>,
    scheduler:   Arc<PriorityScheduler>,
    metrics:     MetricsHandle,
    stop:        Arc<AtomicBool>,
    last_event:  Arc<AtomicI64>,
) -> (u64, u64) {
    let mut events    = 0u64;
    let mut overflows = 0u64;

    while !stop.load(Ordering::Relaxed) {
        let buf = match tokio::time::timeout(
            Duration::from_millis(10),
            raw_rx.recv(),
        ).await {
            Ok(Some(b)) => b,
            _           => continue,
        };

        // ── Zero-copy parse ─────────────────────────────────────────────────
        // WikiChange<'_> borrows string fields directly from `buf`.
        let change: WikiChange<'_> = match serde_json::from_slice(&buf) {
            Ok(c)  => c,
            Err(_) => continue, // skip malformed events
        };

        events += 1;

        // Watchdog heartbeat (Component E): epoch-ms of latest successful parse.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        last_event.store(now_ms, Ordering::Relaxed);

        // Build owned ChangePacket (T0 stamped in from_change, T1 in enqueue).
        let pkt = ChangePacket::from_change(&change);

        // ── Enqueue into priority scheduler (Component C) ───────────────────
        // If the priority queue is full, drop-oldest: evict head then retry.
        if !scheduler.enqueue(pkt.clone()) {
            overflows += 1;
            let ev = OverflowEvent {
                dropped_at:  Instant::now(),
                domain:      pkt.server_name.clone(),
                priority:    pkt.priority,
                total_drops: overflows,
            };
            eprintln!("[overflow] async drop-oldest at {:?} domain={} priority={:?} total={}",
                      ev.dropped_at, ev.domain, ev.priority, ev.total_drops);
            if let Ok(mut m) = metrics.try_lock() {
                m.async_overflow_count = overflows;
                m.push_overflow(ev);
            }
            // Evict oldest from the appropriate queue and retry.
            scheduler.evict_oldest(pkt.priority);
            scheduler.enqueue(pkt);
        }

        // Yield cooperatively to allow other Tokio tasks to run.
        tokio::task::yield_now().await;
    }

    (events, overflows)
}

/// Async dispatch loop: drains PriorityScheduler (human first) → hot path.
async fn dispatch_loop_async(
    scheduler: Arc<PriorityScheduler>,
    out_tx:    Sender<ChangePacket>,
    stop:      Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        match scheduler.dequeue_next() {
            Some(pkt) => {
                let _ = out_tx.try_send(pkt);
            }
            None => {
                // No packets available; yield to avoid busy-spinning.
                tokio::task::yield_now().await;
            }
        }
    }
}
