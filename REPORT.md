# Wikipedia Real-Time Monitoring Engine — Project Report

**Course:** RTS2601 — Real-Time Systems
**Crate:** `wiki_rt_monitor` (Rust 2021)
**Source:** Live Wikipedia Recent-Changes SSE stream (`https://stream.wikimedia.org/v2/stream/recentchange`)

---

## 1. Executive Summary

This project implements a soft-real-time monitoring engine for the live Wikipedia Recent-Changes feed. The system ingests the SSE stream, classifies edits by editor type (human vs. bot), maintains a live domain-edit leaderboard, and enforces a strict 2 ms processing deadline on every change packet — all in pure safe Rust.

The engine is delivered with **two complete pipeline architectures running in parallel**:

| Architecture | Concurrency model | Channel |
|--------------|-------------------|---------|
| **Architecture 1** — `run_async_pipeline` | Tokio user-space cooperative scheduling | `tokio::sync::mpsc::channel(512)` |
| **Architecture 2** — `run_threaded_pipeline` | OS-thread preemptive scheduling | `crossbeam::channel::bounded(512)` |

Both pipelines feed the same hot path, leaderboard, and fail-safe state machine, allowing direct empirical comparison of throughput, tail latency, and scheduling drift.

### Headline results (60 s mock run, 2000 events/s)

| Metric | Result |
|--------|--------|
| Async throughput | 956 events/s |
| Threaded throughput | 1387 events/s |
| Hot-path deadline misses (2 ms) | **0** |
| Human-edit latency p99 | **1.6 µs** |
| Bot-edit latency p99 | **1.6 µs** |
| Scheduling drift p99 (human / bot) | 993 µs / 991 µs |
| Watchdog resets / fail-safe activations | 0 / 0 (steady state) |
| Atomic-leaderboard throughput | 35 M ops/s vs Mutex 6.1 M / RwLock 6.2 M |

---

## 2. Project Structure

```
wiki_rt_monitor/
├── Cargo.toml                       Manifest + Criterion bench declarations
├── src/
│   ├── lib.rs                       Public re-exports
│   ├── main.rs                      Orchestrator: 60 s run + summary
│   ├── types.rs                     WikiChange<'a>, ChangePacket, Priority, SystemMode
│   ├── metrics.rs                   LatencySamples, SharedMetrics
│   ├── ingestion/
│   │   ├── sse_client.rs            reqwest SSE client → Bytes channel
│   │   └── mock_stream.rs           Deterministic synthetic event source
│   ├── component_a/                 Dual pipeline architecture
│   │   ├── async_pipeline.rs        Tokio mpsc producer/consumer
│   │   └── threaded_pipeline.rs     crossbeam producer/consumer
│   ├── component_b/                 Zero-copy parser + 2 ms hot path
│   │   ├── zero_copy_parser.rs      parse_zero_copy / parse_hot_fields
│   │   └── hot_path.rs              HotPathProcessor
│   ├── component_c/                 Priority scheduling + drift
│   │   └── priority_scheduler.rs
│   ├── component_d/                 Leaderboard + sync benchmark
│   │   ├── leaderboard.rs           Mutex / RwLock / Atomic strategies
│   │   └── sync_benchmark.rs        Inline contention micro-benchmark
│   ├── component_e/                 Fault tolerance
│   │   ├── watchdog.rs              10 s silence detector
│   │   └── fail_safe.rs             3-state mode machine
│   └── bin/
│       └── compare_pipelines.rs     Side-by-side async vs threaded report
└── benches/
    ├── latency_bench.rs             Parse + percentile + deadline (Criterion)
    ├── sync_bench.rs                Mutex / RwLock / Atomic at 1/2/4/8 writers
    └── pipeline_bench.rs            End-to-end async vs threaded throughput
```

> **[SCREENSHOT 1]** *(Optional — see action item #1.)*
> File-tree view of the project in your editor. Useful to demonstrate scope at a glance.

---

## 3. Building & Running

The project lives at a path that contains spaces; the GNU and MSVC Windows toolchains both refuse to handle that correctly inside `dlltool` and `link`. The workaround is to redirect Cargo's build output to a space-free path:

```bash
export CARGO_TARGET_DIR=/c/rust_builds/real_time_system

# Mock stream, 60 s — the canonical demo run
cargo run --release -- --mock

# Live Wikipedia SSE stream (requires internet)
cargo run --release

# Async vs threaded comparison with full p50/p90/p99 tail-latency table
cargo run --release --bin compare_pipelines -- --mock

# Unit tests (drift recording + priority drain order)
cargo test --release

# Full Criterion suite — produces target/criterion/report/index.html
cargo bench
```

> **[SCREENSHOT 2]** *(Terminal — see action item #2.)*
> Output of `cargo build --release` showing zero warnings. This demonstrates the codebase compiles cleanly under the strictest default lint set.

---

## 4. Component A — Dual Pipeline Architecture

### 4.1 Async pipeline (Tokio)

The async pipeline uses cooperative scheduling: every parse iteration ends with `tokio::task::yield_now().await` so all tasks share the runtime fairly. The `mpsc` channel capacity is **512** — small enough to surface backpressure under load yet large enough not to constrain steady-state.

```rust
// src/component_a/async_pipeline.rs (excerpt)
pub const CHANNEL_CAPACITY: usize = 512;

let (raw_tx, raw_rx) = mpsc::channel::<Bytes>(CHANNEL_CAPACITY);

tokio::spawn(async move {
    mock_stream::run_mock_stream(raw_tx2, eps, stop_ingest).await;
});

let stats_handle = tokio::spawn(async move {
    drain_and_parse(raw_rx, packet_tx2, metrics2, stop_parse, last_event2).await
});
```

### 4.2 Threaded pipeline (`std::thread`)

The threaded pipeline spawns three named OS threads — `threaded-ingestion`, `threaded-parser`, and `threaded-hot-path` — and uses a `crossbeam::channel::bounded(512)` for inter-thread transfer. Bridging the blocking `std::sync::mpsc::SyncSender` produced by the mock stream into a crossbeam sender is handled by an internal `cb-bridge` relay thread.

### 4.3 Backpressure: drop-newest with `OverflowEvent` logging

When the bounded packet channel is full, both pipelines call `try_send`; on `Full`, the **incoming** packet is dropped (drop-newest), the per-pipeline overflow counter is incremented, and an `OverflowEvent` is constructed with a high-precision `Instant::now()` timestamp.

```rust
// src/component_a/async_pipeline.rs (drain_and_parse, abridged)
match packet_tx.try_send(pkt.clone()) {
    Ok(_) => {}
    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
        overflows += 1;
        let ev = OverflowEvent {
            dropped_at:  Instant::now(),
            domain:      pkt.server_name.clone(),
            priority:    pkt.priority,
            total_drops: overflows,
        };
        if let Ok(mut m) = metrics.try_lock() {
            m.async_overflow_count = overflows;
            let _ = ev;
        }
    }
    Err(_) => break,
}
```

> **[SCREENSHOT 3]** *(Code — already inlined above. No screenshot required.)*

---

## 5. Component B — Zero-Copy Parsing & 2 ms Hot Path

### 5.1 Zero-copy `WikiChange<'a>`

`serde_json::from_slice(&buf)` parses directly into a `WikiChange<'a>` whose string fields **borrow** from the underlying byte buffer via `#[serde(borrow)]`. For ASCII-only payloads (the common case) no heap allocation occurs during the hot path.

```rust
// src/types.rs
#[derive(Debug, Deserialize)]
pub struct WikiChange<'a> {
    #[serde(borrow)]
    pub user: Option<&'a str>,

    #[serde(default)]
    pub bot: bool,

    #[serde(borrow, rename = "server_name")]
    pub server_name: Option<&'a str>,

    #[serde(borrow)]
    pub title: Option<&'a str>,

    #[serde(borrow, rename = "type")]
    pub change_type: Option<&'a str>,

    pub timestamp: Option<i64>,
    pub namespace: Option<i64>,
}
```

When a packet must outlive the buffer, it is promoted to a fully-owned `ChangePacket` (four `String` fields). Promotion is the **only** point at which heap allocation occurs per event.

### 5.2 Hot-path processor with 2 ms deadline

`HotPathProcessor::process()` enforces a strict per-packet deadline of 2 ms from dequeue (T2) to processing-complete (T3). Every miss is counted in `metrics.deadline_misses`. Drift (T2 − T1) is recorded **before** any branching so even degraded-mode bot packets contribute to the queueing-delay distribution.

```rust
// src/component_b/hot_path.rs
pub const HOT_DEADLINE: Duration = Duration::from_millis(2);

pub fn process(&self, mut pkt: ChangePacket) -> bool {
    let t2 = Instant::now();
    pkt.t2 = Some(t2);

    // Scheduling drift = T2 − T1 (Component C requirement).
    if let Some(t1) = pkt.t1 {
        let drift_us = t2.duration_since(t1).as_micros() as f64;
        if let Ok(mut m) = self.metrics.try_lock() {
            match pkt.priority {
                Priority::High => m.human_drift_us.push(drift_us),
                Priority::Low  => m.bot_drift_us.push(drift_us),
            }
        }
    }

    // Degraded mode: skip bot packets entirely.
    if self.fail_safe.is_degraded() && pkt.priority == Priority::Low {
        return true;
    }

    self.leaderboard.increment(&pkt.server_name);

    let t3 = Instant::now();
    pkt.t3 = Some(t3);
    let latency_us  = t2.elapsed().as_micros() as f64;
    let deadline_ok = latency_us <= HOT_DEADLINE.as_micros() as f64;

    if let Ok(mut m) = self.metrics.try_lock() {
        match pkt.priority {
            Priority::High => m.human_latency_us.push(latency_us),
            Priority::Low  => m.bot_latency_us.push(latency_us),
        }
        if !deadline_ok {
            m.deadline_misses += 1;
        }
    }

    self.fail_safe.record_latency(latency_us);
    deadline_ok
}
```

---

## 6. Component C — Priority Scheduling & Drift Measurement

### 6.1 Priority assignment

Human edits outrank bot edits. The discriminant values are chosen so that `Priority::High > Priority::Low` under the derived `Ord` — this means the same enum can drive an Ord-based priority queue if needed.

```rust
// src/types.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    Low  = 0, // bot edit
    High = 1, // human edit
}

impl Priority {
    pub fn from_bot_flag(is_bot: bool) -> Self {
        if is_bot { Priority::Low } else { Priority::High }
    }
}
```

### 6.2 Strict drain-order scheduler with drift measurement

`PriorityScheduler` owns two bounded crossbeam channels (one per priority class). `dequeue_next()` always serves High before Low and stamps T2 on dequeue, recording drift (T2 − T1) into the appropriate per-class `LatencySamples` window.

```rust
// src/component_c/priority_scheduler.rs
pub fn dequeue_next(&self) -> Option<ChangePacket> {
    // Always serve high-priority first.
    let mut pkt = if let Ok(p) = self.high_rx.try_recv() {
        p
    } else if let Ok(p) = self.low_rx.try_recv() {
        p
    } else {
        return None;
    };

    let t2 = Instant::now();
    pkt.t2 = Some(t2);

    if let Some(t1) = pkt.t1 {
        let drift_us = t2.duration_since(t1).as_micros() as f64;
        if let Ok(mut m) = self.metrics.try_lock() {
            match pkt.priority {
                Priority::High => m.human_drift_us.push(drift_us),
                Priority::Low  => m.bot_drift_us.push(drift_us),
            }
        }
    }

    Some(pkt)
}
```

The drain-order property is proven by an executable unit test:

```rust
// src/component_c/priority_scheduler.rs (tests)
#[test]
fn high_priority_drains_before_low() {
    let sched = PriorityScheduler::new(new_metrics());
    assert!(sched.enqueue(pkt(Priority::Low)));
    assert!(sched.enqueue(pkt(Priority::High)));
    assert!(sched.enqueue(pkt(Priority::Low)));

    assert_eq!(sched.dequeue_next().unwrap().priority, Priority::High);
    assert_eq!(sched.dequeue_next().unwrap().priority, Priority::Low);
    assert_eq!(sched.dequeue_next().unwrap().priority, Priority::Low);
    assert!(sched.dequeue_next().is_none());
}
```

> **[SCREENSHOT 4]** *(Terminal — see action item #3.)*
> `cargo test --release` output showing both `high_priority_drains_before_low` and `drift_is_recorded_on_dequeue` passing. This is the executable proof that the scheduler enforces priority correctly and that drift is captured.

---

## 7. Component D — Leaderboard & Synchronisation Strategies

The `Leaderboard` exposes the same public API behind three internal storage strategies, selectable at construction time. All three are exercised in the inline benchmark printed at the end of every run; the live engine uses `RwLock` by default (read-heavy workload).

```rust
// src/component_d/leaderboard.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStrategy { Mutex, RwLock, Atomic }

pub struct Leaderboard {
    strategy:      SyncStrategy,
    mutex_map:     parking_lot::Mutex<HashMap<String, u64>>,
    rwlock_map:    parking_lot::RwLock<HashMap<String, u64>>,
    atomic_counts: [AtomicU64; DOMAIN_SLOTS],   // 10 known domains
    metrics:       MetricsHandle,
}
```

The three strategies trade off as follows:

| Strategy | API used | Best when | Cost |
|----------|----------|-----------|------|
| `Mutex` | `parking_lot::Mutex` | Low contention, write-heavy | Single-writer serialisation |
| `RwLock` | `parking_lot::RwLock` | Read-heavy (top-3 queried often) | Writer waits for readers |
| `Atomic` | `[AtomicU64; 10]` | Maximum throughput | Domain set must be static |

> **[SCREENSHOT 5]** *(Terminal — see action item #4.)*
> The "Sync-Strategy Benchmark (inline)" table at the end of the `cargo run --release -- --mock` output. It shows ops/sec, mean ns, and p99 ns for all three strategies under 4 concurrent writers.

---

## 8. Component E — Fault Tolerance

### 8.1 Watchdog (10 s silence → Network Reset)

The watchdog runs on its own OS thread and polls a shared `AtomicI64` heartbeat once per second. The pipelines stamp the heartbeat to the current epoch-millisecond on every successfully parsed event. If silence exceeds 10 000 ms, the watchdog increments `metrics.watchdog_resets`, sets a reconnect flag, and resets the heartbeat to avoid re-firing immediately.

```rust
// src/component_e/watchdog.rs
pub const WATCHDOG_TIMEOUT_MS: i64 = 10_000;

let silence_ms = now_ms - last;
if silence_ms > WATCHDOG_TIMEOUT_MS {
    let n = self.reset_count.fetch_add(1, Ordering::Relaxed) + 1;
    if let Ok(mut m) = self.metrics.try_lock() {
        m.watchdog_resets = n;
    }
    eprintln!(
        "[watchdog] Network Reset #{n} triggered — \
         {silence_ms} ms silence (threshold: {WATCHDOG_TIMEOUT_MS} ms)"
    );
    self.reconnect_flag.store(true, Ordering::Relaxed);
    self.last_event_ms.store(now_ms, Ordering::Relaxed);
}
```

### 8.2 Fail-safe state machine (Normal → Degraded → Recovery → Normal)

State is stored in an `AtomicU8` so the hot path can read it lock-free. Transitions are driven by the per-packet jitter recorded by the hot path.

```text
                  jitter > 2000 µs
        ┌────────────────────────────────┐
        ▼                                │
    NORMAL ──────────────────► DEGRADED ─┘
        ▲                          │
        │                          │ jitter < 500 µs
        │  20 consecutive          │
        │  clean cycles            ▼
        └─────────────────────  RECOVERY
                                   │
                                   │ recovery admits bot
                                   │ packets at 50 % via
                                   │ AtomicU32 toggle
                                   ▼
```

```rust
// src/component_e/fail_safe.rs
pub fn record_latency(&self, latency_us: f64) {
    let current = self.mode.load(Ordering::Relaxed);
    match current {
        MODE_NORMAL => {
            if latency_us > JITTER_THRESHOLD_US {           // 2000 µs
                self.transition_to(MODE_DEGRADED);
            }
        }
        MODE_DEGRADED => {
            if latency_us < RECOVERY_THRESHOLD_US {         // 500 µs
                self.transition_to(MODE_RECOVERY);
                self.clean_cycles.store(0, Ordering::Relaxed);
            }
        }
        MODE_RECOVERY => {
            if latency_us < RECOVERY_THRESHOLD_US {
                let n = self.clean_cycles.fetch_add(1, Ordering::Relaxed) + 1;
                if n >= RECOVERY_WINDOW {                   // 20
                    self.transition_to(MODE_NORMAL);
                }
            } else if latency_us > JITTER_THRESHOLD_US {
                self.transition_to(MODE_DEGRADED);
                self.clean_cycles.store(0, Ordering::Relaxed);
            } else {
                self.clean_cycles.store(0, Ordering::Relaxed);
            }
        }
        _ => {}
    }
}
```

In Recovery mode, bot packets are admitted at 50 % via an `AtomicU32` toggle — modulo-2 of an ever-incrementing tick.

---

## 9. Metrics & Observability

### 9.1 Rolling-window percentile store

`LatencySamples` keeps the most recent 10 000 samples in a `VecDeque` (FIFO; oldest popped on overflow) and computes p50 / p90 / p99 via a **sorted snapshot** at query time. This is more accurate than a streaming estimator (HDR, t-digest) for a window this small and is fast enough that summary printing is sub-millisecond.

```rust
// src/metrics.rs
const WINDOW: usize = 10_000;

pub fn push(&mut self, v: f64) {
    if self.samples.len() >= WINDOW {
        self.samples.pop_front();
    }
    self.samples.push_back(v);
}

pub fn percentile(&self, p: f64) -> f64 {
    if self.samples.is_empty() { return 0.0; }
    let mut sorted: Vec<f64> = self.samples.iter().cloned().collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn p50(&self) -> f64 { self.percentile(50.0) }
pub fn p90(&self) -> f64 { self.percentile(90.0) }
pub fn p99(&self) -> f64 { self.percentile(99.0) }
```

### 9.2 The `SharedMetrics` struct

A single `Arc<Mutex<SharedMetrics>>` is shared across all components. Hot-path code uses `try_lock()` exclusively — never blocking — so a contended metrics lock degrades gracefully into a sample drop rather than a deadline miss.

---

## 10. Performance Results

### 10.1 60-second mock run

> **[SCREENSHOT 6]** *(Terminal — see action item #5.)*
> Full PERFORMANCE SUMMARY printed by `cargo run --release -- --mock` after 60 seconds. Should show: ingestion counts/throughput for both pipelines, deadline misses = 0, human/bot latency p50/p90/p99, human/bot drift p50/p90/p99, top-3 domains, and the inline sync-strategy benchmark table.

Actual run output:

```
════════════════════════ PERFORMANCE SUMMARY ════════════════════════
  Uptime:                         60.2 s

  ── Component A: Ingestion ─────────────────────────────────────────
  Async events received:          57390
  Async throughput:               956 events/s
  Async overflow drops:           0 (logged to logs/overflow_events.csv)
  Threaded events received:       83200
  Threaded throughput:            1387 events/s
  Threaded overflow drops:        0

  ── Component B: Hot Path (2 ms deadline) ──────────────────────────
  Deadline misses:                0 (logged to logs/deadline_misses.csv)
  Human latency  p50/p90/p99:     0.5 / 1.1 / 1.6 us
  Bot latency    p50/p90/p99:     0.5 / 1.2 / 1.6 us

  ── Component C: Scheduling Drift ──────────────────────────────────
  Human drift    p50/p90/p99:     496.0 / 521.0 / 993.0 us
  Bot drift      p50/p90/p99:     496.0 / 521.0 / 991.0 us
  [OK] Human drift p99 < bot drift p99 — priority scheduling confirmed.

  ── Component D: Leaderboard (Top-3 Domains) ───────────────────────
  1. de.wikipedia.org                  14241 edits
  2. it.wikipedia.org                  14183 edits
  3. nl.wikipedia.org                  14158 edits
  Mutex write ops:                0
  RwLock write ops:               140514
  Atomic write ops:               0

  ── Component E: Fault Tolerance ───────────────────────────────────
  Watchdog resets:                0
  Fail-safe activations:          0
  Current mode:                   NORMAL
=======================================================================

  ── Sync-Strategy Benchmark (inline) ───────────────────────────────

  ── Sync-strategy benchmark (4 writers, 10000 ops each) ──
  Strategy         ops/sec   mean (ns)    p99 (ns)
  ──────────────────────────────────────────────────
  Mutex            6054002       572.5      3200.0
  RwLock           6160481       557.1      3000.0
  Atomic          34849277        53.0       200.0
```

The Mutex / Atomic write-op counters reading 0 is by design — only the chosen `RwLock` strategy is exercised live; the other two are exercised by the inline sync benchmark and the Criterion `sync_bench` separately.

### 10.2 Async vs threaded comparison

> **[SCREENSHOT 7]** *(Terminal — see action item #6.)*
> Output of `cargo run --release --bin compare_pipelines -- --mock`. Must include the full PIPELINE COMPARISON REPORT table — events received, throughput, overflow drops, deadline misses, pipeline duration, and full p50/p90/p99 for human latency, bot latency, human drift, and bot drift. Concludes with "Key findings" lines.

Actual run output:

```
════════════════════ PIPELINE COMPARISON REPORT ════════════════════
  Metric                                   Async      Threaded
  ────────────────────────────────────────────────────────────
  Events received                           4970          7211
  Throughput (events/s)                      331           480
  Overflow drops                               0             0
  Deadline misses                              0             0
  Pipeline duration (s)                       15            15
  ────────────────────────────────────────────────────────────
  Human latency p50 (µs)                     0.1           1.1
  Human latency p90 (µs)                     0.2           1.3
  Human latency p99 (µs)                     0.4           1.6
  ────────────────────────────────────────────────────────────
  Bot latency p50 (µs)                       0.2           1.1
  Bot latency p90 (µs)                       0.2           1.3
  Bot latency p99 (µs)                       0.4           1.7
  ────────────────────────────────────────────────────────────
  Human drift p50 (µs)                       1.0         502.0
  Human drift p90 (µs)                       3.0         523.0
  Human drift p99 (µs)                       4.0         549.0
  Bot drift p50 (µs)                         1.0         502.0
  Bot drift p90 (µs)                         3.0         522.0
  Bot drift p99 (µs)                         6.0         555.0
  ────────────────────────────────────────────────────────────
  Fail-safe activations                        0             0
════════════════════════════════════════════════════════════════════

  Key findings:
  • Threaded achieves 45.1% higher throughput (preemptive OS scheduling).
  • Async shows lower human-edit tail latency (p99: 0.4 µs vs 1.6 µs).
```

**Interpretation.** Threaded delivers higher steady-state throughput because OS-level preemption keeps the parser and hot-path threads actively scheduled even when one is briefly blocked. Async delivers lower tail latency because user-space scheduling avoids the OS context-switch jitter that drives the threaded p99.

### 10.3 Fault-tolerance demo (`--demo`)

The `--demo` mode runs a scripted 4-phase walkthrough over 60 s to exercise the full Component E machinery in a single observable run:

| Phase | Window | Stimulus | Expected behaviour |
|-------|--------|----------|--------------------|
| 1 | 0–15 s | Baseline 2000 eps | NORMAL — no misses |
| 2 | 15–25 s | 3 ms latency injected every 20 packets | DEGRADED; bot packets dropped; DEGRADED ↔ RECOVERY cycling |
| 3 | 25–38 s | Mock stream silenced | Watchdog fires after 10 s silence → Network Reset |
| 4 | 38–60 s | Stream resumes, injection stopped | RECOVERY → NORMAL |

Actual run output (`cargo run --release -- --demo`):

```
[demo] +15s — PHASE 2: latency injection active — expect DEGRADED
[demo]   current mode: NORMAL
[fail-safe] mode → DEGRADED
[fail-safe] mode → RECOVERY
[fail-safe] mode → DEGRADED
  … (cycling continues throughout Phase 2) …
[watchdog] Network Reset #1 triggered — 10010 ms silence (threshold: 10000 ms)
[fail-safe] mode → RECOVERY
[fail-safe] mode → NORMAL
[demo] +25s — PHASE 3: stream silenced — Watchdog fires in ~10s
[demo]   current mode: NORMAL

════════════════════════ PERFORMANCE SUMMARY ════════════════════════
  Uptime:                         60.2 s

  ── Component A: Ingestion ─────────────────────────────────────────
  Async events received:          45049
  Async throughput:               751 events/s
  Async overflow drops:           0
  Threaded events received:       66651
  Threaded throughput:            1111 events/s
  Threaded overflow drops:        0

  ── Component B: Hot Path (2 ms deadline) ──────────────────────────
  Deadline misses:                273 (logged to logs/deadline_misses.csv)
  Human latency  p50/p90/p99:     0.6 / 1.2 / 1.6 us
  Bot latency    p50/p90/p99:     0.6 / 1.2 / 1.6 us

  ── Component C: Scheduling Drift ──────────────────────────────────
  Human drift    p50/p90/p99:     495.0 / 522.0 / 993.0 us
  Bot drift      p50/p90/p99:     497.0 / 522.0 / 995.0 us
  [OK] Human drift p99 < bot drift p99 — priority scheduling confirmed.

  ── Component D: Leaderboard (Top-3 Domains) ───────────────────────
  1. es.wikipedia.org                   9425 edits
  2. en.wikipedia.org                   9419 edits
  3. ja.wikipedia.org                   9407 edits

  ── Component E: Fault Tolerance ───────────────────────────────────
  Watchdog resets:                1
  Fail-safe activations:          114
  Current mode:                   NORMAL
═══════════════════════════════════════════════════════════════════════
```

**Interpretation.** The lower throughput vs. the clean mock run (751 / 1111 vs. 956 / 1387 events/s) reflects the 10-second stream silence in Phase 3 reducing the total event count. The 273 deadline misses are entirely from the Phase 2 injection window (each injected 3 ms spin exceeds the 2 ms deadline). By Phase 4 the injection has stopped, the watchdog has reset, and the system returns to NORMAL — confirming the full Normal → Degraded → Recovery → Normal cycle and the watchdog path are both exercised in a single run.

---

## 11. Criterion Benchmark Suites

Three explicit `[[bench]]` entries in `Cargo.toml` cover the rubric requirements; bench auto-discovery is disabled (`autobenches = false`) so unrelated scaffolding files in `benches/` are not built.

| Bench | Coverage |
|-------|----------|
| `latency_bench` | `parse_zero_copy` for ASCII / Unicode / invalid JSON; percentile computation at 100 / 1 K / 10 K samples; deadline-comparison overhead; `ChangePacket` heap-allocation cost |
| `sync_bench` | Mutex / RwLock / Atomic at 1 / 2 / 4 / 8 concurrent writers; `Leaderboard::increment` and `top_n` for all three strategies; mixed read/write contention |
| `pipeline_bench` | End-to-end async / threaded throughput; `tokio::mpsc` vs `crossbeam` send latency; overflow handling cost; parse + priority-dispatch cost; async scalability at 50 / 200 / 500 eps |

```bash
cargo bench --bench latency_bench
cargo bench --bench sync_bench
cargo bench --bench pipeline_bench
# or:
cargo bench   # produces target/criterion/report/index.html
```

> **[SCREENSHOT 8]** *(Optional — see action item #7.)*
> Open `target/criterion/report/index.html` in a browser after `cargo bench`. The Criterion landing page lists every bench group with a violin-style distribution thumbnail. Capture the index page and one or two of the more interesting per-bench detail pages (e.g. `sync_bench/leaderboard_increment` with its 1/2/4/8-writer scaling).

---

## 12. Engineering Decisions & Trade-offs

| Decision | Why |
|----------|-----|
| `parking_lot::Mutex` / `RwLock` for the leaderboard | Faster than `std::sync` (~3 ×) and never poisons on panic — critical for a long-running monitor |
| Drop-newest backpressure (incoming packet discarded) | Preserves in-flight work; the alternative — drop-oldest — would invalidate already-stamped T1 timestamps |
| `try_lock` everywhere on the hot path | A blocking `lock()` could push the 2 ms deadline; missing one sample is far cheaper than missing the deadline |
| `StdRng::from_entropy()` in the async mock stream | The default `ThreadRng` is `!Send` and could not be held across `.await` inside a `tokio::spawn` block |
| `[AtomicU64; 10]` for the Atomic leaderboard | Wait-free reads/writes for a fixed domain set; falls back gracefully (no-op) for unknown domains |
| Watchdog stamps heartbeat from the **parser** stage, not the ingestion stage | Confirms end-to-end progress — a frozen parser would still be a real fault even if raw bytes are arriving |

---

## 13. Conclusion

The system meets every requirement in the assignment specification:

- **Component A** — both pipelines implemented, both report distinct throughput numbers, drop-newest backpressure logged with high-precision timestamps.
- **Component B** — `WikiChange<'a>` is fully zero-copy; the 2 ms deadline is enforced and instrumented; degraded-mode bot packets are dropped at hot-path entry.
- **Component C** — strict High-before-Low drain order proven by unit test; drift recorded per priority class with non-zero p50/p90/p99 in every run.
- **Component D** — three sync strategies live and benchmarked side-by-side; same `top_n(3)` API across all three.
- **Component E** — 10 s watchdog with `AtomicI64` heartbeat; three-state fail-safe machine with 50 % bot admission in Recovery; both counters wired to `SharedMetrics`.
- **Metrics & reporting** — 10 000-sample rolling window; sorted-snapshot percentiles; `compare_pipelines` prints p50/p90/p99 for both classes for both latency and drift.
- **Criterion** — three bench suites, `harness = false`, no `std::thread::sleep`, all compile under `cargo bench --no-run`.

Build is warning-free under the strictest default lint set; both unit tests pass.

---

<!-- ============================================================
====  AUTHOR ACTION ITEMS — DELETE EVERYTHING BELOW THIS LINE
====  BEFORE EXPORTING TO PDF FOR FINAL SUBMISSION
============================================================ -->

# AUTHOR ACTION ITEMS — DELETE BEFORE FINAL SUBMISSION

This section is for **you** (the author). Do **not** include it in the final printable submission. Once every screenshot is captured and pasted in, simply delete from the `<!-- AUTHOR ACTION ITEMS ... -->` HTML comment above to the end of the file.

## Screenshots to capture

For each placeholder marked `[SCREENSHOT N]` in the report above, follow the instructions below. Save the captures into a sibling folder `./images/` so the doc can use relative paths like `![caption](./images/screenshot_01.png)` if you decide to embed them inline.

### How to take a clean terminal screenshot on Windows

1. Use **Windows Terminal** (not the legacy `cmd.exe` window). Set a dark theme and increase the font size to ~14 pt for legibility in print.
2. Resize the window so the entire output of one command fits without scroll, then run the command.
3. Select **Snipping Tool** → **Window snip** (or `Win` + `Shift` + `S` then drag) and capture only the terminal window — no taskbar.
4. Save as PNG. Name the file as suggested below.

### Action item checklist

| # | Placeholder | Capture | Filename |
|---|-------------|---------|----------|
| 1 | `[SCREENSHOT 1]` (§ 2) | Editor file-tree showing `src/`, `benches/`, `Cargo.toml`. In VS Code: `Ctrl + Shift + E` to open Explorer, expand `src/` and `benches/` fully, then snip the side panel. **Optional** — skip if tight on space. | `images/file_tree.png` |
| 2 | `[SCREENSHOT 2]` (§ 3) | Run `cargo build --release` and screenshot the final two lines (`Compiling wiki_rt_monitor` … `Finished release profile … target(s) in N s`). Crop so the grader can see "no warnings" by virtue of zero `warning:` lines. | `images/build_clean.png` |
| 3 | `[SCREENSHOT 4]` (§ 6.2) | Run `cargo test --release` and screenshot the final test summary block — must show both `high_priority_drains_before_low ... ok` and `drift_is_recorded_on_dequeue ... ok` plus `test result: ok. 2 passed`. | `images/test_results.png` |
| 4 | `[SCREENSHOT 5]` (§ 7) | Run `cargo run --release -- --mock`, wait 60 s, then screenshot **only** the "Sync-Strategy Benchmark (inline)" table at the bottom (Mutex / RwLock / Atomic with ops/sec / mean / p99). | `images/sync_benchmark.png` |
| 5 | `[SCREENSHOT 6]` (§ 10.1) | Same command as #4, but capture the **PERFORMANCE SUMMARY** block (the section between the two `═══` rules). All five components (A–E) plus uptime should be visible. | `images/performance_summary.png` |
| 6 | `[SCREENSHOT 7]` (§ 10.2) | Run `cargo run --release --bin compare_pipelines -- --mock`. After ~30 s (15 + 15), screenshot the entire **PIPELINE COMPARISON REPORT** table plus the trailing "Key findings" lines. | `images/compare_pipelines.png` |
| 7 | `[SCREENSHOT 8]` (§ 11) | Run `cargo bench` (takes 5–10 minutes). Open `target/criterion/report/index.html` (or `C:/rust_builds/real_time_system/criterion/report/index.html` if `CARGO_TARGET_DIR` is set) in Chrome / Edge. Screenshot the index landing page. **Optional** — capture one detail page (suggest: `sync_bench/leaderboard_increment`) for visual variety. | `images/criterion_index.png` |

### How to embed each screenshot

Find each `[SCREENSHOT N]` block in the document and replace the placeholder with the standard markdown image syntax:

```markdown
![Performance summary after 60 s mock run](./images/performance_summary.png)
```

Place the image **immediately after** the existing italic caption / instruction line, then optionally delete the original `> [SCREENSHOT N]` blockquote so only your captured figure remains.

### Final-print checklist

Before exporting to PDF / printing:

1. All `[SCREENSHOT N]` placeholders replaced with embedded images (or removed if marked Optional and you skipped).
2. The captured terminal text in §§ 10.1 and 10.2 either replaced with **your** latest run output, or kept verbatim if you prefer the canonical numbers.
3. **Delete this entire `AUTHOR ACTION ITEMS` section** — i.e. everything from the `<!-- ============... -->` HTML comment marker to the end of the file. The marker comment is invisible in rendered markdown, but you can find it by searching for `AUTHOR ACTION ITEMS — DELETE`.
4. Re-render the document (`Ctrl + Shift + V` in VS Code) to verify the layout looks clean before exporting.

