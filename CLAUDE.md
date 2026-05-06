# CLAUDE.md — Wikipedia Real-Time Monitoring Engine (RTS2601)

This file instructs Claude Code on how to implement, extend, debug, and benchmark
every part of this system to Distinction/A+ standard. Read it in full before
touching any source file.

---

## Project overview

A soft-real-time Wikipedia recent-changes monitoring engine that ingests the live
SSE stream from `https://stream.wikimedia.org/v2/stream/recentchange`, classifies
edits by priority (human vs bot), maintains a live domain-edit leaderboard, and
enforces a 2 ms hot-path deadline per packet — all in pure Rust.

Package name: **`wiki_rt_monitor`** — crate root at `src/lib.rs`.

Two binaries:
- `wiki_rt_monitor` (`src/main.rs`) — runs both pipelines for 60 s and prints a
  full performance summary.
- `compare_pipelines` (`src/bin/compare_pipelines.rs`) — runs each pipeline for
  15 s in isolation and prints a p50/p90/p99 tail-latency comparison table.

Three Criterion benchmark suites: `latency_bench`, `sync_bench`, `pipeline_bench`.

---

## Non-negotiable requirements

| Requirement | Target |
|---|---|
| Language | Pure Rust 2021 edition — no Python, C, shell scripts |
| Hot-path deadline | ≤ 2 ms per ChangePacket (T2 → T3) |
| Zero-copy parsing | `WikiChange<'a>` with `#[serde(borrow)]` / `&'a str` |
| Dual pipeline | Architecture 1: Tokio async; Architecture 2: std::thread |
| Backpressure | Drop-oldest + log `OverflowEvent` with precise timestamp |
| Priority scheduling | Human edits always drain before bot edits |
| Scheduling drift | Measured per priority class at p50/p90/p99 |
| Leaderboard sync | Three strategies: Mutex, RwLock, AtomicU64 |
| Watchdog | 10 s silence → trigger "Network Reset" |
| Fail-safe | Normal → Degraded (>2000 µs) → Recovery → Normal |
| Percentile stats | Rolling 10 000-sample window, p50/p90/p99 |
| Criterion benches | latency, sync contention, pipeline comparison |

---

## File map

```
src/
├── lib.rs                       ← pub re-exports all 7 modules
├── main.rs                      ← orchestrator, 60 s run, summary
├── types.rs                     ← WikiChange<'a>, ChangePacket, Priority, SystemMode
├── metrics.rs                   ← LatencySamples, SharedMetrics, MetricsHandle
├── ingestion/
│   ├── mod.rs                   ← StreamSource enum
│   ├── sse_client.rs            ← reqwest SSE → Bytes channel
│   └── mock_stream.rs           ← synthetic events at N eps
├── component_a/
│   ├── mod.rs
│   ├── async_pipeline.rs        ← Tokio mpsc, cooperative scheduling
│   └── threaded_pipeline.rs     ← crossbeam, OS threads
├── component_b/
│   ├── mod.rs
│   ├── zero_copy_parser.rs      ← parse_zero_copy(), parse_hot_fields()
│   └── hot_path.rs              ← HotPathProcessor, 2 ms deadline
├── component_c/
│   ├── mod.rs
│   └── priority_scheduler.rs   ← dual-queue dispatch, drift measurement
├── component_d/
│   ├── mod.rs
│   ├── leaderboard.rs           ← Leaderboard, SyncStrategy (Mutex/RwLock/Atomic)
│   └── sync_benchmark.rs        ← benchmark_mutex/rwlock/atomic, run_all_and_print
├── component_e/
│   ├── mod.rs
│   ├── watchdog.rs              ← 10 s AtomicI64-based watchdog
│   └── fail_safe.rs             ← 3-state machine, AtomicU8
└── bin/
    └── compare_pipelines.rs     ← sequential pipeline comparison

benches/
├── latency_bench.rs             ← zero-copy parse, percentile, deadline
├── sync_bench.rs                ← Mutex vs RwLock vs Atomic contention
└── pipeline_bench.rs            ← async vs threaded throughput + scalability
```

---

## Zero-copy parsing — how it works

`WikiChange<'a>` borrows string fields directly from the raw JSON buffer:

```rust
#[derive(Debug, Deserialize)]
pub struct WikiChange<'a> {
    #[serde(borrow)]
    pub user: Option<&'a str>,
    #[serde(borrow, rename = "server_name")]
    pub server_name: Option<&'a str>,
    // ... other &'a str fields
}
```

`serde_json::from_slice(&buf)` produces a `WikiChange<'_>` with zero heap
allocations for ASCII-only strings. When the packet needs to outlive the buffer
it is promoted to a `ChangePacket` (4 `String` fields — unavoidable).

**Never change these fields to `String` inside `WikiChange`.** That would defeat
the zero-copy design and increase hot-path latency.

---

## Priority scheduling

`Priority::High` = human editor, `Priority::Low` = bot.  
The `HotPathProcessor` drains **all** High-priority packets before touching Low
ones. In Degraded fail-safe mode, Low packets are dropped at entry:

```rust
if self.fail_safe.is_degraded() && pkt.priority == Priority::Low {
    return; // drop bot packet
}
```

Scheduling drift = T2 (dequeue instant) − T1 (enqueue instant), measured per
priority class, stored in `SharedMetrics.human_drift_us` / `bot_drift_us`.

---

## Fail-safe state machine

```
NORMAL ──(jitter > 2000 µs)──► DEGRADED ──(jitter < 500 µs)──► RECOVERY
  ▲                                                                   │
  └─────────────────(20 consecutive clean cycles)────────────────────┘
```

State stored in an `AtomicU8` for lock-free reads on the hot path.  
In Recovery: bot packets admitted at 50 % (every other packet via `AtomicU32`
toggle). Transitions log to stderr and increment `fail_safe_activations`.

---

## Backpressure / overflow

When a bounded channel is full:
1. Log an `OverflowEvent` with `dropped_at: Instant::now()` and domain.
2. Increment `async_overflow_count` or `threaded_overflow_count` in metrics.
3. **Drop the incoming packet** (drop-newest strategy; oldest stays in queue).

For `tokio::sync::mpsc`: use `try_send` → `TrySendError::Full`.  
For `crossbeam::channel`: use `try_send` → `TrySendError::Full`.

---

## Leaderboard strategies

| Strategy | API | Lock type | Best for |
|---|---|---|---|
| `Mutex` | `parking_lot::Mutex<HashMap>` | Exclusive | Low contention |
| `RwLock` | `parking_lot::RwLock<HashMap>` | Read-write | Read-heavy (default) |
| `Atomic` | `[AtomicU64; 10]` per known domain | Lock-free | Max throughput |

`Leaderboard::new(strategy, metrics)` returns `Arc<Leaderboard>`.  
Write with `leaderboard.increment(domain)`.  
Read with `leaderboard.top_n(3)` → `Vec<(String, u64)>` sorted descending.

---

## Throughput & latency targets (Distinction-level)

These targets are aspirational under mock mode on a laptop. They distinguish a
passable submission from a Distinction one:

| Metric | Pass | Distinction |
|---|---|---|
| Throughput | > 50 events/s | > 400 events/s |
| Human latency p99 | < 50 ms | < 500 µs |
| Deadline misses | < 5 % | < 0.5 % |
| Overflow rate | < 10 % | < 1 % |
| Benchmark coverage | basic | p50/p90/p99 all paths |

---

## Running the project

```bash
# Mock stream, offline (recommended for testing)
cargo run --release -- --mock

# Live Wikipedia SSE stream (requires internet)
cargo run --release

# Detailed async vs threaded comparison with tail latencies
cargo run --release --bin compare_pipelines -- --mock

# Full Criterion benchmark suite
cargo bench

# Single benchmark group
cargo bench --bench latency_bench
cargo bench --bench sync_bench
cargo bench --bench pipeline_bench
```

---

## Grading checklist

### Component A – Dual Pipeline Architecture
- [ ] `run_async_pipeline` uses `tokio::sync::mpsc::channel(512)`
- [ ] `run_threaded_pipeline` uses `crossbeam::channel::bounded(512)`
- [ ] Both pipelines update `async_events_received` / `threaded_events_received`
- [ ] Drop-oldest backpressure with `OverflowEvent` logged with timestamp
- [ ] Overflow counters incremented in `SharedMetrics`

### Component B – Zero-Copy Parsing & Hot Path
- [ ] `WikiChange<'a>` uses `#[serde(borrow)]` on all string fields
- [ ] `parse_zero_copy()` returns `None` for events without `server_name`
- [ ] `HotPathProcessor` enforces `HOT_DEADLINE = Duration::from_millis(2)`
- [ ] `deadline_misses` counter incremented on each miss
- [ ] Fail-safe degraded-mode bot-drop implemented at hot-path entry

### Component C – Priority Scheduling & Drift
- [ ] Human edits assigned `Priority::High`, bot edits `Priority::Low`
- [ ] High queue always drained before Low queue
- [ ] Drift measured as `T2 - T1` per priority class
- [ ] `human_drift_us` and `bot_drift_us` populated in `SharedMetrics`

### Component D – Leaderboard & Sync Benchmark
- [ ] All three `SyncStrategy` variants implemented and callable
- [ ] `run_all_and_print` called from `main.rs` and prints comparison table
- [ ] `top_n(3)` returns sorted `Vec<(String, u64)>` for all strategies
- [ ] `benchmark_mutex` / `benchmark_rwlock` / `benchmark_atomic` return `SyncBenchResult` with `ops_per_sec`, `mean_ns`, `p99_ns`

### Component E – Fault Tolerance
- [ ] `Watchdog` triggers reset after 10 s of silence via `AtomicI64`
- [ ] `FailSafe` state machine: Normal / Degraded / Recovery
- [ ] `fail_safe_activations` counter incremented on each Normal→Degraded transition
- [ ] `watchdog_resets` counter incremented on each Watchdog trigger

### Metrics & Reporting
- [ ] `LatencySamples` rolling window of 10 000 samples
- [ ] `p50()`, `p90()`, `p99()` implemented via sorted snapshot
- [ ] `compare_pipelines` binary prints p50/p90/p99 for both pipelines
- [ ] `main.rs` summary prints all KPIs: ingestion, hot-path, drift, leaderboard, fault tolerance

### Criterion Benchmarks
- [ ] `latency_bench`: parse ASCII, parse Unicode, percentile computation
- [ ] `sync_bench`: Mutex/RwLock/Atomic under 1/2/4/8 writer threads
- [ ] `pipeline_bench`: async/threaded throughput, channel send, scalability

---

## Common mistakes to avoid

1. **Do not** use `String` fields in `WikiChange<'a>` — always `&'a str` with `#[serde(borrow)]`.
2. **Do not** block inside a Tokio task — use `tokio::task::yield_now().await` to cooperate.
3. **Do not** use `std::sync::Mutex` in the hot path — use `parking_lot::Mutex` (no poisoning, faster).
4. **Do not** deadlock by holding a `metrics.lock()` while calling `leaderboard.top_n()` (which also locks metrics internally).  Drop the guard first.
5. **Do not** share a single `Leaderboard` between the sync benchmark and the live run — they use different `SyncStrategy` variants.
6. **Do not** forget `stop.store(true, Ordering::Relaxed)` after the main pipeline returns — all spawned threads check this flag.
7. **Do not** add Unicode/emoji to log output — it may not render on all terminals.
8. **Do not** add `unsafe` code — the system is entirely safe Rust.

---

## Key invariants (never break)

- `ChangePacket.t0` is always set at parse time; `t1`/`t2`/`t3` are set by the scheduler.
- `Priority::High > Priority::Low` (`High = 1, Low = 0`) — used for `Ord`-based queue ordering.
- `SystemMode` transitions are monotonic per cycle: Normal→Degraded or Degraded→Recovery (never skip).
- The `LatencySamples` window is FIFO (oldest popped on overflow, newest pushed).
- Benchmark functions must never call `std::thread::sleep` — they must exit in bounded time for Criterion.
