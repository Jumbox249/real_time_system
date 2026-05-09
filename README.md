# Wikipedia Real-Time Monitoring Engine

**Course:** RTS2601 – Real-Time Systems  
**Crate:** `wiki_rt_monitor` (Rust 2021)  
**Stream:** Live Wikipedia Recent-Changes SSE — `https://stream.wikimedia.org/v2/stream/recentchange`

A high-pressure data-ingestion and analytics pipeline built in Rust, implementing two concurrent architectures (async/Tokio and multi-threaded/OS-thread), zero-copy JSON parsing, priority-aware scheduling, lock-benchmarked shared state, and fault-tolerance machinery (watchdog + fail-safe FSM).

---

## Prerequisites

- **Rust stable** (1.75 or later) — install via [rustup.rs](https://rustup.rs)
- **Windows note:** the project path contains spaces. Both MSVC and GNU Windows toolchains refuse to run `dlltool`/`link` against space-containing paths. Redirect Cargo's build output before building:

```powershell
$env:CARGO_TARGET_DIR = "C:\rust_builds\real_time_system"
```

or set it permanently in your shell profile. All commands below assume this is set.

---

## Build

```bash
# debug build (fast compile)
cargo build

# release build (required for accurate timing measurements)
cargo build --release
```

---

## Run

### Default — live Wikipedia SSE stream (60 s)

```bash
cargo run --release
```

Connects to the live Wikipedia stream. Requires internet access.

### Mock stream — no network needed (60 s)

```bash
cargo run --release -- --mock
```

Uses a deterministic synthetic event generator at 2 000 events/s. Suitable for reproducible benchmarks and offline demos.

### Stress mode — injects latency to trigger fail-safe

```bash
cargo run --release -- --stress
```

Adds ~3 ms busy-spin every 50 packets. Pushes the system into Degraded and Recovery modes so the fail-safe FSM is exercised.

### Demo mode — scripted 4-phase fault-tolerance walkthrough

```bash
cargo run --release -- --demo
```

Scripted run with four annotated phases:

| Time | Phase | Expected mode |
|------|-------|---------------|
| 0–15 s | Baseline 2 000 eps | NORMAL |
| 15–25 s | 3 ms latency injected every 50 packets | DEGRADED |
| 25–35 s | Mock stream silenced (watchdog fires at +10 s) | Watchdog reset |
| 35–60 s | Latency recovers | RECOVERY → NORMAL |

---

## Additional binaries

| Binary | Command | Purpose |
|--------|---------|---------|
| `compare_pipelines` | `cargo run --release --bin compare_pipelines -- --mock` | Side-by-side async vs threaded report with p50/p90/p99 |
| `alloc_proof` | `cargo run --release --bin alloc_proof` | Proves zero heap allocations on the hot path via custom allocator |

---

## Benchmarks

```bash
# Run all three Criterion benchmark suites
cargo bench

# Individual suites
cargo bench --bench latency_bench    # parse latency + deadline overhead
cargo bench --bench sync_bench       # Mutex / RwLock / Atomic at 1/2/4/8 writers
cargo bench --bench pipeline_bench   # async vs threaded end-to-end throughput
```

HTML reports are generated at `target/criterion/report/index.html`.

---

## Tests

```bash
cargo test --release
```

Includes unit tests for priority drain order and drift recording (see `src/component_c/priority_scheduler.rs`).

---

## Output artefacts

| File | Produced by | Contents |
|------|-------------|---------|
| `logs/overflow_events.csv` | `cargo run` (any mode) | Drop-oldest overflow events: `total_drops,domain,priority` |
| `logs/deadline_misses.csv` | `cargo run --stress` or `--demo` | Packets that exceeded 2 ms: `latency_us,domain,priority` |
| `target/criterion/` | `cargo bench` | Criterion HTML + PNG benchmark reports |

---

## Component map (assignment rubric → source)

| Component | Requirement | Source file(s) |
|-----------|-------------|---------------|
| A1 | Async/Tokio pipeline | `src/component_a/async_pipeline.rs` |
| A2 | Multi-threaded pipeline | `src/component_a/threaded_pipeline.rs` |
| B | Zero-copy serde parsing | `src/component_b/zero_copy_parser.rs`, `src/types.rs` |
| B | 2 ms hot-path deadline | `src/component_b/hot_path.rs` |
| C | Priority scheduling (human > bot) | `src/component_c/priority_scheduler.rs` |
| C | Scheduling drift (T2−T1, p50/p90/p99) | `src/component_c/priority_scheduler.rs`, `src/metrics.rs` |
| D | Shared leaderboard (top-3 domains) | `src/component_d/leaderboard.rs` |
| D | Mutex/RwLock/Atomic benchmark | `src/component_d/sync_benchmark.rs` |
| E | Watchdog timer (10 s silence → reset) | `src/component_e/watchdog.rs` |
| E | Fail-safe FSM (Normal/Degraded/Recovery) | `src/component_e/fail_safe.rs` |
| — | Metrics + percentile math | `src/metrics.rs` |
| — | Custom allocator (heap-alloc proof) | `src/alloc_counter.rs` |

---

## Analysis script

```bash
python scripts/analyze_logs.py logs/
```

Reads `overflow_events.csv` and `deadline_misses.csv` and prints a summary table of miss counts by priority and p50/p90/p99 latency.

---

## Research report

See `report/RTS2601_Report.md` (academic article, 3 000–4 000 words) and the compiled PDF at `report/RTS2601_Report.pdf`.

The engineering notes that informed the report are in `REPORT.md` at the repo root.
