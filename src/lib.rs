//! # wiki_rt_monitor
//!
//! Real-time Wikipedia edit monitoring engine — RTS2601 assignment.
//!
//! ## Architecture
//!
//! Two concurrent pipelines process the same Wikipedia SSE firehose:
//!
//! | Module | Pipeline model | Channel |
//! |--------|---------------|---------|
//! | [`component_a::async_pipeline`] | Tokio cooperative tasks | `tokio::sync::mpsc` |
//! | [`component_a::threaded_pipeline`] | OS-thread preemptive | `crossbeam::bounded` |
//!
//! Both pipelines share a hot path ([`component_b`]), priority scheduler
//! ([`component_c`]), leaderboard ([`component_d`]), and fail-safe FSM
//! ([`component_e`]) via `Arc<Mutex<SharedMetrics>>`.
//!
//! ## Component map (assignment rubric)
//!
//! - **A** — dual pipelines with drop-oldest backpressure
//! - **B** — zero-copy parsing (`WikiChange<'a>`) + 2 ms deadline enforcement
//! - **C** — fixed-priority scheduling (human > bot) + scheduling-drift measurement
//! - **D** — shared leaderboard with Mutex / RwLock / Atomic strategies
//! - **E** — watchdog (10 s silence → Network Reset) + fail-safe FSM
//!
//! ## Running
//!
//! ```text
//! cargo run --release -- --mock     # deterministic synthetic stream
//! cargo run --release               # live Wikipedia SSE stream
//! cargo run --release -- --demo     # 4-phase fault-tolerance walkthrough
//! cargo run --release -- --stress   # inject latency to exercise fail-safe
//! cargo bench                       # Criterion benchmark suite
//! ```

pub mod component_a;
pub mod component_b;
pub mod component_c;
pub mod component_d;
pub mod component_e;
pub mod ingestion;
pub mod metrics;
pub mod types;
pub mod alloc_counter;

/// Experimental / scaffolding code not wired into the main pipeline.
/// Retained for reference but excluded from all builds via `cfg(any())`.
/// The files remain on disk for inspection; they are not compiled.
#[cfg(any())]
pub mod advanced;
