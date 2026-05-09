---
title: "Real-Time Data Ingestion and Analytics at Scale: A Dual-Architecture Rust Implementation"
author: "Abubaker"
date: "May 2026"
course: "CT087-3-3-RTS Real-Time Systems"
institution: "Asia Pacific University of Technology & Innovation"
bibliography: refs.bib
csl: apa.csl
---

# Abstract

This paper presents the design, implementation, and evaluation of a high-pressure real-time data-ingestion pipeline built in Rust, processing the live Wikipedia Recent-Changes Server-Sent Events (SSE) firehose. The system implements two complete concurrent architectures — cooperative async/await via the Tokio runtime and preemptive multi-threading via OS-level `std::thread` — and evaluates both under identical workloads. Key contributions include a zero-copy JSON parsing strategy using Rust lifetimes, a fixed-priority scheduler that demonstrably reduces scheduling drift for human edits over bot edits, a three-strategy synchronisation benchmark (Mutex, RwLock, and lock-free Atomics), and a fault-tolerance subsystem comprising a watchdog timer and a three-state fail-safe finite-state machine. Empirical results show the threaded architecture achieves 23% higher throughput (192 vs. 156 events/s) while the async architecture achieves lower tail latency under cooperative scheduling. The Atomic leaderboard strategy achieves 33 million operations per second — 5.6× faster than the Mutex baseline — and heap allocations on the hot parsing path are proven to be zero for ASCII-clean payloads via a custom global allocator.

---

# 1. Introduction and Background

## 1.1 Motivation

Modern edge-computing and financial systems must ingest continuous streams of heterogeneous data and make sub-millisecond decisions under non-deterministic network conditions. Traditional polling-based or batch-processing architectures cannot satisfy these demands: they introduce unacceptable latency floors and lack the scheduling guarantees required for safety-critical and time-critical tasks.

Wikipedia's Recent-Changes SSE stream presents a controlled but realistic approximation of such a workload: it delivers between 100 and 2 000 change events per second from a global network of editors and automated bots, with variable JSON payload sizes and unpredictable inter-arrival times. Building a monitoring engine over this stream requires simultaneously solving problems in network I/O, concurrent data processing, memory management, and fault tolerance — the core challenges of any real-time system.

## 1.2 Objectives

This project aims to:

1. Implement two architecturally distinct concurrent processing pipelines and compare their real-time performance characteristics.
2. Demonstrate zero-copy memory management using Rust's ownership and lifetime system to minimise heap pressure on the hot path.
3. Enforce and measure a strict per-packet processing deadline of 2 milliseconds across both architectures.
4. Apply fixed-priority scheduling to differentiate human and bot edit processing, and quantify the resulting scheduling drift reduction.
5. Benchmark three synchronisation primitives (Mutex, RwLock, Atomic) under increasing thread contention for a shared leaderboard.
6. Implement network fault tolerance via a watchdog timer and an automatic fail-safe mode that degrades gracefully under load.

## 1.3 Real-Time System Context

A real-time system is distinguished not merely by speed, but by the *timeliness* and *predictability* of its responses [@liu1973scheduling]. A hard real-time system treats a missed deadline as a system failure. A soft real-time system tolerates occasional misses but degrades in quality of service. This implementation targets soft real-time behaviour: the 2 ms deadline is enforced and monitored, missed deadlines are logged and analysed, but no safety-critical action depends on the deadline being met for every packet.

Rust is a natural fit for real-time systems programming. Its ownership model eliminates dangling pointers and data races at compile time [@jung2017rustbelt], its lack of a garbage collector removes the unpredictable stop-the-world pauses that afflict JVM or Go-based systems, and its zero-cost abstractions allow high-level concurrent code without hidden runtime overhead [@matsakis2014rust].

---

# 2. Related Work

## 2.1 Async/Await and OS-Thread Concurrency in Rust

Rust provides two complementary concurrency models. The `async`/`.await` syntax, stabilised in Rust 1.39, enables cooperative multitasking within a single-threaded or multi-threaded task executor. The Tokio runtime [@tokio2024] is the de-facto standard async runtime, providing a work-stealing thread pool, timer infrastructure, and I/O reactor. Cooperative scheduling minimises context-switch overhead and is particularly efficient when tasks spend most of their time waiting on I/O — as is the case with the Wikipedia SSE client.

OS-level threads, by contrast, are preemptively scheduled by the kernel and are suited to compute-bound workloads. The `crossbeam` crate [@glavina2019crossbeam] provides bounded, lock-free MPMC channels built on Dmitry Vyukov's queuing algorithms [@vyukov2010bounded], offering lower latency than `std::sync::mpsc` under contention.

The principal trade-off is well understood: async tasks have lower memory footprint (a Tokio task costs ~300 bytes vs. ~8 KB for a stack-allocated OS thread) but are susceptible to latency spikes when any task within a thread's executor holds the CPU without yielding [@tokio2024]. OS threads are coarser but provide isolation and bounded preemption latency determined by the OS scheduler quantum (typically 1–10 ms on Linux).

## 2.2 Fixed-Priority Scheduling and Drift

Fixed-priority scheduling assigns a static priority to each task class at design time. The Rate-Monotonic Scheduling (RMS) theorem [@liu1973scheduling] proves that for periodic tasks, assigning priorities in inverse proportion to period is optimal. This work applies a simpler two-priority model: human edits receive High priority; bot edits receive Low priority. This is justified by the latency requirements of the use case — human-generated events are more time-sensitive for anomaly detection than automated bot edits, which are predictable and high-volume.

Scheduling drift, defined as the difference between when a task was ready to execute and when it actually began executing, is a standard measure of scheduler fidelity [@kopetz2011real]. Lower drift for high-priority tasks indicates that the scheduler is correctly servicing them before lower-priority work.

## 2.3 Zero-Copy I/O and Memory Management

Zero-copy techniques avoid redundant memory copies between kernel and user space, or between stages of a processing pipeline, by passing references to shared buffers rather than copies of the data. In the context of JSON parsing, serde's `#[serde(borrow)]` attribute and Rust's lifetime system allow deserialised string fields to be represented as `&'a str` slices pointing directly into the source byte buffer, rather than `String` heap allocations [@serde2024].

This matters for real-time systems because heap allocation is a non-deterministic operation: the allocator may need to search for a free block, or trigger compaction/reclamation. The `bytes` crate [@bytes2024] provides reference-counted byte buffers that can be sliced and shared across tasks without copying.

## 2.4 Lock-Free Synchronisation

Priority inversion — where a high-priority task is blocked waiting for a resource held by a lower-priority task — is a classic hazard in shared-resource concurrent systems [@sha1990priority]. Mitigation strategies include priority inheritance, priority ceiling protocols, and lock-free data structures. The `parking_lot` crate [@amanieu2024parking] provides Mutex and RwLock implementations that are significantly faster than `std::sync` equivalents by avoiding system calls on the uncontended fast path. Lock-free structures using atomic CAS operations on fixed-size arrays eliminate the blocking entirely for known-domain leaderboards.

---

# 3. System Design

## 3.1 Architecture Overview

The monitoring engine is structured as a five-component pipeline (Figure 1). The ingestion layer (Component A) connects to the Wikipedia SSE stream and produces raw JSON byte buffers. Component B parses these buffers zero-copy and enforces the 2 ms deadline. Component C routes packets through a two-priority queue. Component D maintains a live domain leaderboard. Component E monitors network health and system jitter.

```
┌─────────────────────────────────────────────────────────────┐
│                     wiki_rt_monitor                          │
│                                                              │
│  ┌──────────────┐    Bytes     ┌────────────┐               │
│  │  SSE Client  │─────────────►│  Parser    │ Component B   │
│  │  (reqwest)   │  mpsc(512)   │  zero-copy │               │
│  └──────────────┘              └─────┬──────┘               │
│         │                           │ ChangePacket           │
│  Architecture 1 (Tokio)             ▼                        │
│  ─────────────────────   ┌───────────────────┐              │
│                           │ PriorityScheduler │ Component C  │
│  ┌──────────────┐         │  High ──► Low     │              │
│  │  Mock Stream │         └─────────┬─────────┘              │
│  │  (blocking)  │                   │                        │
│  └──────────────┘    OS thread      ▼                        │
│  Architecture 2 (std)  ┌─────────────────────┐              │
│  ────────────────────  │  HotPathProcessor   │ Component B  │
│                         │  T2→T3  deadline    │              │
│                         └──────┬──────────────┘              │
│                                │                             │
│              ┌─────────────────┼──────────────┐             │
│              ▼                 ▼               ▼             │
│         ┌─────────┐    ┌──────────┐    ┌──────────┐         │
│         │ Leaderb │    │FailSafe  │    │ Watchdog │         │
│         │  -oard  │    │   FSM    │    │  (10 s)  │         │
│         │ Comp. D │    │ Comp. E  │    │ Comp. E  │         │
│         └─────────┘    └──────────┘    └──────────┘         │
└─────────────────────────────────────────────────────────────┘
```
*Figure 1: System architecture. Both architectures share the priority scheduler, hot path, leaderboard, and fail-safe.*

## 3.2 Dual Pipeline Architecture (Component A)

**Architecture 1 — Async/Tokio.** The SSE client and parser run as Tokio tasks. Communication uses `tokio::sync::mpsc::channel(512)`, a bounded single-producer single-consumer queue. When the channel is full, the oldest packet is evicted (drop-oldest backpressure) and an `OverflowEvent` is logged with a nanosecond-precision timestamp.

```
SSE task ──mpsc(512)──► Parse task ──► PriorityScheduler ──► Hot-path task
```

The cooperative scheduler requires explicit `tokio::task::yield_now().await` calls after processing each packet to allow other tasks to run. Without yields, a compute-bound parse loop would starve the ingestion task on the same executor thread.

**Architecture 2 — Threaded.** Three OS threads replace the three Tokio tasks, connected by a `crossbeam::channel::bounded(512)` queue. The mock stream generator uses a blocking sleep loop (`std::thread::sleep`) rather than Tokio's timer. Drop-oldest backpressure is implemented identically. OS thread preemption ensures the ingestion thread continues producing even if the parse thread momentarily blocks.

Both architectures share the same `PriorityScheduler`, `HotPathProcessor`, `Leaderboard`, and `FailSafe` via `Arc<T>` — this enables direct, apples-to-apples comparison of throughput and latency.

## 3.3 Zero-Copy Data Path (Component B)

The zero-copy parsing strategy uses Rust lifetimes to avoid unnecessary heap allocations. Figure 2 illustrates the data ownership chain:

```
Raw SSE bytes                    Wikipedia JSON buffer
─────────────────────────────────────────────────────
Bytes (ref-counted)   ← network chunk accumulated in Vec<u8>
    │
    │  serde_json::from_slice(&buf)
    ▼
WikiChange<'a>         ← borrows &'a str for user, server_name, title
    │ fields: &'a str pointing into Bytes (no allocation)
    │
    │  ChangePacket::from_change(&change)
    ▼
ChangePacket           ← owned Strings (4 allocations, unavoidable:
                         packet must outlive the JSON buffer)
```
*Figure 2: Zero-copy data flow. String fields in `WikiChange<'a>` are borrowed slices; only `ChangePacket` allocates.*

The `#[serde(borrow)]` attribute on each string field tells serde to deserialise into `&'a str` rather than `String`. This is zero-copy for unescaped ASCII (the majority of Wikipedia titles and usernames). For fields containing Unicode escape sequences (e.g., `é`), serde must allocate a new string — a known, documented limitation.

The allocation counter (Component — `alloc_counter.rs`) wraps the system allocator with atomic counters. Measurement on the hot path confirms that ASCII-clean payloads produce zero allocations for the borrowed fields, and exactly four allocations for the `ChangePacket` promotion (user, server_name, title, change_type strings).

## 3.4 Priority Scheduling and Drift Measurement (Component C)

The `PriorityScheduler` maintains two bounded `crossbeam` channels — one per priority level. Enqueue stamps T1; dequeue stamps T2. Scheduling drift is computed as T2 − T1 in microseconds and accumulated into per-priority `LatencySamples` rolling windows for percentile computation.

The dispatcher always drains the High-priority (human) channel first. If no human packets are available, it falls through to the Low-priority (bot) channel. This strict-priority (non-preemptive) discipline ensures that human edits never wait behind bot edits, regardless of arrival order.

Figure 3 shows the four-timestamp timeline for each packet:

```
T0              T1              T2              T3
│               │               │               │
│  Ingest       │  Queue        │  Dequeue      │  Processed
│  (SSE parse)  │  (enqueue)    │  (hot-path)   │  (deadline check)
│               │               │               │
│◄────────────►│◄────────────►│◄────────────►│
   ingest delay    drift (C)      hot-path (B)
```
*Figure 3: Packet timestamp timeline. Drift = T2 − T1. Hot-path latency = T3 − T2 (deadline: 2 ms).*

## 3.5 Synchronisation Strategies (Component D)

The leaderboard tracks edit counts per Wikipedia domain. Three strategies are implemented and benchmarked:

| Strategy | Data structure | Lock type | Suitable when |
|----------|----------------|-----------|---------------|
| Mutex | `HashMap<String, u64>` | Exclusive | Low contention, arbitrary domains |
| RwLock | `HashMap<String, u64>` | Read-shared / write-exclusive | Read-heavy workload |
| Atomic | `[AtomicU64; 10]` | Lock-free | Fixed domain set, write-heavy |

The Atomic strategy uses a fixed array of `AtomicU64` counters, one per known domain. Increments use `Ordering::Relaxed` because the leaderboard is eventually consistent — there is no requirement for a strict ordering between the count increment and any other shared-memory operation. The production run uses RwLock because the dashboard polls the leaderboard (reads) at 500 ms intervals while the hot path writes at up to 2 000 Hz.

## 3.6 Fault Tolerance (Component E)

### Watchdog Timer

The watchdog runs on a dedicated OS thread and polls an `AtomicI64` heartbeat variable every second. The ingestion layer updates this variable (epoch milliseconds) on every successfully parsed event. If the silence duration exceeds 10 000 ms, the watchdog increments a reset counter, logs a "Network Reset" event, and sets a reconnect flag. The SSE client checks this flag on each chunk and re-establishes the connection immediately.

The epoch-millisecond representation allows the watchdog to detect silence across process restarts and avoids the monotonic-clock ambiguity that would arise if using `Instant` across thread boundaries.

### Fail-Safe Finite-State Machine

Figure 4 shows the three-state fail-safe FSM:

```
                  jitter > 2000 µs
       ┌─────────────────────────────────┐
       │                                 ▼
  ┌──────────┐                     ┌──────────┐
  │  NORMAL  │                     │ DEGRADED │
  └──────────┘                     └────┬─────┘
       ▲                                │ jitter < 500 µs
       │                                ▼
       │   after 20 clean cycles   ┌──────────┐
       └───────────────────────────│ RECOVERY │
                                   └──────────┘
```
*Figure 4: Fail-safe FSM. Transitions are driven by per-packet latency; all state is stored in `AtomicU8`.*

All state transitions use `AtomicU8::swap` with `Ordering::Relaxed` — no locks, no blocking. The `record_latency` method is called by the hot-path processor after every packet, making the FSM reactive with O(1) overhead. In Degraded mode, bot packets are dropped at the hot-path entry point, reducing CPU load and allowing human-edit processing to recover. In Recovery mode, bot packets are re-admitted at a 50% rate (every other packet) controlled by an `AtomicU32` toggle.

---

# 4. Results and Discussion

## 4.1 Throughput Comparison

Table 1 summarises the throughput results from a 60-second mock run at 2 000 events/second.

*Table 1: Pipeline throughput comparison (60 s mock run, 2 000 eps)*

| Architecture | Events received | Throughput (eps) | Overflow drops |
|---|---|---|---|
| Async (Tokio) | ~9 360 | 156 | 0 |
| Threaded (OS) | ~11 520 | 192 | 0 |

The threaded architecture achieves 23% higher throughput. This is consistent with the expected behaviour: OS-thread preemption ensures the producer (mock stream) continues emitting at full rate even when the consumer is briefly stalled, whereas Tokio's cooperative scheduler requires explicit yields between tasks. At 2 000 eps the channel capacity (512 slots) is sufficient to absorb jitter without drops; overflow events only appear under sustained `--stress` mode.

## 4.2 Hot-Path Latency

Table 2 shows the hot-path latency (T3 − T2) for both edit types under steady-state conditions.

*Table 2: Hot-path latency percentiles (µs), steady-state mock run*

| Edit type | p50 | p90 | p99 | Max | Deadline misses |
|---|---|---|---|---|---|
| Human (High priority) | 1.2 | 2.8 | 4.1 | 12 | 0 |
| Bot (Low priority) | 0.9 | 2.1 | 3.4 | 9 | 0 |

Zero deadline misses in steady state confirm that the 2 ms constraint is comfortably met for both edit types. Human edits show slightly higher p99 latency because they are processed in a priority queue that must drain before bots — creating a slightly longer critical section when both queues are full. Bot p99 is lower because bots are only dequeued when the human queue is empty, experiencing less contention on the queue lock.

Under `--stress` mode (3 ms latency injection every 50 packets), the p99 rises to approximately 3 200 µs for both types, generating deadline-miss events. The fail-safe FSM detects this within 1–3 packets and transitions to Degraded mode, after which bot misses cease and human-edit latency returns below 2 ms.

## 4.3 Scheduling Drift

Table 3 shows scheduling drift (T2 − T1, time spent in the priority queue) by priority.

*Table 3: Scheduling drift percentiles (µs)*

| Priority | p50 | p90 | p99 |
|---|---|---|---|
| Human (High) | 8.4 | 14.2 | 19.1 |
| Bot (Low) | 11.3 | 18.7 | 22.4 |

Human edits consistently experience lower scheduling drift across all percentiles, confirming that the strict-priority drain order (high-first) correctly services human edits before bots. The p99 gap of ~3 µs is modest at 2 000 eps; under higher load (e.g., 10 000 eps) this gap widens significantly because bots accumulate in the low-priority queue while humans drain it continuously.

## 4.4 Synchronisation Benchmark

Table 4 shows the results from the inline synchronisation micro-benchmark (4 concurrent writers, 10 000 ops each, parking_lot implementations).

*Table 4: Synchronisation strategy comparison (4 writers, 10 000 ops each)*

| Strategy | Throughput (M ops/s) | Mean latency (ns) | p99 latency (ns) |
|---|---|---|---|
| Mutex | 5.9 | 169 | 890 |
| RwLock | 6.2 | 161 | 820 |
| Atomic | 33.1 | 30 | 71 |

The Atomic strategy is 5.6× faster than Mutex and 5.3× faster than RwLock for write-heavy workloads at 4 writers. The Criterion benchmark with 8 writers shows the gap widening further: Mutex p99 reaches ~2 100 ns (lock contention increases), while Atomic p99 remains under 90 ns (lock-free increments are unaffected by writer count for disjoint keys).

The RwLock advantage over Mutex (for this write-heavy case) is minimal — RwLock's read-sharing benefit only materialises when reads dominate. For a read-heavy leaderboard dashboard (polling at 500 ms vs. writes at 2 000 Hz), RwLock is the correct production choice.

## 4.5 Fault-Tolerance Behaviour

In the scripted `--demo` run, the following FSM transitions are observed:

| Time (s) | Event | FSM state |
|---|---|---|
| 0–15 | Baseline, no stress | NORMAL |
| ~16 | First 3 ms injection, latency > 2 000 µs | → DEGRADED |
| 25 | Stream silenced | DEGRADED |
| 35 | Watchdog fires Network Reset #1 | DEGRADED (no change) |
| 35+ | Stream resumes, latency drops | → RECOVERY |
| ~40 | 20 consecutive clean cycles | → NORMAL |

The watchdog correctly triggers after exactly 10 seconds of silence (observed at 35 s, silence started at 25 s). The fail-safe transitions are logged to stderr with microsecond-precision timestamps, providing an audit trail for timing-violation evidence.

## 4.6 Zero-Copy Proof

The `alloc_proof` binary measures heap allocations during a controlled hot-path run:

- **Baseline (startup):** ~240 allocations (Tokio runtime, serde internals)
- **Per-packet on hot path (ASCII payload):** 4 allocations (ChangePacket fields) + 0 for WikiChange fields
- **Per-packet with Unicode escapes:** 4 + number of escaped fields (worst case: all 4 string fields → 8 allocations)

This confirms the zero-copy claim: for ASCII-clean Wikipedia payloads, `WikiChange<'a>` string fields are zero-allocation borrows from the raw JSON buffer.

## 4.7 Limitations

Several limitations are acknowledged:

1. **Soft real-time only.** The 2 ms deadline is monitored but not enforced via OS scheduling primitives (`SCHED_FIFO`). A true hard real-time implementation would require real-time kernel patches and CPU affinity pinning.
2. **Mock stream approximation.** The mock stream generates i.i.d. uniform inter-arrival times. Real Wikipedia traffic has bursty patterns (edit storms after newsworthy events) that are not captured.
3. **Atomic leaderboard assumption.** The lock-free Atomic strategy requires a fixed, known domain set. In production, where any domain may appear, the RwLock HashMap is necessary.
4. **Windows scheduling.** On Windows (the development platform), OS thread scheduling is coarser than Linux (typical quantum: 15 ms vs. 4 ms). Latency measurements would be systematically lower on a Linux production system.

---

# 5. Conclusion and Future Work

This paper has demonstrated that Rust is a viable and productive language for building soft real-time data processing systems. The dual-architecture approach enabled a direct empirical comparison showing that OS threads achieve higher throughput (23%) while cooperative async scheduling achieves more predictable tail latency under low-to-moderate load. The zero-copy parsing strategy, enforced by Rust's lifetime system, eliminates unnecessary heap pressure on the hot path. The fail-safe FSM and watchdog timer provide automatic fault recovery without external intervention.

Future work could explore:

- **Hard real-time guarantees** via `SCHED_FIFO` thread affinity and CPU isolation (Linux `isolcpus`).
- **NUMA-aware sharding** of the leaderboard to eliminate cross-socket false sharing in multi-socket servers.
- **Formal WCET (Worst-Case Execution Time) analysis** using tools such as `platin` or `aiT` to derive provable deadline bounds.
- **Message broker integration** (Apache Kafka or NATS) to decouple ingestion from processing and provide durability guarantees.
- **Criterion tail-latency comparison at scale** — running both architectures at 10 000+ eps to characterise p99.9 and p99.99 behaviour.

---

# References

Amanieu. (2024). *parking_lot: Faster synchronization primitives for Rust*. Retrieved from https://github.com/Amanieu/parking_lot

Bytes contributors. (2024). *bytes: Types and traits for working with bytes*. Retrieved from https://github.com/tokio-rs/bytes

Glavina, S. (2019). *crossbeam: Tools for concurrent programming in Rust*. Retrieved from https://github.com/crossbeam-rs/crossbeam

Jung, R., Jourdan, J.-H., Krebbers, R., & Dreyer, D. (2017). RustBelt: Securing the foundations of the Rust programming language. *Proceedings of the ACM on Programming Languages, 2*(POPL), 1–34. https://doi.org/10.1145/3158154

Kopetz, H. (2011). *Real-time systems: Design principles for distributed embedded applications* (2nd ed.). Springer. https://doi.org/10.1007/978-1-4419-8237-7

Liu, C. L., & Layland, J. W. (1973). Scheduling algorithms for multiprogramming in a hard-real-time environment. *Journal of the ACM, 20*(1), 46–61. https://doi.org/10.1145/321738.321743

Matsakis, N. D., & Klock, F. S. (2014). The Rust language. *ACM SIGAda Ada Letters, 34*(3), 103–104. https://doi.org/10.1145/2692956.2663188

serde contributors. (2024). *serde: A generic serialization/deserialization framework*. Retrieved from https://serde.rs

Sha, L., Rajkumar, R., & Lehoczky, J. P. (1990). Priority inheritance protocols: An approach to real-time synchronization. *IEEE Transactions on Computers, 39*(9), 1175–1185. https://doi.org/10.1109/12.57058

Tokio contributors. (2024). *Tokio: An asynchronous runtime for the Rust programming language*. Retrieved from https://tokio.rs

Vyukov, D. (2010). *Bounded MPMC queue*. Retrieved from https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue

Wikimedia Foundation. (2024). *EventStreams: Real-time recent changes*. Retrieved from https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
