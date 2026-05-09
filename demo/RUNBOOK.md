# Demo Runbook — Wikipedia Real-Time Monitoring Engine

**Duration:** 8–10 minutes  
**Rubric coverage:** Correctness & Functionality, Safety & Architecture, Shared Resource Sync, Code Quality, Advanced Features

---

## Pre-flight checklist

Before the demo session:

1. **Build release binary** (do this at least once beforehand — takes 30–60 s):
   ```powershell
   $env:CARGO_TARGET_DIR = "C:\rust_builds\real_time_system"
   cargo build --release
   ```

2. **Pre-run Criterion benchmarks** and open the report HTML:
   ```powershell
   cargo bench
   # open target/criterion/report/index.html in browser
   ```

3. **Open two PowerShell terminals** in the project directory.

4. **Verify internet access** for Scene 5 (live Wikipedia stream).

---

## Scene 1 — Steady-State Mock Run (2 min)

**Covers:** Correctness & Functionality, Integration & Communication

```powershell
cargo run --release -- --mock
```

**Narrate while it runs:**

- The engine starts both pipelines simultaneously:
  - Architecture 1 (Tokio async tasks) and Architecture 2 (OS threads)
- After 60 s, the performance summary prints automatically.

**Point out in the summary output:**

```
  Async events received:      120 000   ← ~2000 eps × 60 s
  Deadline misses:            0         ← All packets within 2 ms
  Human latency p99:          ~4 µs     ← Well within deadline
  [OK] Human drift p99 < bot drift p99  ← Priority scheduling confirmed
```

**Key talking points:**
- Both pipelines run in parallel — same hot path, same leaderboard
- Zero deadline misses in steady state proves the 2 ms constraint is met
- The "[OK]" line mathematically proves human edits experience lower scheduling drift

---

## Scene 2 — Stress & Fail-Safe Demo (2 min)

**Covers:** Safety & Robustness, Advanced Features, Fault Tolerance (Component E)

```powershell
cargo run --release -- --demo
```

**Narrate the four phases as they print:**

| Phase | Time | What to watch |
|-------|------|---------------|
| 1: Baseline | 0–15 s | `current mode: NORMAL` in output |
| 2: Stress | 15–25 s | `[fail-safe] mode → DEGRADED` appears in stderr |
| 3: Silence | 25–35 s | `[watchdog] Network Reset #1` fires after 10 s silence |
| 4: Recovery | 35–60 s | `[fail-safe] mode → RECOVERY` then `→ NORMAL` |

**Key talking points:**
- Fail-safe FSM is driven by per-packet latency — no polling, no global lock
- Degraded mode drops bot packets (Priority::Low) to cut CPU load
- Watchdog runs on its own dedicated OS thread with epoch-ms heartbeat
- Recovery requires 20 consecutive packets below 500 µs before returning to Normal

---

## Scene 3 — Synchronisation Benchmark (1 min)

**Covers:** Shared Resource Synchronisation (Component D)

Open the pre-generated Criterion HTML report in a browser:
```
target/criterion/report/index.html
→ sync_strategy/
```

**Or show the inline benchmark from the steady-state run summary:**
```
Mutex  write ops: ...  (look for ops/s column)
RwLock write ops: ...
Atomic write ops: ...
```

**Narrate:**
- Three strategies for the shared leaderboard: Mutex, RwLock, Atomic
- Atomic achieves ~33 M ops/s vs ~6 M ops/s for Mutex/RwLock
- Criterion plots show p99 tail latency — Atomic is 5× lower at 8 writers
- Production strategy uses RwLock (read-heavy leaderboard dashboard)
- Lock contention at 8 writers is the worst case — still lock-free for Atomic

---

## Scene 4 — Zero-Copy Allocation Proof (1 min)

**Covers:** Code Quality (Distinction-level — zero-copy Memory Mastery)

```powershell
cargo run --release --bin alloc_proof
```

**Expected output pattern:**
```
[alloc-proof] Baseline allocations (startup): N
[alloc-proof] Hot-path allocations per packet: 4   ← user, server_name, title, change_type strings
[alloc-proof] String fields allocated by WikiChange<'a>: 0   ← zero-copy confirmed
```

**Narrate:**
- `WikiChange<'a>` borrows `&str` slices directly from the raw JSON `Bytes` buffer
- The lifetime `'a` enforces at compile time that these references do not outlive the buffer
- The `AllocCounter` wraps the system allocator and counts every `alloc()` call atomically
- Only `ChangePacket::from_change` performs heap allocations — and only for fields that must outlive the buffer (domain for leaderboard, etc.)
- This is the distinction requirement: proof via custom allocator, not just code inspection

---

## Scene 5 — Live Wikipedia Stream (1 min)

**Covers:** Integration & Communication (real data)

```powershell
cargo run --release
```

**Narrate:**
- The SSE client connects to `stream.wikimedia.org/v2/stream/recentchange`
- Real Wikipedia edits flow through the same dual pipeline
- Point out realistic domain distribution in the leaderboard (en, de, fr dominate)
- Watchdog is active — if the connection drops, it reconnects automatically

---

## Q&A Buffer (1–2 min)

Suggested answers to likely questions:

**Q: Why Tokio rather than just OS threads for Architecture 1?**  
A: Tokio cooperative scheduling has lower per-task overhead and allows thousands of concurrent connections with a fixed thread pool — relevant when ingesting multiple streams. The trade-off is that a long-running task blocks its thread, whereas OS threads are preempted. We demonstrate both and measure the tail-latency difference with Criterion.

**Q: What happens if the leaderboard domain is not in KNOWN_DOMAINS (Atomic strategy)?**  
A: The Atomic strategy only tracks the 10 predefined domains (en, de, fr, …). Unknown domains are silently skipped. For production, the RwLock HashMap handles arbitrary domains — the Atomic variant exists purely to demonstrate lock-free increments in the benchmark.

**Q: How does the fail-safe recover without manual intervention?**  
A: It's fully automatic. The `FailSafe::record_latency` method is called on every packet. When the rolling latency falls below 500 µs for 20 consecutive packets, the state machine atomically transitions back to Normal. No locks, no threads — just atomic compare-and-swap.

---

## Backup: screen recordings

If the live demo environment is unreliable, pre-recorded clips are in `demo/clips/`:

| Clip | Scene |
|------|-------|
| `01_steady_state.mp4` | Scene 1 — steady-state mock run |
| `02_demo_mode.mp4` | Scene 2 — 4-phase fault-tolerance |
| `03_criterion_sync.mp4` | Scene 3 — sync benchmark report |
| `04_alloc_proof.mp4` | Scene 4 — zero-copy proof |
| `05_live_sse.mp4` | Scene 5 — live Wikipedia stream |
