# logs/

Runtime artefacts produced by the `wiki_rt_monitor` binary.

---

## overflow_events.csv

**Produced by:** any `cargo run --release` invocation (when overflows occur)  
**Rubric evidence:** Component A backpressure — "If the channel reaches capacity, the system must drop the oldest packet and log a high-precision timestamped Overflow Event"

| Column | Type | Description |
|--------|------|-------------|
| `total_drops` | integer | Running cumulative overflow count at the time of this event |
| `domain` | string | Wikipedia domain of the dropped packet (e.g. `en.wikipedia.org`) |
| `priority` | string | `High` (human edit) or `Low` (bot edit) |

**What to look for:** In steady-state mock runs at 200 eps this file will be empty (no overflows). Run `--stress` or increase `MOCK_EPS` in `main.rs` to saturate the 512-slot channel and generate drops.

---

## deadline_misses.csv

**Produced by:** `cargo run --release -- --stress` or `--demo`  
**Rubric evidence:** Component B — "A strict 2 ms completion deadline applies to each packet from the moment it leaves the ingestion channel until processing is finalized"

| Column | Type | Description |
|--------|------|-------------|
| `latency_us` | float | Hot-path latency in microseconds (T3 − T2) |
| `domain` | string | Wikipedia domain of the offending packet |
| `priority` | string | `High` or `Low` |

**What to look for:**  
- **Steady-state:** file absent or empty — zero misses.  
- **Stress mode:** entries with `latency_us` around 3000–5000 µs, mostly `Low` priority (bots processed after humans, more susceptible to injected jitter).  
- **Recovery phase:** miss rate drops back toward zero once the fail-safe drops bot traffic.

---

## run_*.txt (generated manually)

Capture full stdout/stderr of each run for the submission:

```bash
cargo run --release -- --mock  2>&1 | tee logs/run_mock_steady.txt
cargo run --release -- --stress 2>&1 | tee logs/run_stress.txt
cargo run --release -- --demo   2>&1 | tee logs/run_demo.txt
cargo run --release             2>&1 | tee logs/run_live.txt
cargo run --release --bin compare_pipelines -- --mock 2>&1 | tee logs/compare_pipelines.txt
cargo run --release --bin alloc_proof 2>&1 | tee logs/alloc_proof.txt
```

---

## Analysis

```bash
python scripts/analyze_logs.py logs/
```

Prints p50/p90/p99 latency percentiles and miss counts by priority from the CSV files above.
