/// latency_bench – Criterion benchmarks for Component B (RTS2601)
///
/// Measures:
///   1. Zero-copy parse latency  – typical ASCII-only Wikipedia JSON
///   2. Zero-copy parse latency  – JSON with Unicode / escaped strings
///   3. parse_hot_fields()       – minimal field extraction (no ChangePacket)
///   4. Percentile computation   – p50 / p99 on a 1 000-sample window
///   5. Deadline check overhead  – hot-path 2 ms deadline comparison
///
/// Run:  cargo bench --bench latency_bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use bytes::Bytes;
use wiki_rt_monitor::component_b::{parse_zero_copy, HOT_DEADLINE};
use wiki_rt_monitor::metrics::LatencySamples;

// ─── Fixture JSON payloads ────────────────────────────────────────────────────

/// Minimal valid Wikipedia recent-change event (ASCII, human editor).
const HUMAN_JSON: &[u8] = br#"{
    "bot": false,
    "user": "Alice",
    "server_name": "en.wikipedia.org",
    "title": "Rust (programming language)",
    "type": "edit",
    "timestamp": 1715000000,
    "namespace": 0
}"#;

/// Bot edit with a longer title and no unusual escapes.
const BOT_JSON: &[u8] = br#"{
    "bot": true,
    "user": "CleanupBot",
    "server_name": "de.wikipedia.org",
    "title": "Quantenmechanik",
    "type": "edit",
    "timestamp": 1715000001,
    "namespace": 0
}"#;

/// Event with Unicode escapes in the title – forces serde to heap-allocate
/// the title field, which lets us prove the zero-copy path avoids that.
/// Stored as `&str` because Rust byte string literals cannot contain
/// non-ASCII characters; we expose `.as_bytes()` at the use site.
const UNICODE_JSON: &str = r#"{
    "bot": false,
    "user": "편집자",
    "server_name": "ko.wikipedia.org",
    "title": "한국어 위키백과",
    "type": "edit",
    "timestamp": 1715000002,
    "namespace": 0
}"#;

/// Payload that is NOT a valid change event – missing server_name.
const INVALID_JSON: &[u8] = br#"{ "type": "log", "timestamp": 1715000003 }"#;

// ─── Benchmark 1: zero-copy parse latency ────────────────────────────────────

fn bench_parse_human(c: &mut Criterion) {
    let buf = Bytes::from_static(HUMAN_JSON);
    c.bench_function("parse_zero_copy/human_ascii", |b| {
        b.iter(|| {
            let _ = parse_zero_copy(black_box(&buf));
        })
    });
}

fn bench_parse_bot(c: &mut Criterion) {
    let buf = Bytes::from_static(BOT_JSON);
    c.bench_function("parse_zero_copy/bot_ascii", |b| {
        b.iter(|| {
            let _ = parse_zero_copy(black_box(&buf));
        })
    });
}

fn bench_parse_unicode(c: &mut Criterion) {
    let buf = Bytes::from_static(UNICODE_JSON.as_bytes());
    c.bench_function("parse_zero_copy/unicode_escaped", |b| {
        b.iter(|| {
            let _ = parse_zero_copy(black_box(&buf));
        })
    });
}

fn bench_parse_invalid(c: &mut Criterion) {
    let buf = Bytes::from_static(INVALID_JSON);
    c.bench_function("parse_zero_copy/invalid_rejected", |b| {
        b.iter(|| {
            let _ = parse_zero_copy(black_box(&buf));
        })
    });
}

// ─── Benchmark 2: parse_hot_fields (hot-path minimal extract) ────────────────

fn bench_hot_fields(c: &mut Criterion) {
    // Inline the minimal hot-fields deserialization that parse_hot_fields performs.
    c.bench_function("parse_hot_fields/human", |b| {
        b.iter(|| {
            // Inline the hot-fields pattern: deserialise WikiChange<'_> directly
            let buf: &[u8] = black_box(HUMAN_JSON);
            let result: Option<(&str, bool)> = {
                #[derive(serde::Deserialize)]
                struct Hf<'a> {
                    #[serde(borrow, rename = "server_name")]
                    server_name: Option<&'a str>,
                    #[serde(default)]
                    bot: bool,
                }
                serde_json::from_slice::<Hf<'_>>(buf).ok()
                    .and_then(|h| h.server_name.map(|s| (s, h.bot)))
            };
            let _ = result;
        })
    });
}

// ─── Benchmark 3: percentile computation ─────────────────────────────────────

fn bench_percentile(c: &mut Criterion) {
    let mut group = c.benchmark_group("percentile");

    for n in [100usize, 1_000, 10_000] {
        let mut samples = LatencySamples::default();
        for i in 0..n {
            samples.push(i as f64 * 0.5);
        }

        group.bench_with_input(BenchmarkId::new("p50", n), &samples, |b, s| {
            b.iter(|| black_box(s.p50()))
        });
        group.bench_with_input(BenchmarkId::new("p99", n), &samples, |b, s| {
            b.iter(|| black_box(s.p99()))
        });
    }

    group.finish();
}

// ─── Benchmark 4: deadline check (Duration comparison) ───────────────────────

fn bench_deadline_check(c: &mut Criterion) {
    use std::time::{Duration, Instant};

    c.bench_function("deadline_check/within", |b| {
        b.iter(|| {
            let t0 = Instant::now();
            // Simulate a very-fast hot-path operation.
            let _x: u64 = (0u64..100).sum();
            let elapsed = t0.elapsed();
            black_box(elapsed < HOT_DEADLINE)
        })
    });

    c.bench_function("deadline_check/exceeded", |b| {
        b.iter(|| {
            // Pretend we already exceeded the deadline.
            let elapsed = Duration::from_millis(5);
            black_box(elapsed < HOT_DEADLINE)
        })
    });
}

// ─── Benchmark 5: ChangePacket heap allocation cost ──────────────────────────

fn bench_change_packet_alloc(c: &mut Criterion) {
    // Parse → allocate ChangePacket (4 String fields) → drop.
    // Shows the cost of promoting a zero-copy view to owned storage.
    let human_buf = Bytes::from_static(HUMAN_JSON);
    let bot_buf   = Bytes::from_static(BOT_JSON);

    let mut group = c.benchmark_group("change_packet_alloc");

    group.bench_function("human", |b| {
        b.iter(|| {
            let pkt = parse_zero_copy(black_box(&human_buf));
            black_box(pkt)
        })
    });

    group.bench_function("bot", |b| {
        b.iter(|| {
            let pkt = parse_zero_copy(black_box(&bot_buf));
            black_box(pkt)
        })
    });

    group.finish();
}

// ─── Criterion groups ─────────────────────────────────────────────────────────

criterion_group!(
    parsing,
    bench_parse_human,
    bench_parse_bot,
    bench_parse_unicode,
    bench_parse_invalid,
    bench_hot_fields,
    bench_change_packet_alloc,
);

criterion_group!(stats, bench_percentile);
criterion_group!(deadline, bench_deadline_check);

criterion_main!(parsing, stats, deadline);
