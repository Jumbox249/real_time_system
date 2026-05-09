/// Mock Wikipedia SSE Stream
///
/// Generates synthetic but realistic Wikipedia change events at a
/// configurable rate.  Used for:
///   • Offline development / testing without network access
///   • Deterministic benchmark runs
///   • Simulating high-velocity spikes to test tail latency
///
/// The mock produces a realistic mix: ~20% human edits, ~80% bot edits,
/// spread across the top Wikipedia domains.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

/// Well-known Wikipedia server domains for realistic simulation.
const DOMAINS: &[&str] = &[
    "en.wikipedia.org",
    "de.wikipedia.org",
    "fr.wikipedia.org",
    "es.wikipedia.org",
    "ru.wikipedia.org",
    "ja.wikipedia.org",
    "zh.wikipedia.org",
    "pt.wikipedia.org",
    "it.wikipedia.org",
    "nl.wikipedia.org",
];

const HUMAN_USERS:  &[&str] = &["Alice", "Bob_wiki", "Carol", "Dave42", "Eve_Editor"];
const BOT_USERS:    &[&str] = &["ClueBot_NG", "AntiVandal", "PageMover_bot", "RefBot"];

/// Run the mock stream indefinitely, pushing synthetic JSON events.
/// `events_per_second` controls the simulated load.
/// `silence_window` (when set) skips emission during `[start, end)` measured
/// from the start of this call — used by the `--stress` demo to fire the
/// watchdog reset path.
pub async fn run_mock_stream(
    tx:                 Sender<Bytes>,
    events_per_second:  u64,
    stop:               Arc<AtomicBool>,
    silence_window:     Option<(Duration, Duration)>,
    program_start:      Option<Instant>,
) {
    let interval = Duration::from_micros(1_000_000 / events_per_second.max(1));
    let mut rng  = StdRng::from_entropy();
    let mut seq  = 0u64;
    let start    = program_start.unwrap_or_else(Instant::now);

    while !stop.load(Ordering::Relaxed) {
        let is_bot      = rng.gen_bool(0.80);
        let domain      = DOMAINS[rng.gen_range(0..DOMAINS.len())];
        let user        = if is_bot {
            BOT_USERS[rng.gen_range(0..BOT_USERS.len())]
        } else {
            HUMAN_USERS[rng.gen_range(0..HUMAN_USERS.len())]
        };
        let namespace   = if rng.gen_bool(0.7) { 0 } else { rng.gen_range(1..=14) };
        let ts          = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let json = format!(
            r#"{{"user":"{user}","bot":{is_bot},"server_name":"{domain}","title":"Page_{seq}","type":"edit","namespace":{namespace},"timestamp":{ts}}}"#,
        );

        seq += 1;
        let in_silence = silence_window
            .map_or(false, |(s, e)| { let now = start.elapsed(); now >= s && now < e });
        if !in_silence {
            let _ = tx.try_send(Bytes::from(json.into_bytes()));
        }
        sleep(interval).await;
    }
}

/// Blocking version for use in `std::thread` (threaded pipeline).
pub fn run_mock_stream_blocking(
    tx:                std::sync::mpsc::SyncSender<Bytes>,
    events_per_second: u64,
    stop:              Arc<AtomicBool>,
    silence_window:    Option<(Duration, Duration)>,
    program_start:     Option<Instant>,
) {
    let interval = Duration::from_micros(1_000_000 / events_per_second.max(1));
    let mut rng  = rand::thread_rng();
    let mut seq  = 0u64;
    let start    = program_start.unwrap_or_else(Instant::now);

    while !stop.load(Ordering::Relaxed) {
        let is_bot  = rng.gen_bool(0.80);
        let domain  = DOMAINS[rng.gen_range(0..DOMAINS.len())];
        let user    = if is_bot { BOT_USERS[rng.gen_range(0..BOT_USERS.len())] }
                      else       { HUMAN_USERS[rng.gen_range(0..HUMAN_USERS.len())] };
        let ts      = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let json = format!(
            r#"{{"user":"{user}","bot":{is_bot},"server_name":"{domain}","title":"Page_{seq}","type":"edit","namespace":0,"timestamp":{ts}}}"#,
        );
        seq += 1;
        let in_silence = silence_window
            .map_or(false, |(s, e)| { let now = start.elapsed(); now >= s && now < e });
        if !in_silence {
            let _ = tx.try_send(Bytes::from(json.into_bytes()));
        }
        std::thread::sleep(interval);
    }
}
