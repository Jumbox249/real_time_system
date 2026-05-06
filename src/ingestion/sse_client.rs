/// Wikipedia SSE Client
///
/// Connects to the Wikipedia Recent Changes firehose
/// (https://stream.wikimedia.org/v2/stream/recentchange)
/// and yields raw JSON strings via an async channel.
///
/// The SSE wire format is:
///   event: message\n
///   data: { ... JSON ... }\n
///   \n
///
/// We accumulate bytes from the HTTP chunked response, split on `\n\n`,
/// and strip the `data: ` prefix.  This is done without copying the raw
/// bytes twice by working with `Bytes` slices directly.
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc::Sender;

pub const WIKI_SSE_URL: &str =
    "https://stream.wikimedia.org/v2/stream/recentchange";

/// Connect to the SSE stream and push raw JSON strings into `tx`.
/// Updates `last_event_ms` with the current epoch-millisecond on each event.
/// Returns when `stop` is set or the connection drops (caller should retry).
pub async fn run_sse_client(
    tx:            Sender<Bytes>,
    last_event_ms: Arc<AtomicI64>,
    stop:          Arc<AtomicBool>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("failed to build HTTP client");

    loop {
        if stop.load(Ordering::Relaxed) { return; }

        eprintln!("[sse] connecting to {WIKI_SSE_URL}");

        let response = match client
            .get(WIKI_SSE_URL)
            .header("Accept", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .send()
            .await
        {
            Ok(r)  => r,
            Err(e) => {
                eprintln!("[sse] connection error: {e} – retrying in 3s");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        eprintln!("[sse] connected – streaming…");

        let mut byte_stream = response.bytes_stream();
        let mut buf          = Vec::<u8>::with_capacity(4096);

        while let Some(chunk) = byte_stream.next().await {
            if stop.load(Ordering::Relaxed) { return; }

            let chunk = match chunk {
                Ok(c)  => c,
                Err(e) => { eprintln!("[sse] stream error: {e}"); break; }
            };

            buf.extend_from_slice(&chunk);

            // Process all complete SSE blocks separated by double newlines.
            while let Some(pos) = find_double_newline(&buf) {
                let block = buf.drain(..pos + 2).collect::<Vec<u8>>();
                buf.drain(..0); // drain the extra \n

                if let Some(json) = extract_data_line(&block) {
                    // Update watchdog timestamp.
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    last_event_ms.store(now_ms, Ordering::Relaxed);

                    // Non-blocking send – drop if channel is full
                    // (backpressure handled upstream).
                    let _ = tx.try_send(Bytes::copy_from_slice(json));
                }
            }
        }

        if !stop.load(Ordering::Relaxed) {
            eprintln!("[sse] stream ended – reconnecting in 2s");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

/// Find the position of the first `\n\n` in a byte slice.
fn find_double_newline(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n")
}

/// Extract the raw JSON from an SSE block.
/// Returns a slice into `block` pointing past the `data: ` prefix.
fn extract_data_line(block: &[u8]) -> Option<&[u8]> {
    for line in block.split(|&b| b == b'\n') {
        let line = line.strip_prefix(b"data: ").or_else(|| line.strip_prefix(b"data:"));
        if let Some(json) = line {
            if !json.is_empty() && json[0] == b'{' {
                return Some(json);
            }
        }
    }
    None
}
