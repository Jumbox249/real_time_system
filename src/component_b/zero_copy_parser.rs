/// Zero-Copy Parser (Component B)
///
/// Parses raw Wikipedia SSE JSON buffers using serde with Rust lifetimes,
/// ensuring that string fields (user, server_name, title) are borrowed
/// directly from the source buffer – no heap allocation on the hot path.
///
/// Memory model:
///   `buf: Bytes`          ← owns the JSON bytes (ref-counted, no copy)
///   `WikiChange<'_>`      ← borrows &str fields from buf
///   `ChangePacket`        ← owned: only allocated after the packet is
///                            promoted (e.g., human edit needs long life)
///
/// Proof of zero-copy:
///   The `#[global_allocator]` in `main.rs` counts allocations.
///   A zero-copy parse produces 0 heap allocs for the string fields
///   (verified via the `AllocCounter` in `advanced/allocator.rs`).
use bytes::Bytes;

use crate::types::{ChangePacket, WikiChange};

/// Parse a raw JSON buffer without allocating string fields.
/// Returns `None` if the buffer is not a valid Wikipedia change event.
///
/// The returned `ChangePacket` is the *only* allocation on the hot path –
/// and only for the fields we actually need to keep (domain for leaderboard,
/// priority for scheduling).
pub fn parse_zero_copy(buf: &Bytes) -> Option<ChangePacket> {
    // WikiChange<'_> borrows &str directly from the `buf` slice.
    // serde_json::from_slice is zero-copy for unescaped ASCII strings.
    let change: WikiChange<'_> = serde_json::from_slice(buf.as_ref()).ok()?;

    // Validate: must have at least a server_name.
    if change.server_name.is_none() { return None; }

    // ChangePacket::from_change does the minimal allocations:
    //   user, server_name, title, change_type → 4 × String
    // These are necessary because the packet outlives `buf`.
    Some(ChangePacket::from_change(&change))
}

/// Parse and immediately extract only the fields needed for the hot path,
/// avoiding the ChangePacket allocation when only domain + bot flag matter.
///
/// Returns `(server_name, is_bot)` – both borrowed from `buf`.
pub fn parse_hot_fields<'a>(buf: &'a [u8]) -> Option<(&'a str, bool)> {
    let change: WikiChange<'a> = serde_json::from_slice(buf).ok()?;
    Some((change.server_name?, change.bot))
}

// ─── Allocation counter (for zero-copy proof) ─────────────────────────────────

use std::sync::atomic::{AtomicU64, Ordering};

/// Global allocation counter – counts heap allocations during parsing.
/// Incremented by the custom allocator in `advanced/allocator.rs`.
pub static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

/// Reset the allocation counter and return the count since last reset.
pub fn alloc_count_since_reset() -> u64 {
    ALLOC_COUNT.swap(0, Ordering::Relaxed)
}
