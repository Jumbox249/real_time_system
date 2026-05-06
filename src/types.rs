/// Core type definitions for the Wikipedia Real-Time Monitoring System.
///
/// Design note on zero-copy parsing
/// ──────────────────────────────────
/// `WikiChange<'a>` borrows directly from the raw JSON byte buffer.
/// The lifetime `'a` ties each string field to the buffer it came from,
/// so no heap allocation is required for user / server_name / title during
/// the hot processing path.
use std::time::Instant;
use serde::Deserialize;

// ─── Wikipedia change event (zero-copy) ──────────────────────────────────────

/// Raw Wikipedia Recent-Changes SSE payload, parsed zero-copy.
///
/// Fields borrow from the original JSON buffer via the `'a` lifetime.
/// serde will only allocate if the source string contains escape sequences
/// (e.g. `A`); otherwise `&'a str` points directly into the buffer.
#[derive(Debug, Deserialize)]
pub struct WikiChange<'a> {
    /// Username of the editor (human or bot).
    #[serde(borrow)]
    pub user: Option<&'a str>,

    /// True when the edit was made by a registered bot account.
    #[serde(default)]
    pub bot: bool,

    /// Domain of the wiki (e.g. "en.wikipedia.org").
    #[serde(borrow, rename = "server_name")]
    pub server_name: Option<&'a str>,

    /// Page title that was modified.
    #[serde(borrow)]
    pub title: Option<&'a str>,

    /// Change type: "edit", "new", "log", "categorize", etc.
    #[serde(borrow, rename = "type")]
    pub change_type: Option<&'a str>,

    /// Unix timestamp of the change.
    pub timestamp: Option<i64>,

    /// Wiki namespace (0 = article space).
    pub namespace: Option<i64>,
}

// ─── Priority ────────────────────────────────────────────────────────────────

/// Processing priority assigned to each incoming change event.
/// Human edits always outrank bot edits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    Low  = 0, // bot edit
    High = 1, // human edit
}

impl Priority {
    pub fn from_bot_flag(is_bot: bool) -> Self {
        if is_bot { Priority::Low } else { Priority::High }
    }
    pub fn label(self) -> &'static str {
        match self { Priority::High => "human", Priority::Low => "bot" }
    }
}

// ─── ChangePacket ────────────────────────────────────────────────────────────

/// Owned version of a change event, produced after the zero-copy parsing stage.
/// Owns heap-allocated strings so it can live beyond the raw JSON buffer.
#[derive(Debug, Clone)]
pub struct ChangePacket {
    pub user:           String,
    pub server_name:    String,
    pub title:          String,
    pub change_type:    String,
    pub priority:       Priority,
    pub wiki_timestamp: i64,

    /// T0 – the instant this packet was created from the raw SSE event.
    pub t0: Instant,
    /// T1 – the instant this packet entered the priority queue.
    pub t1: Option<Instant>,
    /// T2 – the instant this packet was dequeued for processing.
    pub t2: Option<Instant>,
    /// T3 – the instant processing finished.
    pub t3: Option<Instant>,
}

impl ChangePacket {
    /// Construct from a borrowed `WikiChange` and stamp T0.
    pub fn from_change(c: &WikiChange<'_>) -> Self {
        let priority = Priority::from_bot_flag(c.bot);
        Self {
            user:           c.user.unwrap_or("unknown").to_owned(),
            server_name:    c.server_name.unwrap_or("unknown").to_owned(),
            title:          c.title.unwrap_or("").to_owned(),
            change_type:    c.change_type.unwrap_or("unknown").to_owned(),
            priority,
            wiki_timestamp: c.timestamp.unwrap_or(0),
            t0: Instant::now(),
            t1: None, t2: None, t3: None,
        }
    }
}

// ─── OverflowEvent ───────────────────────────────────────────────────────────

/// Logged when the bounded ingestion channel is full and the oldest packet
/// must be dropped to make room (backpressure / fail-fast strategy).
#[derive(Debug, Clone)]
pub struct OverflowEvent {
    /// High-precision system timestamp when the drop occurred.
    pub dropped_at:  Instant,
    /// The server domain that produced the dropped packet.
    pub domain:      String,
    /// Priority of the dropped packet.
    pub priority:    Priority,
    /// How many overflow events have occurred in total (running count).
    pub total_drops: u64,
}

// ─── System mode ─────────────────────────────────────────────────────────────

/// Operating mode of the monitoring engine.
///
/// Transitions:
///   Normal → Degraded   when jitter exceeds `JITTER_THRESHOLD_US`
///   Degraded → Recovery when jitter falls below `RECOVERY_THRESHOLD_US`
///   Recovery → Normal   after `RECOVERY_WINDOW` consecutive clean cycles
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemMode {
    /// All events processed normally.
    Normal,
    /// Jitter threshold exceeded; bot edits dropped to reduce load.
    Degraded,
    /// Jitter recovering; bot edits re-admitted at reduced rate.
    Recovery,
}

impl std::fmt::Display for SystemMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemMode::Normal   => write!(f, "NORMAL"),
            SystemMode::Degraded => write!(f, "DEGRADED"),
            SystemMode::Recovery => write!(f, "RECOVERY"),
        }
    }
}

/// Jitter (µs) at which the system enters Degraded mode.
pub const JITTER_THRESHOLD_US:  f64 = 2_000.0; // 2 ms
/// Jitter (µs) at which the system re-enters Recovery from Degraded.
pub const RECOVERY_THRESHOLD_US: f64 = 500.0;
/// Consecutive cycles below threshold needed to return to Normal.
pub const RECOVERY_WINDOW: u32 = 20;
