pub mod mock_stream;
pub mod sse_client;

/// Whether to connect to the live Wikipedia stream or use the mock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSource {
    /// Live SSE connection to stream.wikimedia.org
    Live,
    /// Synthetic mock at the given events-per-second rate
    Mock(u64),
}
