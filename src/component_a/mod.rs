/// Component A – Dual-Pipeline Implementation
///
/// Exposes both architecture variants so `main.rs` can run them independently
/// or in parallel for direct comparison.
pub mod async_pipeline;
pub mod threaded_pipeline;

pub use async_pipeline::{run_async_pipeline, run_async_pipeline_with_reconnect, AsyncPipelineStats};
pub use threaded_pipeline::{run_threaded_pipeline, ThreadedPipelineStats};
