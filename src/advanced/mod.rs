//! Exploratory scaffolding — not integrated into the main pipeline.
//! Gated behind `#[cfg(debug_assertions)]` in `lib.rs` so it is excluded
//! from release builds and production `cargo doc` output.
pub mod async_pipeline;
pub mod cpu_load;
pub mod dashboard;
