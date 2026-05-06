/// wiki_rt_monitor library crate
///
/// All modules are public so both binaries (main + compare_pipelines)
/// and all criterion bench files can share them without duplication.
pub mod component_a;
pub mod component_b;
pub mod component_c;
pub mod component_d;
pub mod component_e;
pub mod ingestion;
pub mod metrics;
pub mod types;
