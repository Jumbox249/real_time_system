/// Component B – Zero-Copy Parsing, Priority Dispatch & Hot Path
pub mod hot_path;
pub mod zero_copy_parser;

pub use hot_path::{HotPathProcessor, HOT_DEADLINE};
pub use zero_copy_parser::parse_zero_copy;
