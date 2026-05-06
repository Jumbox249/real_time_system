/// Component E – Fault Tolerance: Watchdog & Fail-Safe
pub mod fail_safe;
pub mod watchdog;

pub use fail_safe::FailSafe;
pub use watchdog::Watchdog;
