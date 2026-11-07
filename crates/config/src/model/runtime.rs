//! Runtime owner limits and production options.
use super::*;

mod options;
pub use options::RuntimeOptionsConfig;

pub const DEFAULT_LATENCY_TEST_URL: &str = "http://www.gstatic.com/generate_204";
pub const DEFAULT_EVENT_LOG_CAPACITY: usize = 1024;

mod budgets;
mod defaults;
