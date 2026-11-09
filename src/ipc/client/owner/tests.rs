//! Actual OS suspension and the same installed driver/Scope/native originals.
use super::*;
#[cfg(unix)]
#[path = "tests/unix.rs"]
mod unix;
