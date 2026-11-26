//! Actual OS suspension and the same installed driver/Scope/native originals.
#[cfg(unix)]
use super::*;
#[cfg(unix)]
#[path = "tests/unix.rs"]
pub(in crate::ipc::client::owner) mod unix;
