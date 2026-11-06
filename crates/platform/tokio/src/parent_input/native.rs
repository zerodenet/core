use super::*;
#[cfg(unix)]
#[path = "native/unix.rs"]
mod platform;
#[cfg(windows)]
#[path = "native/windows.rs"]
mod platform;
pub(super) use platform::*;
