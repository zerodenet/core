//! Minimal `windows-sys` dynamic bindings and receipt-preserving handles for `wintun.dll`.

mod adapter;
mod raw;
mod session;

pub use adapter::{Adapter, AdapterIdentity, AdapterOpenError, CloseAdapterReceipt};
pub use session::{
    EndSessionReceipt, Session, SessionCloseError, SessionIdentity, SessionNotAcquired,
    SessionStartError,
};

use std::{ffi::OsStr, fmt, io, sync::Arc};

/// Maximum size of Wintun's internal ring buffer (in bytes).
pub const MAX_RING_CAPACITY: u32 = 0x400_0000;

/// Minimum size of Wintun's internal ring buffer (in bytes).
pub const MIN_RING_CAPACITY: u32 = 0x2_0000;

pub type Wintun = Arc<raw::WintunLibrary>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    CapacityNotPowerOfTwo(u32),
    CapacityOutOfRange { min: u32, max: u32, value: u32 },
    Message(String),
    ShuttingDown,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => err.fmt(f),
            Self::CapacityNotPowerOfTwo(cap) => {
                write!(f, "ring capacity {cap:#x} is not a power of two")
            }
            Self::CapacityOutOfRange { min, max, value } => {
                write!(f, "ring capacity {value:#x} outside {min:#x}..={max:#x}")
            }
            Self::Message(msg) => f.write_str(msg),
            Self::ShuttingDown => f.write_str("Wintun session shutting down"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<Error> for io::Error {
    fn from(value: Error) -> Self {
        match value {
            Error::Io(io) => io,
            other => io::Error::other(other),
        }
    }
}

/// Load `wintun.dll` from the default system search path.
///
/// # Safety
/// Executes `DllMain` of the resolved `wintun.dll` library.
pub unsafe fn load() -> Result<Wintun, Error> {
    unsafe { load_from_path("wintun.dll") }
}

/// Load `wintun.dll` from the specified path.
///
/// # Safety
/// Executes `DllMain` of the target dynamic library and binds Wintun C symbols.
pub unsafe fn load_from_path<P: AsRef<OsStr>>(path: P) -> Result<Wintun, Error> {
    unsafe { raw::WintunLibrary::open(path.as_ref()).map(Arc::new) }.map_err(Error::Io)
}
