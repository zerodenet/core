//! Optional read-only host interface facts. No endpoint/peer attribution.
use std::io;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(any(target_os = "linux", test))]
mod procfs;

pub const SUPPORTED: bool = cfg!(any(target_os = "linux", target_os = "macos"));
pub const MAX_INTERFACES: usize = 256;
#[cfg(any(target_os = "linux", target_os = "macos", test))]
pub(super) const MAX_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceStatistics {
    pub name: String,
    pub index: u32,
    pub accounting_basis: &'static str,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
    pub rx_dropped_packets: Option<u64>,
    pub tx_dropped_packets: Option<u64>,
    pub rx_errors: Option<u64>,
    pub tx_errors: Option<u64>,
}
pub fn read() -> io::Result<Vec<InterfaceStatistics>> {
    #[cfg(target_os = "linux")]
    return linux::read();
    #[cfg(target_os = "macos")]
    return macos::read();
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "host interface statistics unavailable on this platform",
    ))
}
#[cfg(any(target_os = "linux", target_os = "macos", test))]
pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
