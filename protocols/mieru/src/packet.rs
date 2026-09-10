//! Native UDP carrier framing and reliable, ordered logical sessions.
mod codec;
mod congestion;
mod driver;
mod reliability;
pub(crate) use codec::PacketCodec;
pub(crate) use driver::{run, Driver, PacketIo};
pub use reliability::{ReliableSession, Transmission};

#[cfg(test)]
mod tests;

mod replay;
