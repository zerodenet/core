//! Xray v26.3.27 mKCP carrier. Wire framing and reliability are transport-owned.
mod config;
mod receive;
mod rtt;
mod send;
mod state;
mod wire;
pub use config::Settings;

mod client;
mod peer;
mod stream;
pub use client::connect;
pub use peer::{ListenerProfile, PeerStreams};
pub use stream::MkcpStream;
#[cfg(test)]
#[path = "../tests/mkcp/state.rs"]
mod tests;
