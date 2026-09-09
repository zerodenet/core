//! Optional receive flow-control policy, independent of application protocols.
use crate::{Duration, Instant, StreamId};

/// Builds isolated receive-window state for each connection.
pub trait ReceiveWindowFactory: Send + Sync {
    /// Initial values are the limits advertised in transport parameters.
    fn build(&self, stream: u64, connection: u64) -> Box<dyn ReceiveWindowController>;
}

/// Owns credit-update policy; the carrier still validates offsets, manages stream
/// lifetime, and retransmits the latest advertised MAX_DATA/MAX_STREAM_DATA.
pub trait ReceiveWindowController: Send + Sync + std::fmt::Debug {
    /// First/new stream data arrived. Called only after carrier validation.
    fn received(&mut self, id: StreamId, now: Instant);
    /// Application consumed bytes on a stream that remains open.
    fn read_stream(&mut self, id: StreamId, bytes: u64) -> bool;
    /// Connection credits released by reading, stopping, or resetting streams.
    fn read_connection(&mut self, bytes: u64) -> bool;
    /// Return a new absolute stream credit limit, or None for retransmission.
    fn stream_update(&mut self, id: StreamId, now: Instant, rtt: Duration) -> Option<u64>;
    /// Return a new absolute connection credit limit, or None for retransmission.
    fn connection_update(&mut self, now: Instant, rtt: Duration) -> Option<u64>;
    /// Release state when the receive half closes, including 0-RTT rejection.
    fn closed(&mut self, id: StreamId);
    /// Current connection window size, excluding the consumed offset.
    fn connection_window(&self) -> u64;
    /// Synchronize an explicit application override of the connection window.
    fn set_connection_window(&mut self, window: u64, limit: u64);
}
