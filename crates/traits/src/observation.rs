//! Optional, runtime-neutral observations of actual I/O boundaries.
/// A positively observed local discard, never an inferred network loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketDropReason {
    Unspecified,
    QueueFull,
    QueueClosed,
    InvalidPacket,
    SourceRejected,
    FragmentRejected,
    PolicyRejected,
    IoFailure,
    NoRoute,
    HopLimit,
}
/// Implementations must be nonblocking and allocation-free on the I/O path.
/// Bytes describe the boundary at which the observer was installed. Stream
/// calls are not packets; datagram/packet counts are chosen by the owner.
pub trait IoObserver: Send + Sync + core::fmt::Debug {
    /// Preparation facts: a scope combining stream and datagram carriers
    /// cannot claim complete packet counts.
    fn stream_boundary(&self) {}
    fn datagram_boundary(&self) {}
    /// Bounded attribution lost RX coverage. Must not interrupt data forwarding.
    fn receive_coverage_lost(&self) {}
    fn received_datagram(&self, bytes: usize) {
        self.received(bytes);
    }
    fn sent_datagram(&self, bytes: usize) {
        self.sent(bytes);
    }
    fn received(&self, bytes: usize);
    fn sent(&self, bytes: usize);
    fn error(&self);
    fn dropped(&self);
    /// Compatible with observers that only expose an aggregate count.
    fn dropped_reason(&self, _reason: PacketDropReason) {
        self.dropped();
    }
}
