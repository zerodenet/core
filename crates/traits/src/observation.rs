//! Optional, runtime-neutral observations of actual I/O boundaries.
/// Implementations must be nonblocking and allocation-free on the I/O path.
/// Bytes describe the boundary at which the observer was installed. Stream
/// calls are not packets; datagram/packet counts are chosen by the owner.
pub trait IoObserver: Send + Sync + core::fmt::Debug {
    fn received(&self, bytes: usize);
    fn sent(&self, bytes: usize);
    fn error(&self);
    fn dropped(&self);
}
