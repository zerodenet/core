//! Logic for controlling the rate at which data is sent

use crate::connection::RttEstimator;
use crate::Instant;
use std::any::Any;
use std::sync::Arc;

mod bbr;
mod cubic;
mod new_reno;

pub use bbr::{Bbr, BbrConfig};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};

/// Common interface for different congestion controllers
pub trait Controller: Send + Sync {
    /// Exact packet feedback for controllers with delivery-rate samplers.
    /// Legacy callbacks remain unchanged; implementations opt into this stream.
    fn on_packet_event(&mut self, _event: PacketEvent) {}
    /// One or more packets were just sent
    #[allow(unused_variables)]
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {}

    /// Packet deliveries were confirmed
    ///
    /// `app_limited` indicates whether the connection was blocked on outgoing
    /// application data prior to receiving these acknowledgements.
    #[allow(unused_variables)]
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those batches has completed.
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
    }

    /// Packets were deemed lost or marked congested
    ///
    /// `in_persistent_congestion` indicates whether all packets sent within the persistent
    /// congestion threshold period ending when the most recent packet in this batch was sent were
    /// lost.
    /// `lost_bytes` indicates how many bytes were lost. This value will be 0 for ECN triggers.
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    );

    /// Optional explicit pacing rate in bytes per second. None keeps RFC 9002 pacing.
    fn pacing_rate(&self) -> Option<u64> {
        None
    }

    /// Non-probe packets declared lost in this event (not an ECN notification).
    #[allow(unused_variables)]
    fn on_packets_lost(&mut self, now: Instant, count: u64) {}

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Retrieve implementation-specific metrics used to populate `qlog` traces when they are enabled
    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            pacing_rate: None,
        }
    }

    /// Duplicate the controller's state
    fn clone_box(&self) -> Box<dyn Controller>;

    /// Initial congestion window
    fn initial_window(&self) -> u64;

    /// Returns Self for use in down-casting to extract implementation details
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Packet identity includes its number space (Initial, Handshake, or Data).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PacketKey(
    /// Packet number space, encoded as Initial=0, Handshake=1, Data=2.
    pub u8,
    /// Packet number within that space.
    pub u64,
);

/// Neutral, per-packet feedback. `FeedbackEnd` follows ACK processing and loss
/// detection, including timer-driven loss, so a sampler sees one coherent batch.
#[derive(Debug, Clone, Copy)]
pub enum PacketEvent {
    /// One QUIC packet has been built, before it enters flight accounting.
    Sent {
        /// Packet identity.
        key: PacketKey,
        /// Send time.
        now: Instant,
        /// Congestion-accounted packet size in bytes.
        bytes: u64,
        /// Bytes in flight before this packet.
        in_flight: u64,
        /// Whether the packet contributes to congestion-controlled flight.
        ack_eliciting: bool,
    },
    /// A tracked packet was acknowledged in the current feedback batch.
    Acked {
        /// Packet identity.
        key: PacketKey,
    },
    /// A non-PMTU-probe packet was declared lost in the current batch.
    Lost {
        /// Packet identity.
        key: PacketKey,
        /// Lost congestion-accounted bytes.
        bytes: u64,
    },
    /// Forget a packet without attributing congestion loss (for example an MTU probe).
    Discarded {
        /// Packet identity.
        key: PacketKey,
    },
    /// Forget all samples in a retired packet number space.
    DiscardSpace {
        /// Packet number space, using the same encoding as `PacketKey`.
        space: u8,
    },
    /// ACK and loss accounting, and the RTT estimator update, are complete.
    FeedbackEnd {
        /// Feedback time.
        now: Instant,
        /// Bytes still in flight after this batch.
        in_flight: u64,
        /// Transport's minimum measured RTT.
        min_rtt: std::time::Duration,
    },
}

/// Common congestion controller metrics
#[derive(Default)]
#[non_exhaustive]
pub struct ControllerMetrics {
    /// Congestion window (bytes)
    pub congestion_window: u64,
    /// Slow start threshold (bytes)
    pub ssthresh: Option<u64>,
    /// Pacing rate (bits/s)
    pub pacing_rate: Option<u64>,
}

/// Constructs controllers on demand
pub trait ControllerFactory {
    /// Construct a fresh `Controller`
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller>;
}

const BASE_DATAGRAM_SIZE: u64 = 1200;
