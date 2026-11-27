//! Original ownership of controlled stream and datagram carriers.
//! One persistent caller owner holds every original control before connect await;
//! the admitted protocol driver is joined before native close releases capacity.
mod adoption;
mod buffers;
mod caller_credit;
pub use buffers::{
    CarrierBufferBudget, CarrierBufferCredit, CarrierBufferFrameCredit, CarrierBufferLimits,
    CarrierBufferWorkingLane, CarrierBufferWorkingSet, CarrierBufferWorkingShape,
};
mod accepted;
mod totals;
pub use accepted::{
    AcceptedCarrierProvider, AcceptedCarrierUse, AcceptedCarrierUses, AcceptedSourcesReturned,
};
pub use totals::preparation::{CarrierPreparationRequest, CarrierPreparationTotalsSnapshot};
pub(crate) use totals::{BoundCustody, MetadataCustody, SourceCustody};
pub use totals::{CarrierOriginalFailure, CarrierOriginalTotals, CarrierOriginalTotalsSnapshot};
mod auxiliary;
mod binding;
mod business;
mod driver_retained;
mod endpoints;
pub use driver_retained::CarrierDriverRetained;
mod grant;
pub mod loan;
mod loan_retained;
mod native;
pub(crate) use loan_retained::AcceptedLoanReceiver;
pub use loan_retained::CarrierLoanRetained;
mod native_settlement;
mod origin;
mod owner;
mod packet;
mod preparation;
mod protocol_children;
mod protocol_original;
pub use protocol_children::ProtocolOriginalChildren;
pub use protocol_original::{ProtocolOriginal, ProtocolOriginalReceiver, ProtocolReceiverPrepared};
mod prepared;
mod registration;
mod resolution;
mod service;
mod slot;
mod users;
pub use preparation::{
    CarrierPreparation, CarrierPreparationClosed, CarrierPreparationJob,
    CarrierPreparationJobRetained, StagedCarrierPreparationJob,
};
mod view;
use crate::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};
pub use adoption::{CarrierAdoption, CarrierClosed};
pub use binding::CarrierOwnerBinding;
pub use business::{CarrierBusinessDemand, CarrierBusinessGrant, CarrierBusinessIssuer};
pub use endpoints::{CarrierEndpointObserver, CarrierEndpoints};
pub use grant::{CarrierGrant, CarrierGrants};
pub use native::NativeCarrierOpener;
pub use native_settlement::CarrierNativeSettlement;
pub use origin::{
    CarrierOrigin, CarrierOriginClosed, CarrierOriginOwner, CarrierOriginOwnerClosed,
    CarrierOriginUsersClosed, LogicalPacketProviderRegistration, PacketProviderClosed,
    PacketProviderRegistration,
};
pub use owner::CarrierOwner;
pub use packet::CarrierDatagram;
pub use prepared::{CarrierStreamPreparation, ControlledCarrier, PreparedCarrier};
pub use registration::{
    CarrierNativeCloseObservation, CarrierNativeKind, CarrierOriginObservation, CarrierOwnerClosed,
    CarrierOwnerObservation, CarrierOwnerRegistration, CarrierSlotObservation,
};
pub use resolution::{CarrierCandidates, ResolvingCarrierOpener};
pub use service::{CarrierServiceGrant, CarrierServiceIssuer};
use std::sync::Arc;
pub use users::{CarrierOwnerUsersClosed, CarrierUserView, CarrierUsersClosed};
pub use view::{CarrierDriver, CarrierIo};
use zero_core::component::ComponentInstanceId;
fn error(
    instance: ComponentInstanceId,
    stage: Stage,
    reason: Reason,
    delivery: DeliveryState,
) -> ChannelError {
    let mut error = ChannelError::new(stage, reason, Impact::Component);
    error.component = Some(instance.component);
    error.delivery = delivery;
    error
}
#[derive(Debug)]
struct ConnectSource(Arc<crate::TcpConnectError>, std::net::SocketAddr);
impl std::fmt::Display for ConnectSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0.stage(), self.0.error())
    }
}
impl std::error::Error for ConnectSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.error())
    }
}
fn connect_error(
    instance: ComponentInstanceId,
    peer: std::net::SocketAddr,
    source: Arc<crate::TcpConnectError>,
) -> ChannelError {
    let reason = match source.error().kind() {
        std::io::ErrorKind::TimedOut => Reason::Timeout,
        std::io::ErrorKind::ConnectionAborted
            if source.stage() == "connect_socket" && source.error().raw_os_error().is_none() =>
        {
            Reason::Cancelled
        }
        _ => Reason::Io,
    };
    error(instance, Stage::Dial, reason, DeliveryState::Unknown)
        .with_source(ConnectSource(source, peer))
}
#[cfg(test)]
#[path = "../tests/controlled_carrier/mod.rs"]
mod tests;

/// Read-only metadata from the preserved original native error object. This
/// neither reconstructs its OS source nor grants acquisition/closure authority.
pub fn original_connect_error(error: &ChannelError) -> Option<&crate::TcpConnectError> {
    original_connect_source(error).map(|source| source.0.as_ref())
}

/// Qualify a setup result for the existing preparation receiver. Wire
/// rejections preserve their delivery/retry fields and do not affect native
/// connect retry selection or permit a cleanup failure to be consumed here.
pub fn is_connect_rejection(error: &ChannelError) -> bool {
    if original_connect_error(error).is_some() {
        return true;
    }
    if error.cleanup_failure_receipt().is_some()
        || error.stage == Stage::Close
        || error.reason == Reason::Internal
    {
        return false;
    }
    error.impact == crate::contract::Impact::Session
        || crate::contract::network::UpstreamRejection::present_in(error)
        || matches!(
            (error.stage, error.reason),
            (
                Stage::Prepare | Stage::Dial | Stage::Handshake,
                Reason::StalePlan
                    | Reason::Revoked
                    | Reason::Cancelled
                    | Reason::Timeout
                    | Reason::Capacity,
            )
        )
}
/// The actual selected native peer of this same preserved failure. A configured
/// hostname, logical destination or successful endpoint is never inferred here.
pub fn original_connect_peer(error: &ChannelError) -> Option<std::net::SocketAddr> {
    original_connect_source(error).map(|source| source.1)
}
fn original_connect_source(error: &ChannelError) -> Option<&ConnectSource> {
    let mut source = std::error::Error::source(error);
    // Protocol/host forwarding retains the same typed source behind Arc/error
    // wrappers. A generic IO kind/string can never establish this provenance.
    // Bound traversal also makes malformed cyclic external chains harmless.
    for _ in 0..64 {
        let current = source?;
        if let Some(original) = current.downcast_ref::<ConnectSource>() {
            return Some(original);
        }
        source = current.source();
    }
    None
}
mod related;

mod retirement_progress;

pub use crate::original_protocol::{CarrierProtocolRetirement, RejectedOriginalProtocolControl};
mod protocol_control;
