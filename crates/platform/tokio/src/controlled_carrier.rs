//! Library-only controlled physical TCP carriers. No production grants/defaults.
//! One persistent caller owner holds every original control before connect await;
//! the admitted protocol driver is joined before native close releases capacity.
mod adoption;
mod grant;
mod native;
mod owner;
mod prepared;
mod slot;
mod view;
use crate::contract::{ChannelError, DeliveryState, Impact, Reason, Stage};
pub use adoption::{CarrierAdoption, CarrierClosed};
pub use grant::{CarrierGrant, CarrierGrants};
pub use native::NativeCarrierOpener;
pub use owner::CarrierOwner;
pub use prepared::{ControlledCarrier, PreparedCarrier};
use std::sync::Arc;
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
struct ConnectSource(Arc<crate::TcpConnectError>);
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
    error(instance, Stage::Dial, reason, DeliveryState::Unknown).with_source(ConnectSource(source))
}
#[cfg(test)]
#[path = "../tests/controlled_carrier/mod.rs"]
mod tests;
