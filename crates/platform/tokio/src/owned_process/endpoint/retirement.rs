//! Observation of the actual Host declaration, including failed pre-child IO.
use crate::contract::{BoxFuture, ChannelError, scope::TaskScopeIdentity};
use zero_core::component::ComponentInstanceId;
/// Read-only view issued by that exact existing ResourceAccess provider before
/// any reservation attempt. It issues no native/start/cleanup authority and is
/// not an actor join or registration ACK. Missing/expired providers are unknown.
pub trait ProcessEndpointRetirement: Send + Sync {
    fn instance(&self) -> ComponentInstanceId;
    fn process_ordinal(&self) -> usize;
    fn scope_identity(&self) -> TaskScopeIdentity;
    /// Published only after that declaration's actual job/native and endpoint
    /// original receipts match, with the provider's acquisition admission closed.
    fn originals_closed(&self) -> bool;
    fn wait_originals_closed(
        &self,
        deadline: tokio::time::Instant,
    ) -> BoxFuture<'_, Result<(), ChannelError>>;
}
