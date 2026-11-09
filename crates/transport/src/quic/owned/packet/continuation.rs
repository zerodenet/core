//! Persistent endpoint custody crosses an actual unique-queue hop; it is not an
//! endpoint-close receipt. Final retirement additionally needs its sealed graph.
use super::*;
use zero_platform_tokio::controlled_datagram::metadata::users::drivers::DriverOwner;
type EndpointDrivers = DriverOwner<OwnedDriverOrigin, DriverBody>;
#[derive(Default)]
pub(super) struct PacketIdentity {
    endpoint: Mutex<Option<EndpointDrivers>>,
}
impl PacketSegment {
    pub(in crate::quic::owned) fn bind_endpoint(
        &self,
        socket: &PacketSocket,
        drivers: EndpointDrivers,
    ) -> Result<(), ChannelError> {
        if !Arc::ptr_eq(&self.identity, &socket.identity)
            || !self
                .tasks
                .scope_identity()
                .same_scope(&drivers.scope_identity())
        {
            return Err(error(Reason::StalePlan));
        }
        let mut endpoint = self.identity.endpoint.lock().unwrap();
        if endpoint.is_some() {
            return Err(error(Reason::StalePlan));
        }
        // This is the original owner itself, observed live rather than an empty
        // startup snapshot. Later H3/connection registrations belong to it too.
        *endpoint = Some(drivers);
        Ok(())
    }
}
impl PacketIdentity {
    #[cfg(feature = "split_http")]
    pub(in crate::quic::owned) fn names_endpoint(&self, actual: &EndpointDrivers) -> bool {
        self.endpoint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|bound| bound.same_owner(actual))
    }
    pub(super) fn endpoint_bound(&self) -> bool {
        self.endpoint.lock().unwrap().is_some()
    }
    pub(super) fn final_receipt(
        &self,
        receipt: MetadataUsersClosedReceipt,
    ) -> Result<MetadataUsersClosedReceipt, MetadataUsersClosedReceipt> {
        let endpoint = { self.endpoint.lock().unwrap().clone() };
        let Some(endpoint) = endpoint else {
            return Ok(receipt);
        };
        // The endpoint must have sealed registration and delivered every actual
        // driver. ACK remains separate and is carried on the original receipt.
        let Some(closed) = endpoint.owned_registration_completion() else {
            return Err(receipt);
        };
        receipt.with_related_driver_receipt(closed)
    }
}
