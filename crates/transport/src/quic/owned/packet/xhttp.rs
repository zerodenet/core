//! Exact native-claim endpoint family and original packet identity.
use super::*;
impl PacketSocket {
    #[cfg(feature = "split_http")]
    pub(in crate::quic::owned) fn matches_buffer_budget(
        &self,
        book: &zero_platform_tokio::controlled_carrier::CarrierBufferBudget,
    ) -> bool {
        self.memory
            .as_ref()
            .is_some_and(|memory| memory.book.same_budget(book))
    }
}
#[cfg(feature = "split_http")]
impl PacketSegment {
    pub(in crate::quic::owned) fn retained_receiver_views(
        &self,
        socket: &PacketSocket,
        endpoint: &MetadataUserGraph<OwnedDriverOrigin, DriverBody>,
        protocol: zero_core::component::TaskId,
    ) -> Option<zero_platform_tokio::controlled_datagram::metadata::MetadataViewsRetained> {
        if !Arc::ptr_eq(&self.identity, &socket.identity)
            || !self.identity.names_endpoint(endpoint.drivers())
        {
            return None;
        }
        let driver = self.driver?;
        if self.graph.drivers().origin(driver) != Some(OwnedDriverOrigin::Carrier) {
            return None;
        }
        self.graph.retained_views(driver, endpoint, protocol)
    }
    pub(in crate::quic::owned) fn take_endpoint_users(
        &mut self,
        socket: &PacketSocket,
        tasks: &TaskRegistrar,
        limits: crate::quic::owned::OwnedEndpointLimits,
    ) -> Result<
        (
            MetadataUserGraph<OwnedDriverOrigin, DriverBody>,
            crate::quic::owned::DriverExitReceiver,
        ),
        ChannelError,
    > {
        let matching = Arc::ptr_eq(&self.identity, &socket.identity)
            && self
                .tasks
                .scope_identity()
                .same_scope(&tasks.scope_identity())
            && self.driver.is_none()
            && self.endpoint_users.as_ref().is_some_and(|(actual, _, _)| {
                actual.drivers == limits.drivers && actual.users == limits.users
            });
        if !matching {
            return Err(error(Reason::StalePlan));
        }
        let (_, graph, receiver) = self
            .endpoint_users
            .take()
            .expect("same paired claim endpoint");
        Ok((graph, receiver))
    }
}
