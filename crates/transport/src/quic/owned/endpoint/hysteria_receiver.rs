//! Real authenticated H3 driver, exact packet/loan identity and original Root.
use super::*;
use zero_platform_tokio::controlled_carrier::{
    CarrierProtocolRetirement, ProtocolReceiverPrepared,
};
impl OwnedEndpointOwner {
    pub(crate) fn owns_hysteria_connection(&self, connection: &super::OwnedConnection) -> bool {
        Arc::ptr_eq(connection.permit.users(), &self.shared.users)
    }
    fn hysteria_registration(
        &self,
        client: &crate::hysteria::owned::OwnedClient,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<zero_core::component::TaskId, ChannelError> {
        self.shared.check()?;
        let id = client
            .original_task()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        if self.shared.phase.load(Ordering::Acquire) != 2
            || !client.names_retirement(retirement)
            || !client.names_endpoint(self)
            || self.drivers.origin(id) != Some(super::super::OwnedDriverOrigin::Http3)
            || self
                .drivers
                .application_ledger()
                .live_registration(id)
                .is_none_or(|actual| !client.names_registration(&actual))
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        Ok(id)
    }
    pub fn prepare_native_hysteria_receiver(
        &self,
        client: &crate::hysteria::owned::OwnedClient,
        segment: &super::super::packet::PacketSegment,
        origin: &zero_platform_tokio::controlled_datagram::metadata_pool::MetadataPoolOrigin,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<ProtocolReceiverPrepared, ChannelError> {
        let id = self.hysteria_registration(client, retirement)?;
        let Native::Packet(socket) = &self.native else {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        };
        let graph = self
            .users_graph
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let views = segment
            .retained_receiver_views(socket, graph, id)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        origin.prepare_protocol_receiver(views, retirement)
    }
    pub fn prepare_borrowed_hysteria_receiver(
        &self,
        client: &crate::hysteria::owned::OwnedClient,
        segment: &super::super::packet::BorrowedPacketSegment,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<ProtocolReceiverPrepared, ChannelError> {
        let id = self.hysteria_registration(client, retirement)?;
        let Native::Packet(socket) = &self.native else {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        };
        let keeper = segment
            .retained_receiver(socket)
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        self.carrier_graph
            .as_ref()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?
            .prepare_protocol_receiver(keeper, id, retirement)
    }
}
