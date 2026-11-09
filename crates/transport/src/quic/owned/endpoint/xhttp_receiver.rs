//! Actual ready XMUX H3 sender, unique packet identity and same real ledgers.
//! The returned receipt is handoff only, never native/users/driver ACK closure.
use super::*;
use zero_platform_tokio::{
    controlled_carrier::{CarrierProtocolRetirement, ProtocolReceiverPrepared},
    controlled_datagram::metadata_pool::MetadataPoolOrigin,
};
impl OwnedEndpointOwner {
    fn xhttp_registration(
        &self,
        ready: &crate::split_http::OwnedXhttp3,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<zero_core::component::TaskId, ChannelError> {
        self.shared.check()?;
        let id = ready.original_task();
        if self.shared.phase.load(Ordering::Acquire) != 2
            || !ready.names_retirement(retirement)
            || self.drivers.origin(id) != Some(super::super::OwnedDriverOrigin::Http3)
            || self
                .drivers
                .application_ledger()
                .live_registration(id)
                .is_none()
        {
            return Err(error(Stage::Prepare, Reason::StalePlan));
        }
        Ok(id)
    }
    pub fn prepare_native_xhttp_receiver(
        &self,
        ready: &crate::split_http::OwnedXhttp3,
        segment: &super::super::packet::PacketSegment,
        origin: &MetadataPoolOrigin,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<ProtocolReceiverPrepared, ChannelError> {
        let id = self.xhttp_registration(ready, retirement)?;
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
    pub fn prepare_borrowed_xhttp_receiver(
        &self,
        ready: &crate::split_http::OwnedXhttp3,
        segment: &super::super::packet::BorrowedPacketSegment,
        retirement: &CarrierProtocolRetirement,
    ) -> Result<ProtocolReceiverPrepared, ChannelError> {
        let id = self.xhttp_registration(ready, retirement)?;
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

impl OwnedEndpointOwner {
    pub fn new_packet_endpoint(
        socket: super::super::packet::PacketSocket,
        segment: &mut super::super::packet::PacketSegment,
        registrar: TaskRegistrar,
        limits: OwnedEndpointLimits,
    ) -> Result<(Self, DriverExitReceiver), ChannelError> {
        let (graph, receiver) = segment.take_endpoint_users(&socket, &registrar, limits)?;
        let drivers = QuinnDriverOwner::from_ledger(graph.drivers().clone());
        let users = graph.users().clone();
        let (owner, receiver) = Self::construct_users(
            Native::Packet(socket),
            drivers,
            users,
            receiver,
            Some(graph),
        );
        owner.bind_packet_segment(segment)?;
        Ok((owner, receiver))
    }
}
